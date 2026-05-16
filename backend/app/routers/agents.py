"""
agents.py — Agent execution and result endpoints.

  POST /api/agents/public-records/run?npi=...    Trigger a new run (background)
  GET  /api/agents/runs/{run_id}                 Get full result for one run
  GET  /api/agents/runs?target_id=NPI            List runs for one target

All endpoints require authentication.  The user's identity is recorded as
``triggered_by_user_id`` on the run so the audit trail is complete.
"""
from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.runtime import context_from_provider_npi, run_agent_persistent
from app.agents.workflows.public_records import PublicRecordsAgent
from app.auth import get_current_user
from app.database import AsyncSessionLocal, get_db
from app.models import AgentRun, User

logger = logging.getLogger(__name__)
router = APIRouter()


# Registry of workflows the API can trigger.  Future workflows added by name.
_WORKFLOWS = {
    "public_records": PublicRecordsAgent,
}


@router.post("/{workflow}/run", status_code=202)
async def trigger_agent_run(
    workflow: str,
    npi: Annotated[str, Query(..., min_length=10, max_length=10)],
    background_tasks: BackgroundTasks,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Kick off an agent run on the given NPI in the background.

    Returns 202 immediately with the agent_run_id; client polls
    ``/api/agents/runs/{id}`` until status != 'running'.
    """
    AgentClass = _WORKFLOWS.get(workflow)
    if AgentClass is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown workflow '{workflow}'.  Available: {list(_WORKFLOWS)}",
        )

    context = await context_from_provider_npi(
        db, npi, triggered_by_user_id=current_user.id,
    )
    if context is None:
        raise HTTPException(status_code=404, detail=f"Provider NPI {npi} not found")

    # State-access enforcement: respect the per-user state restrictions
    allowed_states = current_user.state_access or []
    if allowed_states and context.state not in allowed_states:
        raise HTTPException(
            status_code=403,
            detail=f"Access denied — provider state {context.state} not in your authorised states",
        )

    # Create the agent row up front (status=running) so the client can poll
    # immediately.  The actual tool dispatch happens in the background.
    agent = AgentClass()
    from app.models import AgentRun
    from datetime import datetime, timezone
    pending = AgentRun(
        workflow=agent.name,
        target_type=agent.target_type,
        target_id=npi,
        status="running",
        started_at=datetime.now(timezone.utc),
        triggered_by_user_id=current_user.id,
    )
    db.add(pending)
    await db.flush()
    run_id = pending.id
    await db.commit()

    async def _execute():
        try:
            # Run agent — this updates the row inline
            result = await agent.run(context)
            async with AsyncSessionLocal() as session:
                row = await session.get(AgentRun, run_id)
                if row is None:
                    return
                from datetime import datetime, timezone
                row.completed_at = datetime.now(timezone.utc)
                row.status = "succeeded" if result.success else (
                    "partial" if result.n_tools_succeeded > 0 else "failed"
                )
                row.duration_ms  = result.duration_ms
                row.n_findings   = result.n_findings
                row.max_severity = result.max_severity.value
                row.result_json  = result.to_dict()
                await session.commit()
        except Exception:
            logger.exception("Agent run %s crashed in background", run_id)
            async with AsyncSessionLocal() as session:
                row = await session.get(AgentRun, run_id)
                if row:
                    from datetime import datetime, timezone
                    row.completed_at = datetime.now(timezone.utc)
                    row.status = "failed"
                    row.error = "background execution crashed"
                    await session.commit()

    background_tasks.add_task(_execute)

    return {
        "agent_run_id": run_id,
        "workflow":     workflow,
        "npi":          npi,
        "status":       "running",
        "poll_url":     f"/api/agents/runs/{run_id}",
    }


@router.get("/runs/{run_id}")
async def get_agent_run(
    run_id: int,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """Return the full AgentRun row including result_json on completion."""
    row = await db.get(AgentRun, run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Agent run not found")
    return _serialize_run(row)


@router.get("/runs")
async def list_agent_runs_for_target(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    target_id:   Annotated[str, Query(..., min_length=1, max_length=64)],
    target_type: str = "provider",
    limit:       int = Query(20, ge=1, le=100),
):
    """List recent runs against a specific target, newest first."""
    rows = (await db.execute(
        select(AgentRun)
        .where(AgentRun.target_type == target_type, AgentRun.target_id == target_id)
        .order_by(AgentRun.started_at.desc())
        .limit(limit)
    )).scalars().all()
    return [_serialize_run(r) for r in rows]


def _serialize_run(r: AgentRun) -> dict:
    """Common AgentRun → dict serialization."""
    return {
        "id":             r.id,
        "workflow":       r.workflow,
        "target_type":    r.target_type,
        "target_id":      r.target_id,
        "status":         r.status,
        "started_at":     r.started_at.isoformat() if r.started_at else None,
        "completed_at":   r.completed_at.isoformat() if r.completed_at else None,
        "duration_ms":    r.duration_ms,
        "n_findings":     r.n_findings,
        "max_severity":   r.max_severity,
        "triggered_by_user_id": str(r.triggered_by_user_id) if r.triggered_by_user_id else None,
        "result":         r.result_json,
        "error":          r.error,
    }
