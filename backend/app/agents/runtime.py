"""
runtime.py — Execute and persist agent runs.

The Agent class in base.py knows nothing about the database — it's pure
business logic.  This module is the bridge: it persists an AgentRun row
before dispatch, runs the agent, and updates the row with results.

Two access patterns:

  run_agent_persistent(...)
      Foreground execution.  Caller awaits the result.  Use for ad-hoc
      single-target runs where the request can wait.

  schedule_agent_background(...)
      Background execution via FastAPI BackgroundTasks.  Returns immediately
      with the agent_run_id; caller polls /api/agents/runs/{id} for results.
      Use for any UI-triggered run so the HTTP response time is bounded.
"""
from __future__ import annotations

import logging
import traceback
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.base import Agent, AgentContext, AgentRunResult, Severity
from app.database import AsyncSessionLocal
from app.models import AgentRun, AuditLog

logger = logging.getLogger(__name__)


async def run_agent_persistent(
    agent: Agent,
    context: AgentContext,
) -> int:
    """
    Run an agent in the current task; persist the result.  Returns the
    agent_run_id so the caller can fetch the full result later.

    Always returns an ID, even on failure — the failed run is persisted with
    status='failed' and an error message for forensics.
    """
    # Step 1: insert a 'running' row so we can fail it if we crash
    run_id = await _create_pending_row(agent, context)

    # Step 2: run the agent.  Agent.run() never raises but the persistence
    # layer might (DB hiccup, etc.).  Wrap in a try/except so partial
    # results still get recorded.
    try:
        result = await agent.run(context)
        await _update_run_row(run_id, result, error=None)
        return run_id
    except Exception as e:
        logger.exception("Agent run %s crashed", run_id)
        await _update_run_row(
            run_id,
            result=None,
            error=f"{type(e).__name__}: {e}\n{traceback.format_exc()}",
        )
        return run_id


# ── Persistence helpers ───────────────────────────────────────────────────────

async def _create_pending_row(agent: Agent, context: AgentContext) -> int:
    """Insert a 'running' AgentRun row + an audit log entry; return its id."""
    async with AsyncSessionLocal() as db:
        row = AgentRun(
            workflow=agent.name,
            target_type=agent.target_type,
            target_id=context.npi,
            status="running",
            started_at=datetime.now(timezone.utc),
            triggered_by_user_id=context.triggered_by_user_id,
        )
        db.add(row)
        await db.flush()  # populate row.id

        # Audit-log entry — ties the agent run to the triggering user
        # for the chain-of-custody methodology requirements.
        db.add(AuditLog(
            user_id=context.triggered_by_user_id,
            action="agent_run_started",
            target_type=agent.target_type,
            target_id=context.npi,
            details={
                "workflow":     agent.name,
                "agent_run_id": row.id,
            },
        ))
        await db.commit()
        return row.id


async def _update_run_row(run_id: int, result: AgentRunResult | None, error: str | None) -> None:
    """Finalise the AgentRun row with the result or the error."""
    async with AsyncSessionLocal() as db:
        row = await db.get(AgentRun, run_id)
        if row is None:
            logger.error("AgentRun id=%s vanished before update", run_id)
            return

        row.completed_at = datetime.now(timezone.utc)
        if result is not None:
            row.status = "succeeded" if result.success else (
                "partial" if result.n_tools_succeeded > 0 else "failed"
            )
            row.duration_ms  = result.duration_ms
            row.n_findings   = result.n_findings
            row.max_severity = result.max_severity.value
            row.result_json  = result.to_dict()
        else:
            row.status = "failed"
            row.error  = error

        # Final audit log entry
        db.add(AuditLog(
            user_id=row.triggered_by_user_id,
            action="agent_run_completed",
            target_type=row.target_type,
            target_id=row.target_id,
            details={
                "workflow":     row.workflow,
                "agent_run_id": row.id,
                "status":       row.status,
                "duration_ms":  row.duration_ms,
                "n_findings":   row.n_findings,
                "max_severity": row.max_severity,
            },
        ))
        await db.commit()


# ── Helper: build an AgentContext from a Provider row ─────────────────────────

async def context_from_provider_npi(
    db: AsyncSession,
    npi: str,
    triggered_by_user_id: UUID | None = None,
) -> AgentContext | None:
    """
    Populate an AgentContext from the providers table.  Returns None if the
    NPI isn't in our database.
    """
    from sqlalchemy import select
    from app.models import Provider

    row = (await db.execute(select(Provider).where(Provider.npi == npi))).scalar_one_or_none()
    if row is None:
        return None
    return AgentContext(
        npi=row.npi,
        name_last=row.name_last,
        name_first=row.name_first,
        # busname is stored in name_last for organization-type providers in our schema
        busname=row.name_last if row.name_first is None else None,
        specialty=row.specialty,
        state=row.state,
        city=row.city,
        triggered_by_user_id=triggered_by_user_id,
    )
