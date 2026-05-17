"""
case_watch.py — Nightly cross-reference watch over all open cases.

Runs the Public Records agent against every open case's provider, then
computes the delta between the new findings and the previous run for the
same NPI.  "New findings since last check" surface as the digest that
investigators see Monday morning.

Why this exists
---------------
A one-shot lookup tells you what's in the public record TODAY.  A nightly
watch tells you what CHANGED.  For an investigator with 47 open cases,
the change-set is the actionable signal:
  - "This provider was indicted last week"   → urgent
  - "A new federal civil case was filed"     → worth a deeper look
  - "Name on file at the NPI Registry changed" → maybe alias scheme

The watch agent doesn't replace per-record manual review; it does the
busywork of repeatedly polling sources so investigators only spend time
on records where something actually moved.

Reuses
------
- agent_runs table — each watch invocation persists a normal AgentRun row
- PublicRecordsAgent — same workflow, just batched + scheduled

Delta detection
---------------
A "finding" is identified by its (source, title, url, date) tuple.  When
the new run produces a finding tuple that doesn't appear in the most
recent prior run for the same NPI, it's flagged as new.  This is
deliberately conservative — we want to surface ANY change, not just
adverse signals.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.runtime import context_from_provider_npi
from app.agents.workflows.public_records import PublicRecordsAgent
from app.database import AsyncSessionLocal
from app.models import AgentRun, AuditLog, Case

logger = logging.getLogger(__name__)


# Cases in these statuses get included in the nightly watch sweep.
# "closed" and "referred" cases are no longer being actively investigated;
# we don't waste API quota re-checking them.
_WATCHABLE_STATUSES = ("open", "under_review")

# How far back to look for the "previous" run when computing deltas.
# Older than this and we treat all current findings as new (the prior data
# is effectively stale).  30 days is a good balance for a weekly cadence.
_DELTA_LOOKBACK_DAYS = 30


def _finding_key(f: dict) -> tuple:
    """
    Stable identity for a Finding across runs.  Two findings are "the same"
    when their source, title, URL, and date all match.  Severity and summary
    can change wording between runs without indicating a new event, so they
    aren't part of the key.
    """
    return (
        (f.get("source")   or "").strip(),
        (f.get("title")    or "").strip(),
        (f.get("url")      or "").strip(),
        (f.get("date")     or "").strip(),
    )


async def _previous_finding_keys(db: AsyncSession, npi: str, before: datetime) -> set[tuple]:
    """
    Pull the most-recent prior PublicRecordsAgent run for this NPI and
    return the set of finding-keys it contained.  If no prior run exists
    (or it's older than the lookback window), returns an empty set so
    every current finding is treated as new.
    """
    cutoff = before - timedelta(days=_DELTA_LOOKBACK_DAYS)
    prior = (await db.execute(
        select(AgentRun)
        .where(
            AgentRun.workflow    == "public_records",
            AgentRun.target_type == "provider",
            AgentRun.target_id   == npi,
            AgentRun.status.in_(("succeeded", "partial")),
            AgentRun.completed_at >= cutoff,
            AgentRun.completed_at <  before,
        )
        .order_by(AgentRun.completed_at.desc())
        .limit(1)
    )).scalar_one_or_none()
    if prior is None or not prior.result_json:
        return set()
    findings = (prior.result_json or {}).get("findings") or []
    return {_finding_key(f) for f in findings}


async def watch_case(
    db: AsyncSession,
    case: Case,
) -> dict[str, Any]:
    """
    Run the Public Records agent for one case's provider and compute the
    delta against the prior run.

    Returns a per-case summary dict suitable for inclusion in a digest.
    The full AgentRun is persisted to the database (same as a user-
    triggered run, only with triggered_by_user_id=None for "system").
    """
    npi = case.provider_npi
    start = datetime.now(timezone.utc)

    # Capture the prior run BEFORE we kick off the new one — otherwise our
    # "prior run" comparison would include the run we just created.
    prior_keys = await _previous_finding_keys(db, npi, before=start)

    # Build context from the providers table
    ctx = await context_from_provider_npi(db, npi, triggered_by_user_id=None)
    if ctx is None:
        return {
            "case_id":  case.id,
            "npi":      npi,
            "status":   "skipped_no_provider",
            "n_new_findings": 0,
            "new_findings": [],
        }

    # Persist the running AgentRun row
    pending = AgentRun(
        workflow="public_records",
        target_type="provider",
        target_id=npi,
        status="running",
        started_at=start,
        triggered_by_user_id=None,
    )
    db.add(pending)
    await db.flush()
    run_id = pending.id
    await db.commit()

    # Run the agent (parallel tool dispatch happens inside)
    agent = PublicRecordsAgent()
    try:
        result = await agent.run(ctx)
    except Exception as e:
        # Even though agent.run() catches everything internally, belt-and-
        # braces here so a single bad case can't break the whole nightly sweep.
        logger.exception("watch_case agent crash for npi=%s case=%s", npi, case.id)
        async with AsyncSessionLocal() as failsession:
            row = await failsession.get(AgentRun, run_id)
            if row:
                row.completed_at = datetime.now(timezone.utc)
                row.status = "failed"
                row.error = f"{type(e).__name__}: {e}"
                await failsession.commit()
        return {
            "case_id":  case.id,
            "npi":      npi,
            "status":   "failed",
            "n_new_findings": 0,
            "new_findings": [],
        }

    # Persist the completed run
    async with AsyncSessionLocal() as session:
        row = await session.get(AgentRun, run_id)
        if row:
            row.completed_at = datetime.now(timezone.utc)
            row.status = "succeeded" if result.success else (
                "partial" if result.n_tools_succeeded > 0 else "failed"
            )
            row.duration_ms  = result.duration_ms
            row.n_findings   = result.n_findings
            row.max_severity = result.max_severity.value
            row.result_json  = result.to_dict()
            await session.commit()

    # Compute the delta: which findings weren't in the prior run?
    current_findings_dicts = [f.to_dict() for f in result.findings]
    current_keys = [_finding_key(f) for f in current_findings_dicts]
    new_findings = [
        f for f, k in zip(current_findings_dicts, current_keys, strict=False)
        if k not in prior_keys
    ]

    return {
        "case_id":         case.id,
        "case_number":     getattr(case, "case_number", None),
        "npi":             npi,
        "status":          "ok",
        "agent_run_id":    run_id,
        "n_total_findings": result.n_findings,
        "n_new_findings":  len(new_findings),
        "new_findings":    new_findings,
        "max_severity":    result.max_severity.value,
        "ran_at":          start.isoformat(),
    }


async def run_nightly_watch(
    db: AsyncSession | None = None,
    max_cases: int = 500,
) -> dict[str, Any]:
    """
    The entry point invoked by the scheduler.  Iterates every open case
    and runs the watch agent against each.

    Throttling
    ----------
    Runs cases SERIALLY rather than in parallel.  Each PublicRecordsAgent
    invocation already dispatches its 4 tools in parallel, so we get
    plenty of concurrency at the tool level.  Running cases serially keeps
    us under SAM.gov's public-tier rate limit (10 req/sec) without complex
    rate-limit accounting.

    max_cases caps the sweep so a runaway case-load can't exhaust our
    daily API quotas in a single sweep.  At max_cases=500 and ~4 tool
    calls per case, that's 2000 API calls / sweep — well under our
    weakest source's daily limit.
    """
    own_session = db is None
    if own_session:
        db = AsyncSessionLocal()
        await db.__aenter__()

    try:
        # Find all open/under-review cases, oldest-touched first so we don't
        # starve long-running investigations of fresh updates.
        cases = (await db.execute(
            select(Case)
            .where(Case.status.in_(_WATCHABLE_STATUSES))
            .order_by(Case.updated_at.asc())
            .limit(max_cases)
        )).scalars().all()

        logger.info("Case watch sweep starting", extra={"n_cases": len(cases)})

        summaries: list[dict] = []
        for case in cases:
            try:
                summary = await watch_case(db, case)
                summaries.append(summary)
            except Exception:
                logger.exception("Case watch failed for case=%s", case.id)
                summaries.append({
                    "case_id": case.id,
                    "npi":     case.provider_npi,
                    "status":  "exception",
                    "n_new_findings": 0,
                    "new_findings":   [],
                })

        # Aggregate stats for ops monitoring
        n_with_changes = sum(1 for s in summaries if s.get("n_new_findings", 0) > 0)
        total_new_findings = sum(s.get("n_new_findings", 0) for s in summaries)

        digest = {
            "ran_at":              datetime.now(timezone.utc).isoformat(),
            "n_cases_watched":     len(summaries),
            "n_cases_with_changes": n_with_changes,
            "n_new_findings_total": total_new_findings,
            "cases":               summaries,
        }

        # Audit log entry for the sweep itself (system action, no user id)
        db.add(AuditLog(
            user_id=None,
            action="case_watch_sweep",
            target_type="system",
            target_id=None,
            details={
                "n_cases_watched":     digest["n_cases_watched"],
                "n_cases_with_changes": digest["n_cases_with_changes"],
                "n_new_findings_total": digest["n_new_findings_total"],
            },
        ))
        await db.commit()

        logger.info(
            "Case watch sweep complete",
            extra={
                "n_cases_watched":     digest["n_cases_watched"],
                "n_cases_with_changes": digest["n_cases_with_changes"],
                "n_new_findings_total": digest["n_new_findings_total"],
            },
        )
        return digest
    finally:
        if own_session and db is not None:
            await db.__aexit__(None, None, None)


async def get_user_digest(
    db: AsyncSession,
    user_id,
    since_hours: int = 168,    # 7 days
) -> dict[str, Any]:
    """
    Per-user digest API for the dashboard widget.

    Returns the case-watch deltas for cases assigned-to or created-by the
    given user, within the last ``since_hours``.  Only includes cases where
    the most-recent watch run found NEW findings (no point surfacing cases
    that are stable).

    The widget renders this as: "Updates on N of your M open cases this week."
    """
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    # Cases the user owns (assigned or created)
    user_cases = (await db.execute(
        select(Case)
        .where(
            and_(
                Case.status.in_(_WATCHABLE_STATUSES),
                # Case the user is responsible for
                (Case.assigned_to == user_id) | (Case.created_by == user_id),
            )
        )
    )).scalars().all()

    if not user_cases:
        return {
            "n_open_cases":     0,
            "n_cases_with_updates": 0,
            "since":            since.isoformat(),
            "updates":          [],
        }

    npis = [c.provider_npi for c in user_cases]

    # Most recent watch run per NPI within the window.  We're not joining on
    # a watch-specific table — we just take the latest succeeded/partial
    # PublicRecords run per NPI.
    runs = (await db.execute(
        select(AgentRun)
        .where(
            AgentRun.workflow    == "public_records",
            AgentRun.target_type == "provider",
            AgentRun.target_id.in_(npis),
            AgentRun.status.in_(("succeeded", "partial")),
            AgentRun.completed_at >= since,
        )
        .order_by(AgentRun.completed_at.desc())
    )).scalars().all()

    # First-seen per NPI = latest run per NPI within the window
    latest_by_npi: dict[str, AgentRun] = {}
    for r in runs:
        if r.target_id not in latest_by_npi:
            latest_by_npi[r.target_id] = r

    # For each user case, compute its delta against the previous run
    updates: list[dict] = []
    for case in user_cases:
        latest = latest_by_npi.get(case.provider_npi)
        if not latest or not latest.result_json:
            continue
        prior_keys = await _previous_finding_keys(
            db, case.provider_npi, before=latest.started_at,
        )
        current = (latest.result_json or {}).get("findings") or []
        new = [f for f in current if _finding_key(f) not in prior_keys]
        if not new:
            continue
        updates.append({
            "case_id":        case.id,
            "case_number":    case.case_number,
            "provider_npi":   case.provider_npi,
            "agent_run_id":   latest.id,
            "ran_at":         latest.completed_at.isoformat() if latest.completed_at else None,
            "n_new_findings": len(new),
            "max_severity":   max(
                (f.get("severity", "info") for f in new),
                key=lambda s: {"critical":5, "high":4, "medium":3, "low":2, "info":1}.get(s, 0),
                default="info",
            ),
            "new_findings":   new,
        })

    # Most-severe updates first; ties broken by recency
    severity_order = {"critical":5, "high":4, "medium":3, "low":2, "info":1}
    updates.sort(key=lambda u: (
        -severity_order.get(u.get("max_severity", "info"), 0),
        u.get("ran_at") or "",
    ))

    return {
        "n_open_cases":         len(user_cases),
        "n_cases_with_updates": len(updates),
        "since":                since.isoformat(),
        "updates":              updates,
    }
