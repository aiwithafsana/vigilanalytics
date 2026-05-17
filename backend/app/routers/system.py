"""
system.py — system metadata and operational endpoints.

  GET  /api/system/data-vintage        Show how fresh the data is (any user)
  POST /api/system/leie-refresh        Manually trigger an immediate LEIE refresh (admin only)

The data-vintage payload is consumed by the frontend to display "Data through
2022 — LEIE refreshed 2 days ago" badges next to any provider score, satisfying
the legal-defensibility requirement that data freshness be visible at the
point of use.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import AdminOnly, get_current_user
from app.database import AsyncSessionLocal, get_db
from app.models import LeieExclusion, Provider, User
from app.services.leie_refresh import refresh_leie

logger = logging.getLogger(__name__)
router = APIRouter()

# Bump this when the model methodology changes; surfaced in /data-vintage
# so attorneys can verify which model version generated a given score.
MODEL_VERSION = "2.1.0"

# The CMS Part B Public Use File covers a calendar year that's surfaced here
# as the "scoring data vintage."  Update when the production scoring year
# changes (currently 2022).
SCORING_DATA_YEAR = 2022


@router.get("/data-vintage", summary="Report how fresh the underlying data is")
async def get_data_vintage(
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """
    Return data-freshness information for the UI to display alongside scores.

    Returns:
      model_version            — Vigil model version that produced current scores
      scoring_data_year        — calendar year of CMS billing data scored
      scoring_data_through     — last day of the scoring year (for legal use)
      providers_last_scored_at — most recent Provider.scored_at across the table
      leie_last_refreshed_at   — most recent LEIE record creation timestamp
      leie_active_count        — count of providers currently flagged is_excluded
    """
    # Most recent provider scoring run
    last_scored = (await db.execute(select(func.max(Provider.scored_at)))).scalar()

    # Most recent LEIE row insertion = last refresh time
    last_leie_refresh = (await db.execute(select(func.max(LeieExclusion.created_at)))).scalar()

    # How many providers currently marked excluded (matches the LEIE feed)
    leie_active_count = (
        await db.execute(
            select(func.count()).select_from(Provider).where(Provider.is_excluded == True)  # noqa: E712
        )
    ).scalar() or 0

    return {
        "model_version":            MODEL_VERSION,
        "scoring_data_year":        SCORING_DATA_YEAR,
        "scoring_data_through":     f"{SCORING_DATA_YEAR}-12-31",
        "providers_last_scored_at": last_scored.isoformat() if last_scored else None,
        "leie_last_refreshed_at":   last_leie_refresh.isoformat() if last_leie_refresh else None,
        "leie_active_count":        leie_active_count,
        "as_of":                    datetime.now(timezone.utc).isoformat(),
    }


@router.post("/leie-refresh", summary="Manually trigger LEIE refresh (admin only)")
async def trigger_leie_refresh(
    background_tasks: BackgroundTasks,
    current_user: Annotated[User, AdminOnly],
):
    """
    Kick off an immediate LEIE refresh in the background.

    The endpoint returns immediately with a 202; the actual download + DB
    write happens off-request so the HTTP response time is bounded.  Admin-
    only because the refresh is heavy and writes to provider and fraud_flags
    tables.

    Use the regular weekly schedule (set up in app.main lifespan) for normal
    operation.  This endpoint is for ad-hoc use after a court decision or
    after pushing an investigative case to a state AG.
    """

    async def _run():
        try:
            async with AsyncSessionLocal() as db:
                delta = await refresh_leie(db)
                await db.commit()
            logger.info(
                "Manual LEIE refresh applied",
                extra={
                    "triggered_by":     current_user.email,
                    "newly_excluded":   delta.newly_excluded,
                    "newly_reinstated": delta.newly_reinstated,
                    "flags_inserted":   delta.flags_inserted,
                },
            )
        except Exception:
            logger.exception("Manual LEIE refresh failed", extra={
                "triggered_by": current_user.email,
            })

    background_tasks.add_task(_run)
    return {
        "status":       "accepted",
        "message":      "LEIE refresh started in background",
        "triggered_at": datetime.now(timezone.utc).isoformat(),
        "triggered_by": current_user.email,
    }
