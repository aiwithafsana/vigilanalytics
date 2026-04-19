"""
alerts.py — New fraud flag alert feed.

GET /api/alerts   — Returns all active fraud flags created since the current user's
                    last login, filtered to their state jurisdiction.
                    Investigators see their personalised "new since you were last here" feed.
"""

from typing import Annotated
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.database import get_db
from app.models import FraudFlag, Provider, User
from app.schemas import AlertItem, AlertResponse

router = APIRouter()

# Severity label mapping for display
_SEV_LABEL = {1: "critical", 2: "high", 3: "medium"}


@router.get("", response_model=AlertResponse)
async def get_alerts(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = Query(default=50, ge=1, le=200),
    severity: int | None = Query(default=None, ge=1, le=3),
):
    """
    Return active fraud flags created since the user's last login.
    Falls back to flags from the last 7 days if last_login is null.
    Respects state_access jurisdiction — analysts only see their states.
    """
    since: datetime | None = current_user.last_login
    if since is None:
        # First-time login or no recorded login — show last 7 days
        since = datetime.now(timezone.utc) - timedelta(days=7)

    # Build query: join FraudFlag → Provider
    query = (
        select(FraudFlag, Provider)
        .join(Provider, FraudFlag.npi == Provider.npi)
        .where(FraudFlag.is_active == True)  # noqa: E712
        .where(FraudFlag.created_at > since)
    )

    # Jurisdiction filter
    allowed_states = current_user.state_access or []
    if allowed_states:
        query = query.where(Provider.state.in_(allowed_states))

    # Optional severity filter
    if severity is not None:
        query = query.where(FraudFlag.severity == severity)

    # Prioritise: severity asc (1=critical first), then most recent
    query = query.order_by(FraudFlag.severity.asc(), FraudFlag.created_at.desc()).limit(limit)

    rows = (await db.execute(query)).all()

    items: list[AlertItem] = []
    for flag, provider in rows:
        # Build a readable provider name
        if provider.name_first:
            pname = f"{provider.name_first} {provider.name_last}".strip()
        else:
            pname = provider.name_last or provider.npi

        items.append(
            AlertItem(
                flag_id=flag.id,
                npi=flag.npi,
                provider_name=pname,
                specialty=provider.specialty,
                state=provider.state,
                risk_score=provider.risk_score,
                flag_type=flag.flag_type,
                severity=flag.severity or 3,
                explanation=flag.explanation,
                estimated_overpayment=flag.estimated_overpayment,
                created_at=flag.created_at,
            )
        )

    return AlertResponse(items=items, total=len(items), since=since)
