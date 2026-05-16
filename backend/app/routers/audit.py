from datetime import datetime, timezone
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user, require_role
from app.database import get_db
from app.models import AuditLog, User
from app.schemas import AuditLogOut, AuditLogResponse

router = APIRouter()


# ── Attestations ──────────────────────────────────────────────────────────────
# Required by the methodology doc (§8) for legal defensibility.  Before a user
# takes an action that produces an artefact for external use (PDF export, CSV
# export, marking a case substantiated, sending a referral), they must
# attest that they understand the system's limitations and the requirement
# to verify findings against underlying claim records.

ActionType = Literal[
    "pdf_export",
    "csv_export",
    "case_outcome_substantiated",
    "case_referral",
]


class AttestationRequest(BaseModel):
    """User-acknowledged attestation that they understand the limitations
    before performing a downstream action."""
    action:    ActionType
    target_id: str | None = Field(default=None, description="Provider NPI or case ID")
    target_type: str | None = Field(default=None, description="'provider' | 'case' | …")
    methodology_version: str = Field(
        default="2.1.0",
        description="Methodology doc version the user attested to",
    )


@router.get("/timeline", response_model=AuditLogResponse)
async def get_audit_timeline(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    target_type: Literal["provider", "case", "user"] = Query(...),
    target_id:   str = Query(..., min_length=1, max_length=64),
    limit:       int = Query(50, ge=1, le=200),
):
    """
    Return the audit timeline for a specific target — every login, view,
    export, attestation, outcome change, etc. recorded against this entity.

    Available to all authenticated users (admin/analyst/viewer).  Investigators
    reviewing a provider record need to see who has previously touched that
    record before adding their own findings, both for collaboration and for
    chain-of-custody.

    Per methodology doc §10 (Data chain of custody), every record-action is
    logged with user, timestamp, IP, and action context.  This endpoint
    surfaces that timeline at the per-target level.
    """
    query = (
        select(AuditLog)
        .options(selectinload(AuditLog.user))
        .where(AuditLog.target_type == target_type, AuditLog.target_id == target_id)
        .order_by(AuditLog.created_at.desc())
        .limit(limit)
    )
    rows = (await db.execute(query)).scalars().all()

    items = []
    for log in rows:
        out = AuditLogOut.model_validate(log)
        if log.user:
            out.user_name = log.user.name
        items.append(out)

    return AuditLogResponse(
        items=items,
        total=len(items),
        page=1,
        page_size=limit,
    )


@router.post("/attestation", status_code=201)
async def create_attestation(
    payload: AttestationRequest,
    request: Request,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Record that the user has attested to the methodology limitations before
    performing a sensitive action.  The attestation is stored in audit_log
    with action="attestation" and the action they're about to perform in
    `details.action`.

    The frontend modal posts to this endpoint when the user clicks
    "I acknowledge".  The returned `attestation_id` is included in the
    subsequent action request.
    """
    log = AuditLog(
        user_id=current_user.id,
        action="attestation",
        target_type=payload.target_type,
        target_id=payload.target_id,
        details={
            "for_action":          payload.action,
            "methodology_version": payload.methodology_version,
            "user_email":          current_user.email,
            "user_role":           current_user.role,
            "attested_at":         datetime.now(timezone.utc).isoformat(),
        },
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()
    await db.refresh(log)
    return {
        "attestation_id": log.id,
        "for_action":     payload.action,
        "attested_at":    log.created_at.isoformat() if log.created_at else None,
        "expires_in":     900,    # 15 minutes — frontend re-prompts after this
    }


@router.get("", response_model=AuditLogResponse)
async def list_audit_logs(
    current_user: Annotated[User, Depends(require_role("admin"))],
    db: Annotated[AsyncSession, Depends(get_db)],
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    user_id: UUID | None = None,
    action: str | None = None,
    target_type: str | None = None,
):
    query = select(AuditLog).options(selectinload(AuditLog.user))

    if user_id:
        query = query.where(AuditLog.user_id == user_id)
    if action:
        query = query.where(AuditLog.action == action)
    if target_type:
        query = query.where(AuditLog.target_type == target_type)

    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar_one()

    query = query.order_by(AuditLog.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(query)
    logs = result.scalars().all()

    items = []
    for log in logs:
        out = AuditLogOut.model_validate(log)
        if log.user:
            out.user_name = log.user.name
        items.append(out)

    return AuditLogResponse(items=items, total=total, page=page, page_size=page_size)
