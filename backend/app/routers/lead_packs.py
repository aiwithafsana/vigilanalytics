"""
lead_packs.py — Generate jurisdiction-specific investigative lead packs.

  POST /api/lead-packs/generate                  Generate (returns JSON)
  GET  /api/lead-packs/generate/pdf              Generate + stream as PDF

Both accept the same filters via query params: state, specialty, limit,
min_score.  The PDF endpoint is a GET so it can be opened directly in
a new tab from the UI without dealing with form submission ceremony.

All requests audit-logged with the user identity + filter for chain-of-
custody (so we can answer "who generated the LA hospice pack on June 15?").
"""
from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.database import get_db
from app.models import AuditLog, User
from app.services.lead_pack import generate_lead_pack
from app.services.lead_pack_pdf import filename_for_pack, render_lead_pack_pdf

logger = logging.getLogger(__name__)
router = APIRouter()


def _enforce_state_access(current_user: User, state: str | None) -> str | None:
    """Apply the per-user state_access policy to the requested filter."""
    allowed = current_user.state_access or []
    if not allowed:
        return state    # unrestricted (admin) — pass through
    # If user is state-restricted and didn't specify, force to first allowed
    if state is None:
        return allowed[0]
    if state.upper() not in {s.upper() for s in allowed}:
        raise HTTPException(
            status_code=403,
            detail=(
                f"Access denied — state {state} not in your authorised "
                f"states ({', '.join(allowed)})"
            ),
        )
    return state


@router.get("/generate")
async def generate_lead_pack_json(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    state:     str | None = Query(None, min_length=2, max_length=2),
    specialty: str | None = Query(None, min_length=2, max_length=80),
    limit:     int        = Query(25, ge=5, le=100),
    min_score: float      = Query(70.0, ge=0, le=100),
):
    """Generate a lead pack and return as JSON."""
    state = _enforce_state_access(current_user, state)
    pack = await generate_lead_pack(
        db, state=state, specialty=specialty, limit=limit, min_score=min_score,
    )

    db.add(AuditLog(
        user_id=current_user.id,
        action="lead_pack_generated",
        target_type="jurisdiction",
        target_id=f"{state or 'all'}/{specialty or 'all'}",
        details={
            "format":    "json",
            "limit":     limit,
            "min_score": min_score,
            "leads_returned": len(pack.leads),
        },
    ))
    await db.commit()
    return pack.to_dict()


@router.get("/generate/pdf")
async def generate_lead_pack_pdf(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    state:     str | None = Query(None, min_length=2, max_length=2),
    specialty: str | None = Query(None, min_length=2, max_length=80),
    limit:     int        = Query(25, ge=5, le=100),
    min_score: float      = Query(70.0, ge=0, le=100),
):
    """Generate a lead pack and stream the rendered PDF."""
    state = _enforce_state_access(current_user, state)
    pack = await generate_lead_pack(
        db, state=state, specialty=specialty, limit=limit, min_score=min_score,
    )
    if not pack.leads:
        raise HTTPException(
            status_code=404,
            detail=(
                "No leads matched the filter.  Lower min_score or broaden the "
                "jurisdiction filter."
            ),
        )

    pdf_bytes = render_lead_pack_pdf(pack)

    db.add(AuditLog(
        user_id=current_user.id,
        action="lead_pack_generated",
        target_type="jurisdiction",
        target_id=f"{state or 'all'}/{specialty or 'all'}",
        details={
            "format":    "pdf",
            "limit":     limit,
            "min_score": min_score,
            "leads_returned": len(pack.leads),
            "pdf_bytes": len(pdf_bytes),
        },
    ))
    await db.commit()

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename_for_pack(pack)}"',
        },
    )
