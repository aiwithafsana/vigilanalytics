from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import require_role
from app.database import get_db
from app.models import AuditLog, User
from app.schemas import AuditLogOut, AuditLogResponse

router = APIRouter()


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
