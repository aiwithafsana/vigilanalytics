import os
import re
import secrets
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user, require_role
from app.config import get_settings
from app.database import get_db
from app.models import Case, CaseNote, CaseDocument, Provider, User, AuditLog
from app.schemas import (
    CaseCreate, CaseDocumentOut, CaseListResponse, CaseNoteCreate,
    CaseNoteOut, CaseOut, CaseUpdate, ProviderSummary
)

router = APIRouter()
settings = get_settings()

# ── Constants ─────────────────────────────────────────────────────────────────

_VALID_STATUSES = {"open", "under_review", "closed", "referred"}

# Allowed upload MIME types (evidence packages only)
_ALLOWED_MIME = {
    "application/pdf",
    "image/jpeg",
    "image/png",
    "image/tiff",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "text/plain",
    "text/csv",
}

_MAX_FILE_SIZE = 25 * 1024 * 1024   # 25 MB
_SAFE_FILENAME_RE = re.compile(r"[^\w.\-]")  # strip anything not alphanumeric / dot / dash


def _generate_case_number() -> str:
    """Cryptographically secure 8-char hex suffix — 4 billion possible values."""
    return f"VGL-{secrets.token_hex(4).upper()}"


def _sanitize_filename(name: str) -> str:
    """Strip path components and unsafe characters from an uploaded filename."""
    # Remove directory components
    name = os.path.basename(name)
    # Replace unsafe characters with underscores
    name = _SAFE_FILENAME_RE.sub("_", name)
    # Truncate to prevent excessively long filenames
    return name[:128]


def _can_write_case(user: User, case: Case) -> bool:
    if user.role == "admin":
        return True
    if user.role == "analyst" and (case.assigned_to == user.id or case.created_by == user.id):
        return True
    return False


# ── List cases ────────────────────────────────────────────────────────────────

@router.get("", response_model=CaseListResponse)
async def list_cases(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    page: int = Query(1, ge=1, le=10_000),
    page_size: int = Query(20, ge=1, le=100),
    status: str | None = Query(None, pattern="^(open|under_review|closed|referred)$"),
    state: str | None = Query(None, max_length=2),
    assigned_to_me: bool = False,
):
    query = (
        select(Case)
        .options(
            selectinload(Case.provider),
            selectinload(Case.case_notes).selectinload(CaseNote.user),
            selectinload(Case.documents),
        )
    )

    allowed_states = current_user.state_access or []
    if allowed_states:
        query = query.where(Case.state.in_(allowed_states))
    if status:
        query = query.where(Case.status == status)
    if state:
        query = query.where(Case.state == state.upper())
    if assigned_to_me:
        query = query.where(Case.assigned_to == current_user.id)

    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar_one()

    query = query.order_by(Case.updated_at.desc()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(query)
    cases = result.scalars().all()

    items = []
    for c in cases:
        out = CaseOut.model_validate(c)
        if c.provider:
            out.provider = ProviderSummary.model_validate(c.provider)
        items.append(out)

    return CaseListResponse(items=items, total=total, page=page, page_size=page_size)


# ── Create case ───────────────────────────────────────────────────────────────

@router.post("", response_model=CaseOut, status_code=status.HTTP_201_CREATED)
async def create_case(
    body: CaseCreate,
    current_user: Annotated[User, Depends(require_role("admin", "analyst"))],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    provider_result = await db.execute(select(Provider).where(Provider.npi == body.provider_npi))
    provider = provider_result.scalar_one_or_none()
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")

    # State access check
    allowed_states = current_user.state_access or []
    target_state = body.state or provider.state
    if allowed_states and target_state not in allowed_states:
        raise HTTPException(status_code=403, detail=f"Access denied for state: {target_state}")

    case = Case(
        case_number=_generate_case_number(),
        provider_npi=body.provider_npi,
        title=body.title,
        state=target_state,
        estimated_loss=body.estimated_loss,
        notes=body.notes,
        assigned_to=body.assigned_to,
        created_by=current_user.id,
    )
    db.add(case)
    db.add(AuditLog(
        user_id=current_user.id,
        action="create_case",
        target_type="case",
        details={"provider_npi": body.provider_npi, "title": body.title},
    ))
    await db.flush()
    await db.refresh(case, ["provider", "case_notes", "documents"])
    return CaseOut.model_validate(case)


# ── Get case ──────────────────────────────────────────────────────────────────

@router.get("/{case_id}", response_model=CaseOut)
async def get_case(
    case_id: int,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(
        select(Case)
        .options(
            selectinload(Case.provider),
            selectinload(Case.case_notes).selectinload(CaseNote.user),
            selectinload(Case.documents),
        )
        .where(Case.id == case_id)
    )
    case = result.scalar_one_or_none()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    allowed_states = current_user.state_access or []
    if allowed_states and case.state not in allowed_states:
        raise HTTPException(status_code=403, detail="Access denied for this state")

    db.add(AuditLog(
        user_id=current_user.id,
        action="view_case",
        target_type="case",
        target_id=str(case_id),
    ))
    out = CaseOut.model_validate(case)
    if case.provider:
        out.provider = ProviderSummary.model_validate(case.provider)
    return out


# ── Update case ───────────────────────────────────────────────────────────────

@router.patch("/{case_id}", response_model=CaseOut)
async def update_case(
    case_id: int,
    body: CaseUpdate,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(select(Case).where(Case.id == case_id))
    case = result.scalar_one_or_none()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    # State access check (same as read)
    allowed_states = current_user.state_access or []
    if allowed_states and case.state not in allowed_states:
        raise HTTPException(status_code=403, detail="Access denied for this state")

    if not _can_write_case(current_user, case):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    updates = body.model_dump(exclude_none=True)

    # Validate status if being changed
    if "status" in updates and updates["status"] not in _VALID_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid status. Must be one of: {sorted(_VALID_STATUSES)}"
        )

    for field, value in updates.items():
        setattr(case, field, value)

    db.add(AuditLog(
        user_id=current_user.id,
        action="update_case",
        target_type="case",
        target_id=str(case_id),
        details=updates,
    ))
    await db.flush()
    await db.refresh(case, ["provider", "case_notes", "documents"])
    return CaseOut.model_validate(case)


# ── Notes ─────────────────────────────────────────────────────────────────────

@router.post("/{case_id}/notes", response_model=CaseNoteOut, status_code=status.HTTP_201_CREATED)
async def add_note(
    case_id: int,
    body: CaseNoteCreate,
    current_user: Annotated[User, Depends(require_role("admin", "analyst"))],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(select(Case).where(Case.id == case_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Case not found")

    note = CaseNote(case_id=case_id, user_id=current_user.id, content=body.content)
    db.add(note)
    await db.flush()
    await db.refresh(note)
    out = CaseNoteOut.model_validate(note)
    out.user_name = current_user.name
    return out


# ── Documents ─────────────────────────────────────────────────────────────────

@router.post("/{case_id}/documents", response_model=CaseDocumentOut, status_code=status.HTTP_201_CREATED)
async def upload_document(
    case_id: int,
    file: UploadFile = File(...),
    current_user: User = Depends(require_role("admin", "analyst")),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Case).where(Case.id == case_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Case not found")

    # ── Security checks ───────────────────────────────────────────────────────

    # 1. MIME type whitelist
    if file.content_type not in _ALLOWED_MIME:
        raise HTTPException(
            status_code=415,
            detail=f"File type '{file.content_type}' is not allowed. "
                   f"Accepted types: PDF, images, Word documents, CSV, plain text."
        )

    # 2. Read content and enforce size limit
    content = await file.read()
    if len(content) > _MAX_FILE_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File exceeds the 25 MB size limit ({len(content) // 1024 // 1024} MB uploaded)."
        )

    # 3. Sanitize filename — strip path traversal and unsafe chars
    original_name = file.filename or "upload"
    safe_name = _sanitize_filename(original_name)

    # 4. Store with UUID prefix (no original filename in path)
    os.makedirs(settings.storage_path, exist_ok=True)
    stored_name = f"{uuid.uuid4()}.bin"  # extension-agnostic storage
    file_path = os.path.join(settings.storage_path, stored_name)

    with open(file_path, "wb") as f:
        f.write(content)

    doc = CaseDocument(
        case_id=case_id,
        filename=safe_name,          # sanitized display name only
        file_path=file_path,         # UUID-based storage path
        file_size=len(content),
        uploaded_by=current_user.id,
    )
    db.add(doc)
    db.add(AuditLog(
        user_id=current_user.id,
        action="upload_document",
        target_type="case",
        target_id=str(case_id),
        details={"original_filename": original_name, "stored_as": stored_name, "size": len(content)},
    ))
    await db.flush()
    await db.refresh(doc)
    return CaseDocumentOut.model_validate(doc)
