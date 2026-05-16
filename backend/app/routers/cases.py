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
from app.models import Case, CaseNote, CaseDocument, FraudFlag, Provider, User, AuditLog
from app.schemas import (
    CaseCreate, CaseDocumentOut, CaseListResponse, CaseNoteCreate,
    CaseNoteOut, CaseOut, CaseOutcomeUpdate, CaseUpdate, ProviderSummary
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

# Magic-bytes validation: (prefix_bytes, set_of_valid_mime_types)
# Prevents bypassing the MIME whitelist by setting a fake Content-Type header.
_MAGIC_BYTES: list[tuple[bytes, set[str]]] = [
    (b"%PDF",                    {"application/pdf"}),
    (b"\xFF\xD8\xFF",            {"image/jpeg"}),
    (b"\x89PNG\r\n\x1a\n",      {"image/png"}),
    (b"II*\x00",                 {"image/tiff"}),   # little-endian TIFF
    (b"MM\x00*",                 {"image/tiff"}),   # big-endian TIFF
    # ZIP container — used by modern Office formats (.docx etc.)
    (b"PK\x03\x04", {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }),
    # OLE2 compound document — legacy Word/Excel (.doc, .xls)
    (b"\xD0\xCF\x11\xE0",       {"application/msword"}),
]


def _validate_magic_bytes(content: bytes, declared_mime: str) -> bool:
    """
    Return True when the file's magic bytes are consistent with the declared MIME type.

    text/plain and text/csv have no reliable magic bytes; they are validated by
    checking that the first 1 KB decodes as UTF-8.  All other allowed MIME types
    must produce a matching magic-byte prefix.
    """
    for magic, allowed_mimes in _MAGIC_BYTES:
        if content.startswith(magic):
            return declared_mime in allowed_mimes

    if declared_mime in {"text/plain", "text/csv"}:
        try:
            content[:1024].decode("utf-8")
            return True
        except (UnicodeDecodeError, ValueError):
            return False

    return False


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
    outcome: str | None = Query(
        None,
        pattern=r"^(substantiated|unsubstantiated|referred_to_doj|referred_to_state_ag|closed_no_action)$",
    ),
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
    if outcome:
        query = query.where(Case.outcome == outcome)
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

    # Validate assigned_to — must be an existing active user whose state_access
    # overlaps with the case's state (prevents assigning to out-of-jurisdiction analysts).
    if body.assigned_to:
        assignee_res = await db.execute(select(User).where(User.id == body.assigned_to))
        assignee = assignee_res.scalar_one_or_none()
        if not assignee:
            raise HTTPException(status_code=422, detail="assigned_to user not found")
        if not assignee.is_active:
            raise HTTPException(status_code=422, detail="assigned_to user is not active")
        assignee_states = assignee.state_access or []
        if assignee_states and target_state and target_state not in assignee_states:
            raise HTTPException(
                status_code=422,
                detail=f"Assigned user does not have access to state: {target_state}",
            )

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
    await db.flush()
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

    # Validate assigned_to if being changed
    if "assigned_to" in updates and updates["assigned_to"] is not None:
        assignee_res = await db.execute(
            select(User).where(User.id == updates["assigned_to"])
        )
        assignee = assignee_res.scalar_one_or_none()
        if not assignee:
            raise HTTPException(status_code=422, detail="assigned_to user not found")
        if not assignee.is_active:
            raise HTTPException(status_code=422, detail="assigned_to user is not active")
        assignee_states = assignee.state_access or []
        if assignee_states and case.state and case.state not in assignee_states:
            raise HTTPException(
                status_code=422,
                detail=f"Assigned user does not have access to state: {case.state}",
            )

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
    # Re-fetch with populate_existing=True so server-updated columns (e.g.
    # updated_at via onupdate=func.now()) are visible before Pydantic reads them.
    result = await db.execute(
        select(Case)
        .options(
            selectinload(Case.provider),
            selectinload(Case.case_notes).selectinload(CaseNote.user),
            selectinload(Case.documents),
        )
        .where(Case.id == case_id)
        .execution_options(populate_existing=True)
    )
    case = result.scalar_one()
    out = CaseOut.model_validate(case)
    if case.provider:
        out.provider = ProviderSummary.model_validate(case.provider)
    return out


# ── Notes ─────────────────────────────────────────────────────────────────────

@router.post("/{case_id}/notes", response_model=CaseNoteOut, status_code=status.HTTP_201_CREATED)
async def add_note(
    case_id: int,
    body: CaseNoteCreate,
    current_user: Annotated[User, Depends(require_role("admin", "analyst"))],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(select(Case).where(Case.id == case_id))
    case = result.scalar_one_or_none()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    # State access check — analysts are scoped to their jurisdiction
    allowed_states = current_user.state_access or []
    if allowed_states and case.state not in allowed_states:
        raise HTTPException(status_code=403, detail="Access denied for this state")

    note = CaseNote(case_id=case.id, user_id=current_user.id, content=body.content)
    db.add(note)
    db.add(AuditLog(
        user_id=current_user.id,
        action="add_note",
        target_type="case",
        target_id=str(case_id),
        details={"content_length": len(body.content)},
    ))
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
    case = result.scalar_one_or_none()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    # State access check — analysts are scoped to their jurisdiction
    allowed_states = current_user.state_access or []
    if allowed_states and case.state not in allowed_states:
        raise HTTPException(status_code=403, detail="Access denied for this state")

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

    # 2b. Magic-bytes validation — Content-Type header is client-controlled and
    # trivially spoofed.  Check the actual file header against the declared MIME.
    if not _validate_magic_bytes(content, file.content_type):
        raise HTTPException(
            status_code=415,
            detail=(
                f"File content does not match declared type '{file.content_type}'. "
                "The file may be corrupt or the Content-Type header may have been spoofed."
            ),
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


# ── Outcome recording ─────────────────────────────────────────────────────────

_CONFIRMED_OUTCOMES = {"substantiated", "referred_to_doj", "referred_to_state_ag"}


@router.patch("/{case_id}/outcome", response_model=CaseOut)
async def record_outcome(
    case_id: int,
    body: CaseOutcomeUpdate,
    current_user: Annotated[User, Depends(require_role("admin", "analyst"))],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Record the final disposition of a case.

    Outcomes that confirm fraud (substantiated, referred_to_doj, referred_to_state_ag)
    trigger a feedback update: all active fraud_flags for the provider have their
    confidence set to 1.0 and are marked reviewed. This creates the training signal
    loop — confirmed cases directly update the flag confidence scores used in reports
    and future model retraining.

    Outcomes that clear the provider (unsubstantiated, closed_no_action) do not
    affect flag confidence — the statistical signal remains for monitoring, but
    the case is closed.
    """
    from datetime import datetime, timezone

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

    if not _can_write_case(current_user, case):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    now = datetime.now(timezone.utc)
    case.outcome      = body.outcome
    case.outcome_note = body.outcome_note
    case.resolved_at  = now
    # Mirror status: close the case if not already referred
    if case.status not in ("referred", "closed"):
        case.status = "closed"

    # ── Feedback loop: confirmed outcomes raise flag confidence to 1.0 ─────────
    flags_updated = 0
    if body.outcome in _CONFIRMED_OUTCOMES:
        flags_result = await db.execute(
            select(FraudFlag)
            .where(FraudFlag.npi == case.provider_npi)
            .where(FraudFlag.is_active == True)  # noqa: E712
        )
        flags = flags_result.scalars().all()
        for flag in flags:
            flag.confidence  = 1.000
            flag.reviewed_by = current_user.id
            flag.reviewed_at = now
        flags_updated = len(flags)

    db.add(AuditLog(
        user_id=current_user.id,
        action="record_outcome",
        target_type="case",
        target_id=str(case_id),
        details={
            "outcome":        body.outcome,
            "outcome_note":   body.outcome_note,
            "flags_confirmed": flags_updated,
            "provider_npi":   case.provider_npi,
        },
    ))

    await db.flush()
    # Re-fetch with populate_existing=True so server-updated columns (e.g.
    # updated_at via onupdate=func.now()) are visible before Pydantic reads them.
    result = await db.execute(
        select(Case)
        .options(
            selectinload(Case.provider),
            selectinload(Case.case_notes).selectinload(CaseNote.user),
            selectinload(Case.documents),
        )
        .where(Case.id == case_id)
        .execution_options(populate_existing=True)
    )
    case = result.scalar_one()
    out = CaseOut.model_validate(case)
    if case.provider:
        out.provider = ProviderSummary.model_validate(case.provider)
    return out
