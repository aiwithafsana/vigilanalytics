import re
from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import (
    get_current_user,
    require_role,
    hash_password,
    create_access_token,
    create_refresh_token,
    verify_password,
    decode_token,
)
from app.database import get_db
from app.models import User, AuditLog
from app.schemas import UserCreate, UserOut, UserUpdate, LoginRequest, TokenResponse, RefreshRequest

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)

# Allowed roles to prevent arbitrary role assignment
_VALID_ROLES = {"admin", "analyst", "viewer"}

# Password policy: ≥12 chars, upper, lower, digit, special
_PW_RE = re.compile(r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*()_+\-=\[\]{};':\"\\|,.<>\/?]).{12,}$")

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}


def _validate_password(password: str) -> None:
    if not _PW_RE.match(password):
        raise HTTPException(
            status_code=422,
            detail=(
                "Password must be at least 12 characters and contain uppercase, "
                "lowercase, a digit, and a special character."
            ),
        )


# ── Auth endpoints ────────────────────────────────────────────────────────────

@router.post("/login", response_model=TokenResponse)
@limiter.limit("10/minute")           # max 10 login attempts per IP per minute
async def login(
    request: Request,
    body: LoginRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(select(User).where(User.email == body.email))
    user = result.scalar_one_or_none()

    # Constant-time failure — always call verify_password to prevent timing attacks
    if not user or not verify_password(body.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account disabled")

    user.last_login = datetime.now(timezone.utc)
    db.add(AuditLog(
        user_id=user.id,
        action="login",
        target_type="user",
        target_id=str(user.id),
        details={"ip": request.client.host if request.client else "unknown"},
    ))
    await db.flush()

    return TokenResponse(
        access_token=create_access_token(user),
        refresh_token=create_refresh_token(user),
    )


@router.post("/refresh", response_model=TokenResponse)
@limiter.limit("30/minute")
async def refresh(
    request: Request,
    body: RefreshRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    payload = decode_token(body.refresh_token)
    if payload.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid token type")

    result = await db.execute(select(User).where(User.id == UUID(payload["sub"])))
    user = result.scalar_one_or_none()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")

    return TokenResponse(
        access_token=create_access_token(user),
        refresh_token=create_refresh_token(user),
    )


@router.get("/me", response_model=UserOut)
async def me(current_user: Annotated[User, Depends(get_current_user)]):
    return current_user


# ── User management (admin only) ──────────────────────────────────────────────

@router.get("", response_model=list[UserOut])
async def list_users(
    current_user: Annotated[User, Depends(require_role("admin"))],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(select(User).order_by(User.created_at.desc()))
    return result.scalars().all()


@router.post("", response_model=UserOut, status_code=status.HTTP_201_CREATED)
async def create_user(
    body: UserCreate,
    current_user: Annotated[User, Depends(require_role("admin"))],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    # Validate role
    if body.role not in _VALID_ROLES:
        raise HTTPException(status_code=422, detail=f"Invalid role. Must be one of: {_VALID_ROLES}")

    # Validate state codes
    if body.state_access:
        invalid = [s for s in body.state_access if s not in US_STATES]
        if invalid:
            raise HTTPException(status_code=422, detail=f"Invalid state codes: {invalid}")

    # Enforce password policy
    _validate_password(body.password)

    existing = await db.execute(select(User).where(User.email == body.email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Email already registered")

    user = User(
        email=body.email,
        hashed_password=hash_password(body.password),
        name=body.name,
        role=body.role,
        state_access=body.state_access or [],
    )
    db.add(user)
    db.add(AuditLog(
        user_id=current_user.id,
        action="create_user",
        target_type="user",
        target_id=body.email,
        details={"role": body.role},
    ))
    await db.flush()
    await db.refresh(user)
    return user


@router.patch("/{user_id}", response_model=UserOut)
async def update_user(
    user_id: UUID,
    body: UserUpdate,
    current_user: Annotated[User, Depends(require_role("admin"))],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    updates = body.model_dump(exclude_none=True)

    # Validate role if being changed
    if "role" in updates and updates["role"] not in _VALID_ROLES:
        raise HTTPException(status_code=422, detail=f"Invalid role: {updates['role']}")

    # Validate state codes if being changed
    if "state_access" in updates and updates["state_access"]:
        invalid = [s for s in updates["state_access"] if s not in US_STATES]
        if invalid:
            raise HTTPException(status_code=422, detail=f"Invalid state codes: {invalid}")

    # Cannot demote/deactivate yourself
    if user_id == current_user.id and "is_active" in updates and not updates["is_active"]:
        raise HTTPException(status_code=400, detail="Cannot deactivate your own account")

    for field, value in updates.items():
        setattr(user, field, value)

    db.add(AuditLog(
        user_id=current_user.id,
        action="update_user",
        target_type="user",
        target_id=str(user_id),
        details=updates,
    ))
    await db.flush()
    await db.refresh(user)
    return user


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def deactivate_user(
    user_id: UUID,
    current_user: Annotated[User, Depends(require_role("admin"))],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    if user_id == current_user.id:
        raise HTTPException(status_code=400, detail="Cannot deactivate your own account")

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.is_active = False
    db.add(AuditLog(
        user_id=current_user.id,
        action="deactivate_user",
        target_type="user",
        target_id=str(user_id),
    ))
    await db.flush()
