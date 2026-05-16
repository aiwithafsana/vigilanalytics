import re
from datetime import datetime, timedelta, timezone
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

# Pre-hashed dummy password used to keep verify_password timing constant even
# when the email doesn't exist (prevents user enumeration via response time).
# Generated once: bcrypt.hashpw(b"dummy", bcrypt.gensalt(12)).decode()
_DUMMY_HASH = "$2b$12$LVJBgMrXGxShbGKGMdgBRuugrJNu6c7tlsNqkFxGkUKdxP/LBqkOa"

# Account lockout: 5 consecutive failures → 15-minute lock
_MAX_FAILURES   = 5
_LOCKOUT_MINUTES = 15
from app.database import get_db
from app.models import User, AuditLog
from app.schemas import (
    UserCreate, UserOut, UserUpdate, LoginRequest, TokenResponse, RefreshRequest,
    MfaChallengeResponse, MfaVerifyRequest, MfaSetupResponse,
    MfaActivateRequest, MfaActivateResponse, MfaDisableRequest,
)
from app.services import mfa as mfa_service

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

@router.post("/login")
@limiter.limit("10/minute")           # max 10 login attempts per IP per minute
async def login(
    request: Request,
    body: LoginRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Step 1 of login.  Verifies email + password.

    Response shape depends on MFA state:
      - MFA disabled  → TokenResponse (real access + refresh tokens)
      - MFA enabled   → MfaChallengeResponse (mfa_token, expires_in)
                        Caller must follow up with POST /api/users/login/mfa.
    """
    result = await db.execute(select(User).where(User.email == body.email))
    user = result.scalar_one_or_none()

    # Constant-time password check — always call verify_password even when the
    # email is unknown to prevent user-enumeration via response timing.
    password_ok = await verify_password(
        body.password,
        user.hashed_password if user else _DUMMY_HASH,
    )

    if not user or not password_ok:
        if user:
            # Track consecutive failures; lock after threshold.
            # IMPORTANT: commit before raising — the exception handler in the
            # test client (and production) rolls back uncommitted changes, so
            # using flush() here would undo the lockout state on every 401.
            now = datetime.now(timezone.utc)
            user.failed_login_attempts = (user.failed_login_attempts or 0) + 1
            if user.failed_login_attempts >= _MAX_FAILURES:
                user.locked_until = now + timedelta(minutes=_LOCKOUT_MINUTES)
            await db.commit()
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Check lockout after password verification (avoid leaking account existence
    # via a 423 response for non-existent email addresses).
    now = datetime.now(timezone.utc)
    if user.locked_until and user.locked_until > now:
        raise HTTPException(
            status_code=423,
            detail=(
                f"Account locked due to repeated failed login attempts. "
                f"Try again after {user.locked_until.strftime('%H:%M UTC')}."
            ),
        )

    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account disabled")

    # Password verified — but if MFA is enabled, issue a short-lived challenge
    # token instead of full credentials.  Failure counter is NOT reset yet —
    # only after MFA is also verified.
    if user.mfa_enabled and user.mfa_secret:
        challenge = mfa_service.create_mfa_challenge_token(str(user.id))
        # Audit the partial-success state for forensics (SOC playbooks need this)
        db.add(AuditLog(
            user_id=user.id,
            action="login_mfa_challenge_issued",
            target_type="user",
            target_id=str(user.id),
            details={"ip": request.client.host if request.client else "unknown"},
        ))
        await db.commit()
        return MfaChallengeResponse(mfa_token=challenge, expires_in=300)

    # Successful login — no MFA required.  Reset failure counters and emit tokens.
    user.failed_login_attempts = 0
    user.locked_until = None
    user.last_login = datetime.now(timezone.utc)
    db.add(AuditLog(
        user_id=user.id,
        action="login",
        target_type="user",
        target_id=str(user.id),
        details={"ip": request.client.host if request.client else "unknown",
                 "mfa": False},
    ))
    await db.flush()

    return TokenResponse(
        access_token=create_access_token(user),
        refresh_token=create_refresh_token(user),
    )


@router.post("/login/mfa", response_model=TokenResponse)
@limiter.limit("10/minute")
async def login_mfa(
    request: Request,
    body: MfaVerifyRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Step 2 of login — verify the second factor.

    Accepts:
      - 6-digit TOTP code from authenticator app, OR
      - 10-character one-time backup code

    On success, issues a real access + refresh token pair.  On failure,
    increments failed_login_attempts the same way as a wrong password
    (so an attacker who steals a password and tries random TOTP codes
    still trips the lockout).
    """
    user_id = mfa_service.decode_mfa_challenge_token(body.mfa_token)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid or expired MFA challenge")

    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user or not user.is_active or not user.mfa_enabled or not user.mfa_secret:
        raise HTTPException(status_code=401, detail="MFA not configured for this user")

    # Re-check lockout in case the user got locked between step 1 and step 2
    now = datetime.now(timezone.utc)
    if user.locked_until and user.locked_until > now:
        raise HTTPException(
            status_code=423,
            detail=f"Account locked. Try again after {user.locked_until.strftime('%H:%M UTC')}.",
        )

    # Try TOTP first; fall back to backup code
    used_method: str | None = None
    if mfa_service.verify_totp(user.mfa_secret, body.code):
        used_method = "totp"
    else:
        ok, remaining = mfa_service.consume_backup_code(
            body.code, list(user.mfa_backup_codes or []),
        )
        if ok:
            user.mfa_backup_codes = remaining
            used_method = "backup_code"

    if used_method is None:
        # MFA failure — increment lockout counter so a stolen password +
        # random TOTP guessing still trips the limit.
        user.failed_login_attempts = (user.failed_login_attempts or 0) + 1
        if user.failed_login_attempts >= _MAX_FAILURES:
            user.locked_until = now + timedelta(minutes=_LOCKOUT_MINUTES)
        db.add(AuditLog(
            user_id=user.id, action="login_mfa_failed",
            target_type="user", target_id=str(user.id),
            details={"ip": request.client.host if request.client else "unknown"},
        ))
        await db.commit()
        raise HTTPException(status_code=401, detail="Invalid MFA code")

    # Success — reset counters, issue tokens
    user.failed_login_attempts = 0
    user.locked_until = None
    user.last_login = now
    db.add(AuditLog(
        user_id=user.id, action="login",
        target_type="user", target_id=str(user.id),
        details={
            "ip": request.client.host if request.client else "unknown",
            "mfa": True,
            "mfa_method": used_method,
            "backup_codes_remaining": len(user.mfa_backup_codes or []),
        },
    ))
    await db.commit()
    return TokenResponse(
        access_token=create_access_token(user),
        refresh_token=create_refresh_token(user),
    )


@router.post("/refresh", response_model=TokenResponse)
@limiter.limit("10/minute")
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

    # Validate token version — rejects a refresh token that was already rotated
    # (i.e. used before) or explicitly revoked via logout / password change.
    token_ver = payload.get("ver")
    if token_ver is None or token_ver != user.token_version:
        raise HTTPException(status_code=401, detail="Refresh token has been revoked")

    # Rotate: increment token_version so the just-used refresh token can never
    # be replayed.  Any outstanding access tokens with the old version also
    # become invalid, giving us immediate session rotation on every refresh.
    user.token_version = (user.token_version or 0) + 1
    await db.flush()

    return TokenResponse(
        access_token=create_access_token(user),
        refresh_token=create_refresh_token(user),
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Invalidate all active sessions for the current user by incrementing
    token_version.  Any access or refresh tokens issued before this call
    (on all devices) will be rejected by get_current_user and /refresh.
    """
    current_user.token_version = (current_user.token_version or 0) + 1
    db.add(AuditLog(
        user_id=current_user.id,
        action="logout",
        target_type="user",
        target_id=str(current_user.id),
    ))
    await db.flush()


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
        hashed_password=await hash_password(body.password),
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


# ── MFA enrollment ────────────────────────────────────────────────────────────

@router.post("/mfa/setup", response_model=MfaSetupResponse)
async def mfa_setup(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Step 1 of MFA enrollment.  Generates a fresh TOTP secret and stores it
    on the user row, but does NOT activate MFA — that requires a successful
    verify-setup call confirming the user has the secret in their authenticator.

    Idempotent: calling this on a user who already has mfa_enabled=True will
    refuse so we don't overwrite a working secret by accident.  Disable first
    if you want to re-enroll.
    """
    if current_user.mfa_enabled:
        raise HTTPException(
            status_code=409,
            detail="MFA is already enabled.  Disable it first if you want to re-enroll.",
        )

    secret = mfa_service.generate_totp_secret()
    current_user.mfa_secret = secret
    # Don't flip mfa_enabled yet — that requires a successful verify
    db.add(AuditLog(
        user_id=current_user.id, action="mfa_setup_started",
        target_type="user", target_id=str(current_user.id),
    ))
    await db.commit()

    return MfaSetupResponse(
        secret=secret,
        provisioning_uri=mfa_service.provisioning_uri(secret, current_user.email),
        issuer=mfa_service.MFA_ISSUER,
    )


@router.post("/mfa/activate", response_model=MfaActivateResponse)
async def mfa_activate(
    body: MfaActivateRequest,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Step 2 of MFA enrollment.  Verifies the user can produce a valid TOTP code
    from the secret returned by /mfa/setup.  On success, flips mfa_enabled=True
    and returns 10 one-time backup codes (shown to the user ONCE).
    """
    if current_user.mfa_enabled:
        raise HTTPException(status_code=409, detail="MFA already enabled")
    if not current_user.mfa_secret:
        raise HTTPException(status_code=400, detail="Run /mfa/setup first")
    if not mfa_service.verify_totp(current_user.mfa_secret, body.code):
        raise HTTPException(status_code=401, detail="Invalid TOTP code")

    plain_codes, hashed_codes = mfa_service.generate_backup_codes()
    current_user.mfa_enabled      = True
    current_user.mfa_backup_codes = hashed_codes
    current_user.mfa_enrolled_at  = datetime.now(timezone.utc)
    # Bump token_version so any stale tokens are invalidated — forces all
    # existing sessions to re-authenticate, now picking up MFA.
    current_user.token_version    = (current_user.token_version or 0) + 1
    db.add(AuditLog(
        user_id=current_user.id, action="mfa_enabled",
        target_type="user", target_id=str(current_user.id),
    ))
    await db.commit()

    return MfaActivateResponse(
        mfa_enabled=True,
        backup_codes=plain_codes,
    )


@router.post("/mfa/disable", status_code=status.HTTP_204_NO_CONTENT)
async def mfa_disable(
    body: MfaDisableRequest,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Disable MFA.  Requires a current TOTP code (or backup code) to confirm
    intent — this prevents an attacker with a stolen access token from
    silently disabling the second factor.
    """
    if not current_user.mfa_enabled or not current_user.mfa_secret:
        raise HTTPException(status_code=409, detail="MFA is not enabled")

    is_totp_ok = mfa_service.verify_totp(current_user.mfa_secret, body.code)
    is_backup_ok = False
    if not is_totp_ok:
        is_backup_ok, remaining = mfa_service.consume_backup_code(
            body.code, list(current_user.mfa_backup_codes or []),
        )
        if is_backup_ok:
            current_user.mfa_backup_codes = remaining

    if not (is_totp_ok or is_backup_ok):
        raise HTTPException(status_code=401, detail="Invalid TOTP or backup code")

    current_user.mfa_enabled      = False
    current_user.mfa_secret       = None
    current_user.mfa_backup_codes = []
    # Note: mfa_enrolled_at intentionally retained for audit trail.
    current_user.token_version    = (current_user.token_version or 0) + 1
    db.add(AuditLog(
        user_id=current_user.id, action="mfa_disabled",
        target_type="user", target_id=str(current_user.id),
    ))
    await db.commit()


@router.post("/mfa/regenerate-backup-codes", response_model=MfaActivateResponse)
async def mfa_regenerate_backup_codes(
    body: MfaActivateRequest,    # require a TOTP code to confirm intent
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Generate a fresh batch of backup codes.  Invalidates any old codes.
    Requires a current TOTP code to confirm intent.
    """
    if not current_user.mfa_enabled or not current_user.mfa_secret:
        raise HTTPException(status_code=409, detail="MFA is not enabled")
    if not mfa_service.verify_totp(current_user.mfa_secret, body.code):
        raise HTTPException(status_code=401, detail="Invalid TOTP code")

    plain_codes, hashed_codes = mfa_service.generate_backup_codes()
    current_user.mfa_backup_codes = hashed_codes
    db.add(AuditLog(
        user_id=current_user.id, action="mfa_backup_codes_regenerated",
        target_type="user", target_id=str(current_user.id),
    ))
    await db.commit()

    return MfaActivateResponse(mfa_enabled=True, backup_codes=plain_codes)
