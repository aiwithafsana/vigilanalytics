import asyncio
import functools
from datetime import datetime, timedelta, timezone
from typing import Annotated
from uuid import UUID

import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import get_db
from app.models import User
from app.schemas import TokenData

settings = get_settings()
bearer_scheme = HTTPBearer()


# ── Password hashing ──────────────────────────────────────────────────────────
# bcrypt is intentionally slow (CPU-bound, ~100–300 ms).  Running it on the
# event loop blocks all other requests for that duration.  Offload to the
# default thread-pool executor so the loop stays free.

async def hash_password(password: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        lambda: bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode(),
    )


async def verify_password(plain: str, hashed: str) -> bool:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        functools.partial(bcrypt.checkpw, plain.encode(), hashed.encode()),
    )


# ── JWT ───────────────────────────────────────────────────────────────────────

def _create_token(data: dict, expires_delta: timedelta) -> str:
    payload = data.copy()
    payload["exp"] = datetime.now(timezone.utc) + expires_delta
    return jwt.encode(payload, settings.secret_key, algorithm=settings.algorithm)


def create_access_token(user: User) -> str:
    return _create_token(
        {
            "sub": str(user.id),
            "email": user.email,
            "role": user.role,
            "state_access": user.state_access or [],
            "type": "access",
            # ver is the token_version at issuance time.  If the user's DB row is
            # incremented (logout, password change, account takeover response) all
            # previously issued tokens are immediately rejected by get_current_user.
            "ver": user.token_version,
        },
        timedelta(minutes=settings.access_token_expire_minutes),
    )


def create_refresh_token(user: User) -> str:
    return _create_token(
        {
            "sub": str(user.id),
            "type": "refresh",
            # ver in the refresh token enables single-use rotation: the refresh
            # endpoint increments token_version after each use, making old
            # refresh tokens invalid immediately.
            "ver": user.token_version,
        },
        timedelta(days=settings.refresh_token_expire_days),
    )


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from e


# ── Current user dependency ───────────────────────────────────────────────────

async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer_scheme)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> User:
    payload = decode_token(credentials.credentials)

    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token type")

    user_id = payload.get("sub")
    result = await db.execute(select(User).where(User.id == UUID(user_id)))
    user = result.scalar_one_or_none()

    if user is None or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")

    # Validate token version — rejects all tokens issued before the last logout,
    # password change, or forced re-auth.  Tokens without a "ver" claim (issued
    # before this field was added) are rejected as a safe migration default.
    token_ver = payload.get("ver")
    if token_ver is None or token_ver != user.token_version:
        raise HTTPException(
            status_code=401,
            detail="Session revoked — please log in again",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


# ── Role-based access helpers ─────────────────────────────────────────────────

def require_role(*roles: str):
    """Dependency factory: enforce that the current user has one of the given roles."""

    async def _check(current_user: Annotated[User, Depends(get_current_user)]) -> User:
        if current_user.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires role: {' or '.join(roles)}",
            )
        return current_user

    return _check


def require_state_access(state: str):
    """Check that the current user has access to the given state (empty list = all states)."""

    async def _check(current_user: Annotated[User, Depends(get_current_user)]) -> User:
        allowed = current_user.state_access or []
        if allowed and state not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"You do not have access to state: {state}",
            )
        return current_user

    return _check


# Convenience aliases
AdminOnly = Depends(require_role("admin"))
AnalystOrAbove = Depends(require_role("admin", "analyst"))
AnyUser = Depends(get_current_user)
