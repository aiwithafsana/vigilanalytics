"""
mfa.py — TOTP (RFC 6238) MFA + backup-code helpers.

Three-step enrollment flow:
  1. POST /api/users/mfa/setup           Generate secret, return provisioning URI
  2. User scans the QR / enters secret in their authenticator app
  3. POST /api/users/mfa/verify-setup    Verify a TOTP code → activate MFA

Login becomes a two-step flow when MFA is enabled:
  1. POST /api/users/login                → 200 with { mfa_required: true, mfa_token }
  2. POST /api/users/login/mfa            → 200 with full TokenResponse

Backup codes:
  Issued at enrollment (10 codes).  Each is one-time-use; bcrypt-hashed at rest.
  When a user uses one, that hash is removed from the array so it cannot be
  replayed.
"""
from __future__ import annotations

import secrets

import bcrypt
import pyotp

from app.config import get_settings

settings = get_settings()

# Number of backup codes issued at enrollment (and on regeneration).
BACKUP_CODE_COUNT = 10
BACKUP_CODE_LENGTH = 10        # 10 chars from secrets.token_hex(5)

# TOTP parameters — RFC 6238 defaults are 30s window, 6-digit code.
TOTP_DIGITS = 6
TOTP_INTERVAL = 30
# Allow ±1 step (≈30s) clock drift between server and authenticator.
TOTP_VALID_WINDOW = 1

# Issuer label shown in the authenticator app.  Per the methodology doc this
# should be the deployment name so a user with multiple Vigil environments
# (dev, staging, prod) can tell them apart in their authenticator.
MFA_ISSUER = "Vigil"


# ── TOTP ──────────────────────────────────────────────────────────────────────

def generate_totp_secret() -> str:
    """Generate a fresh base32 TOTP secret (160 bits = 32 base32 chars)."""
    return pyotp.random_base32()


def provisioning_uri(secret: str, account_email: str) -> str:
    """
    Build an otpauth:// URI for QR code generation.

    The frontend renders this as a QR code; the user scans it with Google
    Authenticator / Authy / 1Password / etc.  Some authenticators also accept
    the raw base32 secret directly.
    """
    return pyotp.TOTP(secret).provisioning_uri(
        name=account_email,
        issuer_name=MFA_ISSUER,
    )


def verify_totp(secret: str, code: str) -> bool:
    """Return True if `code` is a valid TOTP for `secret` within the drift window."""
    if not secret or not code:
        return False
    code = code.strip().replace(" ", "")
    if not code.isdigit() or len(code) != TOTP_DIGITS:
        return False
    return pyotp.TOTP(secret, digits=TOTP_DIGITS, interval=TOTP_INTERVAL).verify(
        code, valid_window=TOTP_VALID_WINDOW,
    )


# ── Backup codes ──────────────────────────────────────────────────────────────

def generate_backup_codes() -> tuple[list[str], list[str]]:
    """
    Generate a fresh batch of one-time backup codes.

    Returns:
        (plain_codes, hashed_codes) — plain_codes are shown to the user ONCE
        at enrollment; hashed_codes are persisted to the DB.
    """
    plain_codes = [secrets.token_hex(5) for _ in range(BACKUP_CODE_COUNT)]
    hashed = [
        bcrypt.hashpw(code.encode(), bcrypt.gensalt()).decode()
        for code in plain_codes
    ]
    return plain_codes, hashed


def consume_backup_code(provided: str, stored_hashes: list[str]) -> tuple[bool, list[str]]:
    """
    Check `provided` against the list of bcrypt-hashed backup codes.  If a
    match is found, return (True, hashes_with_match_removed) — caller must
    persist the new list so the same code cannot be reused.

    Returns (False, stored_hashes) on no match.
    """
    if not provided:
        return False, stored_hashes
    provided = provided.strip().replace("-", "").replace(" ", "").lower()
    if len(provided) != BACKUP_CODE_LENGTH:
        return False, stored_hashes

    for i, h in enumerate(stored_hashes):
        try:
            if bcrypt.checkpw(provided.encode(), h.encode()):
                # Remove this hash so the code can't be re-used
                return True, stored_hashes[:i] + stored_hashes[i + 1:]
        except (ValueError, TypeError):
            # Skip malformed hashes — they can't authenticate anyone
            continue
    return False, stored_hashes


# ── Short-lived MFA challenge token (used between login step 1 and step 2) ────

def create_mfa_challenge_token(user_id: str, ttl_seconds: int = 300) -> str:
    """
    Issue a short-lived JWT used to authenticate the second login step.

    After a correct password but before TOTP verification, the user is in a
    half-authenticated state.  We don't want to issue a real access token
    until they prove the second factor.  Instead we issue a "challenge"
    token — short-lived, restricted purpose, accepted only by the
    /login/mfa endpoint.
    """
    from datetime import datetime, timedelta, timezone

    from jose import jwt

    payload = {
        "sub":  user_id,
        "type": "mfa_challenge",
        "exp":  datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds),
    }
    return jwt.encode(payload, settings.secret_key, algorithm=settings.algorithm)


def decode_mfa_challenge_token(token: str) -> str | None:
    """Return user_id if token is a valid mfa_challenge token; else None."""
    from jose import JWTError, jwt

    try:
        payload = jwt.decode(
            token, settings.secret_key, algorithms=[settings.algorithm],
        )
        if payload.get("type") != "mfa_challenge":
            return None
        return payload.get("sub")
    except JWTError:
        return None
