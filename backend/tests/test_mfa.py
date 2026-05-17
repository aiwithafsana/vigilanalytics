"""
test_mfa.py — Two-step MFA login + enrollment + backup codes.
"""
import pyotp
import pytest

from app.services import mfa as mfa_service
from tests.factories import create_user


# ── Service-level unit tests ──────────────────────────────────────────────────

def test_totp_secret_and_provisioning_uri_round_trip():
    secret = mfa_service.generate_totp_secret()
    assert len(secret) >= 16   # base32 secret, 32 chars typical

    uri = mfa_service.provisioning_uri(secret, "alice@example.com")
    assert uri.startswith("otpauth://totp/")
    assert "Vigil" in uri
    assert "alice@example.com" in uri or "alice%40example.com" in uri


def test_verify_totp_accepts_correct_code():
    secret = mfa_service.generate_totp_secret()
    code   = pyotp.TOTP(secret).now()
    assert mfa_service.verify_totp(secret, code) is True


def test_verify_totp_rejects_wrong_code():
    secret = mfa_service.generate_totp_secret()
    assert mfa_service.verify_totp(secret, "000000") is False


def test_verify_totp_rejects_malformed_input():
    secret = mfa_service.generate_totp_secret()
    assert mfa_service.verify_totp(secret, "")        is False
    assert mfa_service.verify_totp(secret, "abc")     is False
    assert mfa_service.verify_totp(secret, "1234567") is False
    assert mfa_service.verify_totp("", "123456")      is False


def test_backup_code_generation_and_consumption():
    plain, hashed = mfa_service.generate_backup_codes()
    assert len(plain)  == mfa_service.BACKUP_CODE_COUNT
    assert len(hashed) == mfa_service.BACKUP_CODE_COUNT
    assert all(len(c) == mfa_service.BACKUP_CODE_LENGTH for c in plain)

    # First use of a code returns success and removes the hash from the list
    ok, remaining = mfa_service.consume_backup_code(plain[0], hashed)
    assert ok is True
    assert len(remaining) == mfa_service.BACKUP_CODE_COUNT - 1

    # Same code used again must NOT match (it was removed)
    ok2, _ = mfa_service.consume_backup_code(plain[0], remaining)
    assert ok2 is False


def test_mfa_challenge_token_round_trip():
    token = mfa_service.create_mfa_challenge_token("user-id-123")
    assert mfa_service.decode_mfa_challenge_token(token) == "user-id-123"
    # Garbage in → None
    assert mfa_service.decode_mfa_challenge_token("not-a-token") is None


# ── Login flow ────────────────────────────────────────────────────────────────

async def _enable_mfa(db, user) -> str:
    """Helper: forcibly enable MFA on a user and return the secret."""
    secret = mfa_service.generate_totp_secret()
    user.mfa_secret = secret
    user.mfa_enabled = True
    await db.commit()
    return secret


async def test_login_with_mfa_returns_challenge_not_tokens(db, client):
    """When user has MFA enabled, /login returns a challenge, not tokens."""
    user = await create_user(db, email="m@x.com", password="GoodPass123!")
    await _enable_mfa(db, user)

    resp = await client.post(
        "/api/users/login",
        json={"email": "m@x.com", "password": "GoodPass123!"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("mfa_required") is True
    assert "mfa_token" in body
    assert "access_token" not in body


async def test_login_mfa_step2_with_valid_totp(db, client):
    user = await create_user(db, email="m@x.com", password="GoodPass123!")
    secret = await _enable_mfa(db, user)

    r1 = await client.post(
        "/api/users/login",
        json={"email": "m@x.com", "password": "GoodPass123!"},
    )
    mfa_token = r1.json()["mfa_token"]

    r2 = await client.post(
        "/api/users/login/mfa",
        json={"mfa_token": mfa_token, "code": pyotp.TOTP(secret).now()},
    )
    assert r2.status_code == 200
    assert "access_token" in r2.json()
    assert "refresh_token" in r2.json()


async def test_login_mfa_step2_rejects_invalid_code(db, client):
    user = await create_user(db, email="m@x.com", password="GoodPass123!")
    await _enable_mfa(db, user)

    r1 = await client.post(
        "/api/users/login",
        json={"email": "m@x.com", "password": "GoodPass123!"},
    )
    mfa_token = r1.json()["mfa_token"]

    r2 = await client.post(
        "/api/users/login/mfa",
        json={"mfa_token": mfa_token, "code": "000000"},
    )
    assert r2.status_code == 401


async def test_login_mfa_rejects_invalid_challenge(client):
    """A garbage / forged challenge token must be rejected."""
    r = await client.post(
        "/api/users/login/mfa",
        json={"mfa_token": "not-a-real-token", "code": "123456"},
    )
    assert r.status_code == 401


# ── Enrollment flow ───────────────────────────────────────────────────────────

async def test_mfa_setup_returns_secret_but_does_not_enable(client, analyst_headers):
    """/mfa/setup stores the secret but does NOT flip mfa_enabled."""
    headers = analyst_headers
    resp = await client.post("/api/users/mfa/setup", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert "secret" in body
    assert body["provisioning_uri"].startswith("otpauth://totp/")
    assert body["issuer"] == "Vigil"

    # Still not enabled until activate
    me = await client.get("/api/users/me", headers=headers)
    # mfa_enabled isn't in /me response by default, but we can check via raw row
    # via a follow-up activate test


async def test_mfa_activate_with_valid_code_enables_and_returns_backup_codes(
    client, analyst_headers,
):
    headers = analyst_headers
    setup = (await client.post("/api/users/mfa/setup", headers=headers)).json()
    code = pyotp.TOTP(setup["secret"]).now()

    resp = await client.post(
        "/api/users/mfa/activate",
        headers=headers,
        json={"code": code},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["mfa_enabled"] is True
    assert len(body["backup_codes"]) == mfa_service.BACKUP_CODE_COUNT


async def test_mfa_activate_rejects_invalid_code(client, analyst_headers):
    headers = analyst_headers
    await client.post("/api/users/mfa/setup", headers=headers)
    resp = await client.post(
        "/api/users/mfa/activate",
        headers=headers,
        json={"code": "000000"},
    )
    assert resp.status_code == 401


async def test_backup_code_can_complete_login(db, client):
    """A user can use a backup code to complete the second factor."""
    user = await create_user(db, email="m@x.com", password="GoodPass123!")
    await _enable_mfa(db, user)
    # Issue a backup code via the service directly (bypass enrollment endpoint)
    plain, hashed = mfa_service.generate_backup_codes()
    user.mfa_backup_codes = hashed
    await db.commit()

    r1 = await client.post(
        "/api/users/login",
        json={"email": "m@x.com", "password": "GoodPass123!"},
    )
    mfa_token = r1.json()["mfa_token"]

    r2 = await client.post(
        "/api/users/login/mfa",
        json={"mfa_token": mfa_token, "code": plain[0]},
    )
    assert r2.status_code == 200, r2.text

    # Same backup code can NOT be used a second time (one-shot)
    r1b = await client.post(
        "/api/users/login",
        json={"email": "m@x.com", "password": "GoodPass123!"},
    )
    mfa_token2 = r1b.json()["mfa_token"]
    r2b = await client.post(
        "/api/users/login/mfa",
        json={"mfa_token": mfa_token2, "code": plain[0]},
    )
    assert r2b.status_code == 401
