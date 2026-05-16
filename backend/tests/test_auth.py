"""
test_auth.py — Login, token refresh, /me endpoint, token rotation, lockout, logout.
"""
import pytest
from app.auth import create_access_token, create_refresh_token
from tests.factories import create_user


# ── Login ─────────────────────────────────────────────────────────────────────

async def test_login_success(db, client):
    """Valid credentials return access and refresh tokens."""
    await create_user(db, email="jane@example.com", password="GoodPass123!")
    await db.commit()

    resp = await client.post(
        "/api/users/login",
        json={"email": "jane@example.com", "password": "GoodPass123!"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "access_token" in body
    assert "refresh_token" in body
    assert body["token_type"] == "bearer"


async def test_login_wrong_password(db, client):
    """Wrong password is rejected with 401."""
    await create_user(db, email="jane@example.com", password="GoodPass123!")
    await db.commit()

    resp = await client.post(
        "/api/users/login",
        json={"email": "jane@example.com", "password": "WrongPass999!"},
    )
    assert resp.status_code == 401


async def test_login_unknown_email(client):
    """Unknown email is rejected with 401 (no user enumeration timing difference in response code)."""
    resp = await client.post(
        "/api/users/login",
        json={"email": "ghost@example.com", "password": "Whatever123!"},
    )
    assert resp.status_code == 401


async def test_login_inactive_user(db, client):
    """Deactivated accounts are rejected with 403."""
    await create_user(db, email="inactive@example.com", password="GoodPass123!", is_active=False)
    await db.commit()

    resp = await client.post(
        "/api/users/login",
        json={"email": "inactive@example.com", "password": "GoodPass123!"},
    )
    assert resp.status_code == 403


# ── Token refresh ─────────────────────────────────────────────────────────────

async def test_refresh_success(db, client):
    """Valid refresh token returns a new access token."""
    user = await create_user(db, email="jane@example.com", password="GoodPass123!")
    await db.commit()

    refresh_token = create_refresh_token(user)
    resp = await client.post(
        "/api/users/refresh",
        json={"refresh_token": refresh_token},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "access_token" in body
    assert "refresh_token" in body


async def test_refresh_rejects_access_token(db, client):
    """Sending an access token to /refresh must be rejected."""
    user = await create_user(db, email="jane@example.com")
    await db.commit()

    access_token = create_access_token(user)
    resp = await client.post(
        "/api/users/refresh",
        json={"refresh_token": access_token},
    )
    assert resp.status_code == 401


async def test_refresh_rejects_garbage(client):
    """Malformed token returns 401."""
    resp = await client.post(
        "/api/users/refresh",
        json={"refresh_token": "not.a.real.token"},
    )
    assert resp.status_code == 401


# ── /me ───────────────────────────────────────────────────────────────────────

async def test_me_returns_current_user(db, client, analyst_user, analyst_headers):
    """GET /api/users/me returns the authenticated user's profile."""
    resp = await client.get("/api/users/me", headers=analyst_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["email"] == analyst_user.email
    assert body["role"] == "analyst"


async def test_me_requires_auth(client):
    """GET /api/users/me without a token returns 403 (HTTPBearer auto-error)."""
    resp = await client.get("/api/users/me")
    assert resp.status_code == 403


async def test_me_rejects_refresh_token(db, client):
    """Refresh tokens must not be accepted on /me."""
    user = await create_user(db)
    await db.commit()

    refresh_token = create_refresh_token(user)
    resp = await client.get(
        "/api/users/me",
        headers={"Authorization": f"Bearer {refresh_token}"},
    )
    assert resp.status_code == 401


async def test_me_rejects_inactive_user(db, client):
    """Inactive users can hold valid tokens but must be rejected at /me."""
    user = await create_user(db, is_active=False)
    await db.commit()

    token = create_access_token(user)
    resp = await client.get(
        "/api/users/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 401


# ── Token version / revocation ────────────────────────────────────────────────

async def test_token_version_mismatch_rejected(db, client):
    """
    An access token issued before a version bump must be rejected even if it
    has not expired.  This is the revocation mechanism: logout, password change,
    and forced re-auth all increment token_version.
    """
    user = await create_user(db, email="alice@example.com")
    await db.commit()

    # Token issued now (version=0)
    old_token = create_access_token(user)

    # Simulate a logout / forced re-auth: bump token_version
    user.token_version += 1
    await db.commit()

    resp = await client.get(
        "/api/users/me",
        headers={"Authorization": f"Bearer {old_token}"},
    )
    assert resp.status_code == 401


async def test_refresh_rotation_invalidates_old_refresh_token(db, client):
    """
    After a successful /refresh call, the old refresh token must be rejected.
    Prevents a stolen refresh token from being replayed after one legitimate use.
    """
    user = await create_user(db, email="bob@example.com")
    await db.commit()

    old_refresh = create_refresh_token(user)

    # First refresh — succeeds and rotates the token version
    resp1 = await client.post(
        "/api/users/refresh",
        json={"refresh_token": old_refresh},
    )
    assert resp1.status_code == 200

    # Second refresh with the same token — must be rejected
    resp2 = await client.post(
        "/api/users/refresh",
        json={"refresh_token": old_refresh},
    )
    assert resp2.status_code == 401, (
        "Old refresh token must be invalid after rotation"
    )


async def test_refresh_new_token_works_after_rotation(db, client):
    """The new refresh token returned by /refresh must itself be usable."""
    user = await create_user(db, email="carol@example.com")
    await db.commit()

    refresh1 = create_refresh_token(user)
    resp1 = await client.post("/api/users/refresh", json={"refresh_token": refresh1})
    assert resp1.status_code == 200
    new_refresh = resp1.json()["refresh_token"]

    # New refresh token from the rotation must work
    resp2 = await client.post("/api/users/refresh", json={"refresh_token": new_refresh})
    assert resp2.status_code == 200


async def test_logout_invalidates_all_tokens(db, client):
    """
    POST /logout must increment token_version so that any previously issued
    access token (e.g. on another device) is immediately rejected.
    """
    user = await create_user(db, email="dave@example.com")
    await db.commit()

    access_token = create_access_token(user)
    headers = {"Authorization": f"Bearer {access_token}"}

    # Confirm token works before logout
    assert (await client.get("/api/users/me", headers=headers)).status_code == 200

    # Logout
    resp_logout = await client.post("/api/users/logout", headers=headers)
    assert resp_logout.status_code == 204

    # Same token must now be rejected
    resp_after = await client.get("/api/users/me", headers=headers)
    assert resp_after.status_code == 401, (
        "Token must be invalid after logout"
    )


# ── Account lockout ───────────────────────────────────────────────────────────

async def test_account_locks_after_five_failures(db, client):
    """
    Five consecutive wrong-password attempts must lock the account and return
    423 on the next attempt (even with the correct password).
    """
    await create_user(db, email="eve@example.com", password="CorrectHorse1!")
    await db.commit()

    for i in range(5):
        resp = await client.post(
            "/api/users/login",
            json={"email": "eve@example.com", "password": "WrongPass999!"},
        )
        assert resp.status_code == 401, f"Attempt {i+1} should return 401"

    # 6th attempt — account should now be locked
    resp = await client.post(
        "/api/users/login",
        json={"email": "eve@example.com", "password": "CorrectHorse1!"},
    )
    assert resp.status_code == 423, "Account must be locked after 5 failures"


async def test_locked_account_rejects_correct_password(db, client):
    """
    A pre-locked account must return 423 regardless of whether the password
    is correct.  The lockout expires naturally after the lockout window.
    """
    from datetime import timedelta
    from app.models import User as UserModel
    from sqlalchemy import select as sa_select

    user = await create_user(db, email="frank@example.com", password="RightPass123!")
    # Directly set locked_until to future to simulate a locked state
    user.failed_login_attempts = 5
    from datetime import datetime, timezone
    user.locked_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    await db.commit()

    resp = await client.post(
        "/api/users/login",
        json={"email": "frank@example.com", "password": "RightPass123!"},
    )
    assert resp.status_code == 423


async def test_lockout_clears_on_expired_lockout(db, client):
    """
    When locked_until is in the past the account should accept valid credentials.
    """
    from datetime import timedelta, datetime, timezone
    user = await create_user(db, email="grace@example.com", password="StrongPass1!")
    # Set lockout in the past — should be ignored
    user.failed_login_attempts = 5
    user.locked_until = datetime.now(timezone.utc) - timedelta(minutes=1)
    await db.commit()

    resp = await client.post(
        "/api/users/login",
        json={"email": "grace@example.com", "password": "StrongPass1!"},
    )
    assert resp.status_code == 200, "Expired lockout must not block login"


async def test_failed_attempts_reset_on_success(db, client):
    """Successful login resets the failed_login_attempts counter."""
    from sqlalchemy import select as sa_select
    from app.models import User as UserModel

    user = await create_user(db, email="hank@example.com", password="GoodPass123!")
    user.failed_login_attempts = 3  # 3 previous failures, not yet locked
    await db.commit()

    resp = await client.post(
        "/api/users/login",
        json={"email": "hank@example.com", "password": "GoodPass123!"},
    )
    assert resp.status_code == 200

    await db.refresh(user)
    assert user.failed_login_attempts == 0, "Counter must reset after successful login"

    # Release the NullPool connection while still in the function event loop
    # (db.refresh opened a new connection after the HTTP endpoint committed).
    await db.rollback()
