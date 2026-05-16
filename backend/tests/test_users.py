"""
test_users.py — User management CRUD and role enforcement.
"""
import pytest
from tests.factories import create_user


# ── List users ────────────────────────────────────────────────────────────────

async def test_list_users_admin_only(client, admin_headers, analyst_headers, viewer_headers):
    """Only admins can list all users."""
    assert (await client.get("/api/users", headers=admin_headers)).status_code == 200
    assert (await client.get("/api/users", headers=analyst_headers)).status_code == 403
    assert (await client.get("/api/users", headers=viewer_headers)).status_code == 403


async def test_list_users_returns_all(db, client, admin_headers):
    """Admin sees all active + inactive users (including themselves)."""
    await create_user(db, email="a@x.com")
    await create_user(db, email="b@x.com")
    await db.commit()

    resp = await client.get("/api/users", headers=admin_headers)
    assert resp.status_code == 200
    # admin_user from fixture + 2 created above
    emails = {u["email"] for u in resp.json()}
    assert "a@x.com" in emails
    assert "b@x.com" in emails


# ── Create user ───────────────────────────────────────────────────────────────

async def test_create_user_success(client, admin_headers):
    """Admin can create a new analyst user."""
    resp = await client.post(
        "/api/users",
        json={
            "email": "new.analyst@example.com",
            "password": "StrongPass123!",
            "name": "New Analyst",
            "role": "analyst",
            "state_access": ["CA", "TX"],
        },
        headers=admin_headers,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["email"] == "new.analyst@example.com"
    assert body["role"] == "analyst"
    assert body["state_access"] == ["CA", "TX"]
    assert "hashed_password" not in body


async def test_create_user_weak_password(client, admin_headers):
    """Passwords that fail the policy return 422."""
    resp = await client.post(
        "/api/users",
        json={
            "email": "weak@example.com",
            "password": "short",          # too short, no uppercase/digit/special
            "name": "Weak User",
            "role": "analyst",
        },
        headers=admin_headers,
    )
    assert resp.status_code == 422


async def test_create_user_duplicate_email(db, client, admin_headers):
    """Duplicate email returns 400."""
    await create_user(db, email="dup@example.com")
    await db.commit()

    resp = await client.post(
        "/api/users",
        json={
            "email": "dup@example.com",
            "password": "StrongPass123!",
            "name": "Dup User",
            "role": "viewer",
        },
        headers=admin_headers,
    )
    assert resp.status_code == 409


async def test_create_user_requires_admin(client, analyst_headers):
    """Analysts cannot create users."""
    resp = await client.post(
        "/api/users",
        json={
            "email": "new@example.com",
            "password": "StrongPass123!",
            "name": "New User",
            "role": "viewer",
        },
        headers=analyst_headers,
    )
    assert resp.status_code == 403


# ── Update user ───────────────────────────────────────────────────────────────

async def test_update_user_role(db, client, admin_headers):
    """Admin can change a user's role."""
    user = await create_user(db, role="viewer")
    await db.commit()

    resp = await client.patch(
        f"/api/users/{user.id}",
        json={"role": "analyst"},
        headers=admin_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["role"] == "analyst"


async def test_update_user_state_access(db, client, admin_headers):
    """Admin can update state_access."""
    user = await create_user(db, state_access=[])
    await db.commit()

    resp = await client.patch(
        f"/api/users/{user.id}",
        json={"state_access": ["NY", "NJ"]},
        headers=admin_headers,
    )
    assert resp.status_code == 200
    assert set(resp.json()["state_access"]) == {"NY", "NJ"}


# ── Deactivate user ───────────────────────────────────────────────────────────

async def test_deactivate_user(db, client, admin_headers):
    """Admin can deactivate a user."""
    user = await create_user(db)
    await db.commit()

    resp = await client.delete(f"/api/users/{user.id}", headers=admin_headers)
    assert resp.status_code == 204  # No Content — user soft-deleted


async def test_deactivate_requires_admin(db, client, analyst_headers):
    """Analysts cannot deactivate users."""
    user = await create_user(db)
    await db.commit()

    resp = await client.delete(f"/api/users/{user.id}", headers=analyst_headers)
    assert resp.status_code == 403
