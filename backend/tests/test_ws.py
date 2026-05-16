"""
test_ws.py — WebSocket endpoint and alert-fetching logic.

WebSocket handshake testing requires a synchronous TestClient (starlette).
The `_fetch_new_alerts` helper is tested directly as an async function so
we can cover the state-access filtering and message-shape logic without
standing up a real WebSocket connection per test.
"""
from datetime import datetime, timedelta, timezone

import pytest

from tests.factories import create_fraud_flag, create_provider, create_user
from app.routers.ws import _fetch_new_alerts
# Use the NullPool test session factory, NOT the app's pooled AsyncSessionLocal.
# Passing AsyncSessionLocal (pooled) while the db fixture also holds a NullPool
# connection to the same DB causes asyncpg "another operation is in progress".
from tests.conftest import _TestSession as _session_factory


# ── _fetch_new_alerts unit tests (no WS handshake needed) ────────────────────

async def test_fetch_alerts_returns_new_flags(db):
    """Flags created after `since` must be returned."""
    await create_provider(db, npi="5001111111", state="CA")
    await create_fraud_flag(db, npi="5001111111", severity=1, is_active=True)
    user = await create_user(db, role="analyst", state_access=[])
    await db.commit()

    since = datetime.now(timezone.utc) - timedelta(minutes=10)
    alerts = await _fetch_new_alerts(_session_factory, user, since)
    npis = [a["npi"] for a in alerts]
    assert "5001111111" in npis


async def test_fetch_alerts_excludes_old_flags(db):
    """Flags created before `since` must NOT be returned."""
    await create_provider(db, npi="5002222222", state="CA")
    await create_fraud_flag(db, npi="5002222222", severity=1, is_active=True)
    user = await create_user(db, role="analyst", state_access=[])
    await db.commit()

    # `since` is in the future — no flags should be newer than this
    since = datetime.now(timezone.utc) + timedelta(minutes=5)
    alerts = await _fetch_new_alerts(_session_factory, user, since)
    npis = [a["npi"] for a in alerts]
    assert "5002222222" not in npis


async def test_fetch_alerts_respects_state_access(db):
    """
    State-scoped users must only receive alerts for their allowed states.
    A CA analyst must not see TX flags even if they are newer and higher severity.
    """
    await create_provider(db, npi="5003333333", state="CA")
    await create_provider(db, npi="5004444444", state="TX")
    await create_fraud_flag(db, npi="5003333333", severity=1, is_active=True)
    await create_fraud_flag(db, npi="5004444444", severity=1, is_active=True)
    ca_user = await create_user(db, role="analyst", state_access=["CA"])
    await db.commit()

    since = datetime.now(timezone.utc) - timedelta(minutes=10)
    alerts = await _fetch_new_alerts(_session_factory, ca_user, since)
    npis = {a["npi"] for a in alerts}
    assert "5003333333" in  npis, "CA flag must appear for CA analyst"
    assert "5004444444" not in npis, "TX flag must not appear for CA analyst"


async def test_fetch_alerts_unrestricted_user_sees_all_states(db):
    """Unrestricted users (empty state_access) receive alerts from all states."""
    await create_provider(db, npi="5005555555", state="CA")
    await create_provider(db, npi="5006666666", state="TX")
    await create_fraud_flag(db, npi="5005555555", severity=2, is_active=True)
    await create_fraud_flag(db, npi="5006666666", severity=2, is_active=True)
    admin = await create_user(db, role="admin", state_access=[])
    await db.commit()

    since = datetime.now(timezone.utc) - timedelta(minutes=10)
    alerts = await _fetch_new_alerts(_session_factory, admin, since)
    npis = {a["npi"] for a in alerts}
    assert "5005555555" in npis
    assert "5006666666" in npis


async def test_fetch_alerts_excludes_inactive_flags(db):
    """Inactive flags must never appear in alerts."""
    await create_provider(db, npi="5007777777", state="CA")
    await create_fraud_flag(db, npi="5007777777", severity=1, is_active=False)
    user = await create_user(db, role="analyst", state_access=[])
    await db.commit()

    since = datetime.now(timezone.utc) - timedelta(minutes=10)
    alerts = await _fetch_new_alerts(_session_factory, user, since)
    npis = [a["npi"] for a in alerts]
    assert "5007777777" not in npis


async def test_fetch_alerts_excludes_low_severity(db):
    """Only severity 1 (critical) and 2 (high) flags are sent over the wire."""
    await create_provider(db, npi="5008888888", state="CA")
    await create_fraud_flag(db, npi="5008888888", severity=3, is_active=True)   # medium
    user = await create_user(db, role="analyst", state_access=[])
    await db.commit()

    since = datetime.now(timezone.utc) - timedelta(minutes=10)
    alerts = await _fetch_new_alerts(_session_factory, user, since)
    npis = [a["npi"] for a in alerts]
    assert "5008888888" not in npis, "Severity-3 (medium) flag must not be pushed as alert"


async def test_fetch_alerts_message_shape(db):
    """Every alert dict must contain the required fields for the frontend."""
    await create_provider(db, npi="5009999999", state="CA", risk_score=85.0)
    await create_fraud_flag(db, npi="5009999999", severity=1, is_active=True,
                            estimated_overpayment=250_000)
    user = await create_user(db, role="analyst", state_access=[])
    await db.commit()

    since = datetime.now(timezone.utc) - timedelta(minutes=10)
    alerts = await _fetch_new_alerts(_session_factory, user, since)
    assert len(alerts) >= 1
    a = next(x for x in alerts if x["npi"] == "5009999999")
    for key in ("flag_id", "npi", "provider_name", "specialty", "state",
                "risk_score", "flag_type", "severity", "explanation",
                "estimated_overpayment", "created_at"):
        assert key in a, f"Alert missing required field: {key}"


# ── WS ticket endpoint ───────────────────────────────────────────────────────

async def test_ws_ticket_requires_auth(client):
    """GET /api/ws/ticket must require a valid access token."""
    resp = await client.get("/api/ws/ticket")
    assert resp.status_code == 403


async def test_ws_ticket_issued_to_authenticated_user(db, client):
    """Authenticated users get a short-lived ticket with the right shape."""
    from app.auth import create_access_token
    user = await create_user(db, role="analyst", state_access=[])
    await db.commit()

    headers = {"Authorization": f"Bearer {create_access_token(user)}"}
    resp = await client.get("/api/ws/ticket", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert "ticket" in body
    assert body["expires_in"] == 30


async def test_ws_ticket_is_short_lived(db, client):
    """The ticket JWT must expire in ≤30 seconds (not 60 minutes like an access token)."""
    from app.auth import create_access_token, decode_token
    from datetime import datetime, timezone
    user = await create_user(db, role="analyst", state_access=[])
    await db.commit()

    headers = {"Authorization": f"Bearer {create_access_token(user)}"}
    resp = await client.get("/api/ws/ticket", headers=headers)
    assert resp.status_code == 200

    ticket = resp.json()["ticket"]
    payload = decode_token(ticket)
    assert payload["type"] == "ws_ticket"

    exp = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
    now = datetime.now(timezone.utc)
    ttl = (exp - now).total_seconds()
    assert ttl <= 31, f"Ticket must expire in ≤30 s; got {ttl:.1f}s"
    assert ttl > 0,   "Ticket must not already be expired"


async def test_ws_ticket_contains_token_version(db, client):
    """The ticket must embed the user's current token_version for revocation checks."""
    from app.auth import create_access_token, decode_token
    user = await create_user(db, role="analyst", state_access=[])
    await db.commit()

    headers = {"Authorization": f"Bearer {create_access_token(user)}"}
    resp = await client.get("/api/ws/ticket", headers=headers)
    assert resp.status_code == 200

    payload = decode_token(resp.json()["ticket"])
    assert "ver" in payload
    assert payload["ver"] == user.token_version


# ── WebSocket auth rejection ──────────────────────────────────────────────────

def test_ws_rejects_missing_token():
    """
    Connecting without a token query param must close the connection immediately.
    Uses the synchronous Starlette TestClient which supports WebSocket.
    """
    from starlette.testclient import TestClient
    from app.main import app

    with TestClient(app) as tc:
        with pytest.raises(Exception):
            # No token param — server should reject (close with 4001 or HTTP 422)
            with tc.websocket_connect("/api/ws"):
                pass


def test_ws_rejects_garbage_token():
    """Garbage JWT must cause a 4001 close."""
    from starlette.testclient import TestClient
    from app.main import app

    with TestClient(app) as tc:
        with pytest.raises(Exception):
            with tc.websocket_connect("/api/ws?token=not-a-real-jwt"):
                pass
