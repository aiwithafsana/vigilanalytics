"""
test_alerts.py — Alert feed: auth, severity filtering, and state-access scoping.
"""
from tests.factories import create_fraud_flag, create_provider, create_user
from app.auth import create_access_token


async def test_alerts_requires_auth(client):
    """Alert feed is not public."""
    resp = await client.get("/api/alerts")
    assert resp.status_code == 403


async def test_alerts_response_shape(db, client, admin_headers):
    """Response contains items list and total count."""
    resp = await client.get("/api/alerts", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert "items" in body
    assert "total" in body


async def test_alerts_severity_filter(db, client, admin_headers):
    """severity= query param restricts returned flags."""
    await create_provider(db, npi="1111111111", state="CA")
    await create_fraud_flag(db, npi="1111111111", severity=1)   # critical
    await create_fraud_flag(db, npi="1111111111", severity=2)   # high
    await create_fraud_flag(db, npi="1111111111", severity=3)   # medium
    await db.commit()

    resp = await client.get("/api/alerts?severity=1", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    # The alerts endpoint filters by exact severity match, so only severity=1 (critical)
    assert all(item["severity"] == 1 for item in body["items"])


async def test_alerts_state_access_scoping(db, client):
    """State-restricted users only see flags for providers in their state(s)."""
    await create_provider(db, npi="1111111111", state="CA")
    await create_provider(db, npi="2222222222", state="TX")
    await create_fraud_flag(db, npi="1111111111", severity=1)   # CA — should appear
    await create_fraud_flag(db, npi="2222222222", severity=1)   # TX — must not appear
    ca_user = await create_user(db, role="analyst", state_access=["CA"])
    await db.commit()

    token = create_access_token(ca_user)
    headers = {"Authorization": f"Bearer {token}"}

    resp = await client.get("/api/alerts", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    npis = {item["npi"] for item in body["items"]}
    assert "1111111111" in npis
    assert "2222222222" not in npis
