"""
test_health.py — Liveness and readiness probes.
"""


async def test_health_returns_ok(client):
    """GET /api/health is always 200 — no auth, no DB call."""
    resp = await client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_ready_returns_200_when_db_up(client):
    """GET /api/ready should be 200 when the test DB is reachable."""
    resp = await client.get("/api/ready")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ready"
    assert body["db"] == "ok"


async def test_health_requires_no_auth(client):
    """Health endpoint must not require Authorization."""
    resp = await client.get("/api/health")
    assert resp.status_code == 200


async def test_ready_requires_no_auth(client):
    """Readiness endpoint must not require Authorization."""
    resp = await client.get("/api/ready")
    assert resp.status_code == 200
