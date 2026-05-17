"""
test_audit_timeline.py — /api/audit/timeline returns per-target audit history.
"""
import uuid

from app.models import AuditLog
from tests.factories import create_provider, create_user


async def _seed_logs(db, user_id, npi, count=3):
    for i in range(count):
        db.add(AuditLog(
            user_id=user_id,
            action=f"test_action_{i}",
            target_type="provider",
            target_id=npi,
            details={"i": i},
        ))
    await db.flush()


async def test_timeline_returns_target_specific_logs(db, client, analyst_headers, analyst_user):
    """Timeline returns only logs matching target_type + target_id."""
    p1 = await create_provider(db, npi="1234567890")
    p2 = await create_provider(db, npi="9876543210")

    await _seed_logs(db, analyst_user.id, p1.npi, count=3)
    await _seed_logs(db, analyst_user.id, p2.npi, count=2)
    await db.commit()

    resp = await client.get(
        f"/api/audit/timeline?target_type=provider&target_id={p1.npi}",
        headers=analyst_headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    # Only logs for p1 (not p2)
    assert all(item["target_id"] == p1.npi for item in body["items"])
    actions = {item["action"] for item in body["items"]}
    assert actions == {"test_action_0", "test_action_1", "test_action_2"}


async def test_timeline_empty_for_unknown_target(client, analyst_headers):
    resp = await client.get(
        "/api/audit/timeline?target_type=provider&target_id=0000000000",
        headers=analyst_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["total"] == 0


async def test_timeline_requires_authentication(client):
    resp = await client.get(
        "/api/audit/timeline?target_type=provider&target_id=1234567890",
    )
    assert resp.status_code in (401, 403)


async def test_timeline_invalid_target_type_rejected(client, analyst_headers):
    resp = await client.get(
        "/api/audit/timeline?target_type=garbage&target_id=1234567890",
        headers=analyst_headers,
    )
    assert resp.status_code == 422


async def test_timeline_includes_user_name(db, client, analyst_headers, analyst_user):
    p = await create_provider(db, npi="1111111111")
    await _seed_logs(db, analyst_user.id, p.npi, count=1)
    await db.commit()

    resp = await client.get(
        f"/api/audit/timeline?target_type=provider&target_id={p.npi}",
        headers=analyst_headers,
    )
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert len(items) == 1
    assert items[0].get("user_name") is not None
