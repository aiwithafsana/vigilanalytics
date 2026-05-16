"""
test_providers.py — Provider list, filters, detail, billing, flags, and state-access enforcement.
"""
import pytest
from sqlalchemy import select

from app.models import Provider
from tests.factories import create_billing_record, create_fraud_flag, create_provider, create_user
from app.auth import create_access_token


# ── List providers ────────────────────────────────────────────────────────────

async def test_list_providers_requires_auth(client):
    """Unauthenticated requests are rejected."""
    resp = await client.get("/api/providers")
    assert resp.status_code == 403


async def test_list_providers_returns_paginated(db, client, admin_headers):
    """List returns items + pagination metadata."""
    await create_provider(db, npi="1111111111", state="CA")
    await create_provider(db, npi="2222222222", state="TX")
    await db.commit()

    resp = await client.get("/api/providers", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert len(body["items"]) == 2
    assert body["page"] == 1


async def test_list_providers_filter_by_state(db, client, admin_headers):
    """state= query param filters by provider state."""
    await create_provider(db, npi="1111111111", state="CA")
    await create_provider(db, npi="2222222222", state="TX")
    await db.commit()

    resp = await client.get("/api/providers?state=CA", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["state"] == "CA"


async def test_list_providers_filter_by_min_risk(db, client, admin_headers):
    """min_risk= excludes providers below the threshold."""
    await create_provider(db, npi="1111111111", risk_score=90.0)
    await create_provider(db, npi="2222222222", risk_score=30.0)
    await db.commit()

    resp = await client.get("/api/providers?min_risk=70", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert float(body["items"][0]["risk_score"]) >= 70


async def test_list_providers_search(db, client, admin_headers):
    """search= filters by provider last name."""
    await create_provider(db, npi="1111111111", name_last="Johnson")
    await create_provider(db, npi="2222222222", name_last="Martinez")
    await db.commit()

    resp = await client.get("/api/providers?q=Johnson", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["name_last"] == "Johnson"


# ── State-access enforcement ──────────────────────────────────────────────────

async def test_state_access_restricts_list(db, client):
    """Users with state_access only see providers in their allowed states."""
    await create_provider(db, npi="1111111111", state="CA")
    await create_provider(db, npi="2222222222", state="TX")
    state_user = await create_user(db, role="analyst", state_access=["CA"])
    await db.commit()

    token = create_access_token(state_user)
    headers = {"Authorization": f"Bearer {token}"}

    resp = await client.get("/api/providers", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["state"] == "CA"


async def test_unrestricted_user_sees_all_states(db, client, admin_headers):
    """Users with empty state_access see providers from all states."""
    await create_provider(db, npi="1111111111", state="CA")
    await create_provider(db, npi="2222222222", state="TX")
    await create_provider(db, npi="3333333333", state="NY")
    await db.commit()

    resp = await client.get("/api/providers", headers=admin_headers)
    assert resp.status_code == 200
    assert resp.json()["total"] == 3


# ── Provider detail ───────────────────────────────────────────────────────────

async def test_get_provider_detail(db, client, admin_headers):
    """GET /providers/{npi} returns full detail for a known NPI."""
    await create_provider(db, npi="9876543210", name_last="Williams", risk_score=85.0)
    await db.commit()

    resp = await client.get("/api/providers/9876543210", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["npi"] == "9876543210"
    assert body["name_last"] == "Williams"
    assert float(body["risk_score"]) == 85.0


async def test_get_provider_detail_not_found(client, admin_headers):
    """Unknown NPI returns 404."""
    resp = await client.get("/api/providers/0000000000", headers=admin_headers)
    assert resp.status_code == 404


# ── Billing records ───────────────────────────────────────────────────────────

async def test_get_provider_billing(db, client, admin_headers):
    """GET /providers/{npi}/billing returns HCPCS billing rows."""
    await create_provider(db, npi="1234567890")
    await create_billing_record(db, npi="1234567890", hcpcs_code="99213", year=2022)
    await create_billing_record(db, npi="1234567890", hcpcs_code="99214", year=2022)
    await db.commit()

    resp = await client.get("/api/providers/1234567890/billing", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 2
    hcpcs_codes = {r["hcpcs_code"] for r in body}
    assert hcpcs_codes == {"99213", "99214"}


async def test_get_provider_billing_empty(db, client, admin_headers):
    """Provider with no billing records returns an empty list (not 404)."""
    await create_provider(db, npi="1234567890")
    await db.commit()

    resp = await client.get("/api/providers/1234567890/billing", headers=admin_headers)
    assert resp.status_code == 200
    assert resp.json() == []


# ── Fraud flags ───────────────────────────────────────────────────────────────

async def test_get_provider_flags(db, client, admin_headers):
    """GET /providers/{npi}/flags returns active fraud signals."""
    await create_provider(db, npi="1234567890")
    await create_fraud_flag(db, npi="1234567890", flag_type="billing_volume", severity=1)
    await create_fraud_flag(db, npi="1234567890", flag_type="upcoding", severity=2)
    # Inactive flag — must not appear
    await create_fraud_flag(db, npi="1234567890", flag_type="referral_cluster", is_active=False)
    await db.commit()

    resp = await client.get("/api/providers/1234567890/flags", headers=admin_headers)
    assert resp.status_code == 200
    flags = resp.json()
    assert len(flags) == 2
    flag_types = {f["flag_type"] for f in flags}
    assert flag_types == {"billing_volume", "upcoding"}


async def test_flags_ordered_by_severity(db, client, admin_headers):
    """Flags are returned ordered critical-first (severity=1 before severity=2)."""
    await create_provider(db, npi="1234567890")
    await create_fraud_flag(db, npi="1234567890", severity=3)
    await create_fraud_flag(db, npi="1234567890", severity=1)
    await create_fraud_flag(db, npi="1234567890", severity=2)
    await db.commit()

    resp = await client.get("/api/providers/1234567890/flags", headers=admin_headers)
    assert resp.status_code == 200
    flags = resp.json()
    severities = [f["severity"] for f in flags]
    assert severities == sorted(severities)   # ascending: 1, 2, 3
