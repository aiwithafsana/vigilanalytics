"""
test_dashboard.py — Dashboard endpoint structure and state-access scoping.
"""
from tests.factories import create_provider, create_fraud_flag, create_user
from app.auth import create_access_token


async def test_dashboard_requires_auth(client):
    """Dashboard is not public."""
    resp = await client.get("/api/dashboard")
    assert resp.status_code == 403


async def test_dashboard_response_shape(db, client, admin_headers):
    """Dashboard returns the expected top-level keys."""
    resp = await client.get("/api/dashboard", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert "stats" in body
    assert "risk_distribution" in body
    assert "top_providers" in body
    assert "recent_cases" in body
    # stats sub-keys
    stats = body["stats"]
    for key in ("total_providers", "total_payment", "leie_matches",
                "open_cases", "high_risk_providers", "states_covered", "new_leads"):
        assert key in stats, f"Missing stats key: {key}"


async def test_dashboard_state_user_sees_restricted_count(db, client):
    """State-restricted user's stats reflect only their allowed states."""
    await create_provider(db, npi="1111111111", state="CA", risk_score=80.0)
    await create_provider(db, npi="2222222222", state="TX", risk_score=85.0)
    ca_user = await create_user(db, role="analyst", state_access=["CA"])
    await db.commit()

    token = create_access_token(ca_user)
    headers = {"Authorization": f"Bearer {token}"}

    resp = await client.get("/api/dashboard", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    # The state-restricted user should see 1 provider (CA only)
    assert body["stats"]["total_providers"] == 1


async def test_dashboard_top_providers_excludes_leie(db, client):
    """
    LEIE-excluded providers must never appear in top_providers even if
    their risk_score is the highest in their state.

    Regression: previously excluded providers always ranked first because
    the ML scoring floor (85.0) guaranteed they beat genuine new leads.
    The dashboard query must filter is_excluded = FALSE so analysts see
    investigation opportunities, not already-caught criminals.
    """
    leie_npi   = "3111111111"
    active_npi = "3222222222"

    await create_provider(db, npi=leie_npi,   state="CA", risk_score=95.0, is_excluded=True)
    await create_provider(db, npi=active_npi, state="CA", risk_score=80.0, is_excluded=False)
    ca_user = await create_user(db, role="analyst", state_access=["CA"])
    await db.commit()

    token = create_access_token(ca_user)
    headers = {"Authorization": f"Bearer {token}"}

    resp = await client.get("/api/dashboard", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    npis = [p["npi"] for p in body["top_providers"]]
    assert leie_npi   not in npis, "Excluded provider must not appear in top_providers"
    assert active_npi in     npis, "Non-excluded provider must appear in top_providers"


async def test_dashboard_top_leads_excludes_leie(db, client):
    """
    Top leads (top_leads) must never contain LEIE-excluded providers.

    top_leads comes from _get_top_leads() which queries fraud_flags joined to
    providers.  The SQL must filter AND p.is_excluded = FALSE so the list
    surfaces new investigation targets, not already-excluded billing activity.
    """
    leie_npi   = "3333333331"
    active_npi = "3333333332"

    excluded_provider = await create_provider(
        db, npi=leie_npi, state="CA", risk_score=95.0, is_excluded=True
    )
    active_provider = await create_provider(
        db, npi=active_npi, state="CA", risk_score=75.0, is_excluded=False
    )

    # Give the excluded provider a severity-1 (critical) flag — the most likely
    # to surface at the top if the filter is missing.
    await create_fraud_flag(db, npi=leie_npi,   severity=1, is_active=True,
                            estimated_overpayment=500_000)
    await create_fraud_flag(db, npi=active_npi, severity=1, is_active=True,
                            estimated_overpayment=400_000)
    ca_user = await create_user(db, role="analyst", state_access=["CA"])
    await db.commit()

    token = create_access_token(ca_user)
    headers = {"Authorization": f"Bearer {token}"}

    resp = await client.get("/api/dashboard", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    lead_npis = [lead["npi"] for lead in body["top_leads"]]
    assert leie_npi   not in lead_npis, "Excluded provider must not appear in top_leads"
    assert active_npi in     lead_npis, "Non-excluded provider must appear in top_leads"
