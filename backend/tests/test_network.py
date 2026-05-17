"""
test_network.py — Referral network graph endpoints.

Covers:
  - Auth requirement on all three endpoints
  - 404 for unknown NPI
  - State-access enforcement (CA analyst cannot view TX network)
  - Response shape (nodes / edges / stats)
  - Search result restriction by jurisdiction
"""
from tests.factories import create_provider, create_referral_edge, create_user
from app.auth import create_access_token


# ── Auth guard ────────────────────────────────────────────────────────────────

async def test_network_search_requires_auth(client):
    resp = await client.get("/api/network/search?q=Smith")
    assert resp.status_code == 403


async def test_network_1hop_requires_auth(client):
    resp = await client.get("/api/network/1234567890")
    assert resp.status_code == 403


async def test_network_2hop_requires_auth(client):
    resp = await client.get("/api/network/1234567890/2hop")
    assert resp.status_code == 403


# ── 404 handling ──────────────────────────────────────────────────────────────

async def test_network_1hop_unknown_npi(client, admin_headers):
    resp = await client.get("/api/network/0000000000", headers=admin_headers)
    assert resp.status_code == 404


async def test_network_2hop_unknown_npi(client, admin_headers):
    resp = await client.get("/api/network/0000000000/2hop", headers=admin_headers)
    assert resp.status_code == 404


# ── Response shape ────────────────────────────────────────────────────────────

async def test_network_1hop_response_shape(db, client, admin_headers):
    """1-hop response includes center node, neighbor nodes, and edges."""
    p1 = await create_provider(db, npi="4001111111", state="CA")
    p2 = await create_provider(db, npi="4002222222", state="CA")
    await create_referral_edge(db, source_npi="4001111111", target_npi="4002222222",
                               referral_count=100, shared_patients=60)
    await db.commit()

    resp = await client.get("/api/network/4001111111", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert "nodes" in body
    assert "edges" in body
    assert "stats" in body
    assert body["center_npi"] == "4001111111"

    npis = {n["npi"] for n in body["nodes"]}
    assert "4001111111" in npis   # center
    assert "4002222222" in npis   # neighbor

    center_node = next(n for n in body["nodes"] if n["npi"] == "4001111111")
    assert center_node["is_center"] is True

    assert len(body["edges"]) == 1
    assert body["stats"]["total_nodes"] == 2
    assert body["stats"]["total_edges"] == 1


async def test_network_2hop_response_shape(db, client, admin_headers):
    """2-hop response includes nodes up to max_nodes and the hop stats."""
    await create_provider(db, npi="4003333333", state="CA")
    await create_provider(db, npi="4004444444", state="CA")
    await create_provider(db, npi="4005555555", state="CA")
    await create_referral_edge(db, source_npi="4003333333", target_npi="4004444444")
    await create_referral_edge(db, source_npi="4004444444", target_npi="4005555555")
    await db.commit()

    resp = await client.get("/api/network/4003333333/2hop", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert "nodes" in body
    assert "edges" in body
    assert "stats" in body
    assert "hop1_count" in body["stats"]
    assert "hop2_count" in body["stats"]


async def test_network_search_response_shape(db, client, admin_headers):
    """Search returns a list of provider node objects."""
    await create_provider(db, npi="4006666666", name_last="Turner", state="CA")
    await db.commit()

    resp = await client.get("/api/network/search?q=Turner", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
    assert len(body) >= 1
    node = body[0]
    for key in ("npi", "name", "specialty", "state", "risk_score", "is_excluded"):
        assert key in node, f"Missing key in search result: {key}"


# ── State-access enforcement ──────────────────────────────────────────────────

async def test_network_1hop_state_access_blocks_out_of_jurisdiction(db, client):
    """
    A CA-scoped analyst must receive 403 when requesting the 1-hop network of
    a TX provider.  Previously the endpoint used `_user=Depends(get_current_user)`
    and ignored state_access entirely.
    """
    await create_provider(db, npi="4007777777", state="TX")
    ca_analyst = await create_user(db, role="analyst", state_access=["CA"])
    await db.commit()

    headers = {"Authorization": f"Bearer {create_access_token(ca_analyst)}"}
    resp = await client.get("/api/network/4007777777", headers=headers)
    assert resp.status_code == 403


async def test_network_2hop_state_access_blocks_out_of_jurisdiction(db, client):
    """CA-scoped analyst must receive 403 for a TX center provider's 2-hop graph."""
    await create_provider(db, npi="4008888888", state="TX")
    ca_analyst = await create_user(db, role="analyst", state_access=["CA"])
    await db.commit()

    headers = {"Authorization": f"Bearer {create_access_token(ca_analyst)}"}
    resp = await client.get("/api/network/4008888888/2hop", headers=headers)
    assert resp.status_code == 403


async def test_network_1hop_state_access_allows_own_jurisdiction(db, client):
    """CA-scoped analyst can view a CA provider's network."""
    await create_provider(db, npi="4009999999", state="CA")
    ca_analyst = await create_user(db, role="analyst", state_access=["CA"])
    await db.commit()

    headers = {"Authorization": f"Bearer {create_access_token(ca_analyst)}"}
    resp = await client.get("/api/network/4009999999", headers=headers)
    assert resp.status_code == 200


async def test_network_search_state_access_filters_results(db, client):
    """
    Search results must be restricted to the user's allowed states.
    A CA-scoped analyst searching for 'Rivera' must not see the TX provider
    even if the TX provider's name matches better.
    """
    await create_provider(db, npi="4010000001", name_last="Rivera", state="CA",
                          risk_score=50.0)
    await create_provider(db, npi="4010000002", name_last="Rivera", state="TX",
                          risk_score=99.0)  # higher risk but wrong state
    ca_analyst = await create_user(db, role="analyst", state_access=["CA"])
    await db.commit()

    headers = {"Authorization": f"Bearer {create_access_token(ca_analyst)}"}
    resp = await client.get("/api/network/search?q=Rivera", headers=headers)
    assert resp.status_code == 200
    npis = {p["npi"] for p in resp.json()}
    assert "4010000001" in npis,     "CA provider must appear in CA analyst's search"
    assert "4010000002" not in npis, "TX provider must not appear in CA analyst's search"


async def test_network_search_unrestricted_admin_sees_all_states(db, client, admin_headers):
    """Admins with no state_access restriction see results from every state."""
    await create_provider(db, npi="4011000001", name_last="Nguyen", state="CA")
    await create_provider(db, npi="4011000002", name_last="Nguyen", state="TX")
    await db.commit()

    resp = await client.get("/api/network/search?q=Nguyen", headers=admin_headers)
    assert resp.status_code == 200
    npis = {p["npi"] for p in resp.json()}
    assert "4011000001" in npis
    assert "4011000002" in npis
