"""
test_case_watch.py — Case-watch service correctness.

Tests the delta-detection logic that powers the "what's new since last
check" digest.  The agent dispatch + persistence is exercised by the
existing agent tests; here we focus on:

  1. Finding-key stability (same finding, two runs → "same")
  2. Delta computation (new finding appears → flagged as new)
  3. Per-user filtering (one user's cases don't show in another's digest)
"""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from app.models import AgentRun, Case
from app.services.case_watch import _finding_key, get_user_digest
from tests.factories import create_provider, create_user


# ── Pure-function tests ──────────────────────────────────────────────────────

def test_finding_key_treats_same_event_as_identical():
    """Severity wording can change between runs without indicating a new event."""
    a = {"source": "OIG", "title": "Indictment of Dr Smith", "url": "https://...",
         "date": "2026-04-12", "severity": "high",   "summary": "Old summary text"}
    b = {"source": "OIG", "title": "Indictment of Dr Smith", "url": "https://...",
         "date": "2026-04-12", "severity": "critical", "summary": "Updated wording"}
    assert _finding_key(a) == _finding_key(b)


def test_finding_key_treats_different_events_differently():
    base = {"source": "OIG", "title": "Indictment", "url": "u", "date": "2026-04-12"}
    assert _finding_key(base) != _finding_key({**base, "url": "u2"})
    assert _finding_key(base) != _finding_key({**base, "title": "Civil case"})
    assert _finding_key(base) != _finding_key({**base, "date": "2026-04-13"})
    assert _finding_key(base) != _finding_key({**base, "source": "SAM.gov"})


def test_finding_key_handles_missing_fields():
    """A finding with null URL/date shouldn't crash; should still de-dup correctly."""
    a = {"source": "OIG", "title": "X", "url": None, "date": None}
    b = {"source": "OIG", "title": "X", "url": None, "date": None}
    assert _finding_key(a) == _finding_key(b)


# ── Integration test against the DB ──────────────────────────────────────────

async def test_user_digest_surfaces_only_owned_cases_with_new_findings(db):
    """
    Setup: two users.  User A has a case with a new finding; User B has a
    case with no new findings (the same finding existed in the prior run).
    Expectation: A's digest shows the update; B's digest is empty.
    """
    alice = await create_user(db, email="alice@watch.test")
    bob   = await create_user(db, email="bob@watch.test")
    npi_a = "1234567890"
    npi_b = "9876543210"
    await create_provider(db, npi=npi_a, state="CA")
    await create_provider(db, npi=npi_b, state="CA")
    await db.commit()

    case_a = Case(case_number="C-001", provider_npi=npi_a, state="CA",
                  title="Test case A", status="open",
                  assigned_to=alice.id, created_by=alice.id)
    case_b = Case(case_number="C-002", provider_npi=npi_b, state="CA",
                  title="Test case B", status="open",
                  assigned_to=bob.id, created_by=bob.id)
    db.add_all([case_a, case_b])
    await db.commit()

    now = datetime.now(timezone.utc)
    finding_old = {"source": "OIG", "title": "Old finding",
                   "url": "https://x", "date": "2026-01-01", "severity": "low",
                   "summary": "..."}
    finding_new = {"source": "OIG", "title": "BRAND NEW finding",
                   "url": "https://y", "date": "2026-04-01", "severity": "high",
                   "summary": "..."}

    # User A: prior run with [old], latest run with [old, new] → new is "new"
    db.add(AgentRun(
        workflow="public_records", target_type="provider", target_id=npi_a,
        status="succeeded",
        started_at=now - timedelta(days=14), completed_at=now - timedelta(days=14),
        result_json={"findings": [finding_old]},
    ))
    db.add(AgentRun(
        workflow="public_records", target_type="provider", target_id=npi_a,
        status="succeeded",
        started_at=now - timedelta(hours=1), completed_at=now - timedelta(hours=1),
        result_json={"findings": [finding_old, finding_new]},
    ))

    # User B: prior run and latest run identical → no delta
    db.add(AgentRun(
        workflow="public_records", target_type="provider", target_id=npi_b,
        status="succeeded",
        started_at=now - timedelta(days=14), completed_at=now - timedelta(days=14),
        result_json={"findings": [finding_old]},
    ))
    db.add(AgentRun(
        workflow="public_records", target_type="provider", target_id=npi_b,
        status="succeeded",
        started_at=now - timedelta(hours=1), completed_at=now - timedelta(hours=1),
        result_json={"findings": [finding_old]},
    ))
    await db.commit()

    alice_digest = await get_user_digest(db, alice.id, since_hours=24)
    bob_digest   = await get_user_digest(db, bob.id,   since_hours=24)

    # Alice sees her one update with one new finding
    assert alice_digest["n_open_cases"]         == 1
    assert alice_digest["n_cases_with_updates"] == 1
    assert len(alice_digest["updates"])         == 1
    assert alice_digest["updates"][0]["n_new_findings"] == 1
    assert alice_digest["updates"][0]["new_findings"][0]["title"] == "BRAND NEW finding"

    # Bob has an open case but no new findings → empty update list
    assert bob_digest["n_open_cases"]         == 1
    assert bob_digest["n_cases_with_updates"] == 0
    assert bob_digest["updates"]              == []
    # Release the NullPool connection while still in the function event loop
    await db.rollback()


async def test_user_digest_empty_when_no_cases(db):
    user = await create_user(db, email="nocases@watch.test")
    await db.commit()
    digest = await get_user_digest(db, user.id)
    assert digest["n_open_cases"] == 0
    assert digest["n_cases_with_updates"] == 0
    assert digest["updates"] == []
    await db.rollback()


# ── API endpoint smoke tests ──────────────────────────────────────────────────

async def test_get_watch_digest_endpoint_requires_auth(client):
    resp = await client.get("/api/cases/watch-digest")
    assert resp.status_code in (401, 403)


async def test_get_watch_digest_endpoint_returns_empty_for_new_user(
    client, analyst_headers,
):
    """A user with no cases should get a well-formed empty digest, not 500."""
    resp = await client.get("/api/cases/watch-digest", headers=analyst_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["n_open_cases"] == 0
    assert body["n_cases_with_updates"] == 0
    assert body["updates"] == []


async def test_manual_sweep_requires_admin(client, analyst_headers):
    """Non-admins can't trigger the sweep — would otherwise burn API quota."""
    resp = await client.post(
        "/api/cases/watch-digest/refresh", headers=analyst_headers,
    )
    assert resp.status_code == 403
