"""
test_cases.py — Full case lifecycle: create, update, notes, outcome recording,
and the investigator feedback loop (confirmed outcome → flag confidence = 1.0).
"""
import io
import pytest
from decimal import Decimal
from sqlalchemy import select

from app.models import AuditLog, Case, FraudFlag
from tests.factories import create_case, create_fraud_flag, create_provider, create_user
from app.auth import create_access_token


# ── Create case ───────────────────────────────────────────────────────────────

async def test_create_case_analyst(db, client, analyst_user, analyst_headers):
    """Analysts can open a new case; case_number follows VGL-* format."""
    await create_provider(db, npi="1234567890", state="CA")
    await db.commit()

    resp = await client.post(
        "/api/cases",
        json={
            "provider_npi": "1234567890",
            "title": "High billing investigation",
            "state": "CA",
            "estimated_loss": 120000,
        },
        headers=analyst_headers,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["case_number"].startswith("VGL-")
    assert body["status"] == "open"
    assert body["provider_npi"] == "1234567890"


async def test_create_case_viewer_forbidden(db, client, viewer_headers):
    """Viewers cannot create cases."""
    await create_provider(db, npi="1234567890")
    await db.commit()

    resp = await client.post(
        "/api/cases",
        json={"provider_npi": "1234567890", "title": "Test"},
        headers=viewer_headers,
    )
    assert resp.status_code == 403


async def test_create_case_unknown_provider(client, analyst_headers):
    """Creating a case for a non-existent NPI returns 404."""
    resp = await client.post(
        "/api/cases",
        json={"provider_npi": "0000000000", "title": "Ghost"},
        headers=analyst_headers,
    )
    assert resp.status_code == 404


# ── List cases ────────────────────────────────────────────────────────────────

async def test_list_cases_paginated(db, client, admin_user, admin_headers):
    """List returns all cases with pagination metadata."""
    await create_provider(db, npi="1111111111")
    await create_provider(db, npi="2222222222")
    await create_case(db, provider_npi="1111111111", created_by=admin_user.id)
    await create_case(db, provider_npi="2222222222", created_by=admin_user.id)
    await db.commit()

    resp = await client.get("/api/cases", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert len(body["items"]) == 2


async def test_list_cases_filter_by_status(db, client, admin_user, admin_headers):
    """status= filter returns only matching cases."""
    await create_provider(db, npi="1111111111")
    await create_case(db, provider_npi="1111111111", created_by=admin_user.id, status="open")
    await create_case(db, provider_npi="1111111111", created_by=admin_user.id, status="closed")
    await db.commit()

    resp = await client.get("/api/cases?status=open", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["status"] == "open"


async def test_list_cases_filter_by_outcome(db, client, admin_user, admin_headers):
    """outcome= filter returns only cases with that disposition."""
    await create_provider(db, npi="1111111111")
    c1 = await create_case(db, provider_npi="1111111111", created_by=admin_user.id,
                            outcome="substantiated", status="closed")
    c2 = await create_case(db, provider_npi="1111111111", created_by=admin_user.id,
                            status="open")
    await db.commit()

    resp = await client.get("/api/cases?outcome=substantiated", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["outcome"] == "substantiated"


# ── Get single case ───────────────────────────────────────────────────────────

async def test_get_case_detail(db, client, admin_user, admin_headers):
    """GET /cases/{id} returns full case detail."""
    await create_provider(db, npi="1234567890")
    case = await create_case(db, provider_npi="1234567890", created_by=admin_user.id,
                              title="Fraud Investigation Alpha")
    await db.commit()

    resp = await client.get(f"/api/cases/{case.id}", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == case.id
    assert body["title"] == "Fraud Investigation Alpha"


async def test_get_case_not_found(client, admin_headers):
    """Unknown case ID returns 404."""
    resp = await client.get("/api/cases/99999", headers=admin_headers)
    assert resp.status_code == 404


# ── Update case ───────────────────────────────────────────────────────────────

async def test_update_case_status(db, client, analyst_user, analyst_headers):
    """Analyst can update a case they created."""
    await create_provider(db, npi="1234567890")
    case = await create_case(db, provider_npi="1234567890", created_by=analyst_user.id,
                              status="open")
    await db.commit()

    resp = await client.patch(
        f"/api/cases/{case.id}",
        json={"status": "under_review"},
        headers=analyst_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "under_review"


async def test_update_case_analyst_cannot_write_others_case(db, client, admin_user, analyst_headers):
    """Analyst cannot update a case they didn't create and aren't assigned to."""
    await create_provider(db, npi="1234567890")
    # Case created by admin, not the analyst
    case = await create_case(db, provider_npi="1234567890", created_by=admin_user.id)
    await db.commit()

    resp = await client.patch(
        f"/api/cases/{case.id}",
        json={"status": "under_review"},
        headers=analyst_headers,
    )
    assert resp.status_code == 403


async def test_viewer_cannot_update_case(db, client, admin_user, viewer_headers):
    """Viewers can never update cases."""
    await create_provider(db, npi="1234567890")
    case = await create_case(db, provider_npi="1234567890", created_by=admin_user.id)
    await db.commit()

    resp = await client.patch(
        f"/api/cases/{case.id}",
        json={"notes": "Note from viewer"},
        headers=viewer_headers,
    )
    assert resp.status_code == 403


# ── Case notes ────────────────────────────────────────────────────────────────

async def test_add_case_note(db, client, analyst_user, analyst_headers):
    """Analysts can add notes to their cases."""
    await create_provider(db, npi="1234567890")
    case = await create_case(db, provider_npi="1234567890", created_by=analyst_user.id)
    await db.commit()

    resp = await client.post(
        f"/api/cases/{case.id}/notes",
        json={"content": "Provider billed 8× peer median in Q3."},
        headers=analyst_headers,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["content"] == "Provider billed 8× peer median in Q3."
    assert body["case_id"] == case.id


async def test_add_note_blocked_by_state_access(db, client):
    """
    An analyst scoped to CA must not be able to add notes on a TX case.
    Previously this check was missing — the endpoint accepted notes from
    any authenticated analyst regardless of jurisdiction.
    """
    await create_provider(db, npi="9991111111", state="TX")
    admin = await create_user(db, role="admin")
    ca_analyst = await create_user(db, role="analyst", state_access=["CA"])
    tx_case = await create_case(db, provider_npi="9991111111", created_by=admin.id, state="TX")
    await db.commit()

    from app.auth import create_access_token
    headers = {"Authorization": f"Bearer {create_access_token(ca_analyst)}"}
    resp = await client.post(
        f"/api/cases/{tx_case.id}/notes",
        json={"content": "Sneaking in a note from CA analyst"},
        headers=headers,
    )
    assert resp.status_code == 403


async def test_upload_document_blocked_by_state_access(db, client):
    """
    An analyst scoped to CA must not be able to upload documents to a TX case.
    """
    import io
    await create_provider(db, npi="9992222222", state="TX")
    admin = await create_user(db, role="admin")
    ca_analyst = await create_user(db, role="analyst", state_access=["CA"])
    tx_case = await create_case(db, provider_npi="9992222222", created_by=admin.id, state="TX")
    await db.commit()

    from app.auth import create_access_token
    headers = {"Authorization": f"Bearer {create_access_token(ca_analyst)}"}
    resp = await client.post(
        f"/api/cases/{tx_case.id}/documents",
        files={"file": ("evidence.pdf", io.BytesIO(b"%PDF-1.4 test"), "application/pdf")},
        headers=headers,
    )
    assert resp.status_code == 403


# ── Outcome recording & feedback loop ─────────────────────────────────────────

async def test_record_outcome_substantiated(db, client, analyst_user, analyst_headers):
    """Recording a substantiated outcome closes the case and sets resolved_at."""
    await create_provider(db, npi="1234567890")
    case = await create_case(db, provider_npi="1234567890", created_by=analyst_user.id)
    await db.commit()

    resp = await client.patch(
        f"/api/cases/{case.id}/outcome",
        json={"outcome": "substantiated", "outcome_note": "OIG referral filed."},
        headers=analyst_headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["outcome"] == "substantiated"
    assert body["outcome_note"] == "OIG referral filed."
    assert body["resolved_at"] is not None
    assert body["status"] == "closed"


async def test_outcome_updates_flag_confidence(db, client, analyst_user, analyst_headers):
    """
    Confirmed outcome (substantiated / referred_to_doj / referred_to_state_ag)
    must flip all active fraud_flags for the provider to confidence=1.0.

    This is the investigator feedback loop — ground-truth labels for retraining.
    """
    await create_provider(db, npi="1234567890")
    flag1 = await create_fraud_flag(db, npi="1234567890", confidence=0.75)
    flag2 = await create_fraud_flag(db, npi="1234567890", confidence=0.60)
    # Inactive flag — must NOT be updated
    flag_inactive = await create_fraud_flag(db, npi="1234567890", confidence=0.50, is_active=False)
    case = await create_case(db, provider_npi="1234567890", created_by=analyst_user.id)
    await db.commit()

    resp = await client.patch(
        f"/api/cases/{case.id}/outcome",
        json={"outcome": "referred_to_doj"},
        headers=analyst_headers,
    )
    assert resp.status_code == 200

    # Refresh each flag individually from the DB so we see the values
    # committed by the endpoint session (READ COMMITTED isolation).
    await db.refresh(flag1)
    await db.refresh(flag2)
    await db.refresh(flag_inactive)

    # Active flags should now have confidence == 1.000
    assert float(flag1.confidence) == 1.0
    assert float(flag2.confidence) == 1.0
    # Inactive flag must remain unchanged
    assert float(flag_inactive.confidence) == 0.50

    # Release the NullPool connection while still in the function event loop.
    # Without this, the db fixture teardown (session loop) would find a
    # connection created in this test's function loop → "attached to a
    # different loop" RuntimeError.
    await db.rollback()


async def test_unconfirmed_outcome_does_not_update_flags(db, client, analyst_user, analyst_headers):
    """
    Unsubstantiated and closed_no_action outcomes must NOT change flag confidence —
    they're not positive ground-truth labels.
    """
    await create_provider(db, npi="1234567890")
    flag = await create_fraud_flag(db, npi="1234567890", confidence=0.70)
    case = await create_case(db, provider_npi="1234567890", created_by=analyst_user.id)
    await db.commit()

    resp = await client.patch(
        f"/api/cases/{case.id}/outcome",
        json={"outcome": "unsubstantiated"},
        headers=analyst_headers,
    )
    assert resp.status_code == 200

    # Refresh from DB to confirm the endpoint did NOT touch this flag's confidence.
    await db.refresh(flag)
    assert float(flag.confidence) == 0.70  # unchanged

    # Release the NullPool connection while still in the function event loop
    # to prevent a "attached to a different loop" teardown error.
    await db.rollback()


async def test_outcome_invalid_value(db, client, analyst_user, analyst_headers):
    """Invalid outcome value returns 422."""
    await create_provider(db, npi="1234567890")
    case = await create_case(db, provider_npi="1234567890", created_by=analyst_user.id)
    await db.commit()

    resp = await client.patch(
        f"/api/cases/{case.id}/outcome",
        json={"outcome": "definitely_guilty"},   # not in enum
        headers=analyst_headers,
    )
    assert resp.status_code == 422


# ── Audit log completeness ────────────────────────────────────────────────────

async def test_add_note_creates_audit_log(db, client, analyst_user, analyst_headers):
    """
    Every note addition must be recorded in audit_log.
    Analysts access sensitive investigation data; a complete audit trail is
    required for HIPAA and law-enforcement compliance.
    """
    await create_provider(db, npi="1234567890")
    case = await create_case(db, provider_npi="1234567890", created_by=analyst_user.id)
    await db.commit()

    resp = await client.post(
        f"/api/cases/{case.id}/notes",
        json={"content": "Checked billing records — 7× peer median in Q4."},
        headers=analyst_headers,
    )
    assert resp.status_code == 201

    logs = (await db.execute(
        select(AuditLog).where(
            AuditLog.user_id == analyst_user.id,
            AuditLog.action == "add_note",
        )
    )).scalars().all()
    assert len(logs) >= 1, "audit_log must contain an add_note entry"
    assert logs[0].target_type == "case"

    # Release the NullPool connection while still in the function event loop
    # to prevent a "Task got Future attached to a different loop" teardown error.
    # The HTTP endpoint committed via _override_get_db; rolling back here is safe.
    await db.rollback()


# ── MIME magic-bytes validation ───────────────────────────────────────────────

async def test_upload_rejects_exe_with_pdf_content_type(db, client, analyst_user, analyst_headers):
    """
    An executable disguised as a PDF (fake Content-Type) must be rejected.
    Previously only the Content-Type header was checked, which is client-controlled.
    Magic-bytes validation must catch the mismatch.
    """
    await create_provider(db, npi="1234567890")
    case = await create_case(db, provider_npi="1234567890", created_by=analyst_user.id)
    await db.commit()

    # MZ magic bytes = Windows PE executable
    fake_content = b"MZ\x90\x00\x03\x00\x00\x00\x04\x00\x00\x00\xFF\xFF"
    resp = await client.post(
        f"/api/cases/{case.id}/documents",
        files={"file": ("malware.exe", io.BytesIO(fake_content), "application/pdf")},
        headers=analyst_headers,
    )
    assert resp.status_code == 415, (
        "Executable with fake PDF Content-Type must be rejected by magic-bytes check"
    )


async def test_upload_accepts_real_pdf(db, client, analyst_user, analyst_headers):
    """A file with correct PDF magic bytes and Content-Type must be accepted."""
    await create_provider(db, npi="1234567890")
    case = await create_case(db, provider_npi="1234567890", created_by=analyst_user.id)
    await db.commit()

    pdf_content = b"%PDF-1.4 1 0 obj << /Type /Catalog >> endobj"
    resp = await client.post(
        f"/api/cases/{case.id}/documents",
        files={"file": ("evidence.pdf", io.BytesIO(pdf_content), "application/pdf")},
        headers=analyst_headers,
    )
    assert resp.status_code == 201


async def test_upload_rejects_jpeg_with_text_content_type(db, client, analyst_user, analyst_headers):
    """A JPEG file declared as text/plain must be rejected."""
    await create_provider(db, npi="1234567890")
    case = await create_case(db, provider_npi="1234567890", created_by=analyst_user.id)
    await db.commit()

    jpeg_content = b"\xFF\xD8\xFF\xE0\x00\x10JFIF"
    resp = await client.post(
        f"/api/cases/{case.id}/documents",
        files={"file": ("image.jpg", io.BytesIO(jpeg_content), "text/plain")},
        headers=analyst_headers,
    )
    assert resp.status_code == 415


# ── assigned_to validation ────────────────────────────────────────────────────

async def test_create_case_rejects_nonexistent_assignee(db, client, analyst_user, analyst_headers):
    """assigned_to with a non-existent user UUID must return 422."""
    import uuid
    await create_provider(db, npi="1234567890", state="CA")
    await db.commit()

    resp = await client.post(
        "/api/cases",
        json={
            "provider_npi": "1234567890",
            "title": "Test",
            "state": "CA",
            "assigned_to": str(uuid.uuid4()),  # random UUID — no such user
        },
        headers=analyst_headers,
    )
    assert resp.status_code == 422


async def test_create_case_rejects_out_of_state_assignee(db, client):
    """
    Cannot assign a CA case to a TX-only analyst.
    Prevents cases from disappearing from analysts' worklists due to
    cross-jurisdiction assignments.
    """
    await create_provider(db, npi="6001111111", state="CA")
    admin = await create_user(db, role="admin", state_access=[])
    tx_analyst = await create_user(db, role="analyst", state_access=["TX"])
    await db.commit()

    headers = {"Authorization": f"Bearer {create_access_token(admin)}"}
    resp = await client.post(
        "/api/cases",
        json={
            "provider_npi": "6001111111",
            "title": "CA investigation",
            "state": "CA",
            "assigned_to": str(tx_analyst.id),
        },
        headers=headers,
    )
    assert resp.status_code == 422, (
        "Assigning a CA case to a TX-only analyst must be rejected"
    )
