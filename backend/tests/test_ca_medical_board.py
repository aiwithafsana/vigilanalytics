"""
test_ca_medical_board.py — CA Medical Board scraper tool correctness.

The actual HTTP call to the DCA system is flaky (anti-bot, occasional
CAPTCHA), so the test suite focuses on the layers we control:

  1. State filter — non-CA providers correctly skip the tool
  2. The manual-verification URL is correctly constructed
  3. Severity escalation: detect-keywords logic maps to right Severity
  4. Failure modes — when scrape crashes, the baseline manual-URL still ships
  5. "No results" detection — scraper succeeded but found nothing

If these pass, the production behaviour is correct even when DCA's site
breaks our scraper.  The investigator always gets a working manual link.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.agents.base import AgentContext, Severity
from app.agents.tools.ca_medical_board import (
    CaliforniaMedicalBoardTool,
    _build_manual_url,
    _detect_severity,
    _extract_detected_phrases,
)


# ── State filter ──────────────────────────────────────────────────────────────

async def test_skips_for_non_ca_provider():
    """Tool returns immediately for a Texas provider — no HTTP calls."""
    tool = CaliforniaMedicalBoardTool()
    ctx  = AgentContext(npi="1000000001", name_last="Smith", state="TX")
    findings, raw = await tool._run(ctx)
    assert findings == []
    assert raw == {"skipped": "not_ca_provider"}


async def test_skips_for_missing_state():
    """Tool returns immediately when no state is set."""
    tool = CaliforniaMedicalBoardTool()
    ctx  = AgentContext(npi="1000000001", name_last="Smith", state=None)
    findings, raw = await tool._run(ctx)
    assert findings == []
    assert raw == {"skipped": "not_ca_provider"}


async def test_skips_when_no_name_to_query():
    """Tool can't search without a name — fail fast."""
    tool = CaliforniaMedicalBoardTool()
    ctx  = AgentContext(npi="1000000001", state="CA")  # no name
    findings, raw = await tool._run(ctx)
    assert findings == []
    assert raw == {"skipped": "no_name"}


# ── Manual URL construction ───────────────────────────────────────────────────

def test_manual_url_includes_name_and_board_code():
    ctx = AgentContext(npi="1", name_last="Smith", name_first="Jane", state="CA")
    url = _build_manual_url(ctx)
    assert "search.dca.ca.gov" in url
    assert "boardCode=800" in url       # Medical Board of California
    assert "lastName=Smith" in url
    assert "firstName=Jane" in url


def test_manual_url_handles_unusual_characters():
    ctx = AgentContext(
        npi="1", name_last="O'Brien-Smith", name_first="Mary Anne", state="CA",
    )
    url = _build_manual_url(ctx)
    # Both characters must be URL-encoded (no raw quotes or spaces)
    assert "'" not in url
    assert " " not in url
    assert "lastName=" in url


# ── Severity detection from HTML keywords ─────────────────────────────────────

def test_severity_critical_on_license_revoked():
    assert _detect_severity("…License Revoked…") == Severity.CRITICAL


def test_severity_critical_on_surrender():
    assert _detect_severity("License Surrendered as of 2020-01-15") == Severity.CRITICAL


def test_severity_high_on_probation():
    assert _detect_severity("Status: Probation through 2027") == Severity.HIGH


def test_severity_high_on_accusation():
    assert _detect_severity("Pending Accusation filed by Board") == Severity.HIGH


def test_severity_medium_on_citation():
    assert _detect_severity("Citation issued: $5,000") == Severity.MEDIUM


def test_severity_medium_on_expired():
    assert _detect_severity("License Status: License Expired") == Severity.MEDIUM


def test_severity_info_on_clean_record():
    """Active, unremarkable license → no escalation."""
    clean = "License: Active. Issued 2010-05-12. No disciplinary actions."
    assert _detect_severity(clean) == Severity.INFO


def test_negation_filter_handles_standard_dca_phrasing():
    """Standard DCA boilerplate must not false-trigger as adverse."""
    # All of these should resolve to INFO — they're stating absence
    assert _detect_severity("No prior disciplinary actions") == Severity.INFO
    assert _detect_severity("Status: Active.  Without probation") == Severity.INFO
    assert _detect_severity("License never been suspended") == Severity.INFO
    assert _detect_severity("No public reproval issued") == Severity.INFO


def test_negation_filter_doesnt_block_real_adverse_when_mixed():
    """Mixed text: 'no prior X' + actual X later → still adverse."""
    text = "No prior probation.  Current status: Probation through 2027."
    assert _detect_severity(text) == Severity.HIGH


def test_severity_critical_wins_when_multiple_buckets_match():
    """Revoked + probation history → CRITICAL (the more severe wins)."""
    mixed = "License Revoked.  Prior history: Probation 2018-2020."
    assert _detect_severity(mixed) == Severity.CRITICAL


def test_extract_detected_phrases_returns_all_hits():
    html = "License Suspended.  Probation period ended 2019.  Citation 12345."
    hits = _extract_detected_phrases(html)
    # Should include all three keyword buckets that fired
    assert "license suspended" in hits
    assert "probation"          in hits
    assert "citation"           in hits


# ── End-to-end integration with mocked HTTP ───────────────────────────────────

async def test_baseline_finding_always_returned_for_ca_provider():
    """Even when the scrape fails entirely, the manual URL ships as INFO."""
    tool = CaliforniaMedicalBoardTool()
    ctx  = AgentContext(npi="1", name_last="Smith", name_first="Jane", state="CA")

    with patch("app.agents.tools.ca_medical_board.httpx.AsyncClient") as mock_client:
        # Make the HTTP client raise on use → scrape fails
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            side_effect=ConnectionError("no internet"),
        )
        findings, raw = await tool._run(ctx)

    assert len(findings) == 1
    f = findings[0]
    assert f.source   == "CA Medical Board"
    assert f.severity == Severity.INFO
    assert "manual verification" in f.title.lower()
    assert f.url is not None and "search.dca.ca.gov" in f.url
    # Meta records the scrape failure so ops can see it
    assert "scrape_error" in raw


async def test_adverse_keyword_in_response_escalates_severity():
    """When DCA HTML contains 'License Revoked', a CRITICAL finding is added."""
    tool = CaliforniaMedicalBoardTool()
    ctx  = AgentContext(npi="1", name_last="Smith", name_first="Jane", state="CA")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = """<html>
        <table><tr><td>Smith, Jane M.D.</td>
        <td>License A12345</td>
        <td>Status: License Revoked</td></tr></table>
    </html>"""

    with patch("app.agents.tools.ca_medical_board.httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response,
        )
        findings, raw = await tool._run(ctx)

    # Two findings: the baseline INFO + the adverse CRITICAL
    assert len(findings) == 2
    severities = {f.severity for f in findings}
    assert Severity.CRITICAL in severities
    assert Severity.INFO     in severities
    # The CRITICAL one references the keyword
    critical = next(f for f in findings if f.severity == Severity.CRITICAL)
    assert "revoked" in critical.summary.lower()


async def test_no_results_response_returns_only_baseline():
    """When DCA says 'no records found', we don't fake an adverse finding."""
    tool = CaliforniaMedicalBoardTool()
    ctx  = AgentContext(npi="1", name_last="Nonexistent", state="CA")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<html>No records found for the search criteria.</html>"

    with patch("app.agents.tools.ca_medical_board.httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response,
        )
        findings, raw = await tool._run(ctx)

    # Only the baseline INFO with the manual URL — no fabricated findings
    assert len(findings) == 1
    assert findings[0].severity == Severity.INFO
    assert raw.get("scrape_status") == "no_results"


async def test_rate_limit_is_treated_as_scrape_failure():
    """HTTP 429 → tool reports scrape error, still returns baseline finding."""
    tool = CaliforniaMedicalBoardTool()
    ctx  = AgentContext(npi="1", name_last="Smith", state="CA")

    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.text = "Too Many Requests"

    with patch("app.agents.tools.ca_medical_board.httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response,
        )
        findings, raw = await tool._run(ctx)

    # Baseline still returned — failure doesn't crash the tool
    assert len(findings) == 1
    assert findings[0].severity == Severity.INFO
    assert "rate-limited" in raw.get("scrape_error", "").lower()


# ── Workflow integration ──────────────────────────────────────────────────────

async def test_tool_is_registered_in_public_records_workflow():
    """The CA Medical Board tool must be in PublicRecordsAgent.tools."""
    from app.agents.workflows.public_records import PublicRecordsAgent
    agent = PublicRecordsAgent()
    tool_names = [t.name for t in agent.tools]
    assert "CA Medical Board" in tool_names
