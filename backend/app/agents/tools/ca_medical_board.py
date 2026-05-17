"""
ca_medical_board.py — California Medical Board license + discipline lookup.

The California Department of Consumer Affairs (DCA) runs a public license
verification system at https://search.dca.ca.gov/ that covers every
licensee of CA's professional boards, including:
  - Medical Board of California (MBC, board code 800)
  - Osteopathic Medical Board (OMBC, board code 870)
  - Board of Pharmacy (BOP, board code 100)
  - Dental Board (BOC, board code 230)
  - Many more — see https://www.dca.ca.gov/boards/index.shtml

Why this tool only runs for California providers
------------------------------------------------
DCA only has data on California-licensed practitioners.  For a Texas
provider, this tool's results would be 100% false negatives ("no record")
that the investigator would have to mentally filter out.  Skipping the tool
entirely keeps the agent's output uncluttered.

When other states' boards get scrapers, they'll be separate tools
(``texas_medical_board.py``, ``florida_doh.py``, etc.) and the
PublicRecordsAgent's plan() method will route based on context.state.

Why this tool is "best-effort with manual fallback"
---------------------------------------------------
The DCA Breeze system is an ASP.NET WebForms application with VIEWSTATE,
session cookies, and occasional CAPTCHA.  Reliable automated scraping is
fragile — the site can change CSS classes, IDs, or anti-bot policies at
any time.

So the tool ALWAYS surfaces a clickable manual-verification URL as an INFO
finding.  This guarantees the investigator can always do the lookup
themselves, even if the scraper is broken.  When the scraper works on top
of that, it adds higher-severity Findings (active discipline, revocation,
probation) above the manual link.
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote_plus

import httpx

from app.agents.base import AgentContext, Finding, Severity, Tool


# Indicators that the DCA detail page shows an adverse license status.  We
# look for these in the parsed result HTML; if any fire, we elevate the
# severity above the INFO baseline.
_ADVERSE_KEYWORDS_CRITICAL = (
    "license revoked",
    "license surrendered",
    "license suspended",
    "license cancelled",
    "license canceled",
)
_ADVERSE_KEYWORDS_HIGH = (
    "probation",
    "stayed suspension",
    "public reprimand",
    "disciplinary action",
    "accusation",
    "interim suspension",
    "stipulated decision",
)
_ADVERSE_KEYWORDS_MEDIUM = (
    "citation",
    "letter of public reproval",
    "license expired",
    "license lapsed",
    "delinquent",
)


def _detect_severity(html: str) -> Severity:
    """
    Map keyword presence in DCA result HTML to a severity bucket.

    Negation-aware: skips matches that are preceded by phrases like "no",
    "no prior", or "without", which would otherwise false-trigger on
    DCA's standard "No disciplinary actions" / "No prior history" phrasing.

    This is intentionally a coarse keyword filter — the tool's INFO baseline
    finding directs the investigator to manually verify any flagged result,
    so a false positive at HIGH severity is recoverable (they'll look and
    see the page actually says "no actions").
    """
    blob = html.lower()
    for kw in _ADVERSE_KEYWORDS_CRITICAL:
        if _keyword_present_not_negated(blob, kw):
            return Severity.CRITICAL
    for kw in _ADVERSE_KEYWORDS_HIGH:
        if _keyword_present_not_negated(blob, kw):
            return Severity.HIGH
    for kw in _ADVERSE_KEYWORDS_MEDIUM:
        if _keyword_present_not_negated(blob, kw):
            return Severity.MEDIUM
    return Severity.INFO


# Negation phrases that, when they appear immediately before a keyword
# (within ~20 chars), invert the match.  Captures DCA's standard
# "No prior disciplinary actions" / "without restrictions" boilerplate.
_NEGATION_PATTERNS = re.compile(
    r"(no\s+(?:prior\s+|other\s+|further\s+|known\s+|public\s+)?|without\s+|"
    r"never\s+been\s+|not\s+(?:been\s+|currently\s+))$",
    re.IGNORECASE,
)


def _keyword_present_not_negated(text: str, keyword: str) -> bool:
    """
    True if ``keyword`` appears in ``text`` somewhere not preceded by a
    negation phrase.  Both inputs assumed lowercase.

    The check is per-occurrence: a page that has "No prior probation" earlier
    and "Probation through 2027" later will still match (the second occurrence
    isn't negated).
    """
    start = 0
    while True:
        idx = text.find(keyword, start)
        if idx == -1:
            return False
        # Look at the ~20 chars right before this occurrence
        window = text[max(0, idx - 20):idx]
        if not _NEGATION_PATTERNS.search(window):
            return True
        start = idx + len(keyword)


def _build_manual_url(context: AgentContext) -> str:
    """
    URL that takes the investigator straight to the DCA search results
    pre-filled with this provider's name.  Always works regardless of
    whether automated scraping succeeds.
    """
    last  = quote_plus((context.name_last  or "").strip())
    first = quote_plus((context.name_first or "").strip())
    # The board-code parameter limits the search to Medical Board licensees
    # (board 800).  Without it, the results would include nurses, dentists,
    # etc., which we'd have to filter out manually.
    return (
        f"https://search.dca.ca.gov/results?"
        f"boardCode=800&licenseType=&licenseNumber=&"
        f"lastName={last}&firstName={first}"
    )


class CaliforniaMedicalBoardTool(Tool):
    name = "CA Medical Board"
    description = "California Medical Board license + discipline lookup (CA providers only)"
    timeout_seconds = 20.0
    max_retries = 0       # DCA rate-limits aggressively — don't retry

    async def _run(self, context: AgentContext) -> tuple[list[Finding], Any]:
        # Only run for California providers.  See module docstring.
        if (context.state or "").strip().upper() != "CA":
            return [], {"skipped": "not_ca_provider"}

        # Need at least a last name to query
        if not (context.name_last or context.busname):
            return [], {"skipped": "no_name"}

        manual_url = _build_manual_url(context)

        # Always include the manual-verification URL as a baseline finding so
        # investigators have a working path even when the scraper fails.
        baseline = Finding(
            source=self.name,
            severity=Severity.INFO,
            title="Open DCA Medical Board license search (manual verification)",
            summary=(
                "California Medical Board license records are public.  Click "
                "the source link to verify license status, expiration, and "
                "any disciplinary actions directly.  Required for any finding "
                "from this tool to be cited in pleadings."
            ),
            url=manual_url,
        )

        # Attempt the automated scrape.  Failures here don't propagate — the
        # baseline finding above is the floor of usefulness.
        scraped_findings: list[Finding] = []
        scrape_meta: dict = {"manual_url": manual_url}

        try:
            scraped_findings, scrape_meta = await self._scrape_dca(context, manual_url)
        except Exception as e:
            scrape_meta["scrape_error"] = f"{type(e).__name__}: {e}"
            # Don't raise — baseline finding still ships

        return [baseline] + scraped_findings, scrape_meta

    async def _scrape_dca(
        self,
        context: AgentContext,
        manual_url: str,
    ) -> tuple[list[Finding], dict]:
        """
        Attempt to fetch DCA search results and detect adverse-license markers.

        Returns (findings, meta) — findings is empty when no adverse markers
        are detected (which is a SUCCESSFUL run, not a failure).
        """
        async with httpx.AsyncClient(
            timeout=self.timeout_seconds, follow_redirects=True,
        ) as client:
            r = await client.get(
                manual_url,
                headers={
                    "User-Agent": "Vigil-PublicRecords-Agent/1.0 (+contact: ops@vigil)",
                    "Accept": "text/html,application/xhtml+xml",
                },
            )

        if r.status_code == 429:
            raise RuntimeError("DCA rate-limited (HTTP 429)")
        if r.status_code >= 400:
            raise RuntimeError(f"DCA returned HTTP {r.status_code}")

        html = r.text

        # If we got a "no results found" page, that's a successful scrape
        # with nothing to surface.  These markers vary across DCA's pages
        # but consistently include the phrase "no record" or "no results".
        no_results_markers = (
            "no records found",
            "no results found",
            "no licensee found",
            "did not return any results",
        )
        if any(m in html.lower() for m in no_results_markers):
            return [], {
                "scrape_status":  "no_results",
                "manual_url":     manual_url,
                "html_bytes":     len(html),
            }

        # We got something back.  Use keyword detection to set severity.
        severity = _detect_severity(html)

        # When severity is INFO it means the scraper succeeded but didn't
        # detect adverse markers — i.e., the provider appears in the
        # registry with an unremarkable status.  Surface that as a positive
        # confirmation rather than a noisy "we found something" Finding.
        if severity == Severity.INFO:
            return [Finding(
                source=self.name,
                severity=Severity.INFO,
                title="CA Medical Board license verified (no adverse markers found)",
                summary=(
                    "DCA search returned a record without obvious discipline "
                    "indicators.  This is a positive baseline — but the "
                    "automated scraper only checks for top-level adverse "
                    "keywords; subtler restrictions may be present.  Manual "
                    "verification recommended for any case action."
                ),
                url=manual_url,
            )], {
                "scrape_status": "found_no_adverse",
                "html_bytes":    len(html),
            }

        # Adverse markers detected.  Extract the matching keyword phrases
        # so the Finding summary tells the investigator what specifically
        # to look for on the DCA page.
        detected_phrases = _extract_detected_phrases(html)
        return [Finding(
            source=self.name,
            severity=severity,
            title=(
                f"Possible adverse California Medical Board status — "
                f"{detected_phrases[0] if detected_phrases else 'verify directly'}"
            ),
            summary=(
                f"Vigil's automated scrape detected the following keyword "
                f"phrase(s) on the DCA license record page: "
                f"{', '.join(detected_phrases[:4]) or '(unknown)'}.  "
                f"This is a HEURISTIC match — confirm via the source link "
                f"before relying.  Severity assessed as {severity.value} "
                f"based on keyword phrasing."
            ),
            url=manual_url,
            raw={"detected_phrases": detected_phrases},
        )], {
            "scrape_status":   "adverse_detected",
            "detected_phrases": detected_phrases,
            "html_bytes":      len(html),
        }


def _extract_detected_phrases(html: str) -> list[str]:
    """
    Find every adverse-keyword phrase that fires in the page text.  Uses
    the same negation-aware check as _detect_severity so the reported hits
    match the assigned severity.
    """
    blob = html.lower()
    hits: list[str] = []
    for bucket in (
        _ADVERSE_KEYWORDS_CRITICAL,
        _ADVERSE_KEYWORDS_HIGH,
        _ADVERSE_KEYWORDS_MEDIUM,
    ):
        for kw in bucket:
            if _keyword_present_not_negated(blob, kw):
                hits.append(kw)
    return hits
