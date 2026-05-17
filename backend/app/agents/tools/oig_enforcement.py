"""
oig_enforcement.py — OIG enforcement-action search tool.

OIG publishes enforcement actions (criminal indictments, civil settlements,
program integrity decisions) on its Newsroom.  These are a richer signal
than LEIE — LEIE captures the exclusion; OIG newsroom captures the
underlying conduct.

We use the OIG Newsroom search endpoint, which is a public-facing search
(no API key, no rate limit declared).  We treat any matching press release
as a finding ranked by date proximity.

For the scope of this v1 tool we do simple substring matching on the
press-release title and snippet.  False-positive risk: provider with a
common name shares a release with another person.  Marked as ``HIGH`` (not
critical) and the summary explicitly invites human verification.
"""
from __future__ import annotations

import re
from typing import Any

import httpx

from app.agents.base import AgentContext, Finding, Severity, Tool


class OigEnforcementTool(Tool):
    name = "OIG enforcement"
    description = "HHS-OIG enforcement actions (newsroom search)"
    timeout_seconds = 20.0
    max_retries = 1

    # OIG Newsroom search URL.  The site is server-rendered HTML; we parse
    # press-release cards from the response.
    BASE_URL = "https://oig.hhs.gov/newsroom/news-releases/"

    async def _run(self, context: AgentContext) -> tuple[list[Finding], Any]:
        # Build a query — last name + state is usually enough to filter
        query_terms = []
        if context.busname:
            # Strip generic corporate suffixes that hurt match precision
            cleaned = re.sub(
                r"\b(llc|inc|corp|p\.?c\.?|pllc|ltd|co|company|llp)\.?\b",
                "",
                context.busname,
                flags=re.IGNORECASE,
            ).strip()
            if cleaned:
                query_terms.append(cleaned)
        else:
            if context.name_last:
                query_terms.append(context.name_last)
            if context.name_first:
                query_terms.append(context.name_first)

        if not query_terms:
            return [], {"skipped": "no name data"}

        query = " ".join(query_terms)
        params = {"search_api_fulltext": query}
        if context.state:
            # The site filters by state via this query param
            params["field_state"] = context.state

        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
            r = await client.get(
                self.BASE_URL,
                params=params,
                headers={"User-Agent": "Vigil-PublicRecords-Agent/1.0"},
            )

        if r.status_code >= 400:
            raise RuntimeError(f"OIG newsroom returned HTTP {r.status_code}")

        # Parse the HTML for press-release cards
        findings = self._parse(r.text, query, context)
        return findings, {"query": query, "url": str(r.url), "html_bytes": len(r.text)}

    # OIG newsroom uses Drupal — release cards follow a predictable pattern.
    # The selectors here are intentionally lenient to survive minor site changes.
    _CARD_RE  = re.compile(
        r'<article[^>]*class="[^"]*node--type-press-release[^>]*>(.*?)</article>',
        re.DOTALL | re.IGNORECASE,
    )
    _TITLE_RE = re.compile(r'<h2[^>]*>\s*<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', re.DOTALL)
    _DATE_RE  = re.compile(r'datetime="([0-9]{4}-[0-9]{2}-[0-9]{2})')
    _SUMMARY_RE = re.compile(r'<div[^>]*class="[^"]*field--name-body[^>]*>(.*?)</div>', re.DOTALL)
    _STRIP_TAGS = re.compile(r"<[^>]+>")
    _WS = re.compile(r"\s+")

    def _parse(self, html: str, query: str, context: AgentContext) -> list[Finding]:
        out: list[Finding] = []
        for m in self._CARD_RE.finditer(html):
            card = m.group(1)

            tm = self._TITLE_RE.search(card)
            if not tm:
                continue
            href, title_html = tm.group(1), tm.group(2)
            title = self._WS.sub(" ", self._STRIP_TAGS.sub("", title_html)).strip()

            url = href if href.startswith("http") else f"https://oig.hhs.gov{href}"

            dm = self._DATE_RE.search(card)
            date = dm.group(1) if dm else None

            sm = self._SUMMARY_RE.search(card)
            summary = self._WS.sub(
                " ", self._STRIP_TAGS.sub("", sm.group(1)),
            ).strip()[:400] if sm else ""

            # Severity: criminal-action keywords get CRITICAL; civil-only get HIGH
            text_blob = (title + " " + summary).lower()
            if any(k in text_blob for k in
                   ["sentenced", "convicted", "guilty", "indicted", "arrested",
                    "fraud charges", "criminal"]):
                severity = Severity.CRITICAL
            elif any(k in text_blob for k in
                     ["settlement", "civil monetary penalty", "cmp", "false claims"]):
                severity = Severity.HIGH
            else:
                severity = Severity.MEDIUM

            out.append(Finding(
                source=self.name,
                severity=severity,
                title=title,
                summary=(
                    f"{summary}  "
                    f"NOTE: matched on name + state search; verify the named "
                    f"defendant/respondent is the same individual or entity "
                    f"as {context.display_name()} (NPI {context.npi}) before "
                    f"relying on this finding."
                ),
                url=url,
                date=date,
                raw={"title": title, "url": url, "date": date, "summary": summary},
            ))
        return out
