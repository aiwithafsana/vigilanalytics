"""
courtlistener.py — CourtListener Search API tool.

CourtListener (https://www.courtlistener.com) is run by the Free Law Project
and indexes federal + state court records, including PACER bulk data and
opinions.  Their REST API is free for non-commercial use and provides
structured JSON for case names, courts, filing dates, and document links.

For Vigil's purposes the key signal is: has this provider been a named
defendant in a federal civil or criminal case?  An active FCA case or
healthcare-fraud indictment is the strongest possible external corroboration
of a Vigil-generated risk score.

API endpoint
------------
  https://www.courtlistener.com/api/rest/v3/search/

An API key (``COURTLISTENER_API_KEY``) is recommended; unauthenticated requests
are rate-limited but functional.
"""
from __future__ import annotations

import os
from typing import Any

import httpx

from app.agents.base import AgentContext, Finding, Severity, Tool


class CourtListenerTool(Tool):
    name = "Federal court records"
    description = "CourtListener federal civil/criminal docket search"
    timeout_seconds = 25.0
    max_retries = 1

    # CourtListener migrated new accounts to V4 only.  V3 returns 403 for any
    # account created after their migration cutoff with the message
    # "you don't have permission to access V3 of the API. Please use V4 instead."
    BASE_URL = "https://www.courtlistener.com/api/rest/v4/search/"

    async def _run(self, context: AgentContext) -> tuple[list[Finding], Any]:
        # CourtListener uses full-text query syntax.  We prefer business name;
        # for individuals, last name + (state OR fraud-related term) reduces
        # false positives.
        if context.busname:
            q = f'"{context.busname}"'
        elif context.name_last and context.name_first:
            q = f'"{context.name_first} {context.name_last}"'
        elif context.name_last:
            # Surnames alone are too noisy — augment with fraud terms
            q = f'"{context.name_last}" AND ("healthcare fraud" OR "False Claims" OR "Medicare")'
        else:
            return [], {"skipped": "no name data"}

        params = {
            "q":            q,
            "type":         "r",   # 'r' = RECAP federal court documents
            "order_by":     "dateFiled desc",
            "stat_Precedential": "on",
        }
        api_key = os.getenv("COURTLISTENER_API_KEY")
        headers = {
            "Accept": "application/json",
            "User-Agent": "Vigil-PublicRecords-Agent/1.0",
        }
        if api_key:
            headers["Authorization"] = f"Token {api_key}"

        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            r = await client.get(self.BASE_URL, params=params, headers=headers)

        if r.status_code == 429:
            raise RuntimeError(
                "CourtListener rate limited; set COURTLISTENER_API_KEY in env",
            )
        if r.status_code >= 400:
            raise RuntimeError(f"CourtListener returned HTTP {r.status_code}")

        data = r.json()
        return self._parse(data, q, context), {
            "query": q,
            "total_count": data.get("count"),
        }

    def _parse(self, data: dict, query: str, context: AgentContext) -> list[Finding]:
        out: list[Finding] = []
        # Cap to top 10 results — older / less-relevant cases are noise
        for r in (data.get("results") or [])[:10]:
            case_name = r.get("caseName") or "(unnamed case)"
            court     = r.get("court") or r.get("court_id") or "federal court"
            filed     = r.get("dateFiled")
            docket    = r.get("docketNumber") or ""
            abs_url   = r.get("absolute_url")
            url = f"https://www.courtlistener.com{abs_url}" if abs_url else None

            # Severity heuristic — criminal case > civil FCA > unspecified civil
            case_blob = (case_name + " " + r.get("nature_of_suit", "")).lower()
            if any(k in case_blob for k in ["united states v.", "indictment", "criminal"]):
                severity = Severity.CRITICAL
                summary_lead = "Criminal docket"
            elif any(k in case_blob for k in
                     ["false claims", "qui tam", "31 u.s.c.", "fca", "medicare fraud"]):
                severity = Severity.HIGH
                summary_lead = "Civil FCA / Medicare-fraud case"
            else:
                severity = Severity.MEDIUM
                summary_lead = "Civil case"

            title = f"{summary_lead}: {case_name}"
            summary = (
                f"Filed {filed or 'unknown'} in {court}. "
                f"Docket {docket}.  "
                f"NOTE: name match only — confirm the named party is "
                f"{context.display_name()} (NPI {context.npi}) before relying."
            )
            out.append(Finding(
                source=self.name,
                severity=severity,
                title=title,
                summary=summary,
                url=url,
                date=filed,
                raw=r,
            ))
        return out
