"""
sam_gov.py — SAM.gov Exclusions API tool.

SAM.gov is the General Services Administration's federal procurement database.
Its Exclusions API surfaces every entity debarred from receiving federal
contracts or grants — overlapping with but distinct from the OIG LEIE.
A provider who's clean on LEIE but debarred via SAM.gov is a meaningful
adverse signal.

API endpoint
------------
  https://api.sam.gov/entity-information/v3/entities

A SAM.gov API key is recommended for production (higher rate limits, JSON
responses), but the public endpoint works key-less at 10 requests/minute.
We use the public endpoint by default and pass a key via ``SAM_GOV_API_KEY``
env var when set.

Search behavior
---------------
We search by provider name (last, first, or business) AND state.  NPI is not
indexed in SAM.gov, so name-based matching has false-positive risk for common
names.  The aggregator workflow handles this by treating SAM.gov findings as
"candidate matches requiring verification" — never as definitive matches.
"""
from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlencode

import httpx

from app.agents.base import AgentContext, Finding, Severity, Tool


class SamGovExclusionsTool(Tool):
    name = "SAM.gov debarment"
    description = "Federal debarment / exclusion list (GSA)"
    timeout_seconds = 15.0
    max_retries = 1

    # SAM.gov v4 Exclusions API.  NOTE: this is NOT the same as the v4
    # /entities endpoint (which returns entity-registration data).
    # Exclusions live in their own resource.
    BASE_URL = "https://api.sam.gov/entity-information/v4/exclusions"

    async def _run(self, context: AgentContext) -> tuple[list[Finding], Any]:
        # Build query — prefer business name, fall back to last+first
        query_name = context.busname or " ".join(
            x for x in (context.name_first, context.name_last) if x
        )
        if not query_name or len(query_name.strip()) < 3:
            # Not enough to query meaningfully — skip without error
            return [], {"skipped": "insufficient name data"}

        params: dict[str, Any] = {"q": query_name}
        api_key = os.getenv("SAM_GOV_API_KEY")
        if api_key:
            params["api_key"] = api_key

        url = f"{self.BASE_URL}?{urlencode(params)}"
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            # IMPORTANT: SAM.gov returns HTTP 406 when Accept is set to
            # 'application/json' (despite the server returning JSON anyway).
            # Use '*/*' to avoid the content-negotiation rejection.
            r = await client.get(url, headers={"Accept": "*/*"})

        if r.status_code == 429:
            raise RuntimeError("SAM.gov rate limited; check daily quota on SAM_GOV_API_KEY")
        if r.status_code == 401 or r.status_code == 403:
            raise RuntimeError(f"SAM.gov auth failed (HTTP {r.status_code}) — verify SAM_GOV_API_KEY")
        if r.status_code >= 400:
            raise RuntimeError(f"SAM.gov returned HTTP {r.status_code}: {r.text[:200]}")

        data = r.json()
        findings = self._parse(data, query_name, context.state)
        return findings, data

    def _parse(self, data: dict, query_name: str, state: str | None) -> list[Finding]:
        """
        Convert SAM.gov v4 Exclusions payload into Findings.

        Response shape:
          { "totalRecords": N, "excludedEntity": [ {...}, ... ] }

        Each excludedEntity has exclusionDetails, exclusionIdentification,
        exclusionActions.listOfActions, exclusionPrimaryAddress, etc.
        """
        from datetime import date as _date

        out: list[Finding] = []
        records = data.get("excludedEntity") or []

        # Only surface records whose state matches (or unknown state) — name
        # alone is too noisy when querying 39+ "fraud" matches nationally.
        for rec in records:
            details = rec.get("exclusionDetails") or {}
            ident   = rec.get("exclusionIdentification") or {}
            actions = (rec.get("exclusionActions") or {}).get("listOfActions") or []
            addr    = rec.get("exclusionPrimaryAddress") or {}
            other   = rec.get("exclusionOtherInformation") or {}

            rec_state = (addr.get("stateOrProvinceCode") or "").upper()
            if state and rec_state and rec_state != state.upper():
                # Hard filter: a CA provider shouldn't match a TX exclusion
                # unless the name is exact + NPI matches (handled below).
                rec_npi = ident.get("npi")
                # If the SAM record's NPI matches our context NPI, keep it
                # regardless of state mismatch.
                if not rec_npi:
                    continue

            entity_name = (
                ident.get("entityName")
                or " ".join(
                    x for x in (ident.get("firstName"), ident.get("lastName")) if x
                )
                or query_name
            )

            # Severity: active record = CRITICAL; expired = MEDIUM
            latest_action = actions[0] if actions else {}
            is_active = (latest_action.get("recordStatus") or "").upper() == "ACTIVE"
            term_date = latest_action.get("terminationDate")
            if not is_active and term_date:
                # Double-check via termination date — sometimes recordStatus
                # is set before termination has actually passed
                try:
                    # SAM.gov uses MM-DD-YYYY
                    m, d, y = term_date.split("-")
                    is_active = _date(int(y), int(m), int(d)) >= _date.today()
                except (ValueError, TypeError):
                    pass

            severity = Severity.CRITICAL if is_active else Severity.MEDIUM

            agency = details.get("excludingAgencyName") or "an unspecified federal agency"
            excl_type = details.get("exclusionType") or "Exclusion"
            classification = details.get("classificationType") or "Entity"

            comments = (other.get("additionalComments") or "")[:500]
            comment_blurb = f"  Reason: {comments}" if comments else ""

            title = (
                f"{'ACTIVE' if is_active else 'EXPIRED'} federal exclusion — "
                f"{entity_name} ({classification})"
            )
            summary = (
                f"Excluded by {agency}.  Type: {excl_type}.  "
                f"Active {latest_action.get('activateDate') or 'unknown'}"
                f"{' through ' + term_date if term_date else ''}.  "
                f"Listed address: {addr.get('city') or '?'}, {rec_state or '?'}.  "
                f"NOTE: matched by name (and state when available); verify against "
                f"NPI {ident.get('npi') or '(none on file)'} before relying."
                f"{comment_blurb}"
            )
            out.append(Finding(
                source=self.name,
                severity=severity,
                title=title,
                summary=summary,
                url="https://sam.gov/search/?index=ex",
                date=latest_action.get("activateDate"),
                raw=rec,
            ))

        return out
