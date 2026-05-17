"""
npi_registry.py — CMS NPI Registry verification tool.

The NPI Registry (https://npiregistry.cms.hhs.gov) is the authoritative
source of NPI assignments.  Every NPI in our providers table came from CMS
billing data, so the registry should always return a hit; the value of
checking it is to surface:

  - Name / business-name mismatches between billing and registry
    (provider billing under a name not on the registry is unusual)
  - Recent address or specialty changes (rebadging is a common fraud signal)
  - Multiple practice locations registered under one NPI (some legitimate,
    but worth surfacing for context)
  - Enumeration date (lets us compute "months in practice" — new providers
    billing high volume is a strong fraud signal)
  - Deactivated NPIs (a deactivated provider still appearing in billing data
    means CMS hasn't cleaned up; or there's an identity-theft scenario)

API
---
  GET https://npiregistry.cms.hhs.gov/api/?version=2.1&number=<NPI>

Public, JSON response, no API key.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx

from app.agents.base import AgentContext, Finding, Severity, Tool


class NpiRegistryTool(Tool):
    name = "NPI Registry"
    description = "CMS NPI registry verification + enumeration date + status"
    timeout_seconds = 10.0
    max_retries = 1

    BASE_URL = "https://npiregistry.cms.hhs.gov/api/"

    async def _run(self, context: AgentContext) -> tuple[list[Finding], Any]:
        params = {"version": "2.1", "number": context.npi}
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            r = await client.get(self.BASE_URL, params=params,
                                 headers={"Accept": "application/json"})
        if r.status_code >= 400:
            raise RuntimeError(f"NPI Registry returned HTTP {r.status_code}")

        data = r.json()
        results = data.get("results") or []
        if not results:
            # An NPI that's in our billing data but not in the registry is itself
            # a finding — possible identity-theft scenario.
            return [Finding(
                source=self.name,
                severity=Severity.HIGH,
                title="NPI not found in CMS registry",
                summary=(
                    f"NPI {context.npi} appears in Medicare Part B billing data but "
                    f"is not present in the CMS NPI Registry.  This is unusual and "
                    f"may indicate a stale registry, a registration error, or "
                    f"identity-theft billing.  Verify the NPI assignment directly "
                    f"with CMS NPPES."
                ),
                url=f"https://npiregistry.cms.hhs.gov/provider-view/{context.npi}",
                raw={"npi": context.npi, "registry_count": 0},
            )], data

        result = results[0]
        return self._parse_record(result, context), data

    def _parse_record(self, result: dict, context: AgentContext) -> list[Finding]:
        out: list[Finding] = []
        basic = result.get("basic") or {}
        addresses = result.get("addresses") or []
        taxonomies = result.get("taxonomies") or []
        other_names = result.get("other_names") or []
        enumeration_date = basic.get("enumeration_date")
        status = basic.get("status", "A")

        # ── Finding 1: deactivation status ───────────────────────────────
        if status and status.upper() != "A":
            out.append(Finding(
                source=self.name,
                severity=Severity.CRITICAL,
                title=f"NPI is deactivated (status={status})",
                summary=(
                    f"CMS NPI Registry reports this NPI's status as '{status}' "
                    f"(not 'A'=Active).  A deactivated NPI should not appear in "
                    f"billing data; if it does, investigate as potential identity-"
                    f"theft billing or a registration error."
                ),
                url=f"https://npiregistry.cms.hhs.gov/provider-view/{context.npi}",
                raw=basic,
            ))

        # ── Finding 2: months in practice ────────────────────────────────
        # New providers billing at high volume is a documented fraud pattern.
        # We surface enumeration date so investigators can compare against
        # the billing pattern Vigil already shows.
        months_in_practice: int | None = None
        if enumeration_date:
            try:
                d = datetime.strptime(enumeration_date, "%Y-%m-%d").date()
                months_in_practice = max(
                    0, (datetime.now().date() - d).days // 30,
                )
            except (ValueError, TypeError):
                pass

        if months_in_practice is not None and months_in_practice <= 36:
            # Within 3 years — informational unless they're billing at high volume
            sev = Severity.MEDIUM if months_in_practice <= 24 else Severity.LOW
            out.append(Finding(
                source=self.name,
                severity=sev,
                title=f"New NPI ({months_in_practice} months since enumeration)",
                summary=(
                    f"NPI was enumerated on {enumeration_date} "
                    f"(approx {months_in_practice} months ago).  Cross-reference "
                    f"against billing volume — new providers running at 5× peer "
                    f"median in their first 24 months is a documented fraud signal."
                ),
                url=f"https://npiregistry.cms.hhs.gov/provider-view/{context.npi}",
                date=enumeration_date,
                raw={"enumeration_date": enumeration_date},
            ))

        # ── Finding 3: name mismatch with CMS billing record ────────────
        # Compare the registry's name to what we have in our providers table.
        reg_first = (basic.get("first_name") or "").strip().upper()
        reg_last  = (basic.get("last_name")  or "").strip().upper()
        reg_org   = (basic.get("organization_name") or "").strip().upper()
        billing_first = (context.name_first or "").strip().upper()
        billing_last  = (context.name_last  or "").strip().upper()
        billing_org   = (context.busname or "").strip().upper()

        name_match = True
        if reg_org and billing_org:
            name_match = reg_org == billing_org or reg_org in billing_org or billing_org in reg_org
        elif reg_last and billing_last:
            name_match = reg_last == billing_last
        if not name_match:
            out.append(Finding(
                source=self.name,
                severity=Severity.HIGH,
                title="Registry name differs from billing record",
                summary=(
                    f"CMS NPI Registry has the provider listed as "
                    f'"{reg_org or (reg_first + " " + reg_last).strip()}", but '
                    f"Vigil's billing record uses "
                    f'"{billing_org or (billing_first + " " + billing_last).strip()}".  '
                    f"Discrepancies between billed and registered names are unusual "
                    f"and may indicate a recent registration change or an aliasing "
                    f"scheme.  Verify directly."
                ),
                url=f"https://npiregistry.cms.hhs.gov/provider-view/{context.npi}",
                raw={
                    "registry":  {"first": reg_first, "last": reg_last, "org": reg_org},
                    "billing":   {"first": billing_first, "last": billing_last, "org": billing_org},
                },
            ))

        # ── Finding 4: other names / aliases ──────────────────────────────
        if other_names:
            alias_list = [o.get("organization_name") or o.get("first_name", "")
                          for o in other_names]
            alias_list = [a for a in alias_list if a]
            if alias_list:
                out.append(Finding(
                    source=self.name,
                    severity=Severity.LOW,
                    title=f"{len(alias_list)} additional name(s) on file",
                    summary=(
                        f"NPI has {len(alias_list)} other name(s) registered: "
                        f"{', '.join(alias_list[:5])}{'…' if len(alias_list) > 5 else ''}.  "
                        f"Multiple legal names per NPI are sometimes legitimate "
                        f"(name change, DBA) but worth noting in the context of "
                        f"other findings."
                    ),
                    url=f"https://npiregistry.cms.hhs.gov/provider-view/{context.npi}",
                    raw={"other_names": other_names},
                ))

        # ── Finding 5: multiple practice locations ────────────────────────
        location_addrs = [
            a for a in addresses
            if (a.get("address_purpose") or "").upper() == "LOCATION"
        ]
        if len(location_addrs) >= 3:
            out.append(Finding(
                source=self.name,
                severity=Severity.LOW,
                title=f"{len(location_addrs)} practice locations registered",
                summary=(
                    f"NPI has {len(location_addrs)} active practice locations on "
                    f"file.  Multi-site practices are normal for large organizations, "
                    f"but for individual providers or small businesses this can "
                    f"indicate aggregator / 'phantom location' billing schemes — "
                    f"worth surfacing in conjunction with other risk signals."
                ),
                url=f"https://npiregistry.cms.hhs.gov/provider-view/{context.npi}",
                raw={"locations": location_addrs},
            ))

        # ── Finding 6: positive baseline confirmation ─────────────────────
        # If nothing adverse, surface the verification as INFO so the
        # investigator sees the tool actually ran and confirmed identity.
        if not out:
            spec = (taxonomies[0].get("desc") if taxonomies else None) or "unspecified"
            out.append(Finding(
                source=self.name,
                severity=Severity.INFO,
                title="Registry identity verified",
                summary=(
                    f"Active NPI registered "
                    f"{('on ' + enumeration_date) if enumeration_date else ''}. "
                    f"Primary taxonomy: {spec}.  No registry-level red flags."
                ),
                url=f"https://npiregistry.cms.hhs.gov/provider-view/{context.npi}",
                date=enumeration_date,
                raw=basic,
            ))

        return out
