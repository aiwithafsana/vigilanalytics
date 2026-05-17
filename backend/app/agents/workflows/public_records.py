"""
public_records.py — Public Records aggregator agent.

The first concrete Vigil agent.  Given an NPI + provider metadata, runs
three external-data lookups in parallel and ranks the combined findings by
severity.  Used by investigators to compress 30-45 minutes of manual
research into a 30-second background task.

What it surfaces
----------------
- Active or expired federal exclusions (SAM.gov)
- OIG enforcement press releases naming the provider
- Federal court dockets (civil or criminal) naming the provider

What it does NOT do
-------------------
- Make any determination about the provider's culpability
- Verify name-match correctness (it explicitly flags every finding as
  "name match — verify before relying")
- Touch any PHI or restricted-access data
- Cite findings without source URLs

Output is rendered in the provider detail page under the existing AI Brief
tab; each finding links to the authoritative source.
"""
from app.agents.base import Agent, AgentContext, Tool
from app.agents.tools.sam_gov import SamGovExclusionsTool
from app.agents.tools.oig_enforcement import OigEnforcementTool
from app.agents.tools.courtlistener import CourtListenerTool
from app.agents.tools.npi_registry import NpiRegistryTool


class PublicRecordsAgent(Agent):
    name = "public_records"
    description = "Cross-references provider against federal public records (NPI registry, SAM.gov, OIG, courts)"
    target_type = "provider"

    @property
    def tools(self) -> list[Tool]:
        return [
            # NpiRegistry first — fastest, always returns a result, anchors
            # the investigator's view with verified identity baseline.
            NpiRegistryTool(),
            SamGovExclusionsTool(),
            OigEnforcementTool(),
            CourtListenerTool(),
        ]

    async def plan(self, context: AgentContext) -> list[Tool]:
        """
        All three tools apply nationally — no per-tool filtering needed for v1.

        Override targets for future workflows:
          - Skip federal-only tools for an explicitly state-Medicaid run
          - Add state-level medical-board tools when the provider's state has one
        """
        # If we don't have any name data, no point running anything — short-circuit
        if not (context.busname or context.name_last or context.name_first):
            return []
        return list(self.tools)
