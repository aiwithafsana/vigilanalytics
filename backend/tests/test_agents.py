"""
test_agents.py — Agent runtime correctness.

Tests the framework, not the external tools.  We use stub Tool subclasses
that produce deterministic results so the runtime's behaviour (parallelism,
error isolation, severity ranking, persistence) can be tested without
depending on the live external APIs.
"""
import asyncio

import pytest

from app.agents.base import Agent, AgentContext, Finding, Severity, Tool


# ── Stub tools ───────────────────────────────────────────────────────────────

class _SucceedingTool(Tool):
    name = "stub_success"

    async def _run(self, context):
        return [
            Finding(source=self.name, severity=Severity.HIGH,
                    title="found something", summary="x"),
        ], {"ok": True}


class _FailingTool(Tool):
    name = "stub_fail"
    max_retries = 0   # don't retry — keep tests fast

    async def _run(self, context):
        raise RuntimeError("intentional failure")


class _TimeoutTool(Tool):
    name = "stub_timeout"
    timeout_seconds = 0.2
    max_retries = 0

    async def _run(self, context):
        await asyncio.sleep(5)
        return [], None


class _CriticalTool(Tool):
    name = "stub_critical"

    async def _run(self, context):
        return [
            Finding(source=self.name, severity=Severity.CRITICAL,
                    title="big problem", summary="y", date="2025-01-01"),
            Finding(source=self.name, severity=Severity.LOW,
                    title="small problem", summary="z", date="2024-06-01"),
        ], None


class _MultiToolAgent(Agent):
    name = "multi_stub"

    @property
    def tools(self):
        return [_SucceedingTool(), _FailingTool(), _CriticalTool(), _TimeoutTool()]


# ── Tests ─────────────────────────────────────────────────────────────────────

async def test_agent_dispatches_all_tools_in_parallel():
    """All tools must run; results returned regardless of individual outcomes."""
    agent = _MultiToolAgent()
    ctx = AgentContext(npi="1234567890", name_last="Smith")
    result = await agent.run(ctx)

    assert result.n_tools_run == 4
    assert result.n_tools_succeeded == 2   # success + critical succeed; fail + timeout fail
    assert result.success is True            # at least one tool succeeded


async def test_failing_tool_does_not_crash_agent():
    agent = _MultiToolAgent()
    ctx = AgentContext(npi="1234567890")
    result = await agent.run(ctx)
    fail_tr = next(r for r in result.tool_results if r.tool_name == "stub_fail")
    assert fail_tr.success is False
    assert "intentional failure" in (fail_tr.error or "")


async def test_timeout_is_enforced_and_caught():
    """Tool that hangs longer than its timeout returns failure, not block forever."""
    import time
    agent = _MultiToolAgent()
    ctx = AgentContext(npi="1234567890")
    t0 = time.perf_counter()
    result = await agent.run(ctx)
    elapsed = time.perf_counter() - t0
    # 5-second tool with 0.2s timeout should not block the agent for 5s
    assert elapsed < 1.5
    timeout_tr = next(r for r in result.tool_results if r.tool_name == "stub_timeout")
    assert timeout_tr.success is False
    assert "timeout" in (timeout_tr.error or "").lower()


async def test_findings_aggregated_and_ranked_by_severity():
    """Aggregated findings should be sorted CRITICAL → HIGH → MEDIUM → LOW → INFO."""
    agent = _MultiToolAgent()
    result = await agent.run(AgentContext(npi="1234567890"))
    severities = [f.severity for f in result.findings]
    # First finding should be critical
    assert severities[0] == Severity.CRITICAL
    # And max_severity reports critical
    assert result.max_severity == Severity.CRITICAL


async def test_agent_short_circuits_when_plan_returns_empty():
    """plan() returning [] should produce a valid empty result, not crash."""
    class _NoToolsAgent(Agent):
        name = "no_tools"

        @property
        def tools(self):
            return [_SucceedingTool()]

        async def plan(self, context):
            return []   # skip everything

    result = await _NoToolsAgent().run(AgentContext(npi="1234567890"))
    assert result.n_tools_run == 0
    assert result.n_findings == 0
    assert result.success is False   # no tools ran = no success
    assert result.max_severity == Severity.INFO


async def test_serialization_round_trip():
    """to_dict output should be JSON-serialisable for storage in result_json."""
    import json
    agent = _MultiToolAgent()
    result = await agent.run(AgentContext(npi="1234567890"))
    serialized = json.dumps(result.to_dict())
    parsed = json.loads(serialized)
    assert parsed["workflow"] == "multi_stub"
    assert parsed["max_severity"] == "critical"
    assert isinstance(parsed["tool_results"], list)
    assert isinstance(parsed["findings"], list)


# ── API endpoint smoke tests ──────────────────────────────────────────────────

async def test_unknown_workflow_returns_404(client, admin_headers):
    resp = await client.post(
        "/api/agents/does_not_exist/run?npi=1234567890",
        headers=admin_headers,
    )
    assert resp.status_code == 404


async def test_run_for_unknown_npi_returns_404(client, admin_headers):
    # Use an NPI we know doesn't exist in the test DB
    resp = await client.post(
        "/api/agents/public_records/run?npi=9999999999",
        headers=admin_headers,
    )
    assert resp.status_code == 404


async def test_fetch_nonexistent_run_returns_404(client, admin_headers):
    resp = await client.get("/api/agents/runs/9999999", headers=admin_headers)
    assert resp.status_code == 404


async def test_list_runs_for_target_returns_empty(client, admin_headers):
    """Empty list for an NPI with no runs."""
    resp = await client.get(
        "/api/agents/runs?target_id=1234567890",
        headers=admin_headers,
    )
    assert resp.status_code == 200
    assert resp.json() == []
