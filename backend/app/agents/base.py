"""
base.py — Agent runtime primitives.

No external framework dependency.  These classes define the contract that
every Vigil agent and tool implements.  The runtime in ``runtime.py``
operates on these abstractions; new tools and agents plug in by subclassing.
"""
from __future__ import annotations

import abc
import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID


# ── Severity ─────────────────────────────────────────────────────────────────
# Used to rank findings within and across tools.  CRITICAL > HIGH > MEDIUM >
# LOW > INFO.  An agent's overall result severity is the max of its findings.

class Severity(str, Enum):
    CRITICAL = "critical"   # confirmed adverse action (exclusion, conviction)
    HIGH     = "high"       # serious pending action (active lawsuit, board complaint)
    MEDIUM   = "medium"     # contextual but adverse (news mention, prior settlement)
    LOW      = "low"        # informational signal (address change, name variant)
    INFO     = "info"       # neutral confirmation (NPI registered to expected name)

    def numeric(self) -> int:
        """Higher = more severe.  Used for sort/ranking."""
        return {"critical": 5, "high": 4, "medium": 3, "low": 2, "info": 1}[self.value]


# ── Finding ──────────────────────────────────────────────────────────────────
# A single piece of evidence the agent surfaces.  All structured the same way
# regardless of which tool produced it, so the UI can render uniformly.

@dataclass
class Finding:
    source:    str            # human-readable tool name, e.g. "SAM.gov debarment"
    severity:  Severity
    title:     str            # one-line headline
    summary:   str            # 1-3 sentence detail
    url:       str | None = None    # link to authoritative source if any
    date:      str | None = None    # ISO date when the event occurred (not when we found it)
    raw:       dict | None = field(default=None, repr=False)   # full source payload, for audit

    def to_dict(self) -> dict:
        return {
            "source":   self.source,
            "severity": self.severity.value,
            "title":    self.title,
            "summary":  self.summary,
            "url":      self.url,
            "date":     self.date,
            "raw":      self.raw,
        }


# ── ToolResult ───────────────────────────────────────────────────────────────
# Output of a single tool run.  Always returned — even on failure — so the
# agent can report which tools worked and which didn't.

@dataclass
class ToolResult:
    tool_name:    str
    success:      bool
    findings:     list[Finding] = field(default_factory=list)
    error:        str | None    = None
    duration_ms:  int           = 0
    raw_response: Any | None    = field(default=None, repr=False)

    def to_dict(self) -> dict:
        return {
            "tool_name":    self.tool_name,
            "success":      self.success,
            "findings":     [f.to_dict() for f in self.findings],
            "error":        self.error,
            "duration_ms":  self.duration_ms,
            "n_findings":   len(self.findings),
        }


# ── AgentContext ─────────────────────────────────────────────────────────────
# Inputs passed to every tool in a single agent run.  Subclass to add
# workflow-specific context.

@dataclass
class AgentContext:
    npi:       str
    name_last:  str | None = None
    name_first: str | None = None
    busname:    str | None = None
    specialty:  str | None = None
    state:      str | None = None
    city:       str | None = None
    # The user who triggered this run (None = scheduled / system)
    triggered_by_user_id: UUID | None = None

    def display_name(self) -> str:
        """Best-effort human name for the target."""
        if self.busname:
            return self.busname
        if self.name_first or self.name_last:
            return f"{self.name_first or ''} {self.name_last or ''}".strip()
        return f"NPI {self.npi}"


# ── Tool ─────────────────────────────────────────────────────────────────────
# Every external-data lookup is a Tool.  Subclasses implement ``_run`` and
# the runtime handles timing, error capture, and ToolResult construction.

class Tool(abc.ABC):
    """One unit of external-data lookup.  Stateless, idempotent, async."""

    # Human-readable name shown in UI and audit logs
    name: str = "tool"

    # One-line description of what this tool does
    description: str = ""

    # Per-run timeout — tools that hang must not block the agent forever
    timeout_seconds: float = 20.0

    # Whether to retry on transient errors (network, 5xx).  Default: yes once.
    max_retries: int = 1

    async def execute(self, context: AgentContext) -> ToolResult:
        """
        Public entry point.  Wraps ``_run`` with timing, timeout, retry, and
        error capture.  Always returns a ToolResult — never raises.
        """
        start = time.perf_counter()
        last_error: str | None = None

        for attempt in range(self.max_retries + 1):
            try:
                async with asyncio.timeout(self.timeout_seconds):
                    findings, raw = await self._run(context)
                return ToolResult(
                    tool_name=self.name,
                    success=True,
                    findings=findings,
                    duration_ms=int((time.perf_counter() - start) * 1000),
                    raw_response=raw,
                )
            except asyncio.TimeoutError:
                last_error = f"timeout after {self.timeout_seconds}s"
            except Exception as e:
                last_error = f"{type(e).__name__}: {e}"
            if attempt < self.max_retries:
                # Linear backoff: 1s, 2s, 3s …
                await asyncio.sleep(1 + attempt)

        return ToolResult(
            tool_name=self.name,
            success=False,
            error=last_error,
            duration_ms=int((time.perf_counter() - start) * 1000),
        )

    @abc.abstractmethod
    async def _run(self, context: AgentContext) -> tuple[list[Finding], Any]:
        """
        Implement the actual lookup.  Return (findings, raw_response).

        The runtime guarantees ``execute`` will catch exceptions; you can
        raise freely.  But don't catch your own — failure is informative.
        """
        ...


# ── AgentRunResult ───────────────────────────────────────────────────────────
# Final output of an agent run.  Persisted to the agent_runs table.

@dataclass
class AgentRunResult:
    workflow:      str
    target_type:   str       # 'provider', 'case', etc.
    target_id:     str       # NPI for providers
    started_at:    datetime
    completed_at:  datetime
    duration_ms:   int
    tool_results:  list[ToolResult]
    findings:      list[Finding]    # aggregated + ranked across all tools
    success:       bool             # True if at least one tool succeeded
    triggered_by_user_id: UUID | None = None

    @property
    def n_tools_run(self) -> int:
        return len(self.tool_results)

    @property
    def n_tools_succeeded(self) -> int:
        return sum(1 for r in self.tool_results if r.success)

    @property
    def n_findings(self) -> int:
        return len(self.findings)

    @property
    def max_severity(self) -> Severity:
        if not self.findings:
            return Severity.INFO
        return max(self.findings, key=lambda f: f.severity.numeric()).severity

    def to_dict(self) -> dict:
        return {
            "workflow":     self.workflow,
            "target_type":  self.target_type,
            "target_id":    self.target_id,
            "started_at":   self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "duration_ms":  self.duration_ms,
            "success":      self.success,
            "n_tools_run":  self.n_tools_run,
            "n_tools_succeeded": self.n_tools_succeeded,
            "n_findings":   self.n_findings,
            "max_severity": self.max_severity.value,
            "tool_results": [r.to_dict() for r in self.tool_results],
            "findings":     [f.to_dict() for f in self.findings],
            "triggered_by_user_id": str(self.triggered_by_user_id) if self.triggered_by_user_id else None,
        }


# ── Agent ────────────────────────────────────────────────────────────────────
# Subclass to define a workflow.  The default implementation dispatches all
# tools in parallel and ranks the combined findings by severity, then date.

class Agent(abc.ABC):
    """A multi-tool investigative workflow."""

    name: str = "agent"
    description: str = ""
    target_type: str = "provider"

    @property
    @abc.abstractmethod
    def tools(self) -> list[Tool]:
        """The tools this agent uses.  Subclasses define."""
        ...

    async def plan(self, context: AgentContext) -> list[Tool]:
        """
        Decide which subset of ``self.tools`` to run for this context.

        Default: run all of them.  Override to skip tools that don't apply —
        e.g., skip ``CaliforniaMedicalBoard`` for a Texas provider.
        """
        return list(self.tools)

    async def aggregate(self, results: list[ToolResult]) -> list[Finding]:
        """
        Collect findings from successful tools and rank them.

        Default ranking: severity descending, then date descending.
        Subclasses can override for workflow-specific dedup or scoring.
        """
        all_findings = [f for r in results if r.success for f in r.findings]
        all_findings.sort(
            key=lambda f: (-f.severity.numeric(), f.date or ""),
            reverse=False,
        )
        return all_findings

    async def run(self, context: AgentContext) -> AgentRunResult:
        """
        Execute the workflow.  Returns AgentRunResult on success or partial.

        Never raises — even a complete tool-suite failure produces a valid
        AgentRunResult with success=False.
        """
        start = datetime.now(timezone.utc)
        t0 = time.perf_counter()

        try:
            selected = await self.plan(context)
        except Exception:
            # Plan failure is an unusual case — fall back to all tools
            selected = list(self.tools)

        # Dispatch all selected tools in parallel
        if selected:
            tool_results = await asyncio.gather(
                *(t.execute(context) for t in selected),
                return_exceptions=False,    # execute() never raises
            )
        else:
            tool_results = []

        findings = await self.aggregate(tool_results)

        return AgentRunResult(
            workflow=self.name,
            target_type=self.target_type,
            target_id=context.npi,
            started_at=start,
            completed_at=datetime.now(timezone.utc),
            duration_ms=int((time.perf_counter() - t0) * 1000),
            tool_results=tool_results,
            findings=findings,
            success=any(r.success for r in tool_results),
            triggered_by_user_id=context.triggered_by_user_id,
        )
