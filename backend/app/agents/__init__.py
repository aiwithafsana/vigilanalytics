"""Vigil agent runtime — from-scratch agent framework.

Design principles:

1. **The agent owns the loop**, not the LLM.  Tools may internally call LLMs
   (for summarization, classification), but the agent's planning + dispatch +
   state machine are deterministic Python.  This is auditable, cheap, and
   predictable — critical properties for a system whose output becomes
   investigative evidence.

2. **Tools are typed, async, idempotent**.  Each tool declares its inputs and
   outputs.  Two runs against the same target with the same data produce
   identical findings (modulo time-sensitive sources).

3. **Every step is persisted**.  AgentRun records start time, status, errors,
   per-tool durations, and the full finding set.  Reproducibility is a
   first-class requirement — investigators need to be able to point at a run
   and say "this is what we knew on 2026-05-15 at 14:32 UTC."

4. **Audit log integration**.  Every agent run emits an AuditLog entry that
   ties back to the user who triggered it (or "system" for scheduled runs).

5. **Failure isolation**.  One tool's failure never breaks the run.  Partial
   results are valid; the agent records which tools succeeded and which
   didn't.
"""
from app.agents.base import (
    Agent,
    AgentContext,
    AgentRunResult,
    Finding,
    Severity,
    Tool,
    ToolResult,
)

__all__ = [
    "Agent",
    "AgentContext",
    "AgentRunResult",
    "Finding",
    "Severity",
    "Tool",
    "ToolResult",
]
