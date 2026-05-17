"use client";

/**
 * PublicRecordsPanel — surfaces the Public Records agent's findings on the
 * provider detail page.
 *
 * Flow:
 *   1. On mount, fetch the most recent run for this NPI (listAgentRuns).
 *   2. If a recent (<24h) succeeded/partial run exists → render it.
 *      Otherwise auto-trigger a fresh run.
 *   3. When a run is `status=running`, poll every 2s until terminal.
 *   4. Render findings ranked by severity, with per-source breakdown
 *      and "rerun" controls.
 *
 * Each finding is a card with severity badge, source link, summary, and
 * date.  Failed tools are shown collapsed at the bottom so investigators
 * know which sources didn't respond — important for legal-defensibility
 * (the absence of a finding from a tool that returned an error is not the
 * same as a clean run).
 */
import { useCallback, useEffect, useRef, useState } from "react";
import {
  triggerAgentRun, getAgentRun, listAgentRuns,
  type AgentRun, type AgentFinding, type AgentSeverity, type AgentToolResult,
} from "@/lib/api";
import {
  AlertCircle, AlertTriangle, CheckCircle2, Info, Loader2, RefreshCw,
  ExternalLink, ChevronDown, ChevronRight,
} from "lucide-react";

interface Props {
  npi: string;
}

const SEVERITY_CONFIG: Record<AgentSeverity, {
  label:  string;
  cls:    string;
  icon:   typeof AlertCircle;
  order:  number;
}> = {
  critical: { label: "CRITICAL", cls: "text-red-400    bg-red-500/10    border-red-500/30",    icon: AlertCircle,   order: 5 },
  high:     { label: "HIGH",     cls: "text-orange-400 bg-orange-500/10 border-orange-500/25", icon: AlertTriangle, order: 4 },
  medium:   { label: "MEDIUM",   cls: "text-yellow-400 bg-yellow-500/10 border-yellow-500/20", icon: AlertTriangle, order: 3 },
  low:      { label: "LOW",      cls: "text-blue-400   bg-blue-500/10   border-blue-500/20",   icon: Info,          order: 2 },
  info:     { label: "INFO",     cls: "text-slate-400  bg-white/[0.03]  border-white/[0.06]",  icon: CheckCircle2,  order: 1 },
};

const STALE_AFTER_HOURS = 24;

function hoursAgo(iso: string | null | undefined): number | null {
  if (!iso) return null;
  return (Date.now() - new Date(iso).getTime()) / (1000 * 60 * 60);
}

function formatTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  const h = hoursAgo(iso) ?? 0;
  if (h < 1) return `${Math.max(1, Math.floor(h * 60))}m ago`;
  if (h < 24) return `${Math.floor(h)}h ago`;
  return d.toISOString().slice(0, 10);
}

export default function PublicRecordsPanel({ npi }: Props) {
  const [run, setRun]         = useState<AgentRun | null>(null);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [error, setError]     = useState<string | null>(null);
  const [showFailed, setShowFailed] = useState(false);
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Stop polling on unmount
  useEffect(() => () => {
    if (pollRef.current) clearTimeout(pollRef.current);
  }, []);

  // Initial load — most recent run for this NPI; trigger fresh if stale.
  useEffect(() => {
    let alive = true;
    setLoading(true); setError(null);
    listAgentRuns(npi)
      .then((runs) => {
        if (!alive) return;
        const recentDone = runs.find(r =>
          r.workflow === "public_records" &&
          (r.status === "succeeded" || r.status === "partial") &&
          (hoursAgo(r.completed_at) ?? 99) < STALE_AFTER_HOURS,
        );
        if (recentDone) {
          setRun(recentDone);
          setLoading(false);
          return;
        }
        // No recent run → trigger one
        return triggerNewRun();
      })
      .catch(e => alive && setError(e?.message ?? "Failed to load"))
      .finally(() => alive && setLoading(false));
    return () => { alive = false };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [npi]);

  const pollRun = useCallback(async (runId: number) => {
    try {
      const r = await getAgentRun(runId);
      setRun(r);
      if (r.status === "running") {
        pollRef.current = setTimeout(() => pollRun(runId), 2000);
      } else {
        setRunning(false);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Polling failed");
      setRunning(false);
    }
  }, []);

  const triggerNewRun = useCallback(async () => {
    setRunning(true); setError(null);
    try {
      const t = await triggerAgentRun("public_records", npi);
      // Immediately poll
      pollRun(t.agent_run_id);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to start agent");
      setRunning(false);
    }
  }, [npi, pollRun]);

  // ── Render ─────────────────────────────────────────────────────────────────

  if (loading && !run) {
    return (
      <div className="flex items-center gap-2 text-sm text-slate-500 p-6">
        <Loader2 size={14} className="animate-spin" /> Loading public records…
      </div>
    );
  }

  if (error && !run) {
    return (
      <div className="rounded-lg border border-red-500/30 bg-red-500/[0.05] px-4 py-3 text-sm text-red-200">
        {error}
        <button onClick={triggerNewRun} className="ml-3 underline">Retry</button>
      </div>
    );
  }

  const isRunning = running || run?.status === "running";

  return (
    <div className="space-y-4">
      {/* ── Header / status bar ─────────────────────────────────────────── */}
      <div className="flex items-start justify-between gap-3">
        <div className="text-xs text-slate-500">
          {run ? (
            <>
              Last run{" "}
              <span className="text-slate-300">{formatTime(run.completed_at ?? run.started_at)}</span>
              {run.duration_ms != null && (
                <span className="text-slate-600"> · {(run.duration_ms / 1000).toFixed(1)}s</span>
              )}
              {run.n_findings != null && (
                <span className="text-slate-600"> · {run.n_findings} finding{run.n_findings === 1 ? "" : "s"}</span>
              )}
              <div className="text-[11px] text-slate-600 mt-0.5">
                Cross-checked: NPI Registry, SAM.gov debarment list, OIG enforcement actions, federal court records.
                All findings are name-match candidates and require verification.
              </div>
            </>
          ) : "No run yet."}
        </div>
        <button
          onClick={triggerNewRun}
          disabled={isRunning}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-white/[0.08] bg-white/[0.03] hover:bg-white/[0.07] text-slate-300 text-xs disabled:opacity-50 transition"
        >
          {isRunning
            ? <Loader2 size={12} className="animate-spin" />
            : <RefreshCw size={12} />}
          {isRunning ? "Running…" : "Refresh"}
        </button>
      </div>

      {/* ── Per-tool status row ─────────────────────────────────────────── */}
      {run?.result && (
        <ToolStatusRow tools={run.result.tool_results} />
      )}

      {/* ── Findings list ──────────────────────────────────────────────── */}
      {run?.result && run.result.findings.length > 0 ? (
        <div className="space-y-2">
          {run.result.findings.map((f, i) => (
            <FindingCard key={i} finding={f} />
          ))}
        </div>
      ) : run?.result ? (
        <div className="rounded-lg border border-white/[0.06] bg-white/[0.02] px-4 py-3 text-sm text-slate-400">
          No public-record findings.  This means the available sources
          ({run.result.n_tools_succeeded}/{run.result.n_tools_run} returned data)
          did not surface any adverse signals at this time.
        </div>
      ) : (
        <div className="rounded-lg border border-white/[0.06] bg-white/[0.02] px-4 py-3 text-sm text-slate-500">
          Agent is running — first findings will appear here within ~15 seconds.
        </div>
      )}

      {/* ── Failed-tools disclosure ─────────────────────────────────────── */}
      {run?.result && run.result.tool_results.some(t => !t.success) && (
        <div className="rounded-lg border border-white/[0.06] bg-white/[0.02] overflow-hidden">
          <button
            onClick={() => setShowFailed(s => !s)}
            className="w-full flex items-center gap-2 px-4 py-2 text-xs text-slate-500 hover:bg-white/[0.02]"
          >
            {showFailed
              ? <ChevronDown  size={12} />
              : <ChevronRight size={12} />}
            <span>
              {run.result.tool_results.filter(t => !t.success).length} source(s) did not respond
            </span>
            <span className="text-slate-600 text-[10px] ml-auto">
              click to expand
            </span>
          </button>
          {showFailed && (
            <ul className="text-[11px] text-slate-500 px-5 pb-3 space-y-1">
              {run.result.tool_results.filter(t => !t.success).map(t => (
                <li key={t.tool_name}>
                  <span className="text-slate-300">{t.tool_name}</span>
                  {": "}
                  <span className="text-slate-500">{t.error ?? "no detail"}</span>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}


function ToolStatusRow({ tools }: { tools: AgentToolResult[] }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {tools.map(t => (
        <div
          key={t.tool_name}
          className={`text-[10px] uppercase tracking-wider px-2 py-1 rounded border ${
            t.success
              ? (t.n_findings > 0
                  ? "border-orange-500/30 bg-orange-500/[0.06] text-orange-300"
                  : "border-emerald-500/25 bg-emerald-500/[0.04] text-emerald-300")
              : "border-slate-600/30 bg-white/[0.02] text-slate-500"
          }`}
          title={t.error ?? `${t.n_findings} finding(s) · ${t.duration_ms}ms`}
        >
          {t.tool_name}
          {t.success && t.n_findings > 0 && (
            <span className="ml-1.5 font-semibold">{t.n_findings}</span>
          )}
        </div>
      ))}
    </div>
  );
}


function FindingCard({ finding }: { finding: AgentFinding }) {
  const cfg = SEVERITY_CONFIG[finding.severity];
  const Icon = cfg.icon;
  return (
    <div className={`rounded-lg border ${cfg.cls.split(" ").filter(c => c.startsWith("border-") || c.startsWith("bg-")).join(" ")} px-4 py-3`}>
      <div className="flex items-start gap-2.5">
        <Icon size={14} className={`${cfg.cls.split(" ").find(c => c.startsWith("text-"))} shrink-0 mt-0.5`} />
        <div className="flex-1 min-w-0">
          <div className="flex flex-wrap items-center gap-2 mb-1">
            <span className={`text-[10px] font-bold tracking-widest ${cfg.cls.split(" ").find(c => c.startsWith("text-"))}`}>
              {cfg.label}
            </span>
            <span className="text-[11px] text-slate-500">{finding.source}</span>
            {finding.date && (
              <span className="text-[11px] text-slate-600">· {finding.date}</span>
            )}
          </div>
          <div className="text-sm text-slate-200 font-medium mb-1">{finding.title}</div>
          <div className="text-xs text-slate-400 leading-relaxed">{finding.summary}</div>
          {finding.url && (
            <a
              href={finding.url}
              target="_blank"
              rel="noopener noreferrer"
              className="mt-2 inline-flex items-center gap-1 text-[11px] text-blue-400 hover:text-blue-300 transition"
            >
              View source <ExternalLink size={10} />
            </a>
          )}
        </div>
      </div>
    </div>
  );
}
