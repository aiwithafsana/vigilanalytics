"use client";

/**
 * CaseWatchDigestWidget — dashboard panel showing what's new on the
 * investigator's open cases since last check.
 *
 * Powered by the nightly case-watch sweep (see backend/app/services/case_watch.py).
 * Each update is one case where new public-record findings appeared since the
 * previous run.  Click through to the case page or to the provider record.
 *
 * Empty state ("no updates this week") is intentionally low-key — silence is
 * good news here.  We don't want to draw attention to it when nothing happened.
 */
import { useEffect, useState } from "react";
import Link from "next/link";
import {
  AlertCircle, AlertTriangle, CheckCircle2, ChevronRight, Clock,
  Info, Loader2, RadioTower,
} from "lucide-react";
import {
  getCaseWatchDigest, type CaseWatchDigest, type AgentSeverity,
} from "@/lib/api";

const SEVERITY_CONFIG: Record<AgentSeverity, {
  icon:  typeof AlertCircle;
  cls:   string;
  order: number;
}> = {
  critical: { icon: AlertCircle,   cls: "text-red-400",    order: 5 },
  high:     { icon: AlertTriangle, cls: "text-orange-400", order: 4 },
  medium:   { icon: AlertTriangle, cls: "text-yellow-400", order: 3 },
  low:      { icon: Info,          cls: "text-blue-400",   order: 2 },
  info:     { icon: CheckCircle2,  cls: "text-slate-400",  order: 1 },
};

function formatTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const ms = Date.now() - new Date(iso).getTime();
  if (ms < 60_000) return "just now";
  const min = Math.floor(ms / 60_000);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const d = Math.floor(hr / 24);
  return `${d}d ago`;
}

export default function CaseWatchDigestWidget() {
  const [digest, setDigest] = useState<CaseWatchDigest | null>(null);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    getCaseWatchDigest(168)
      .then((d) => alive && setDigest(d))
      .catch((e) => alive && setError(e?.message ?? "Failed to load"))
      .finally(() => alive && setLoading(false));
    return () => { alive = false; };
  }, []);

  if (loading) {
    return (
      <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
        <div className="flex items-center gap-2 text-xs text-slate-500">
          <Loader2 size={14} className="animate-spin" />
          Loading case-watch digest…
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white/[0.03] border border-red-500/20 rounded-xl p-5 text-xs text-red-300">
        Couldn&apos;t load case-watch digest: {error}
      </div>
    );
  }

  if (!digest || digest.n_open_cases === 0) {
    // No cases → don't show the widget.  An MFCU analyst opening Vigil for
    // the first time shouldn't see an "0 of 0 cases" widget — it's confusing.
    return null;
  }

  const noUpdates = digest.n_cases_with_updates === 0;

  return (
    <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl overflow-hidden">
      {/* Header */}
      <div className="flex items-center gap-2 px-5 py-3 border-b border-white/[0.05]">
        <RadioTower size={14} className="text-emerald-400" />
        <span className="text-xs uppercase tracking-widest text-slate-500 font-medium flex-1">
          Case watch
        </span>
        <span className="text-[10px] text-slate-600 font-mono">
          last 7 days
        </span>
      </div>

      {noUpdates ? (
        <div className="px-5 py-4">
          <div className="flex items-center gap-2 text-sm text-slate-300 mb-1">
            <CheckCircle2 size={14} className="text-emerald-400 shrink-0" />
            <span>No new external signals this week.</span>
          </div>
          <p className="text-xs text-slate-500">
            Vigil checked {digest.n_open_cases} open case{digest.n_open_cases === 1 ? "" : "s"}{" "}
            against federal records overnight; nothing changed.
          </p>
        </div>
      ) : (
        <>
          <div className="px-5 py-3 bg-amber-500/[0.05] border-b border-amber-500/[0.15]">
            <div className="flex items-baseline gap-2">
              <span className="text-2xl font-black font-mono text-amber-200">
                {digest.n_cases_with_updates}
              </span>
              <span className="text-xs text-amber-100/80">
                of your {digest.n_open_cases} open case{digest.n_open_cases === 1 ? "" : "s"}{" "}
                had new external signals this week
              </span>
            </div>
          </div>

          <ul className="divide-y divide-white/[0.04]">
            {digest.updates.slice(0, 8).map((update) => {
              const sev = SEVERITY_CONFIG[update.max_severity];
              const Icon = sev.icon;
              const top = update.new_findings[0];
              return (
                <li key={update.case_id} className="px-5 py-3 hover:bg-white/[0.02] transition">
                  <Link
                    href={`/cases/${update.case_id}`}
                    className="flex items-start gap-3 text-left"
                  >
                    <Icon size={14} className={`${sev.cls} mt-0.5 shrink-0`} />
                    <div className="flex-1 min-w-0">
                      <div className="flex items-baseline gap-2 mb-0.5">
                        <span className="text-sm font-mono text-slate-200">
                          {update.case_number ?? `Case ${update.case_id}`}
                        </span>
                        <span className="text-[10px] text-slate-600 font-mono">
                          NPI {update.provider_npi}
                        </span>
                      </div>
                      <p className="text-xs text-slate-400 truncate">
                        {update.n_new_findings} new finding{update.n_new_findings === 1 ? "" : "s"}
                        {top && <> · {top.source}: <span className="text-slate-300">{top.title}</span></>}
                      </p>
                      <p className="text-[11px] text-slate-600 mt-0.5 flex items-center gap-1">
                        <Clock size={10} /> {formatTime(update.ran_at)}
                      </p>
                    </div>
                    <ChevronRight size={14} className="text-slate-600 shrink-0 mt-1" />
                  </Link>
                </li>
              );
            })}
          </ul>

          {digest.updates.length > 8 && (
            <div className="px-5 py-2 bg-white/[0.02] text-center">
              <Link
                href="/cases?has_updates=true"
                className="text-[11px] text-slate-500 hover:text-slate-300"
              >
                +{digest.updates.length - 8} more — view all
              </Link>
            </div>
          )}
        </>
      )}
    </div>
  );
}
