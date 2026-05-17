"use client";

/**
 * AuditTrail — chain-of-custody timeline for a specific target (provider or case).
 *
 * Shows every recorded action against this entity: views, exports, attestations,
 * outcome changes, etc.  Required by methodology doc §10 (Data chain of custody)
 * — investigators must be able to see who has touched a record before they
 * add their own findings, both for collaboration and for legal admissibility.
 */
import { useEffect, useState } from "react";
import { getAuditTimeline, type AuditLogItem } from "@/lib/api";
import { Activity, ChevronDown, ChevronRight, Loader2, Flag } from "lucide-react";

interface Props {
  targetType: "provider" | "case";
  targetId:   string;
  /** Initial-state collapsed/expanded.  Default: collapsed for less noise. */
  defaultOpen?: boolean;
  /**
   * When true, surface a "first-to-view" banner if no other users have looked
   * at this record yet.  For qui tam attorneys this signals first-to-file
   * eligibility — worth ~25-30% of recovery in FCA cases.
   */
  highlightFirstView?: boolean;
}

const ACTION_LABEL: Record<string, string> = {
  // ── Provider / case views ─────────────────────────────────────────────
  "view_provider":           "viewed provider",
  "view_provider_flags":     "viewed fraud flags",
  "view_provider_billing":   "viewed billing",
  // ── Exports ───────────────────────────────────────────────────────────
  "export_provider_pdf":     "exported PDF report",
  "export_csv":              "exported CSV",
  // ── Cases ─────────────────────────────────────────────────────────────
  "create_case":             "opened investigation",
  "update_case":             "updated case",
  "add_note":                "added note",
  "upload_document":         "uploaded document",
  "record_outcome":          "recorded outcome",
  // ── Attestations ──────────────────────────────────────────────────────
  "attestation":             "attested to methodology",
  // ── Auth ─────────────────────────────────────────────────────────────
  "login":                   "signed in",
  "login_mfa_challenge_issued": "started MFA login",
  "login_mfa_failed":        "failed MFA verification",
  "mfa_setup_started":       "started MFA enrollment",
  "mfa_enabled":             "enabled MFA",
  "mfa_disabled":            "disabled MFA",
};

function relativeTime(iso: string): string {
  const ms = Date.now() - new Date(iso).getTime();
  if (ms < 0) return "just now";
  const min = Math.floor(ms / 60_000);
  if (min < 1)  return "just now";
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const d = Math.floor(hr / 24);
  if (d < 30) return `${d}d ago`;
  return new Date(iso).toISOString().slice(0, 10);
}

function actionLabel(action: string): string {
  return ACTION_LABEL[action] ?? action.replace(/_/g, " ");
}

export default function AuditTrail({
  targetType,
  targetId,
  defaultOpen = false,
  highlightFirstView = false,
}: Props) {
  const [open, setOpen]       = useState(defaultOpen);
  const [items, setItems]     = useState<AuditLogItem[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError]     = useState<string | null>(null);

  // Lazy load when expanded — OR eagerly when first-to-view banner is needed.
  // The banner needs the count immediately on mount; lazy load doesn't deliver
  // until the user clicks "Audit trail", which defeats the signal.
  const needsEager = highlightFirstView;
  useEffect(() => {
    if (items !== null) return;
    if (!open && !needsEager) return;
    let alive = true;
    // Loading flag for async fetch — React 19's stricter set-state-in-effect
    // rule flags this, but the pattern is intentional: we set loading=true
    // before kicking off the request and clear it on settle.  Deferring via
    // queueMicrotask would only add render latency.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setLoading(true);
    getAuditTimeline(targetType, targetId)
      .then(t => alive && setItems(t.items))
      .catch(e => alive && setError(e?.message ?? "Failed to load audit trail"))
      .finally(() => alive && setLoading(false));
    return () => { alive = false };
  }, [open, items, targetType, targetId, needsEager]);

  // First-to-view: no other users have touched this record at all.  This is
  // an approximation — true first-to-file would also require knowing whether
  // any user at another firm/agency viewed, which Vigil cannot guarantee.
  // What the banner truthfully says: "no Vigil user has touched this yet."
  const isFirstView = highlightFirstView && items !== null && items.length === 0;

  return (
    <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl overflow-hidden">
      {/* First-to-view banner — high-value signal for qui tam attorneys. */}
      {isFirstView && (
        <div className="flex items-center gap-2 px-5 py-2.5 bg-emerald-500/[0.08] border-b border-emerald-500/25 text-emerald-200">
          <Flag size={14} className="shrink-0" />
          <div className="text-xs">
            <span className="font-semibold">First Vigil access.</span>{" "}
            No prior user has viewed or exported this record.
            <span className="text-emerald-300/80 ml-1">
              Relevant for first-to-file qui tam eligibility.
            </span>
          </div>
        </div>
      )}

      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center gap-2 px-5 py-3 text-left hover:bg-white/[0.02] transition"
      >
        {open ? (
          <ChevronDown size={14} className="text-slate-500" />
        ) : (
          <ChevronRight size={14} className="text-slate-500" />
        )}
        <Activity size={14} className="text-slate-500" />
        <span className="text-xs uppercase tracking-widest text-slate-500 font-medium">
          Audit trail
        </span>
        {items && (
          <span className="text-[11px] text-slate-600 ml-auto">
            {items.length} {items.length === 1 ? "event" : "events"}
          </span>
        )}
      </button>

      {open && (
        <div className="border-t border-white/[0.06] px-5 py-3">
          {loading && (
            <div className="flex items-center gap-2 text-xs text-slate-500 py-3">
              <Loader2 size={12} className="animate-spin" /> Loading…
            </div>
          )}

          {error && (
            <p className="text-xs text-red-400 py-2">{error}</p>
          )}

          {items && items.length === 0 && !loading && (
            <p className="text-xs text-slate-500 py-2">
              No recorded actions for this {targetType}.
            </p>
          )}

          {items && items.length > 0 && (
            <ol className="space-y-2.5 mt-1 mb-1">
              {items.map(it => {
                const v = it.details?.methodology_version as string | undefined;
                const score = it.details?.risk_score_at_export as number | undefined;
                return (
                  <li key={it.id} className="flex items-start gap-3 text-xs">
                    <div className="w-1.5 h-1.5 rounded-full bg-slate-500 mt-1.5 shrink-0" />
                    <div className="flex-1 min-w-0">
                      <div className="text-slate-300">
                        <span className="font-medium text-slate-200">
                          {it.user_name ?? "Unknown user"}
                        </span>{" "}
                        <span className="text-slate-400">{actionLabel(it.action)}</span>
                      </div>
                      <div className="text-[11px] text-slate-500 mt-0.5 flex flex-wrap gap-x-3">
                        <span title={it.created_at}>{relativeTime(it.created_at)}</span>
                        {it.ip_address && <span>IP {it.ip_address}</span>}
                        {v && <span>model v{v}</span>}
                        {score !== undefined && score !== null && (
                          <span>score {Number(score).toFixed(1)}</span>
                        )}
                      </div>
                    </div>
                  </li>
                );
              })}
            </ol>
          )}
        </div>
      )}
    </div>
  );
}
