"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { getAlerts } from "@/lib/api";
import { fmt } from "@/lib/utils";
import type { AlertItem, AlertResponse } from "@/types";
import { Bell, AlertTriangle, AlertCircle, Info, ExternalLink, RefreshCw } from "lucide-react";

// ── Severity helpers ──────────────────────────────────────────────────────────

const SEV_CONFIG = {
  1: {
    label: "CRITICAL",
    icon: AlertTriangle,
    bg: "bg-red-500/10 border-red-500/25",
    text: "text-red-400",
    dot: "bg-red-500",
  },
  2: {
    label: "HIGH",
    icon: AlertCircle,
    bg: "bg-orange-500/10 border-orange-500/25",
    text: "text-orange-400",
    dot: "bg-orange-400",
  },
  3: {
    label: "MEDIUM",
    icon: Info,
    bg: "bg-yellow-500/10 border-yellow-500/20",
    text: "text-yellow-400",
    dot: "bg-yellow-400",
  },
} as const;

function getSev(sev: number) {
  return SEV_CONFIG[sev as keyof typeof SEV_CONFIG] ?? SEV_CONFIG[3];
}

// Flag type → readable label
const FLAG_LABELS: Record<string, string> = {
  billing_volume: "Billing Volume Outlier",
  upcoding: "E&M Upcoding",
  impossible_hours: "Impossible Hours Billed",
  wrong_specialty: "Wrong Specialty Billing",
  leie_match: "LEIE Exclusion Match",
  opt_out_billing: "Opt-Out Provider Billing",
  referral_cluster: "Referral Cluster",
  hub_spoke: "Hub-and-Spoke Ring",
  address_cluster: "Address Cluster",
  yoy_surge: "Year-over-Year Surge",
  new_provider_spike: "New Provider Spike",
  billing_cliff: "Billing Cliff",
  deceased_patient: "Deceased Patient Billing",
  npi_reuse: "NPI Reuse",
};

function fmtRelTime(iso: string): string {
  const d = new Date(iso);
  const diff = Date.now() - d.getTime();
  const h = Math.floor(diff / 3_600_000);
  if (h < 1) return "just now";
  if (h < 24) return `${h}h ago`;
  const days = Math.floor(h / 24);
  if (days < 7) return `${days}d ago`;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

// ── Page component ─────────────────────────────────────────────────────────────

export default function AlertsPage() {
  const [data, setData] = useState<AlertResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<number | undefined>(undefined);
  const [refreshing, setRefreshing] = useState(false);

  async function load(sev?: number) {
    try {
      const res = await getAlerts(sev);
      setData(res);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }

  useEffect(() => { load(filter); }, [filter]);

  function handleRefresh() {
    setRefreshing(true);
    load(filter);
  }

  const since = data?.since
    ? new Date(data.since).toLocaleDateString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })
    : null;

  return (
    <AppShell>
      <div className="p-8 max-w-4xl mx-auto fade-in">
        {/* Header */}
        <div className="flex items-start justify-between mb-6">
          <div>
            <div className="flex items-center gap-2.5 mb-1">
              <Bell size={18} className="text-slate-400" />
              <h1 className="text-xl font-bold text-slate-100">Alert Feed</h1>
              {data && data.total > 0 && (
                <span className="bg-red-500/20 text-red-400 text-xs font-mono px-2 py-0.5 rounded border border-red-500/30">
                  {data.total} new
                </span>
              )}
            </div>
            <p className="text-sm text-slate-500">
              {since ? `New detection signals since your last login · ${since}` : "All recent detection signals"}
            </p>
          </div>
          <button
            onClick={handleRefresh}
            disabled={refreshing}
            className="flex items-center gap-1.5 text-xs text-slate-500 hover:text-slate-300 transition disabled:opacity-50"
          >
            <RefreshCw size={12} className={refreshing ? "animate-spin" : ""} />
            Refresh
          </button>
        </div>

        {/* Severity filter */}
        <div className="flex gap-2 mb-6">
          {([undefined, 1, 2, 3] as const).map((sev) => (
            <button
              key={String(sev)}
              onClick={() => setFilter(sev)}
              className={`px-3 py-1.5 rounded-lg text-xs font-mono transition border ${
                filter === sev
                  ? "bg-white/[0.08] border-white/[0.12] text-slate-200"
                  : "bg-white/[0.02] border-white/[0.06] text-slate-500 hover:text-slate-300"
              }`}
            >
              {sev === undefined ? "All" : getSev(sev).label}
            </button>
          ))}
        </div>

        {/* Content */}
        {loading ? (
          <div className="text-slate-600 text-sm animate-pulse">Loading alerts…</div>
        ) : !data || data.items.length === 0 ? (
          <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl p-12 text-center">
            <Bell size={32} className="text-slate-700 mx-auto mb-3" />
            <p className="text-slate-500 text-sm font-medium">No new alerts</p>
            <p className="text-slate-600 text-xs mt-1">
              {since ? `No new signals since ${since}` : "All clear — no detection signals match your filters."}
            </p>
          </div>
        ) : (
          <div className="space-y-3">
            {data.items.map((alert) => (
              <AlertCard key={alert.flag_id} alert={alert} />
            ))}
          </div>
        )}
      </div>
    </AppShell>
  );
}

function AlertCard({ alert }: { alert: AlertItem }) {
  const sev = getSev(alert.severity);
  const SevIcon = sev.icon;
  const flagLabel = FLAG_LABELS[alert.flag_type] ?? alert.flag_type.replace(/_/g, " ");

  return (
    <div className={`border rounded-xl p-4 ${sev.bg} transition hover:border-opacity-50`}>
      <div className="flex items-start gap-3">
        {/* Severity icon */}
        <div className="shrink-0 mt-0.5">
          <SevIcon size={16} className={sev.text} />
        </div>

        {/* Content */}
        <div className="flex-1 min-w-0">
          <div className="flex items-start justify-between gap-2 mb-1">
            <div>
              <span className={`text-[10px] uppercase tracking-widest font-mono font-bold ${sev.text}`}>
                {sev.label}
              </span>
              <span className="text-[10px] text-slate-600 ml-2">·</span>
              <span className="text-[10px] text-slate-500 ml-2 font-mono">{flagLabel}</span>
            </div>
            <span className="text-[10px] text-slate-600 font-mono shrink-0">
              {fmtRelTime(alert.created_at)}
            </span>
          </div>

          {/* Provider */}
          <p className="text-sm font-semibold text-slate-200 mb-1">
            {alert.provider_name ?? alert.npi}
            {alert.state && (
              <span className="text-slate-500 font-normal ml-2 text-xs">· {alert.state}</span>
            )}
            {alert.specialty && (
              <span className="text-slate-500 font-normal ml-1 text-xs">· {alert.specialty}</span>
            )}
          </p>

          {/* Explanation */}
          {alert.explanation && (
            <p className="text-xs text-slate-400 leading-relaxed mb-2">{alert.explanation}</p>
          )}

          {/* Metadata row */}
          <div className="flex items-center gap-4 text-[10px] font-mono">
            {alert.risk_score != null && (
              <span className="text-slate-500">
                Risk: <span className="text-slate-300">{Number(alert.risk_score).toFixed(0)}</span>
              </span>
            )}
            {alert.estimated_overpayment != null && (
              <span className="text-slate-500">
                Est. Overpayment: <span className="text-orange-400">{fmt(alert.estimated_overpayment)}</span>
              </span>
            )}
            <Link
              href={`/providers/${alert.npi}`}
              className="text-blue-400 hover:text-blue-300 flex items-center gap-0.5 ml-auto"
            >
              View Provider <ExternalLink size={9} className="ml-0.5" />
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
