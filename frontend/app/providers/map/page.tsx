"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { getProviderMap } from "@/lib/api";
import { fmt, fmtNum } from "@/lib/utils";
import type { ProviderMapPoint } from "@/types";
import { Map, ArrowUpDown, ExternalLink } from "lucide-react";

// ── Risk colour helpers ───────────────────────────────────────────────────────

function riskColor(pct: number): string {
  // pct = high_risk_count / total_providers * 100
  if (pct >= 20) return "bg-red-500/20 border-red-500/30 text-red-400";
  if (pct >= 10) return "bg-orange-500/15 border-orange-500/25 text-orange-400";
  if (pct >= 5)  return "bg-yellow-500/10 border-yellow-500/20 text-yellow-400";
  return "bg-green-500/10 border-green-500/20 text-green-400";
}

function riskLabel(pct: number): string {
  if (pct >= 20) return "Critical";
  if (pct >= 10) return "High";
  if (pct >= 5)  return "Elevated";
  return "Low";
}

// State full names for display
const STATE_NAMES: Record<string, string> = {
  AL: "Alabama", AK: "Alaska", AZ: "Arizona", AR: "Arkansas", CA: "California",
  CO: "Colorado", CT: "Connecticut", DE: "Delaware", FL: "Florida", GA: "Georgia",
  HI: "Hawaii", ID: "Idaho", IL: "Illinois", IN: "Indiana", IA: "Iowa",
  KS: "Kansas", KY: "Kentucky", LA: "Louisiana", ME: "Maine", MD: "Maryland",
  MA: "Massachusetts", MI: "Michigan", MN: "Minnesota", MS: "Mississippi",
  MO: "Missouri", MT: "Montana", NE: "Nebraska", NV: "Nevada", NH: "New Hampshire",
  NJ: "New Jersey", NM: "New Mexico", NY: "New York", NC: "North Carolina",
  ND: "North Dakota", OH: "Ohio", OK: "Oklahoma", OR: "Oregon", PA: "Pennsylvania",
  RI: "Rhode Island", SC: "South Carolina", SD: "South Dakota", TN: "Tennessee",
  TX: "Texas", UT: "Utah", VT: "Vermont", VA: "Virginia", WA: "Washington",
  WV: "West Virginia", WI: "Wisconsin", WY: "Wyoming", DC: "D.C.",
};

type SortKey = "high_risk_pct" | "total_providers" | "excluded_count" | "avg_risk_score";

// ── Page component ─────────────────────────────────────────────────────────────

export default function FraudMapPage() {
  const [points, setPoints] = useState<ProviderMapPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState<SortKey>("high_risk_pct");
  const [sortAsc, setSortAsc] = useState(false);

  useEffect(() => {
    getProviderMap()
      .then(setPoints)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  function toggleSort(key: SortKey) {
    if (sortKey === key) {
      setSortAsc((a) => !a);
    } else {
      setSortKey(key);
      setSortAsc(false);
    }
  }

  const sorted = [...points].sort((a, b) => {
    let va: number, vb: number;
    if (sortKey === "high_risk_pct") {
      va = a.total_providers > 0 ? (a.high_risk_count / a.total_providers) * 100 : 0;
      vb = b.total_providers > 0 ? (b.high_risk_count / b.total_providers) * 100 : 0;
    } else if (sortKey === "avg_risk_score") {
      va = a.avg_risk_score ?? 0;
      vb = b.avg_risk_score ?? 0;
    } else if (sortKey === "excluded_count") {
      va = a.excluded_count;
      vb = b.excluded_count;
    } else {
      va = a.total_providers;
      vb = b.total_providers;
    }
    return sortAsc ? va - vb : vb - va;
  });

  const totalProviders = points.reduce((s, p) => s + p.total_providers, 0);
  const totalHighRisk = points.reduce((s, p) => s + p.high_risk_count, 0);
  const totalExcluded = points.reduce((s, p) => s + p.excluded_count, 0);

  return (
    <AppShell>
      <div className="p-8 max-w-6xl mx-auto fade-in">
        {/* Header */}
        <div className="flex items-start justify-between mb-6">
          <div>
            <div className="flex items-center gap-2.5 mb-1">
              <Map size={18} className="text-slate-400" />
              <h1 className="text-xl font-bold text-slate-100">Fraud Map</h1>
            </div>
            <p className="text-sm text-slate-500">
              Geographic concentration of high-risk billing by state
            </p>
          </div>
        </div>

        {/* Summary cards */}
        <div className="grid grid-cols-3 gap-4 mb-8">
          {[
            { label: "Total Providers", value: fmtNum(totalProviders) },
            { label: "High-Risk Flagged", value: fmtNum(totalHighRisk), accent: true },
            { label: "LEIE Excluded", value: fmtNum(totalExcluded), danger: true },
          ].map(({ label, value, accent, danger }) => (
            <div
              key={label}
              className={`rounded-xl border p-4 ${
                danger
                  ? "bg-red-500/[0.07] border-red-500/20"
                  : accent
                  ? "bg-orange-500/[0.07] border-orange-500/20"
                  : "bg-white/[0.03] border-white/[0.06]"
              }`}
            >
              <p className="text-xs text-slate-500 mb-1">{label}</p>
              <p className={`text-2xl font-black font-mono ${danger ? "text-red-400" : accent ? "text-orange-400" : "text-slate-100"}`}>
                {value}
              </p>
            </div>
          ))}
        </div>

        {/* Synthetic data notice */}
        <div className="mb-6 flex items-center gap-2 bg-amber-500/10 border border-amber-500/20 rounded-lg px-4 py-2.5">
          <span className="text-amber-400 text-xs">⚠</span>
          <p className="text-xs text-amber-400/80">
            Synthetic CMS data · For demonstration only · Not real Medicare billing
          </p>
        </div>

        {/* State table */}
        {loading ? (
          <div className="text-slate-600 text-sm animate-pulse">Loading state data…</div>
        ) : points.length === 0 ? (
          <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl p-12 text-center">
            <Map size={32} className="text-slate-700 mx-auto mb-3" />
            <p className="text-slate-500 text-sm">No geographic data available.</p>
          </div>
        ) : (
          <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-white/[0.06]">
                  <th className="text-left px-4 py-3 text-xs text-slate-500 font-medium uppercase tracking-widest">
                    State
                  </th>
                  <SortTh label="High-Risk %" sortKey="high_risk_pct" current={sortKey} asc={sortAsc} onClick={toggleSort} />
                  <SortTh label="Providers" sortKey="total_providers" current={sortKey} asc={sortAsc} onClick={toggleSort} />
                  <SortTh label="Avg Risk" sortKey="avg_risk_score" current={sortKey} asc={sortAsc} onClick={toggleSort} />
                  <SortTh label="LEIE Excluded" sortKey="excluded_count" current={sortKey} asc={sortAsc} onClick={toggleSort} />
                  <th className="text-right px-4 py-3 text-xs text-slate-500 font-medium uppercase tracking-widest">
                    Action
                  </th>
                </tr>
              </thead>
              <tbody>
                {sorted.map((pt) => {
                  const pct = pt.total_providers > 0
                    ? (pt.high_risk_count / pt.total_providers) * 100
                    : 0;
                  const colorClass = riskColor(pct);
                  const stateName = STATE_NAMES[pt.state] ?? pt.state;

                  return (
                    <tr key={pt.state} className="border-b border-white/[0.03] hover:bg-white/[0.02] transition">
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2.5">
                          <span className="text-xs font-mono font-bold bg-white/[0.05] px-2 py-0.5 rounded text-slate-400 w-8 text-center shrink-0">
                            {pt.state}
                          </span>
                          <span className="text-slate-300 text-sm">{stateName}</span>
                        </div>
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <span className={`text-xs font-mono px-2 py-0.5 rounded border ${colorClass}`}>
                            {riskLabel(pct)}
                          </span>
                          <span className="text-xs font-mono text-slate-500">
                            {pct.toFixed(1)}%
                          </span>
                        </div>
                      </td>
                      <td className="px-4 py-3 font-mono text-sm text-slate-300">
                        {fmtNum(pt.total_providers)}
                        <span className="text-slate-600 ml-2 text-xs">
                          ({fmtNum(pt.high_risk_count)} critical/high)
                        </span>
                      </td>
                      <td className="px-4 py-3 font-mono text-sm text-slate-300">
                        {pt.avg_risk_score != null ? Number(pt.avg_risk_score).toFixed(1) : "—"}
                      </td>
                      <td className="px-4 py-3 font-mono text-sm">
                        {pt.excluded_count > 0 ? (
                          <span className="text-red-400">{fmtNum(pt.excluded_count)}</span>
                        ) : (
                          <span className="text-slate-600">0</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-right">
                        <Link
                          href={`/providers?state=${pt.state}&min_risk=70`}
                          className="inline-flex items-center gap-1 text-xs text-blue-400 hover:text-blue-300 transition"
                        >
                          View <ExternalLink size={10} />
                        </Link>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </AppShell>
  );
}

function SortTh({
  label,
  sortKey,
  current,
  asc,
  onClick,
}: {
  label: string;
  sortKey: SortKey;
  current: SortKey;
  asc: boolean;
  onClick: (key: SortKey) => void;
}) {
  const active = current === sortKey;
  return (
    <th
      className={`px-4 py-3 text-left cursor-pointer select-none transition ${
        active ? "text-slate-300" : "text-slate-500 hover:text-slate-400"
      } text-xs font-medium uppercase tracking-widest`}
      onClick={() => onClick(sortKey)}
    >
      <span className="flex items-center gap-1">
        {label}
        <ArrowUpDown size={10} className={active ? "text-slate-400" : "text-slate-600"} />
        {active && <span className="text-[9px]">{asc ? "↑" : "↓"}</span>}
      </span>
    </th>
  );
}
