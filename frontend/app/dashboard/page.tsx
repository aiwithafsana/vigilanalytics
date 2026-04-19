"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import AppShell from "@/components/AppShell";
import { getDashboard, createCase } from "@/lib/api";
import { fmt, fmtNum, providerName } from "@/lib/utils";
import type { DashboardResponse, LeadItem } from "@/types";
import { ShieldAlert, AlertTriangle, UserX, FolderOpen, FolderPlus, ArrowRight, TrendingUp } from "lucide-react";

// ── Severity config ────────────────────────────────────────────────────────────

const SEV: Record<number, { label: string; color: string; bg: string; border: string; dot: string }> = {
  1: { label: "CRITICAL",  color: "text-red-400",    bg: "bg-red-500/[0.08]",    border: "border-red-500/25",    dot: "bg-red-500" },
  2: { label: "HIGH",      color: "text-orange-400", bg: "bg-orange-500/[0.07]", border: "border-orange-500/20", dot: "bg-orange-500" },
  3: { label: "MEDIUM",    color: "text-yellow-400", bg: "bg-yellow-500/[0.05]", border: "border-yellow-500/15", dot: "bg-yellow-500" },
};

// ── Lead card ─────────────────────────────────────────────────────────────────

function LeadCard({ lead, onCase }: { lead: LeadItem; onCase: (npi: string) => void }) {
  const sev = SEV[lead.severity] ?? SEV[3];

  return (
    <div className={`rounded-xl border ${sev.border} ${sev.bg} p-4 hover:brightness-110 transition`}>
      {/* Top row */}
      <div className="flex items-start justify-between gap-3 mb-2.5">
        <div className="flex items-center gap-2.5 min-w-0">
          <span className={`w-2 h-2 rounded-full shrink-0 ${sev.dot}`} />
          <div className="min-w-0">
            <Link
              href={`/providers/${lead.npi}`}
              className="text-sm font-semibold text-slate-100 hover:text-white transition truncate block"
            >
              {lead.name}
            </Link>
            <p className="text-[11px] text-slate-500 mt-0.5">
              {lead.specialty ?? "Unknown Specialty"} · {lead.city ? `${lead.city}, ` : ""}{lead.state}
              {lead.is_excluded && (
                <span className="ml-2 text-[9px] bg-red-500/15 text-red-400 border border-red-500/20 px-1.5 py-0.5 rounded font-mono">
                  LEIE EXCLUDED
                </span>
              )}
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          <span className={`text-[10px] font-mono font-bold uppercase tracking-widest px-2 py-0.5 rounded ${sev.color} bg-white/[0.04]`}>
            {sev.label}
          </span>
        </div>
      </div>

      {/* Explanation — the actual investigative signal */}
      {lead.explanation && (
        <p className="text-xs text-slate-300 leading-relaxed mb-3 pl-4 border-l border-white/[0.08]">
          {lead.explanation}
        </p>
      )}

      {/* Stats row */}
      <div className="flex items-center gap-4 text-[11px] font-mono pl-4">
        {lead.estimated_overpayment != null && Number(lead.estimated_overpayment) > 0 && (
          <span className="text-orange-400 font-semibold">
            ~{fmt(lead.estimated_overpayment)} est. excess
          </span>
        )}
        {lead.total_payment != null && (
          <span className="text-slate-500">{fmt(lead.total_payment)} total billed</span>
        )}
        {lead.hcpcs_code && (
          <span className="text-blue-400/70">HCPCS {lead.hcpcs_code}</span>
        )}
      </div>

      {/* Actions */}
      <div className="flex items-center gap-2 mt-3 pl-4">
        <Link
          href={`/providers/${lead.npi}`}
          className="flex items-center gap-1 text-xs text-slate-400 hover:text-slate-200 transition"
        >
          View profile <ArrowRight size={10} />
        </Link>
        <span className="text-slate-700">·</span>
        <button
          onClick={() => onCase(lead.npi)}
          className="flex items-center gap-1 text-xs text-blue-400 hover:text-blue-300 transition"
        >
          <FolderPlus size={10} /> Open investigation
        </button>
      </div>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function DashboardPage() {
  const router = useRouter();
  const [data, setData] = useState<DashboardResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getDashboard().then(setData).catch(console.error).finally(() => setLoading(false));
  }, []);

  async function handleOpenCase(npi: string) {
    try {
      const c = await createCase({ provider_npi: npi, title: `Investigation: ${npi}` });
      router.push(`/cases/${c.id}`);
    } catch (e) { console.error(e); }
  }

  const s = data?.stats;
  const rd = data?.risk_distribution;
  const leads = data?.top_leads ?? [];
  const criticalLeads = leads.filter(l => l.severity === 1);
  const otherLeads    = leads.filter(l => l.severity > 1);

  return (
    <AppShell>
      <div className="p-8 max-w-7xl mx-auto fade-in">

        {/* Header */}
        <div className="mb-6">
          <h1 className="text-xl font-bold text-slate-100">Investigation Queue</h1>
          <p className="text-sm text-slate-500 mt-0.5">
            Priority leads ranked by severity and estimated overpayment
          </p>
        </div>

        {loading ? (
          <div className="text-slate-600 text-sm animate-pulse">Loading…</div>
        ) : (
          <>
            {/* Summary strip */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-8">
              {[
                {
                  label: "Critical Leads",
                  value: fmtNum(rd?.critical),
                  sub: "Need immediate action",
                  icon: ShieldAlert,
                  color: "text-red-400",
                  bg: "bg-red-500/[0.06] border-red-500/15",
                },
                {
                  label: "High-Risk Providers",
                  value: fmtNum(s?.high_risk_providers),
                  sub: "Score 70+",
                  icon: AlertTriangle,
                  color: "text-orange-400",
                  bg: "bg-orange-500/[0.05] border-orange-500/12",
                },
                {
                  label: "LEIE Excluded",
                  value: fmtNum(s?.leie_matches),
                  sub: "Still billing Medicare",
                  icon: UserX,
                  color: "text-red-400",
                  bg: "bg-red-500/[0.05] border-red-500/10",
                },
                {
                  label: "Open Cases",
                  value: fmtNum(s?.open_cases),
                  sub: "Active investigations",
                  icon: FolderOpen,
                  color: "text-blue-400",
                  bg: "bg-blue-500/[0.05] border-blue-500/10",
                },
              ].map(({ label, value, sub, icon: Icon, color, bg }) => (
                <div key={label} className={`rounded-xl border p-4 ${bg}`}>
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-[10px] text-slate-500 uppercase tracking-widest">{label}</span>
                    <Icon size={13} className={color} />
                  </div>
                  <div className={`text-2xl font-black font-mono ${color}`}>{value}</div>
                  <div className="text-[10px] text-slate-600 mt-1">{sub}</div>
                </div>
              ))}
            </div>

            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              {/* ── Priority lead queue (2/3 width) ─────────────────────────── */}
              <div className="xl:col-span-2">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-sm font-semibold text-slate-300">
                    Priority Leads
                    {leads.length > 0 && (
                      <span className="ml-2 text-xs font-normal text-slate-600">
                        top {leads.length} by severity
                      </span>
                    )}
                  </h2>
                  <Link href="/providers?min_risk=70" className="text-xs text-slate-500 hover:text-slate-300 transition">
                    View all high-risk →
                  </Link>
                </div>

                {leads.length === 0 ? (
                  <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl p-12 text-center">
                    <ShieldAlert size={28} className="text-slate-700 mx-auto mb-3" />
                    <p className="text-slate-500 text-sm">No active fraud flags.</p>
                    <p className="text-slate-600 text-xs mt-1">
                      Run <code className="text-slate-500">ml/pipeline/detect_layer1.py</code> to generate leads.
                    </p>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {/* Critical first */}
                    {criticalLeads.length > 0 && (
                      <>
                        <p className="text-[10px] uppercase tracking-widest text-red-500/70 font-medium px-1">
                          Critical — Immediate Action
                        </p>
                        {criticalLeads.map(lead => (
                          <LeadCard key={lead.flag_id} lead={lead} onCase={handleOpenCase} />
                        ))}
                      </>
                    )}
                    {/* High / medium */}
                    {otherLeads.length > 0 && (
                      <>
                        {criticalLeads.length > 0 && (
                          <p className="text-[10px] uppercase tracking-widest text-slate-600 font-medium px-1 pt-2">
                            High Priority
                          </p>
                        )}
                        {otherLeads.map(lead => (
                          <LeadCard key={lead.flag_id} lead={lead} onCase={handleOpenCase} />
                        ))}
                      </>
                    )}
                  </div>
                )}
              </div>

              {/* ── Right sidebar ─────────────────────────────────────────────── */}
              <div className="space-y-4">
                {/* Risk distribution */}
                <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
                  <h3 className="text-[10px] uppercase tracking-widest text-slate-500 font-medium mb-4">
                    Provider Risk Distribution
                  </h3>
                  {rd && (
                    <div className="space-y-3">
                      {[
                        { label: "Critical", count: rd.critical, color: "bg-red-500", text: "text-red-400" },
                        { label: "High",     count: rd.high,     color: "bg-orange-500", text: "text-orange-400" },
                        { label: "Medium",   count: rd.medium,   color: "bg-yellow-500", text: "text-yellow-500" },
                        { label: "Low",      count: rd.low,      color: "bg-slate-600",  text: "text-slate-500" },
                      ].map(({ label, count, color, text }) => {
                        const total = rd.critical + rd.high + rd.medium + rd.low || 1;
                        return (
                          <div key={label}>
                            <div className="flex justify-between text-xs mb-1">
                              <span className="text-slate-500">{label}</span>
                              <span className={`font-mono ${text}`}>{fmtNum(count)}</span>
                            </div>
                            <div className="h-1.5 bg-white/[0.04] rounded-full overflow-hidden">
                              <div
                                className={`h-full rounded-full ${color} transition-all duration-700`}
                                style={{ width: `${(count / total) * 100}%` }}
                              />
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                  <div className="mt-4 pt-4 border-t border-white/[0.05] grid grid-cols-2 gap-3 text-xs">
                    <div>
                      <p className="text-slate-600">Total analyzed</p>
                      <p className="text-slate-300 font-mono font-bold">{fmtNum(s?.total_providers)}</p>
                    </div>
                    <div>
                      <p className="text-slate-600">Total billed</p>
                      <p className="text-slate-300 font-mono font-bold">{fmt(s?.total_payment)}</p>
                    </div>
                  </div>
                </div>

                {/* Quick links */}
                <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
                  <h3 className="text-[10px] uppercase tracking-widest text-slate-500 font-medium mb-3">
                    Quick Filters
                  </h3>
                  <div className="space-y-1.5">
                    {[
                      { label: "All critical providers",  href: "/providers?min_risk=90", color: "text-red-400" },
                      { label: "LEIE excluded + billing",  href: "/providers?is_excluded=true", color: "text-red-400" },
                      { label: "New leads (not on LEIE)",  href: "/providers?is_excluded=false&min_risk=70", color: "text-yellow-400" },
                      { label: "Fraud Map by state",        href: "/providers/map", color: "text-blue-400" },
                      { label: "All open cases",            href: "/cases", color: "text-slate-400" },
                    ].map(({ label, href, color }) => (
                      <Link
                        key={href}
                        href={href}
                        className={`flex items-center justify-between text-xs ${color} hover:brightness-125 transition py-1`}
                      >
                        {label} <ArrowRight size={10} />
                      </Link>
                    ))}
                  </div>
                </div>

                {/* Recent cases */}
                {(data?.recent_cases?.length ?? 0) > 0 && (
                  <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
                    <div className="flex items-center justify-between mb-3">
                      <h3 className="text-[10px] uppercase tracking-widest text-slate-500 font-medium">Recent Cases</h3>
                      <Link href="/cases" className="text-[10px] text-slate-600 hover:text-slate-400 transition">View all →</Link>
                    </div>
                    <div className="space-y-2">
                      {data?.recent_cases.map(c => (
                        <Link
                          key={c.id}
                          href={`/cases/${c.id}`}
                          className="block text-xs text-slate-400 hover:text-slate-200 truncate transition"
                        >
                          <span className="text-slate-600 font-mono mr-2">{c.case_number}</span>
                          {c.title}
                        </Link>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          </>
        )}
      </div>
    </AppShell>
  );
}
