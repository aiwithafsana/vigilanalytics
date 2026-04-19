"use client";
import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { getProvider, createCase, getProviderPdfUrl, getProviderBilling, getProviderFlags, getProviderAnalysis } from "@/lib/api";
import { fmt, fmtNum, providerName } from "@/lib/utils";
import type { ProviderDetail, BillingRecord, FraudFlag, ProviderAnalysis } from "@/types";
import { ArrowLeft, Download, FolderPlus, Network, AlertTriangle, ShieldAlert, Brain, ChevronRight, Users } from "lucide-react";

// ── Severity config ────────────────────────────────────────────────────────────

const SEV: Record<number, { label: string; color: string; bg: string; border: string }> = {
  1: { label: "CRITICAL", color: "text-red-400",    bg: "bg-red-500/[0.08]",    border: "border-red-500/25" },
  2: { label: "HIGH",     color: "text-orange-400", bg: "bg-orange-500/[0.07]", border: "border-orange-500/20" },
  3: { label: "MEDIUM",   color: "text-yellow-400", bg: "bg-yellow-500/[0.05]", border: "border-yellow-500/15" },
};

const FLAG_LABELS: Record<string, string> = {
  billing_volume:    "Billing Volume Outlier",
  upcoding:          "E&M Upcoding",
  impossible_hours:  "Impossible Hours",
  wrong_specialty:   "Wrong Specialty Billing",
  leie_match:        "LEIE Exclusion Match",
  opt_out_billing:   "Opted-Out Provider Billing",
  referral_cluster:  "Referral Cluster",
  hub_spoke:         "Hub-and-Spoke Network",
  yoy_surge:         "Year-over-Year Billing Surge",
  new_provider_spike:"New Provider Spike",
  address_cluster:   "Address Cluster",
  deceased_patient:  "Deceased Patient Billing",
};

// ── Place of service ──────────────────────────────────────────────────────────

const POS: Record<string, string> = {
  "11":"Office", "21":"Inpatient Hospital", "22":"Outpatient Hospital",
  "12":"Home", "23":"Emergency Room", "24":"Ambulatory Surgical",
  "31":"Skilled Nursing", "32":"Nursing Facility", "81":"Independent Lab",
  "O":"Office", "F":"Facility",
};

type Tab = "overview" | "billing" | "signals" | "analysis";

// ── Peer bar ──────────────────────────────────────────────────────────────────

function PeerBar({ label, value, peer }: { label: string; value: number | null; peer: number | null }) {
  const v = Number(value ?? 0), p = Number(peer ?? 0);
  const max = Math.max(v, p) * 1.1 || 1;
  const isHigh = v > p * 2;
  return (
    <div className="mb-4">
      <div className="flex justify-between text-xs text-slate-500 mb-1.5">
        <span>{label}</span>
        <span className="font-mono">{fmt(value)} vs {fmt(peer)} peer</span>
      </div>
      <div className="space-y-1">
        {[
          { pct: (v / max) * 100, color: isHigh ? "bg-red-500" : "bg-orange-400", lbl: "This provider" },
          { pct: (p / max) * 100, color: "bg-slate-600", lbl: "Peer median" },
        ].map(({ pct, color, lbl }) => (
          <div key={lbl} className="flex items-center gap-2">
            <span className="text-[9px] text-slate-600 w-20 text-right font-mono shrink-0">{lbl}</span>
            <div className="flex-1 h-1.5 bg-white/[0.04] rounded-full overflow-hidden">
              <div className={`h-full rounded-full ${color} transition-all`} style={{ width: `${Math.min(pct, 100)}%` }} />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function ProviderDetailPage() {
  const { npi } = useParams<{ npi: string }>();
  const router = useRouter();
  const [provider, setProvider]       = useState<ProviderDetail | null>(null);
  const [loading, setLoading]         = useState(true);
  const [creatingCase, setCreating]   = useState(false);
  const [tab, setTab]                 = useState<Tab>("overview");
  const [billing, setBilling]         = useState<BillingRecord[] | null>(null);
  const [billingLoading, setBillingLoading] = useState(false);
  const [signals, setSignals]         = useState<FraudFlag[] | null>(null);
  const [signalsLoading, setSignalsLoading] = useState(false);
  const [analysis, setAnalysis]       = useState<ProviderAnalysis | null>(null);
  const [analysisLoading, setAnalysisLoading] = useState(false);

  useEffect(() => {
    getProvider(npi).then(setProvider).catch(console.error).finally(() => setLoading(false));
  }, [npi]);

  useEffect(() => {
    if (tab === "billing" && billing === null && !billingLoading) {
      setBillingLoading(true);
      getProviderBilling(npi).then(setBilling).catch(console.error).finally(() => setBillingLoading(false));
    }
  }, [tab, billing, billingLoading, npi]);

  useEffect(() => {
    if (tab === "signals" && signals === null && !signalsLoading) {
      setSignalsLoading(true);
      getProviderFlags(npi).then(setSignals).catch(console.error).finally(() => setSignalsLoading(false));
    }
  }, [tab, signals, signalsLoading, npi]);

  useEffect(() => {
    if (tab === "analysis" && analysis === null && !analysisLoading) {
      setAnalysisLoading(true);
      getProviderAnalysis(npi).then(setAnalysis).catch(console.error).finally(() => setAnalysisLoading(false));
    }
  }, [tab, analysis, analysisLoading, npi]);

  async function handleDownloadPdf() {
    if (!provider) return;
    try {
      const token = localStorage.getItem("vigil_token");
      const res = await fetch(getProviderPdfUrl(provider.npi), {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!res.ok) throw new Error(`${res.status}`);
      const blob = await res.blob();
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = `vigil_provider_${provider.npi}.pdf`;
      a.click();
      URL.revokeObjectURL(a.href);
    } catch (e) { console.error("PDF download failed:", e); }
  }

  async function handleCreateCase() {
    if (!provider) return;
    setCreating(true);
    try {
      const c = await createCase({
        provider_npi: provider.npi,
        title: `Investigation: ${providerName(provider)}`,
        state: provider.state ?? undefined,
      });
      router.push(`/cases/${c.id}`);
    } catch (e) { console.error(e); }
    finally { setCreating(false); }
  }

  if (loading) return <AppShell><div className="p-8 text-slate-600 animate-pulse text-sm">Loading…</div></AppShell>;
  if (!provider) return <AppShell><div className="p-8 text-slate-600 text-sm">Provider not found.</div></AppShell>;

  const p = provider;
  const score = Number(p.risk_score ?? 0);
  const tier = score >= 90 ? 1 : score >= 70 ? 2 : score >= 50 ? 3 : 4;
  const tierConfig = {
    1: { label: "CRITICAL",  color: "text-red-400",    bg: "bg-red-500/10 border-red-500/30" },
    2: { label: "HIGH",      color: "text-orange-400", bg: "bg-orange-500/10 border-orange-500/25" },
    3: { label: "MEDIUM",    color: "text-yellow-400", bg: "bg-yellow-500/10 border-yellow-500/20" },
    4: { label: "LOW",       color: "text-green-400",  bg: "bg-green-500/10 border-green-500/20" },
  }[tier]!;

  return (
    <AppShell>
      <div className="p-8 max-w-5xl mx-auto fade-in">
        <Link href="/providers" className="flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-300 mb-6 transition w-fit">
          <ArrowLeft size={14} /> Providers
        </Link>

        {/* ── Header ──────────────────────────────────────────────────────── */}
        <div className="flex items-start justify-between mb-4">
          <div>
            <div className="flex items-center gap-3 mb-1 flex-wrap">
              <h1 className="text-2xl font-bold text-slate-100">{providerName(p)}</h1>
              <span className={`text-[10px] font-mono font-bold uppercase tracking-widest px-2.5 py-1 rounded border ${tierConfig.bg} ${tierConfig.color}`}>
                {tierConfig.label}
              </span>
              {p.is_excluded && (
                <span className="text-[10px] font-mono bg-red-500/15 text-red-400 border border-red-500/30 px-2 py-1 rounded">
                  ⚠ LEIE EXCLUDED
                </span>
              )}
            </div>
            <p className="text-sm text-slate-500">
              {p.specialty} · NPI {p.npi} · {p.city}, {p.state}
              {p.flag_count != null && p.flag_count > 0 && (
                <span className="ml-3 text-orange-400 font-mono text-xs">
                  {p.flag_count} active signal{p.flag_count !== 1 ? "s" : ""}
                </span>
              )}
            </p>
          </div>
          <div className={`text-4xl font-black font-mono px-4 py-2 rounded-xl border ${tierConfig.bg} ${tierConfig.color} shrink-0`}>
            {score > 0 ? score.toFixed(0) : "—"}
          </div>
        </div>

        {/* ── Investigation Brief ─────────────────────────────────────────── */}
        {/* Load top signal immediately so investigators see "why" without clicking */}
        <InvestigationBrief npi={npi} tier={tier} provider={p} />

        {/* ── Actions ────────────────────────────────────────────────────── */}
        <div className="flex gap-2 mb-6 flex-wrap">
          <button
            onClick={handleCreateCase}
            disabled={creatingCase}
            className="flex items-center gap-2 bg-blue-500/10 hover:bg-blue-500/20 border border-blue-500/25 text-blue-400 px-4 py-2 rounded-lg text-sm transition disabled:opacity-50"
          >
            <FolderPlus size={14} />
            {creatingCase ? "Creating…" : "Open Investigation"}
          </button>
          <Link
            href={`/network?npi=${p.npi}`}
            className="flex items-center gap-2 bg-white/[0.03] hover:bg-white/[0.06] border border-white/[0.08] text-slate-400 px-4 py-2 rounded-lg text-sm transition"
          >
            <Network size={14} /> Referral Network
          </Link>
          <button
            onClick={handleDownloadPdf}
            className="flex items-center gap-2 bg-white/[0.03] hover:bg-white/[0.06] border border-white/[0.08] text-slate-400 px-4 py-2 rounded-lg text-sm transition"
          >
            <Download size={14} /> Export PDF
          </button>
        </div>

        {/* ── Tabs ───────────────────────────────────────────────────────── */}
        <div className="flex gap-0.5 mb-6 border-b border-white/[0.06]">
          {(["overview", "billing", "signals", "analysis"] as Tab[]).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`px-4 py-2.5 text-sm font-medium transition capitalize border-b-2 -mb-px ${
                tab === t
                  ? "border-blue-500 text-blue-400"
                  : "border-transparent text-slate-500 hover:text-slate-300"
              }`}
            >
              {t === "signals"
                ? `Evidence${signals ? ` (${signals.length})` : ""}`
                : t === "billing"
                ? `Billing${billing ? ` (${billing.length})` : ""}`
                : t === "analysis"
                ? "AI Brief"
                : "Overview"}
            </button>
          ))}
        </div>

        {/* ── OVERVIEW ────────────────────────────────────────────────────── */}
        {tab === "overview" && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

            {/* Peer comparison */}
            <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
              <h2 className="text-xs uppercase tracking-widest text-slate-500 font-medium mb-5">vs. Peer Median</h2>
              <PeerBar label="Total Payment" value={p.total_payment} peer={p.peer_median_payment} />
              <PeerBar label="Total Services" value={p.total_services} peer={p.peer_median_services} />
              <PeerBar label="Beneficiaries" value={p.total_beneficiaries} peer={p.peer_median_benes} />
            </div>

            {/* Billing metrics */}
            <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
              <h2 className="text-xs uppercase tracking-widest text-slate-500 font-medium mb-4">Billing Metrics</h2>
              <div className="space-y-0">
                {[
                  { label: "Total Billed (2022)", value: fmt(p.total_payment) },
                  { label: "Total Services",      value: fmtNum(p.total_services) },
                  { label: "Total Beneficiaries", value: fmtNum(p.total_beneficiaries) },
                  { label: "Payment / Beneficiary", value: fmt(p.payment_per_bene) },
                  { label: "Services / Beneficiary", value: p.services_per_bene != null ? Number(p.services_per_bene).toFixed(1) : "—" },
                  { label: "Payment Z-Score",     value: p.payment_zscore != null ? `${Number(p.payment_zscore).toFixed(1)}σ` : "—" },
                  { label: "Billing Entropy",     value: p.billing_entropy != null ? Number(p.billing_entropy).toFixed(3) : "—" },
                  { label: "E&M Upcoding Ratio",  value: p.em_upcoding_ratio != null ? `${(Number(p.em_upcoding_ratio) * 100).toFixed(1)}%` : "—" },
                  { label: "Procedure Types",     value: fmtNum(p.num_procedure_types) },
                ].map(({ label, value }) => (
                  <div key={label} className="flex justify-between py-2 border-b border-white/[0.04] last:border-0">
                    <span className="text-xs text-slate-500">{label}</span>
                    <span className="text-xs text-slate-200 font-mono">{value ?? "—"}</span>
                  </div>
                ))}
              </div>
            </div>

            {/* LEIE status — full width */}
            <div className="lg:col-span-2">
              {p.is_excluded ? (
                <div className="bg-red-500/[0.07] border border-red-500/25 rounded-xl p-5">
                  <div className="flex items-start gap-3">
                    <AlertTriangle size={18} className="text-red-400 shrink-0 mt-0.5" />
                    <div>
                      <p className="text-sm font-semibold text-red-400 mb-1">
                        LEIE Exclusion Confirmed — This provider is barred from Medicare billing
                      </p>
                      <p className="text-xs text-slate-400">
                        Exclusion date: <span className="font-mono text-slate-300">{p.leie_date ?? "unknown"}</span>
                        {p.leie_reason && (
                          <> · Reason: <span className="font-mono text-slate-300">{p.leie_reason}</span></>
                        )}
                      </p>
                      <p className="text-xs text-slate-600 mt-2 italic">
                        Any Medicare billing after the exclusion date may constitute fraud under 42 U.S.C. § 1320a-7.
                        Consider immediate referral to OIG.
                      </p>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="bg-yellow-500/[0.04] border border-yellow-500/15 rounded-xl p-4 flex items-center gap-3">
                  <ShieldAlert size={15} className="text-yellow-500/60 shrink-0" />
                  <p className="text-xs text-slate-500">
                    Not on LEIE — this is a <span className="text-yellow-400">net-new investigative lead</span> identified
                    solely through billing pattern analysis. No prior enforcement record.
                  </p>
                </div>
              )}
            </div>
          </div>
        )}

        {/* ── BILLING TAB ─────────────────────────────────────────────────── */}
        {tab === "billing" && (
          <div>
            <p className="text-xs text-slate-500 mb-4">
              CMS 2022 Part B · HCPCS-level billing · sorted by total Medicare payment
            </p>

            {billingLoading ? (
              <div className="text-slate-600 text-sm animate-pulse">Loading billing records…</div>
            ) : !billing || billing.length === 0 ? (
              <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl p-12 text-center">
                <p className="text-slate-500 text-sm">No HCPCS billing records found for this provider.</p>
              </div>
            ) : (
              <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-white/[0.06]">
                      {["HCPCS", "Procedure", "Setting", "Benes", "Services", "Avg Paid", "Total Paid"].map(h => (
                        <th key={h} className="text-left px-4 py-3 text-[10px] text-slate-500 font-medium uppercase tracking-widest">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {billing.map(row => (
                      <tr key={row.id} className="border-b border-white/[0.03] hover:bg-white/[0.02] transition">
                        <td className="px-4 py-2.5 font-mono text-xs text-blue-400 whitespace-nowrap">{row.hcpcs_code ?? "—"}</td>
                        <td className="px-4 py-2.5 text-slate-300 text-xs max-w-xs">
                          <span className="block truncate" title={row.hcpcs_description ?? ""}>{row.hcpcs_description ?? "—"}</span>
                        </td>
                        <td className="px-4 py-2.5 text-xs text-slate-500 whitespace-nowrap">
                          {row.place_of_service ? (POS[row.place_of_service] ?? row.place_of_service) : "—"}
                        </td>
                        <td className="px-4 py-2.5 font-mono text-xs text-slate-400">{fmtNum(row.total_beneficiaries)}</td>
                        <td className="px-4 py-2.5 font-mono text-xs text-slate-400">{fmtNum(row.total_services)}</td>
                        <td className="px-4 py-2.5 font-mono text-xs text-slate-400">{fmt(row.avg_medicare_payment)}</td>
                        <td className="px-4 py-2.5 font-mono text-xs font-bold text-slate-100">{fmt(row.total_medicare_payment)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}

        {/* ── AI BRIEF TAB ────────────────────────────────────────────────── */}
        {tab === "analysis" && (
          <div className="fade-in">
            {analysisLoading && (
              <div className="flex items-center gap-3 text-slate-500 text-sm animate-pulse py-8">
                <Brain size={16} className="text-blue-400" />
                Generating investigative brief…
              </div>
            )}
            {!analysisLoading && !analysis && (
              <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl p-12 text-center">
                <p className="text-slate-500 text-sm">Could not generate brief.</p>
              </div>
            )}
            {analysis && <AnalysisBrief brief={analysis} />}
          </div>
        )}

        {/* ── EVIDENCE TAB ────────────────────────────────────────────────── */}
        {tab === "signals" && (
          <div>
            <p className="text-xs text-slate-500 mb-4">
              Active fraud detection signals · each is a citable evidence item for referral
            </p>

            {signalsLoading ? (
              <div className="text-slate-600 text-sm animate-pulse">Loading signals…</div>
            ) : !signals || signals.length === 0 ? (
              <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl p-12 text-center">
                <p className="text-slate-500 text-sm">No active detection signals.</p>
              </div>
            ) : (
              <div className="space-y-3">
                {signals.map(flag => {
                  const sev = SEV[flag.severity ?? 3] ?? SEV[3];
                  const typeLabel = FLAG_LABELS[flag.flag_type] ?? flag.flag_type.replace(/_/g, " ");

                  return (
                    <div key={flag.id} className={`rounded-xl border ${sev.border} ${sev.bg} p-4`}>
                      {/* Headline row */}
                      <div className="flex items-center gap-2 mb-2">
                        <span className={`text-[10px] font-mono font-bold uppercase tracking-widest ${sev.color}`}>
                          {sev.label}
                        </span>
                        <span className="text-slate-600 text-xs">·</span>
                        <span className="text-slate-300 text-xs font-medium">{typeLabel}</span>
                        {flag.hcpcs_code && (
                          <span className="text-xs font-mono text-blue-400 bg-blue-500/10 border border-blue-500/20 px-1.5 py-0.5 rounded">
                            {flag.hcpcs_code}
                          </span>
                        )}
                        {flag.year && (
                          <span className="text-[10px] text-slate-600 font-mono ml-auto">{flag.year}</span>
                        )}
                      </div>

                      {/* The actual explanation — this IS the evidence */}
                      {flag.explanation && (
                        <p className="text-sm text-slate-200 leading-relaxed mb-3">
                          {flag.explanation}
                        </p>
                      )}

                      {/* Evidence metrics */}
                      <div className="flex items-center gap-4 flex-wrap">
                        {flag.flag_value != null && flag.peer_value != null && (
                          <div className="bg-white/[0.04] rounded-lg px-3 py-1.5">
                            <p className="text-[9px] text-slate-600 uppercase tracking-widest mb-0.5">Ratio</p>
                            <p className="text-sm font-mono font-bold text-slate-200">
                              {Number(flag.flag_value).toFixed(0)}× peer
                            </p>
                          </div>
                        )}
                        {flag.peer_value != null && (
                          <div className="bg-white/[0.04] rounded-lg px-3 py-1.5">
                            <p className="text-[9px] text-slate-600 uppercase tracking-widest mb-0.5">Peer Median</p>
                            <p className="text-sm font-mono font-bold text-slate-400">{fmt(flag.peer_value)}</p>
                          </div>
                        )}
                        {flag.estimated_overpayment != null && Number(flag.estimated_overpayment) > 0 && (
                          <div className="bg-orange-500/[0.08] border border-orange-500/20 rounded-lg px-3 py-1.5">
                            <p className="text-[9px] text-orange-500/70 uppercase tracking-widest mb-0.5">Est. Excess</p>
                            <p className="text-sm font-mono font-bold text-orange-400">{fmt(flag.estimated_overpayment)}</p>
                          </div>
                        )}
                        {flag.confidence != null && (
                          <div className="bg-white/[0.04] rounded-lg px-3 py-1.5">
                            <p className="text-[9px] text-slate-600 uppercase tracking-widest mb-0.5">Confidence</p>
                            <p className="text-sm font-mono font-bold text-slate-300">
                              {(Number(flag.confidence) * 100).toFixed(0)}%
                            </p>
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}
      </div>
    </AppShell>
  );
}

// ── AI Investigative Brief ────────────────────────────────────────────────────

const PRIORITY_CONFIG: Record<number, { label: string; color: string; bg: string; border: string }> = {
  1: { label: "IMMEDIATE", color: "text-red-400",    bg: "bg-red-500/[0.08]",    border: "border-red-500/30" },
  2: { label: "HIGH",      color: "text-orange-400", bg: "bg-orange-500/[0.07]", border: "border-orange-500/25" },
  3: { label: "MEDIUM",    color: "text-yellow-400", bg: "bg-yellow-500/[0.05]", border: "border-yellow-500/20" },
  4: { label: "ROUTINE",   color: "text-slate-400",  bg: "bg-white/[0.03]",      border: "border-white/[0.06]" },
};

const ACTION_CATEGORY_COLOR: Record<string, string> = {
  "Immediate":           "bg-red-500/20 text-red-300",
  "Legal — FCA Referral":"bg-red-500/15 text-red-400",
  "Claims Audit":        "bg-orange-500/15 text-orange-300",
  "Network Investigation":"bg-purple-500/15 text-purple-300",
  "Case Management":     "bg-blue-500/15 text-blue-300",
};

function NarrativeText({ text }: { text: string }) {
  // Render **bold** markdown and newlines
  const parts = text.split(/\n\n/);
  return (
    <div className="space-y-3">
      {parts.map((para, i) => {
        const segments = para.split(/(\*\*[^*]+\*\*)/g);
        return (
          <p key={i} className="text-sm text-slate-300 leading-relaxed">
            {segments.map((seg, j) =>
              seg.startsWith("**") && seg.endsWith("**")
                ? <strong key={j} className="text-slate-100 font-semibold">{seg.slice(2, -2)}</strong>
                : seg.split("\n").map((line, k) => (
                    <span key={k}>{line}{k < seg.split("\n").length - 1 && <br />}</span>
                  ))
            )}
          </p>
        );
      })}
    </div>
  );
}

function AnalysisBrief({ brief }: { brief: ProviderAnalysis }) {
  const pc = PRIORITY_CONFIG[brief.priority] ?? PRIORITY_CONFIG[4];

  return (
    <div className="space-y-6">

      {/* ── Header card ─────────────────────────────────────────────────── */}
      <div className={`rounded-xl border ${pc.border} ${pc.bg} p-5`}>
        <div className="flex items-start justify-between gap-4 mb-3">
          <div>
            <div className="flex items-center gap-2 mb-1.5">
              <Brain size={14} className="text-blue-400 shrink-0" />
              <span className="text-[10px] uppercase tracking-widest text-slate-500 font-medium">
                AI Investigative Brief
              </span>
            </div>
            <h2 className="text-base font-bold text-slate-100">{brief.scheme_type}</h2>
          </div>
          <div className="flex flex-col items-end gap-1.5 shrink-0">
            <span className={`text-[10px] font-mono font-bold uppercase tracking-widest px-2.5 py-1 rounded border ${pc.bg} ${pc.border} ${pc.color}`}>
              {pc.label} PRIORITY
            </span>
            {brief.estimated_exposure != null && (
              <span className="text-xs font-mono text-orange-400">
                Est. exposure: <strong>${brief.estimated_exposure.toLocaleString()}</strong>
              </span>
            )}
          </div>
        </div>
        <div className="flex gap-4 text-[11px] text-slate-600 font-mono">
          <span>{brief.active_signals} active signal{brief.active_signals !== 1 ? "s" : ""}</span>
          <span>·</span>
          <span>{brief.suspicious_edges} suspicious network edge{brief.suspicious_edges !== 1 ? "s" : ""}</span>
          <span>·</span>
          <span>Generated {new Date(brief.generated_at).toLocaleString()}</span>
        </div>
      </div>

      {/* ── Key findings ────────────────────────────────────────────────── */}
      {brief.key_findings.length > 0 && (
        <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl p-5">
          <h3 className="text-[10px] uppercase tracking-widest text-slate-500 font-medium mb-4">Key Findings</h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {brief.key_findings.map((f, i) => (
              <div key={i} className="bg-white/[0.03] border border-white/[0.05] rounded-lg p-3">
                <p className="text-[10px] uppercase tracking-widest text-slate-600 mb-1">{f.label}</p>
                <p className="text-sm font-mono font-bold text-slate-100 mb-1">{f.value}</p>
                <p className="text-[11px] text-slate-500 leading-relaxed">{f.detail}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Narrative ───────────────────────────────────────────────────── */}
      <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl p-5">
        <h3 className="text-[10px] uppercase tracking-widest text-slate-500 font-medium mb-4">
          Investigative Narrative
        </h3>
        <NarrativeText text={brief.narrative} />
      </div>

      {/* ── Billing anomalies ────────────────────────────────────────────── */}
      {brief.billing_anomalies.length > 0 && (
        <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl overflow-hidden">
          <div className="px-5 py-3.5 border-b border-white/[0.06]">
            <h3 className="text-[10px] uppercase tracking-widest text-slate-500 font-medium">
              Billing Anomalies — Top Codes
            </h3>
          </div>
          <div className="divide-y divide-white/[0.04]">
            {brief.billing_anomalies.map((a, i) => (
              <div key={i} className="px-5 py-3.5">
                <div className="flex items-start justify-between gap-3 mb-1.5">
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-xs text-blue-400">{a.hcpcs ?? "—"}</span>
                    <span className="text-xs text-slate-400 truncate max-w-xs">{a.description}</span>
                  </div>
                  <span className="text-xs font-mono font-bold text-slate-100 shrink-0">
                    ${a.total_paid.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                  </span>
                </div>
                <p className="text-[11px] text-slate-500 leading-relaxed">{a.anomaly_text}</p>
                <div className="flex gap-4 mt-2 text-[10px] font-mono text-slate-600">
                  <span>{a.services.toLocaleString()} services</span>
                  <span>·</span>
                  <span>{a.beneficiaries.toLocaleString()} patients</span>
                  <span>·</span>
                  <span className={a.services_per_bene > 100 ? "text-red-400 font-bold" : a.services_per_bene > 30 ? "text-orange-400" : ""}>
                    {a.services_per_bene}× per patient
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Network suspects ─────────────────────────────────────────────── */}
      {brief.network_suspects.length > 0 && (
        <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl overflow-hidden">
          <div className="px-5 py-3.5 border-b border-white/[0.06] flex items-center gap-2">
            <Users size={13} className="text-purple-400" />
            <h3 className="text-[10px] uppercase tracking-widest text-slate-500 font-medium">
              Co-Investigation Targets — {brief.network_suspects.length} Provider{brief.network_suspects.length !== 1 ? "s" : ""}
            </h3>
          </div>
          <div className="divide-y divide-white/[0.04]">
            {brief.network_suspects.map((s, i) => (
              <div key={i} className="px-5 py-4">
                <div className="flex items-start justify-between gap-3 mb-2">
                  <div>
                    <div className="flex items-center gap-2 mb-0.5">
                      <span className="text-sm font-semibold text-slate-100">{s.name}</span>
                      {s.is_excluded && (
                        <span className="text-[9px] bg-red-500/20 text-red-400 border border-red-500/30 px-1.5 py-0.5 rounded font-mono">EXCLUDED</span>
                      )}
                    </div>
                    <p className="text-xs text-slate-500">
                      {[s.specialty, s.state].filter(Boolean).join(" · ")}
                    </p>
                  </div>
                  <div className="flex flex-col items-end gap-1 shrink-0">
                    <span className={`text-sm font-mono font-bold ${s.risk_score >= 90 ? "text-red-400" : s.risk_score >= 70 ? "text-orange-400" : "text-slate-300"}`}>
                      {s.risk_score.toFixed(0)}
                    </span>
                    <span className="text-[10px] text-slate-600 font-mono">{s.shared_patients.toLocaleString()} shared pts</span>
                  </div>
                </div>
                <p className="text-[11px] text-purple-300/80 font-mono mb-2">{s.direction}</p>
                <p className="text-[11px] text-slate-500 mb-2"><span className="text-slate-400">Signal: </span>{s.reason}</p>
                <div className="bg-white/[0.03] border border-white/[0.04] rounded-lg px-3 py-2 flex items-start gap-2">
                  <ChevronRight size={11} className="text-blue-400 mt-0.5 shrink-0" />
                  <p className="text-[11px] text-slate-400 leading-relaxed">{s.action}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Recommended actions ──────────────────────────────────────────── */}
      {brief.recommended_actions.length > 0 && (
        <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl overflow-hidden">
          <div className="px-5 py-3.5 border-b border-white/[0.06]">
            <h3 className="text-[10px] uppercase tracking-widest text-slate-500 font-medium">
              Recommended Investigative Actions
            </h3>
          </div>
          <div className="divide-y divide-white/[0.04]">
            {brief.recommended_actions.map((a) => {
              const catCls = ACTION_CATEGORY_COLOR[a.category] ?? "bg-slate-500/10 text-slate-400";
              return (
                <div key={a.step} className="px-5 py-4 flex gap-4">
                  <div className="w-7 h-7 rounded-full bg-white/[0.05] border border-white/[0.08] flex items-center justify-center text-xs font-mono font-bold text-slate-400 shrink-0 mt-0.5">
                    {a.step}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1.5 flex-wrap">
                      <span className={`text-[10px] font-medium px-2 py-0.5 rounded-full ${catCls}`}>
                        {a.category}
                      </span>
                    </div>
                    <p className="text-sm font-semibold text-slate-200 mb-1">{a.action}</p>
                    <p className="text-[11px] text-slate-500 leading-relaxed">{a.detail}</p>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── Footer ──────────────────────────────────────────────────────── */}
      <p className="text-[10px] text-slate-700 font-mono text-center pb-2">
        {brief.data_source} · For official use only (FOUO)
      </p>
    </div>
  );
}

// ── Investigation Brief ───────────────────────────────────────────────────────
// Loads top flag on mount and shows it as a callout before the tabs.

function InvestigationBrief({
  npi, tier, provider
}: {
  npi: string; tier: number; provider: ProviderDetail
}) {
  const [topFlag, setTopFlag] = useState<FraudFlag | null | undefined>(undefined);

  useEffect(() => {
    getProviderFlags(npi)
      .then(flags => setTopFlag(flags[0] ?? null))
      .catch(() => setTopFlag(null));
  }, [npi]);

  // Don't show brief for low-risk providers or while loading
  if (topFlag === undefined) return null;
  if (topFlag === null && tier >= 4) return null;

  const sev = SEV[topFlag?.severity ?? 3] ?? SEV[3];

  return (
    <div className={`mb-6 rounded-xl border p-4 ${topFlag ? `${sev.border} ${sev.bg}` : "border-white/[0.06] bg-white/[0.02]"}`}>
      <p className="text-[10px] uppercase tracking-widest text-slate-500 font-medium mb-2">
        Why This Provider Is Flagged
      </p>
      {topFlag ? (
        <>
          <p className="text-sm text-slate-200 leading-relaxed">
            {topFlag.explanation ?? "No explanation available."}
          </p>
          {topFlag.estimated_overpayment != null && Number(topFlag.estimated_overpayment) > 0 && (
            <p className="text-xs font-mono text-orange-400 mt-2">
              Estimated excess above peer P90: <strong>{fmt(topFlag.estimated_overpayment)}</strong>
              {provider.is_excluded && " · Provider is on the LEIE exclusion list."}
            </p>
          )}
        </>
      ) : (
        <p className="text-sm text-slate-500">
          {provider.is_excluded
            ? "This provider is on the LEIE exclusion list and should not be billing Medicare."
            : "Flagged by risk scoring model. Check the Evidence tab for detection signals."}
        </p>
      )}
    </div>
  );
}
