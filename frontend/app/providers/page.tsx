"use client";
import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import AppShell from "@/components/AppShell";
import { getProviders, getProvidersCsvUrl } from "@/lib/api";
import { fmt, fmtNum, riskBadge, providerName } from "@/lib/utils";
import type { ProviderSummary } from "@/types";
import { Search, Download, ShieldAlert } from "lucide-react";

// All 50 states + DC
const ALL_STATES = [
  "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN",
  "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH",
  "NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT",
  "VT","VA","WA","WV","WI","WY",
];

const STATE_NAMES: Record<string, string> = {
  AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",
  CO:"Colorado",CT:"Connecticut",DE:"Delaware",DC:"D.C.",FL:"Florida",
  GA:"Georgia",HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",
  KS:"Kansas",KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",
  MA:"Massachusetts",MI:"Michigan",MN:"Minnesota",MS:"Mississippi",
  MO:"Missouri",MT:"Montana",NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",
  NJ:"New Jersey",NM:"New Mexico",NY:"New York",NC:"North Carolina",
  ND:"North Dakota",OH:"Ohio",OK:"Oklahoma",OR:"Oregon",PA:"Pennsylvania",
  RI:"Rhode Island",SC:"South Carolina",SD:"South Dakota",TN:"Tennessee",
  TX:"Texas",UT:"Utah",VT:"Vermont",VA:"Virginia",WA:"Washington",
  WV:"West Virginia",WI:"Wisconsin",WY:"Wyoming",
};

const SPECIALTIES = [
  "", "Internal Medicine", "Family Practice", "Cardiology", "Oncology",
  "Orthopedic Surgery", "Neurology", "Psychiatry", "Physical Therapy",
  "Pain Management", "Nurse Practitioner", "Clinical Laboratory",
  "Emergency Medicine", "Anesthesiology", "Radiology",
];

function riskTierBadge(score: number | null) {
  const s = Number(score ?? 0);
  if (s >= 90) return "bg-red-500/20 text-red-400 border border-red-500/30";
  if (s >= 70) return "bg-orange-500/15 text-orange-400 border border-orange-500/25";
  if (s >= 50) return "bg-yellow-500/10 text-yellow-400 border border-yellow-500/20";
  return "bg-white/[0.04] text-slate-500 border border-white/[0.06]";
}

export default function ProvidersPage() {
  const router = useRouter();
  const searchParams = useSearchParams();

  // Initialise filters from URL params (e.g. ?state=MS&min_risk=70 from Fraud Map)
  const [q, setQ]               = useState(() => searchParams.get("q") ?? "");
  const [state, setState]       = useState(() => searchParams.get("state") ?? "");
  const [specialty, setSpecialty] = useState(() => searchParams.get("specialty") ?? "");
  const [isExcluded, setIsExcluded] = useState<string>(() => searchParams.get("is_excluded") ?? "");
  const [minRisk, setMinRisk]   = useState<string>(() => searchParams.get("min_risk") ?? "");
  const [physicianOnly, setPhysicianOnly] = useState(() => searchParams.get("physician_only") === "true");

  const [items, setItems]       = useState<ProviderSummary[]>([]);
  const [total, setTotal]       = useState(0);
  const [page, setPage]         = useState(1);
  const [loading, setLoading]   = useState(true);
  const [exportingCsv, setExportingCsv] = useState(false);

  const load = useCallback(async (p = 1) => {
    setLoading(true);
    try {
      const res = await getProviders({
        page: p, page_size: 50,
        ...(q         && { q }),
        ...(state     && { state }),
        ...(specialty && { specialty }),
        ...(isExcluded !== "" && { is_excluded: isExcluded === "true" }),
        ...(minRisk   && { min_risk: Number(minRisk) }),
        ...(physicianOnly && { physician_only: true }),
      });
      setItems(res.items);
      setTotal(res.total);
      setPage(p);
    } finally {
      setLoading(false);
    }
  }, [q, state, specialty, isExcluded, minRisk, physicianOnly]);

  useEffect(() => { load(1); }, [load]);

  // Keep URL in sync with filter state (so links are shareable)
  useEffect(() => {
    const params = new URLSearchParams();
    if (q)         params.set("q", q);
    if (state)     params.set("state", state);
    if (specialty) params.set("specialty", specialty);
    if (isExcluded) params.set("is_excluded", isExcluded);
    if (minRisk)      params.set("min_risk", minRisk);
    if (physicianOnly) params.set("physician_only", "true");
    const qs = params.toString();
    router.replace(`/providers${qs ? `?${qs}` : ""}`, { scroll: false });
  }, [q, state, specialty, isExcluded, minRisk, router]);

  async function handleExportCsv() {
    setExportingCsv(true);
    try {
      const token = localStorage.getItem("vigil_token");
      const url = getProvidersCsvUrl({
        ...(state   && { state }),
        ...(minRisk && { min_risk: minRisk }),
      });
      const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
      if (!res.ok) throw new Error(`${res.status}`);
      const blob = await res.blob();
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = "vigil_providers.csv";
      a.click();
    } catch (e) { console.error("CSV export failed:", e); }
    finally { setExportingCsv(false); }
  }

  const pages = Math.ceil(total / 50);

  // Active filter count (for badge)
  const activeFilters = [q, state, specialty, isExcluded, minRisk, physicianOnly ? "1" : ""].filter(Boolean).length;

  return (
    <AppShell>
      <div className="p-8 max-w-7xl mx-auto fade-in">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-xl font-bold text-slate-100">Providers</h1>
            <p className="text-sm text-slate-500 mt-0.5">
              {loading ? "Loading…" : `${total.toLocaleString()} providers`}
              {state && ` · ${STATE_NAMES[state] ?? state}`}
              {minRisk && ` · Risk ≥ ${minRisk}`}
            </p>
          </div>
          <button
            onClick={handleExportCsv}
            disabled={exportingCsv}
            className="flex items-center gap-2 text-xs text-slate-400 hover:text-slate-200 border border-white/[0.08] hover:border-white/[0.15] px-3 py-2 rounded-lg transition disabled:opacity-50"
          >
            <Download size={13} /> {exportingCsv ? "Exporting…" : "Export CSV"}
          </button>
        </div>

        {/* Active filter pill */}
        {(state || minRisk || isExcluded || specialty) && (
          <div className="flex items-center gap-2 mb-4 flex-wrap">
            {state && (
              <span className="flex items-center gap-1.5 text-xs bg-blue-500/10 border border-blue-500/20 text-blue-400 px-3 py-1 rounded-full">
                {STATE_NAMES[state] ?? state}
                <button onClick={() => setState("")} className="hover:text-white ml-0.5">×</button>
              </span>
            )}
            {minRisk && (
              <span className="flex items-center gap-1.5 text-xs bg-red-500/10 border border-red-500/20 text-red-400 px-3 py-1 rounded-full">
                Risk ≥ {minRisk}
                <button onClick={() => setMinRisk("")} className="hover:text-white ml-0.5">×</button>
              </span>
            )}
            {isExcluded === "true" && (
              <span className="flex items-center gap-1.5 text-xs bg-red-500/10 border border-red-500/20 text-red-400 px-3 py-1 rounded-full">
                LEIE Only
                <button onClick={() => setIsExcluded("")} className="hover:text-white ml-0.5">×</button>
              </span>
            )}
            {isExcluded === "false" && (
              <span className="flex items-center gap-1.5 text-xs bg-yellow-500/10 border border-yellow-500/20 text-yellow-400 px-3 py-1 rounded-full">
                New Leads Only
                <button onClick={() => setIsExcluded("")} className="hover:text-white ml-0.5">×</button>
              </span>
            )}
            {specialty && (
              <span className="flex items-center gap-1.5 text-xs bg-purple-500/10 border border-purple-500/20 text-purple-400 px-3 py-1 rounded-full">
                {specialty}
                <button onClick={() => setSpecialty("")} className="hover:text-white ml-0.5">×</button>
              </span>
            )}
            {physicianOnly && (
              <span className="flex items-center gap-1.5 text-xs bg-green-500/10 border border-green-500/20 text-green-400 px-3 py-1 rounded-full">
                Physicians only
                <button onClick={() => setPhysicianOnly(false)} className="hover:text-white ml-0.5">×</button>
              </span>
            )}
            <button
              onClick={() => { setState(""); setMinRisk(""); setIsExcluded(""); setSpecialty(""); setQ(""); setPhysicianOnly(false); }}
              className="text-xs text-slate-600 hover:text-slate-400 transition"
            >
              Clear all
            </button>
          </div>
        )}

        {/* Filters */}
        <div className="flex flex-wrap gap-2 mb-5">
          <div className="relative flex-1 min-w-[200px]">
            <Search size={13} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" />
            <input
              type="text"
              placeholder="Search name, NPI, specialty, city…"
              value={q}
              onChange={e => setQ(e.target.value)}
              className="w-full bg-white/[0.03] border border-white/[0.07] rounded-lg pl-8 pr-3 py-2 text-sm text-slate-200 placeholder-slate-600 focus:outline-none focus:border-blue-500/40 transition"
            />
          </div>

          {/* State dropdown — all 51 */}
          <select
            value={state}
            onChange={e => setState(e.target.value)}
            className="bg-white/[0.03] border border-white/[0.07] rounded-lg px-3 py-2 text-sm text-slate-300 focus:outline-none focus:border-blue-500/40 min-w-[140px]"
          >
            <option value="" className="bg-[#0f1623]">All States</option>
            {ALL_STATES.map(s => (
              <option key={s} value={s} className="bg-[#0f1623]">
                {s} — {STATE_NAMES[s]}
              </option>
            ))}
          </select>

          <select
            value={specialty}
            onChange={e => setSpecialty(e.target.value)}
            className="bg-white/[0.03] border border-white/[0.07] rounded-lg px-3 py-2 text-sm text-slate-300 focus:outline-none focus:border-blue-500/40"
          >
            {SPECIALTIES.map(s => <option key={s} value={s} className="bg-[#0f1623]">{s || "All Specialties"}</option>)}
          </select>

          <select
            value={isExcluded}
            onChange={e => setIsExcluded(e.target.value)}
            className="bg-white/[0.03] border border-white/[0.07] rounded-lg px-3 py-2 text-sm text-slate-300 focus:outline-none focus:border-blue-500/40"
          >
            <option value=""  className="bg-[#0f1623]">All Providers</option>
            <option value="true"  className="bg-[#0f1623]">LEIE Excluded</option>
            <option value="false" className="bg-[#0f1623]">New Leads Only</option>
          </select>

          <select
            value={minRisk}
            onChange={e => setMinRisk(e.target.value)}
            className="bg-white/[0.03] border border-white/[0.07] rounded-lg px-3 py-2 text-sm text-slate-300 focus:outline-none focus:border-blue-500/40"
          >
            <option value=""   className="bg-[#0f1623]">Any Risk Score</option>
            <option value="90" className="bg-[#0f1623]">Critical (90+)</option>
            <option value="70" className="bg-[#0f1623]">High+ (70+)</option>
            <option value="50" className="bg-[#0f1623]">Medium+ (50+)</option>
          </select>

          {/* Physician-only toggle — removes labs, imaging, DME from list */}
          <button
            onClick={() => setPhysicianOnly(v => !v)}
            title="Exclude labs, imaging centers, DME suppliers and other facility-type providers whose high volumes are normal, not suspicious"
            className={`flex items-center gap-2 px-3 py-2 rounded-lg text-xs font-medium border transition whitespace-nowrap ${
              physicianOnly
                ? "bg-green-500/15 border-green-500/30 text-green-400"
                : "bg-white/[0.03] border-white/[0.07] text-slate-400 hover:text-slate-200 hover:border-white/[0.15]"
            }`}
          >
            <span className={`w-2 h-2 rounded-full ${physicianOnly ? "bg-green-400" : "bg-slate-600"}`} />
            Physicians only
          </button>
        </div>

        {/* Table */}
        <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/[0.06]">
                {["Risk", "Provider", "Specialty", "Location", "Billed (2022)", "Flags"].map(h => (
                  <th key={h} className="text-left text-[10px] uppercase tracking-widest text-slate-600 font-medium px-4 py-3">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr><td colSpan={6} className="px-4 py-12 text-center text-slate-600 text-sm animate-pulse">Loading…</td></tr>
              ) : items.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-4 py-16 text-center">
                    <ShieldAlert size={28} className="text-slate-700 mx-auto mb-3" />
                    <p className="text-slate-500 text-sm">No providers match these filters.</p>
                    {activeFilters > 0 && (
                      <button
                        onClick={() => { setState(""); setMinRisk(""); setIsExcluded(""); setSpecialty(""); setQ(""); }}
                        className="mt-2 text-xs text-blue-400 hover:text-blue-300 transition"
                      >
                        Clear filters
                      </button>
                    )}
                  </td>
                </tr>
              ) : items.map(p => (
                <tr
                  key={p.npi}
                  onClick={() => router.push(`/providers/${p.npi}`)}
                  className="border-b border-white/[0.03] hover:bg-white/[0.05] transition cursor-pointer group"
                >
                  <td className="px-4 py-3">
                    <span className={`text-sm font-bold font-mono px-2 py-0.5 rounded ${riskTierBadge(p.risk_score)}`}>
                      {p.risk_score != null ? Number(p.risk_score).toFixed(0) : "—"}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-2">
                      <div>
                        <span className="text-slate-200 font-medium group-hover:text-white transition">
                          {providerName(p)}
                        </span>
                        {p.is_excluded && (
                          <span className="ml-2 text-[9px] bg-red-500/15 text-red-400 border border-red-500/20 px-1.5 py-0.5 rounded font-mono">LEIE</span>
                        )}
                        <span className="block text-[10px] text-slate-600 font-mono mt-0.5">{p.npi}</span>
                      </div>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-slate-400 text-xs max-w-[160px] truncate">{p.specialty ?? "—"}</td>
                  <td className="px-4 py-3 text-slate-500 text-xs whitespace-nowrap">{p.city}, {p.state}</td>
                  <td className="px-4 py-3 text-slate-300 font-mono text-xs">{fmt(p.total_payment)}</td>
                  <td className="px-4 py-3">
                    {(() => {
                      const displayCount = (p.flag_count ?? 0) > 0
                        ? p.flag_count
                        : (p.flags?.length ?? 0);
                      return p.is_excluded ? (
                        <span className="text-xs font-mono text-red-400 bg-red-500/10 border border-red-500/20 px-2 py-0.5 rounded">
                          LEIE excluded
                        </span>
                      ) : displayCount > 0 ? (
                        <span className="text-xs font-mono text-orange-400 bg-orange-500/10 border border-orange-500/20 px-2 py-0.5 rounded">
                          {displayCount} signal{displayCount !== 1 ? "s" : ""}
                        </span>
                      ) : (
                        <span className="text-xs text-slate-700">—</span>
                      );
                    })()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {pages > 1 && (
          <div className="flex items-center justify-between mt-4">
            <span className="text-xs text-slate-600">
              Page {page} of {pages.toLocaleString()} · {total.toLocaleString()} total
            </span>
            <div className="flex gap-2">
              <button onClick={() => load(page - 1)} disabled={page === 1}
                className="text-xs text-slate-400 hover:text-slate-200 disabled:opacity-30 border border-white/[0.07] px-3 py-1.5 rounded-lg transition">
                ← Prev
              </button>
              <button onClick={() => load(page + 1)} disabled={page === pages}
                className="text-xs text-slate-400 hover:text-slate-200 disabled:opacity-30 border border-white/[0.07] px-3 py-1.5 rounded-lg transition">
                Next →
              </button>
            </div>
          </div>
        )}
      </div>
    </AppShell>
  );
}
