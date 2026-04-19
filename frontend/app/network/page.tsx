"use client";

import { useState, useCallback, useRef, useEffect } from "react";
import { useSearchParams } from "next/navigation";
import dynamic from "next/dynamic";
import AppShell from "@/components/AppShell";
import {
  searchNetworkProviders,
  getProviderNetwork,
  getProviderNetwork2Hop,
} from "@/lib/api";
import type { NetworkGraph, NetworkNode } from "@/types";
import { fmt, fmtNum } from "@/lib/utils";
import Link from "next/link";

const NetworkGraph = dynamic(() => import("@/components/NetworkGraph"), {
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-[640px] bg-slate-950 rounded-xl">
      <span className="text-slate-500 text-sm">Loading graph…</span>
    </div>
  ),
});

const NODE_OPTIONS = [15, 30, 60, 120] as const;
type NodeCount = (typeof NODE_OPTIONS)[number];

export default function NetworkPage() {
  const searchParams = useSearchParams();

  const [query, setQuery]               = useState("");
  const [suggestions, setSuggestions]   = useState<NetworkNode[]>([]);
  const [graph, setGraph]               = useState<NetworkGraph | null>(null);
  const [selected, setSelected]         = useState<NetworkNode | null>(null);
  const [loading, setLoading]           = useState(false);
  const [hop, setHop]                   = useState<1 | 2>(1);
  const [error, setError]               = useState<string | null>(null);
  const [maxNodes, setMaxNodes]         = useState<NodeCount>(30);
  const [suspiciousOnly, setSuspOnly]   = useState(false);

  const searchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleSearch = useCallback((val: string) => {
    setQuery(val);
    if (searchTimer.current) clearTimeout(searchTimer.current);
    if (val.length < 2) { setSuggestions([]); return; }
    searchTimer.current = setTimeout(async () => {
      try { setSuggestions(await searchNetworkProviders(val)); }
      catch { setSuggestions([]); }
    }, 300);
  }, []);

  const loadGraph = useCallback(
    async (npi: string, hopCount: 1 | 2 = hop) => {
      setLoading(true);
      setError(null);
      setSuggestions([]);
      try {
        const data =
          hopCount === 2
            ? await getProviderNetwork2Hop(npi, 100)
            : await getProviderNetwork(npi);
        setGraph(data);
        setSelected(null);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load network");
      } finally {
        setLoading(false);
      }
    },
    [hop]
  );

  const handleSelectSuggestion = (node: NetworkNode) => {
    setQuery(node.name || node.npi);
    loadGraph(node.npi, hop);
  };

  const handleHopChange = (newHop: 1 | 2) => {
    setHop(newHop);
    if (graph) loadGraph(graph.center_npi, newHop);
  };

  useEffect(() => {
    const npi = searchParams.get("npi");
    if (npi) loadGraph(npi, hop);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const center = graph?.nodes.find((n) => n.npi === graph.center_npi) ?? null;
  const suspEdges = graph?.edges.filter((e) => e.is_suspicious) ?? [];

  return (
    <AppShell>
      <div className="p-6 space-y-4">

        {/* ── Header ─────────────────────────────────────────────────────── */}
        <div className="flex items-start justify-between gap-4 flex-wrap">
          <div>
            <h1 className="text-2xl font-bold text-white">Referral Network</h1>
            <p className="text-slate-400 text-sm mt-0.5">
              Explore co-billing clusters and suspicious referral patterns
            </p>
          </div>

          {graph && (
            <div className="flex gap-2 text-xs shrink-0">
              <StatChip label="Nodes" value={graph.stats.total_nodes} />
              <StatChip label="Edges" value={graph.stats.total_edges} />
              <StatChip
                label="Suspicious"
                value={graph.stats.suspicious_edges}
                red={graph.stats.suspicious_edges > 0}
              />
            </div>
          )}
        </div>

        {/* ── Search + hop ───────────────────────────────────────────────── */}
        <div className="flex gap-3 items-start flex-wrap">
          <div className="relative flex-1 min-w-64">
            <input
              type="text"
              value={query}
              onChange={(e) => handleSearch(e.target.value)}
              placeholder="Search provider by name or NPI…"
              className="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
            />
            {suggestions.length > 0 && (
              <div className="absolute top-full left-0 right-0 mt-1 bg-slate-800 border border-slate-700 rounded-lg shadow-2xl z-50 overflow-hidden">
                {suggestions.map((s) => (
                  <button
                    key={s.npi}
                    onClick={() => handleSelectSuggestion(s)}
                    className="w-full text-left px-4 py-2.5 hover:bg-slate-700/70 transition-colors border-b border-slate-700/40 last:border-0"
                  >
                    <div className="flex items-center justify-between">
                      <div>
                        <span className="text-white text-sm font-medium">{s.name}</span>
                        <span className="text-slate-500 text-xs ml-2">{s.npi}</span>
                      </div>
                      <RiskPill score={s.risk_score} />
                    </div>
                    <div className="text-slate-500 text-xs mt-0.5">{s.specialty} · {s.state}</div>
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Hop depth */}
          <div className="flex bg-slate-800 border border-slate-700 rounded-lg overflow-hidden">
            {([1, 2] as const).map((h) => (
              <button
                key={h}
                onClick={() => handleHopChange(h)}
                className={`px-4 py-2.5 text-sm font-medium transition-colors ${
                  hop === h ? "bg-blue-600 text-white" : "text-slate-400 hover:text-white"
                }`}
              >
                {h}-hop
              </button>
            ))}
          </div>
        </div>

        {/* ── View controls ──────────────────────────────────────────────── */}
        <div className="flex flex-wrap items-center gap-4 py-2 border-y border-slate-800">
          {/* Max nodes */}
          <div className="flex items-center gap-2">
            <span className="text-xs text-slate-500">Show top</span>
            {NODE_OPTIONS.map((n) => (
              <button
                key={n}
                onClick={() => setMaxNodes(n)}
                className={`px-2.5 py-1 rounded-md border text-xs font-medium transition-colors ${
                  maxNodes === n
                    ? "bg-blue-600 border-blue-500 text-white"
                    : "bg-slate-800/60 border-slate-700 text-slate-400 hover:text-white hover:border-slate-600"
                }`}
              >
                {n}
              </button>
            ))}
            <span className="text-xs text-slate-500">connections</span>
          </div>

          <div className="h-4 w-px bg-slate-700" />

          {/* Suspicious only */}
          <button
            onClick={() => setSuspOnly((v) => !v)}
            className={`flex items-center gap-2 px-3 py-1.5 rounded-lg border text-xs font-medium transition-all ${
              suspiciousOnly
                ? "bg-red-950/50 border-red-700/60 text-red-300"
                : "bg-slate-800/60 border-slate-700 text-slate-400 hover:text-white hover:border-slate-600"
            }`}
          >
            <span
              className={`inline-flex w-3.5 h-3.5 rounded border items-center justify-center transition-colors ${
                suspiciousOnly ? "bg-red-500 border-red-400" : "border-slate-500"
              }`}
            >
              {suspiciousOnly && (
                <svg viewBox="0 0 10 8" className="w-2.5 h-2 fill-white">
                  <path d="M1 4l3 3 5-6" stroke="white" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              )}
            </span>
            Suspicious edges only
          </button>

          {/* Legend */}
          <div className="flex flex-wrap gap-3 ml-auto">
            {(
              [
                { color: "#ef4444", label: "Critical (90+)" },
                { color: "#f97316", label: "High (70–89)" },
                { color: "#eab308", label: "Medium (50–69)" },
                { color: "#22c55e", label: "Low (<50)" },
                { color: "#7c3aed", label: "LEIE excluded" },
              ] as const
            ).map(({ color, label }) => (
              <div key={label} className="flex items-center gap-1.5 text-xs text-slate-400">
                <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
                {label}
              </div>
            ))}
            <div className="flex items-center gap-1.5 text-xs text-slate-400">
              <svg width="22" height="8" className="shrink-0">
                <line x1="0" y1="4" x2="22" y2="4" stroke="#ef4444" strokeWidth="2" strokeDasharray="4 3" />
              </svg>
              Suspicious edge
            </div>
          </div>
        </div>

        {error && (
          <div className="bg-red-900/20 border border-red-800/50 text-red-300 rounded-lg px-4 py-3 text-sm">
            {error}
          </div>
        )}

        {/* ── Empty state ────────────────────────────────────────────────── */}
        {!graph && !loading && !error && (
          <div className="flex flex-col items-center justify-center h-[460px] bg-slate-900/30 border border-slate-800 rounded-xl text-slate-500">
            <svg className="w-16 h-16 mb-4 opacity-25" viewBox="0 0 64 64" fill="none">
              <circle cx="16" cy="32" r="8" stroke="currentColor" strokeWidth="2" />
              <circle cx="48" cy="16" r="8" stroke="currentColor" strokeWidth="2" />
              <circle cx="48" cy="48" r="8" stroke="currentColor" strokeWidth="2" />
              <line x1="24" y1="32" x2="40" y2="20" stroke="currentColor" strokeWidth="2" />
              <line x1="24" y1="32" x2="40" y2="44" stroke="currentColor" strokeWidth="2" />
              <line x1="48" y1="24" x2="48" y2="40" stroke="currentColor" strokeWidth="2" />
            </svg>
            <p className="text-sm font-medium text-slate-400">Search for a provider to explore their network</p>
            <p className="text-xs mt-1">High-risk providers show the most connections</p>
          </div>
        )}

        {loading && (
          <div className="flex items-center justify-center h-[640px] bg-slate-950 rounded-xl border border-slate-800">
            <div className="flex flex-col items-center gap-3 text-slate-400">
              <div className="w-7 h-7 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
              <span className="text-sm">Building network graph…</span>
            </div>
          </div>
        )}

        {/* ── Graph + sidebar ─────────────────────────────────────────────── */}
        {graph && !loading && (
          <div className="flex gap-4">
            {/* Canvas */}
            <div className="flex-1 min-w-0 rounded-xl overflow-hidden border border-slate-800 relative">
              <NetworkGraph
                nodes={graph.nodes}
                edges={graph.edges}
                centerNpi={graph.center_npi}
                onNodeClick={setSelected}
                height={640}
                maxNodes={maxNodes}
                suspiciousOnly={suspiciousOnly}
              />
              {/* Hint overlay — only when graph is loaded */}
              <div className="absolute bottom-3 left-3 text-xs text-slate-600 pointer-events-none select-none">
                Scroll to zoom · Drag to pan · Hover to focus
              </div>
            </div>

            {/* Sidebar */}
            <div className="w-68 shrink-0 space-y-3" style={{ width: 272 }}>

              {/* Center provider */}
              {center && (
                <div className="bg-slate-900 border border-slate-700 rounded-xl p-4">
                  <div className="text-[10px] font-semibold text-slate-500 uppercase tracking-widest mb-2">Center Node</div>
                  <div className="font-semibold text-white text-sm leading-snug">{center.name}</div>
                  <div className="text-slate-400 text-xs mt-0.5">{center.specialty}</div>
                  <div className="text-slate-500 text-xs">{center.state}</div>
                  {center.flag_count > 0 && (
                    <div className="text-orange-400 text-xs mt-1.5 flex items-center gap-1">
                      <span>⚠</span>
                      {center.flag_count} active fraud signal{center.flag_count !== 1 ? "s" : ""}
                    </div>
                  )}
                  <div className="flex items-center justify-between mt-3">
                    <RiskPill score={center.risk_score} large />
                    {center.is_excluded && (
                      <span className="text-xs bg-purple-900/40 text-purple-300 border border-purple-700/60 px-2 py-0.5 rounded-full">
                        LEIE
                      </span>
                    )}
                  </div>
                  <Link
                    href={`/providers/${center.npi}`}
                    className="mt-3 block text-center text-xs bg-blue-600 hover:bg-blue-500 text-white rounded-lg py-2 transition-colors font-medium"
                  >
                    View Full Profile →
                  </Link>
                </div>
              )}

              {/* Selected node */}
              {selected && selected.npi !== graph.center_npi && (
                <div className="bg-slate-900 border border-blue-900/50 rounded-xl p-4">
                  <div className="text-[10px] font-semibold text-blue-400 uppercase tracking-widest mb-2">Selected</div>
                  <div className="font-semibold text-white text-sm leading-snug">{selected.name}</div>
                  <div className="text-slate-400 text-xs mt-0.5">{selected.specialty}</div>
                  <div className="text-slate-500 text-xs">{selected.state}</div>
                  {selected.total_payment > 0 && (
                    <div className="text-slate-400 text-xs mt-1.5">
                      Billed: <span className="text-white font-medium">{fmt(selected.total_payment)}</span>
                    </div>
                  )}
                  {selected.flag_count > 0 && (
                    <div className="text-orange-400 text-xs mt-1 flex items-center gap-1">
                      <span>⚠</span>
                      {selected.flag_count} active fraud signal{selected.flag_count !== 1 ? "s" : ""}
                    </div>
                  )}
                  <div className="flex items-center justify-between mt-3">
                    <RiskPill score={selected.risk_score} large />
                    {selected.is_excluded && (
                      <span className="text-xs bg-purple-900/40 text-purple-300 border border-purple-700/60 px-2 py-0.5 rounded-full">
                        LEIE
                      </span>
                    )}
                  </div>
                  <div className="flex gap-2 mt-3">
                    <Link
                      href={`/providers/${selected.npi}`}
                      className="flex-1 text-center text-xs bg-slate-800 hover:bg-slate-700 text-white rounded-lg py-2 transition-colors"
                    >
                      View Profile
                    </Link>
                    <button
                      onClick={() => {
                        setQuery(selected.name || selected.npi);
                        loadGraph(selected.npi, hop);
                      }}
                      className="flex-1 text-center text-xs bg-blue-600 hover:bg-blue-500 text-white rounded-lg py-2 transition-colors font-medium"
                    >
                      Expand →
                    </button>
                  </div>
                </div>
              )}

              {/* Suspicious edges list */}
              {suspEdges.length > 0 && (
                <div className="bg-slate-900 border border-red-900/30 rounded-xl p-4">
                  <div className="text-[10px] font-semibold text-red-400 uppercase tracking-widest mb-3">
                    Suspicious Edges · {suspEdges.length}
                  </div>
                  <div className="space-y-2.5 max-h-72 overflow-y-auto pr-1">
                    {suspEdges.slice(0, 12).map((e) => {
                      const src = graph.nodes.find((n) => n.npi === e.source);
                      const tgt = graph.nodes.find((n) => n.npi === e.target);
                      return (
                        <div
                          key={e.id}
                          className="text-xs border-l-2 border-red-700/70 pl-2.5 space-y-0.5"
                        >
                          <div className="text-white font-medium truncate">
                            {src?.name ?? e.source}
                          </div>
                          <div className="text-slate-500 truncate">
                            → {tgt?.name ?? e.target}
                          </div>
                          <div className="text-slate-400">
                            {fmtNum(e.shared_patients)} shared patients
                          </div>
                        </div>
                      );
                    })}
                    {suspEdges.length > 12 && (
                      <div className="text-slate-600 text-xs text-center pt-1">
                        +{suspEdges.length - 12} more
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Data source note */}
              <div className="text-[10px] text-slate-600 leading-relaxed px-1">
                Source: CMS Physician Shared Patient Patterns 2015 (90-day window).
                Suspicious = both providers flagged by fraud detection or LEIE-excluded.
              </div>
            </div>
          </div>
        )}
      </div>
    </AppShell>
  );
}

// ── Small components ──────────────────────────────────────────────────────────

function StatChip({
  label, value, red,
}: { label: string; value: number; red?: boolean }) {
  return (
    <div className={`px-3 py-2 rounded-lg border text-center min-w-[64px] ${
      red
        ? "bg-red-950/30 border-red-800/50 text-red-300"
        : "bg-slate-800/60 border-slate-700 text-slate-300"
    }`}>
      <div className="text-base font-bold leading-none">{fmtNum(value)}</div>
      <div className="text-[10px] mt-0.5 opacity-60">{label}</div>
    </div>
  );
}

function RiskPill({ score, large }: { score: number; large?: boolean }) {
  const cls =
    score >= 90 ? "text-red-400 bg-red-900/30 border-red-800/60"
    : score >= 70 ? "text-orange-400 bg-orange-900/30 border-orange-800/60"
    : score >= 50 ? "text-yellow-400 bg-yellow-900/30 border-yellow-800/60"
    : "text-green-400 bg-green-900/30 border-green-800/60";

  return (
    <span className={`border rounded-full font-bold ${cls} ${
      large ? "px-2.5 py-1 text-sm" : "px-2 py-0.5 text-xs"
    }`}>
      {score.toFixed(0)}
    </span>
  );
}
