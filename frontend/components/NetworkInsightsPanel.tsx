"use client";

/**
 * NetworkInsightsPanel — auto-narrates what the loaded network graph means
 * in plain English.  Solves the "force-directed blob, no idea what to look
 * at" problem by computing a few interpretable graph statistics and
 * surfacing them as a hierarchy:
 *
 *   1. Headline finding (the highest-severity pattern detected)
 *   2. Quantified context (X providers, Y shared patients, $Z combined billing)
 *   3. Specific recommendations (which 3 connections to look at first)
 *
 * The investigator reads top-down and knows in 10 seconds what's in the graph
 * and what to do about it.  Without this, they're staring at a graph of
 * coloured circles with no narrative thread.
 */
import type { NetworkGraph, NetworkNode, NetworkEdge } from "@/types";
import { AlertTriangle, Users, Network, TrendingUp, Eye } from "lucide-react";

interface Props {
  graph: NetworkGraph;
  onSelectNode?: (npi: string) => void;
}

interface Insight {
  severity: "critical" | "high" | "medium" | "info";
  icon:     typeof AlertTriangle;
  title:    string;
  detail:   string;
  npis?:    string[];   // Providers the user should look at first
}

function computeInsights(graph: NetworkGraph): Insight[] {
  const insights: Insight[] = [];
  const nodes = graph.nodes ?? [];
  const edges = graph.edges ?? [];
  if (nodes.length === 0) return insights;

  const center = nodes.find(n => n.npi === graph.center_npi);
  const others = nodes.filter(n => n.npi !== graph.center_npi);

  // Insight 1: high-risk concentration in the neighborhood
  // ─────────────────────────────────────────────────────────────────────
  // A legitimate provider's referral neighborhood is mostly low-risk peers.
  // When a high fraction of their connections are themselves high-risk,
  // that's the "fraud ring" signal.
  const highRiskOthers = others.filter(n => Number(n.risk_score ?? 0) >= 70);
  const fractionHighRisk = highRiskOthers.length / Math.max(others.length, 1);

  if (fractionHighRisk >= 0.4 && others.length >= 3) {
    insights.push({
      severity: fractionHighRisk >= 0.6 ? "critical" : "high",
      icon:     AlertTriangle,
      title: (
        `Possible fraud ring: ${highRiskOthers.length} of ${others.length} `
        + `connections are themselves high-risk (≥70)`
      ),
      detail: (
        `A typical provider's referral network is mostly low-risk peers. `
        + `When ${Math.round(fractionHighRisk * 100)}% of a provider's connections `
        + `are independently flagged, the cluster is probably operating as a group. `
        + `Pull the top 3 below and check whether they share an address or owner.`
      ),
      npis: highRiskOthers
        .slice()
        .sort((a, b) => Number(b.risk_score ?? 0) - Number(a.risk_score ?? 0))
        .slice(0, 3)
        .map(n => n.npi),
    });
  }

  // Insight 2: dense patient sharing (the kickback pattern)
  // ─────────────────────────────────────────────────────────────────────
  // Most referral pairs share 5-30 patients.  Pairs sharing 100+ are
  // unusual — often the "marketing company funnels patients here" pattern.
  const suspiciousEdges = (edges as NetworkEdge[])
    .filter(e => e.is_suspicious)
    .slice()
    .sort((a, b) => Number(b.shared_patients ?? 0) - Number(a.shared_patients ?? 0));

  if (suspiciousEdges.length > 0) {
    const top = suspiciousEdges[0];
    const totalSusp = suspiciousEdges
      .reduce((acc, e) => acc + Number(e.shared_patients ?? 0), 0);
    insights.push({
      severity: "high",
      icon:     Users,
      title: (
        `Disproportionate patient sharing on ${suspiciousEdges.length} edge`
        + `${suspiciousEdges.length === 1 ? "" : "s"} (${totalSusp.toLocaleString()} `
        + `shared patients flagged)`
      ),
      detail: (
        `Top edge: ${top.shared_patients} patients shared. `
        + `Kickback schemes typically appear as a small number of providers `
        + `with disproportionate patient overlap — much higher than the specialty norm. `
        + `Worth investigating these edges first.`
      ),
    });
  }

  // Insight 3: hub centrality (the operator pattern)
  // ─────────────────────────────────────────────────────────────────────
  // Count each provider's degree (number of edges).  A "hub" connected to
  // most of the rest of the cluster is often the operator of the scheme.
  if (others.length >= 5) {
    const degree: Record<string, number> = {};
    for (const e of edges) {
      degree[e.source] = (degree[e.source] ?? 0) + 1;
      degree[e.target] = (degree[e.target] ?? 0) + 1;
    }
    const ranked = nodes
      .filter(n => n.npi !== graph.center_npi)
      .sort((a, b) => (degree[b.npi] ?? 0) - (degree[a.npi] ?? 0));
    const hub = ranked[0];
    const hubDegree = degree[hub?.npi ?? ""] ?? 0;
    // Hub = connected to >50% of the rest of the graph
    if (hubDegree >= Math.max(3, Math.floor(others.length / 2))) {
      insights.push({
        severity: "medium",
        icon:     Network,
        title: (
          `Hub provider detected: ${hub.name ?? hub.npi} `
          + `(${hubDegree} connections, risk ${Math.round(Number(hub.risk_score ?? 0))})`
        ),
        detail: (
          `In organized fraud schemes, one provider is often the operator — `
          + `the person who recruits the others.  They appear as a hub: connected `
          + `to most of the cluster.  Investigate this provider first; the `
          + `others may be downstream beneficiaries of the scheme.`
        ),
        npis: [hub.npi],
      });
    }
  }

  // Insight 4: specialty mismatch (the implausible referral pattern)
  // ─────────────────────────────────────────────────────────────────────
  // Cardiologist ↔ DME supplier sharing 50 patients?  That's not a normal
  // referral.  Surface specialty pairs that don't make clinical sense.
  if (center && others.length >= 1) {
    const centerSpec = (center.specialty ?? "").toLowerCase();
    const implausibleSpecs = others
      .filter(n => {
        const spec = (n.specialty ?? "").toLowerCase();
        if (!centerSpec || !spec) return false;
        // Heuristic: clinical specialties + DME/lab/pharmacy implausible
        // unless there's a clear ordering relationship.
        const isFacility = (
          spec.includes("durable medical") ||
          spec.includes("clinical laboratory") ||
          spec.includes("pharmacy") ||
          spec.includes("ambulance")
        );
        // A pain-management physician referring to pharmacies is plausible;
        // an opthalmologist referring to ambulance services isn't.
        const isClinical = !isFacility;
        return isClinical !== !isFacility &&
               !centerSpec.includes("internal medicine") &&
               !centerSpec.includes("family") &&
               Number(n.risk_score ?? 0) >= 50;
      });
    if (implausibleSpecs.length >= 2) {
      insights.push({
        severity: "medium",
        icon:     TrendingUp,
        title: (
          `${implausibleSpecs.length} cross-specialty referrals worth a closer look`
        ),
        detail: (
          `Some of the connections are to provider types that don't `
          + `obviously fit the central provider's specialty (${center.specialty ?? "?"}).  `
          + `Cross-specialty referrals aren't fraud by themselves, but unusual pairs `
          + `paired with high risk scores are how DME-aggregator and lab-kickback `
          + `schemes show up in the data.`
        ),
        npis: implausibleSpecs.slice(0, 3).map(n => n.npi),
      });
    }
  }

  // Insight 5 (always present): just describe the graph in plain English
  // ─────────────────────────────────────────────────────────────────────
  if (insights.length === 0) {
    insights.push({
      severity: "info",
      icon:     Eye,
      title:    `Network appears typical for this provider's specialty`,
      detail: (
        `${others.length} connection${others.length === 1 ? "" : "s"} found across `
        + `${graph.stats?.total_edges ?? 0} edge${(graph.stats?.total_edges ?? 0) === 1 ? "" : "s"}.  `
        + `No high-risk concentration, no disproportionate sharing, and no hub `
        + `provider detected.  This doesn't mean nothing is wrong — it means the `
        + `network analysis didn't surface an obvious pattern.  The provider's `
        + `solo risk score still applies.`
      ),
    });
  }

  return insights;
}

const SEVERITY_STYLES = {
  critical: { border: "border-red-500/40",    bg: "bg-red-500/[0.08]",    icon: "text-red-400",    label: "CRITICAL" },
  high:     { border: "border-orange-500/30", bg: "bg-orange-500/[0.07]", icon: "text-orange-400", label: "HIGH" },
  medium:   { border: "border-yellow-500/30", bg: "bg-yellow-500/[0.05]", icon: "text-yellow-400", label: "MEDIUM" },
  info:     { border: "border-white/[0.08]",  bg: "bg-white/[0.03]",      icon: "text-slate-400",  label: "INFO" },
} as const;

export default function NetworkInsightsPanel({ graph, onSelectNode }: Props) {
  const insights = computeInsights(graph);
  if (insights.length === 0) return null;

  return (
    <div className="space-y-2.5">
      <div className="text-[10px] uppercase tracking-widest text-slate-500 font-medium px-1">
        What this network shows
      </div>
      {insights.map((ins, i) => {
        const Icon = ins.icon;
        const style = SEVERITY_STYLES[ins.severity];
        return (
          <div
            key={i}
            className={`rounded-lg border ${style.border} ${style.bg} px-4 py-3`}
          >
            <div className="flex items-start gap-2.5">
              <Icon size={14} className={`${style.icon} shrink-0 mt-0.5`} />
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1">
                  <span className={`text-[10px] font-bold tracking-widest ${style.icon}`}>
                    {style.label}
                  </span>
                </div>
                <div className="text-sm font-medium text-slate-100 mb-1">
                  {ins.title}
                </div>
                <p className="text-xs text-slate-400 leading-relaxed">
                  {ins.detail}
                </p>
                {ins.npis && ins.npis.length > 0 && onSelectNode && (
                  <div className="flex gap-1.5 mt-2.5 flex-wrap">
                    {ins.npis.map(npi => (
                      <button
                        key={npi}
                        onClick={() => onSelectNode(npi)}
                        className="text-[11px] font-mono text-slate-300 bg-white/[0.04] hover:bg-white/[0.08] border border-white/[0.08] hover:border-white/[0.15] rounded px-2 py-0.5 transition"
                      >
                        NPI {npi}
                      </button>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}
