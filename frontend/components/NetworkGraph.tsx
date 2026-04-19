"use client";

import { useEffect, useRef, useCallback } from "react";
import * as d3 from "d3";
import type { NetworkNode, NetworkEdge } from "@/types";

interface SimNode extends NetworkNode, d3.SimulationNodeDatum {
  id: string;
}

interface SimLink extends d3.SimulationLinkDatum<SimNode> {
  edge: NetworkEdge;
}

interface Props {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
  centerNpi: string;
  onNodeClick?: (node: NetworkNode) => void;
  height?: number;
  maxNodes?: number;        // max non-center neighbor nodes to render
  suspiciousOnly?: boolean; // when true, hide non-suspicious edges
}

export default function NetworkGraph({
  nodes,
  edges,
  centerNpi,
  onNodeClick,
  height = 600,
  maxNodes = 30,
  suspiciousOnly = false,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef       = useRef<SVGSVGElement>(null);
  const simRef       = useRef<d3.Simulation<SimNode, SimLink> | null>(null);
  const cleanupRef   = useRef<(() => void) | null>(null);

  const draw = useCallback(
    (width: number) => {
      if (!svgRef.current || !containerRef.current || nodes.length === 0) return;

      if (simRef.current) simRef.current.stop();
      if (cleanupRef.current) cleanupRef.current();
      d3.select(svgRef.current).selectAll("*").remove();

      // ── Filter: reduce to top maxNodes neighbors ───────────────────────────
      let workEdges = suspiciousOnly
        ? edges.filter((e) => e.is_suspicious)
        : edges;

      // Score each non-center node by its best (max shared_patients) edge
      const nodeScore = new Map<string, number>();
      workEdges.forEach((e) => {
        for (const npi of [e.source, e.target]) {
          if (npi !== centerNpi)
            nodeScore.set(npi, Math.max(nodeScore.get(npi) ?? 0, e.shared_patients));
        }
      });

      const topNPIs = new Set([
        centerNpi,
        ...[...nodeScore.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, maxNodes)
          .map(([npi]) => npi),
      ]);

      const dispNodes = nodes.filter((n) => topNPIs.has(n.npi));
      const dispEdges = workEdges.filter(
        (e) => topNPIs.has(e.source) && topNPIs.has(e.target)
      );

      if (dispNodes.length === 0) return;

      // ── SVG & zoom ────────────────────────────────────────────────────────
      const svg = d3
        .select(svgRef.current)
        .attr("width", width)
        .attr("height", height);

      const g = svg.append("g");
      svg.call(
        d3
          .zoom<SVGSVGElement, unknown>()
          .scaleExtent([0.1, 6])
          .on("zoom", (ev) => g.attr("transform", ev.transform))
      );

      // ── Radial pre-positioning ─────────────────────────────────────────────
      const cx = width / 2;
      const cy = height / 2;

      const nonCenter = dispNodes
        .filter((n) => !n.is_center)
        .sort((a, b) => (nodeScore.get(b.npi) ?? 0) - (nodeScore.get(a.npi) ?? 0));

      // Radius: enough spacing for nodes not to overlap initially
      const ringR = Math.max(
        170,
        Math.min(
          Math.min(width, height) / 2 - 80,
          (nonCenter.length * 32) / (2 * Math.PI)
        )
      );

      const simNodes: SimNode[] = dispNodes.map((n) => {
        if (n.is_center) return { ...n, id: n.npi, x: cx, y: cy };
        const idx = nonCenter.findIndex((nn) => nn.npi === n.npi);
        const angle = (2 * Math.PI * idx) / nonCenter.length - Math.PI / 2;
        return {
          ...n,
          id: n.npi,
          x: cx + ringR * Math.cos(angle) + (Math.random() - 0.5) * 16,
          y: cy + ringR * Math.sin(angle) + (Math.random() - 0.5) * 16,
        };
      });

      // Fix center
      const centerSim = simNodes.find((n) => n.is_center);
      if (centerSim) { centerSim.fx = cx; centerSim.fy = cy; }

      const nodeById = new Map(simNodes.map((n) => [n.npi, n]));

      const simLinks: SimLink[] = dispEdges
        .filter((e) => nodeById.has(e.source) && nodeById.has(e.target))
        .map((e) => ({
          source: nodeById.get(e.source)!,
          target: nodeById.get(e.target)!,
          edge: e,
        }));

      // ── Force simulation ───────────────────────────────────────────────────
      const simulation = d3
        .forceSimulation<SimNode>(simNodes)
        .force(
          "link",
          d3
            .forceLink<SimNode, SimLink>(simLinks)
            .id((d) => d.id)
            .distance(ringR * 0.8)
            .strength(0.15)
        )
        .force("charge", d3.forceManyBody().strength(-380))
        .force(
          "radial",
          d3
            .forceRadial<SimNode>((d) => (d.is_center ? 0 : ringR), cx, cy)
            .strength((d) => (d.is_center ? 1 : 0.45))
        )
        .force("collision", d3.forceCollide<SimNode>((d) => nodeR(d) + 7))
        .alphaDecay(0.05)
        .velocityDecay(0.42);

      simRef.current = simulation;

      // ── Edges ──────────────────────────────────────────────────────────────
      const edgeW = (d: SimLink) =>
        Math.max(1, Math.min(5, Math.log1p(d.edge.shared_patients) * 0.6));

      const link = g
        .append("g")
        .attr("class", "links")
        .selectAll<SVGLineElement, SimLink>("line")
        .data(simLinks)
        .enter()
        .append("line")
        .attr("stroke", (d) => (d.edge.is_suspicious ? "#ef4444" : "#334155"))
        .attr("stroke-opacity", (d) => (d.edge.is_suspicious ? 0.65 : 0.22))
        .attr("stroke-width", edgeW)
        .attr("stroke-dasharray", (d) => (d.edge.is_suspicious ? "5 3" : "none"))
        .style("cursor", "pointer")
        .on("mouseenter", (ev, d) => {
          const susp = d.edge.is_suspicious
            ? `<div style="color:#f87171;margin-top:5px;font-weight:600;font-size:11px">⚠ Suspicious pattern</div>`
            : "";
          tooltipEl.innerHTML = `
            <div style="font-weight:700;margin-bottom:3px">${d.edge.shared_patients.toLocaleString()} shared patients</div>
            <div style="color:#94a3b8;font-size:11px">${d.edge.referral_count.toLocaleString()} shared encounters</div>
            ${susp}`;
          tooltipEl.style.opacity = "1";
          pos(ev as MouseEvent);
        })
        .on("mousemove", (ev) => pos(ev as MouseEvent))
        .on("mouseleave", () => { tooltipEl.style.opacity = "0"; });

      // ── Nodes ──────────────────────────────────────────────────────────────
      const node = g
        .append("g")
        .attr("class", "nodes")
        .selectAll<SVGGElement, SimNode>("g")
        .data(simNodes)
        .enter()
        .append("g")
        .style("cursor", "pointer")
        .call(
          d3
            .drag<SVGGElement, SimNode>()
            .on("start", (ev, d) => {
              if (!ev.active) simulation.alphaTarget(0.15).restart();
              d.fx = d.x; d.fy = d.y;
            })
            .on("drag", (ev, d) => { d.fx = ev.x; d.fy = ev.y; })
            .on("end", (ev, d) => {
              if (!ev.active) simulation.alphaTarget(0);
              if (!d.is_center) { d.fx = null; d.fy = null; }
            })
        )
        .on("click", (_ev, d) => onNodeClick?.(d));

      // Glow ring for center
      node
        .filter((d) => d.is_center)
        .append("circle")
        .attr("r", (d) => nodeR(d) + 8)
        .attr("fill", "none")
        .attr("stroke", "#3b82f6")
        .attr("stroke-width", 1.5)
        .attr("stroke-opacity", 0.35);

      // Main circle
      node
        .append("circle")
        .attr("r", nodeR)
        .attr("fill", nodeColor)
        .attr("stroke", (d) =>
          d.is_center
            ? "#93c5fd"
            : d.is_excluded
            ? "#c4b5fd"
            : "rgba(255,255,255,0.12)"
        )
        .attr("stroke-width", (d) => (d.is_center ? 2.5 : 1.5));

      // LEIE × mark
      node
        .filter((d) => d.is_excluded)
        .append("text")
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "central")
        .attr("font-size", "9px")
        .attr("fill", "white")
        .attr("pointer-events", "none")
        .text("✕");

      // Center label (always shown)
      node
        .filter((d) => d.is_center)
        .append("text")
        .attr("dy", (d) => nodeR(d) + 14)
        .attr("text-anchor", "middle")
        .attr("font-size", "11px")
        .attr("font-weight", "700")
        .attr("fill", "#e2e8f0")
        .attr("pointer-events", "none")
        .text((d) => (d.name.length > 22 ? d.name.slice(0, 21) + "…" : d.name));

      // Labels for critical-risk neighbors (≥90)
      node
        .filter((d) => !d.is_center && d.risk_score >= 90)
        .append("text")
        .attr("dy", (d) => nodeR(d) + 12)
        .attr("text-anchor", "middle")
        .attr("font-size", "9px")
        .attr("fill", "#fca5a5")
        .attr("pointer-events", "none")
        .text((d) => {
          const last = d.name.split(" ").pop() ?? d.name;
          return last.length > 11 ? last.slice(0, 10) + "…" : last;
        });

      // ── Tooltip ────────────────────────────────────────────────────────────
      const tooltipEl = document.createElement("div");
      Object.assign(tooltipEl.style, {
        position: "absolute",
        background: "#0f172a",
        border: "1px solid #1e3a5f",
        borderRadius: "8px",
        padding: "10px 14px",
        fontSize: "12px",
        color: "#e2e8f0",
        pointerEvents: "none",
        opacity: "0",
        zIndex: "50",
        maxWidth: "230px",
        lineHeight: "1.65",
        transition: "opacity 70ms",
        boxShadow: "0 8px 32px rgba(0,0,0,0.6)",
      });
      containerRef.current.appendChild(tooltipEl);

      // ── Hover: highlight connected, dim everything else ────────────────────
      node
        .on("mouseenter", (ev, d) => {
          const connected = new Set([d.npi]);
          simLinks.forEach((l) => {
            const s = (l.source as SimNode).npi;
            const t = (l.target as SimNode).npi;
            if (s === d.npi) connected.add(t);
            if (t === d.npi) connected.add(s);
          });

          node.attr("opacity", (n) => (connected.has(n.npi) ? 1 : 0.07));
          link
            .attr("stroke-opacity", (l) => {
              const s = (l.source as SimNode).npi;
              const t = (l.target as SimNode).npi;
              if (s === d.npi || t === d.npi)
                return l.edge.is_suspicious ? 0.95 : 0.75;
              return 0.03;
            })
            .attr("stroke-width", (l) => {
              const s = (l.source as SimNode).npi;
              const t = (l.target as SimNode).npi;
              return s === d.npi || t === d.npi
                ? Math.max(2.5, edgeW(l) * 1.8)
                : edgeW(l) * 0.5;
            });

          const connCount = connected.size - 1;
          const excl = d.is_excluded
            ? `<div style="color:#a78bfa;margin-top:5px;font-size:11px">⚠ LEIE Excluded</div>`
            : d.flag_count > 0
            ? `<div style="color:#fb923c;margin-top:5px;font-size:11px">${d.flag_count} active fraud signal${d.flag_count !== 1 ? "s" : ""}</div>`
            : "";
          const pay = d.total_payment > 0
            ? `<span style="color:#64748b;font-size:11px">· $${(d.total_payment / 1e6).toFixed(1)}M billed</span>`
            : "";

          tooltipEl.innerHTML = `
            <div style="font-weight:700;margin-bottom:2px">${d.name}</div>
            <div style="color:#94a3b8;font-size:11px">${d.specialty}</div>
            <div style="color:#64748b;font-size:11px">${d.state}</div>
            <div style="margin-top:6px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
              <span style="font-size:13px;font-weight:700;color:${riskColor(d.risk_score)}">
                Risk ${d.risk_score.toFixed(0)}
              </span>
              ${pay}
            </div>
            <div style="color:#475569;font-size:11px;margin-top:2px">
              ${connCount} connection${connCount !== 1 ? "s" : ""} visible
            </div>
            ${excl}`;
          tooltipEl.style.opacity = "1";
          pos(ev as MouseEvent);
        })
        .on("mousemove", (ev) => pos(ev as MouseEvent))
        .on("mouseleave", () => {
          node.attr("opacity", 1);
          link
            .attr("stroke-opacity", (l) => (l.edge.is_suspicious ? 0.65 : 0.22))
            .attr("stroke-width", edgeW);
          tooltipEl.style.opacity = "0";
        });

      function pos(ev: MouseEvent) {
        const rect = containerRef.current!.getBoundingClientRect();
        let x = ev.clientX - rect.left + 18;
        let y = ev.clientY - rect.top - 10;
        const tw = tooltipEl.offsetWidth || 210;
        const th = tooltipEl.offsetHeight || 90;
        if (x + tw > rect.width - 8) x = ev.clientX - rect.left - tw - 18;
        if (y + th > rect.height - 8) y = ev.clientY - rect.top - th - 4;
        if (y < 4) y = 4;
        tooltipEl.style.left = `${x}px`;
        tooltipEl.style.top = `${y}px`;
      }

      // ── Tick ───────────────────────────────────────────────────────────────
      simulation.on("tick", () => {
        link
          .attr("x1", (d) => (d.source as SimNode).x ?? 0)
          .attr("y1", (d) => (d.source as SimNode).y ?? 0)
          .attr("x2", (d) => (d.target as SimNode).x ?? 0)
          .attr("y2", (d) => (d.target as SimNode).y ?? 0);
        node.attr("transform", (d) => `translate(${d.x ?? 0},${d.y ?? 0})`);
      });

      cleanupRef.current = () => {
        simulation.stop();
        tooltipEl.remove();
      };
    },
    [nodes, edges, centerNpi, onNodeClick, height, maxNodes, suspiciousOnly]
  );

  // ── ResizeObserver ─────────────────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;
    let frame: number;
    const ro = new ResizeObserver((entries) => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(() => {
        const w = entries[0]?.contentRect.width ?? 900;
        if (w > 0) draw(w);
      });
    });
    ro.observe(containerRef.current);
    draw(containerRef.current.clientWidth || 900);
    return () => {
      ro.disconnect();
      cancelAnimationFrame(frame);
      if (simRef.current) simRef.current.stop();
      if (cleanupRef.current) cleanupRef.current();
    };
  }, [draw]);

  return (
    <div
      ref={containerRef}
      className="relative w-full bg-slate-950 overflow-hidden"
      style={{ height }}
    >
      <svg ref={svgRef} className="w-full h-full" />
    </div>
  );
}

// ── Helpers ────────────────────────────────────────────────────────────────

function nodeR(d: SimNode): number {
  if (d.is_center)   return 20;
  if (d.is_excluded) return 13;
  // Scale 8–16px by risk score
  return 8 + (d.risk_score / 100) * 8;
}

function nodeColor(d: SimNode): string {
  if (d.is_excluded) return "#7c3aed";
  if (d.is_center)   return "#2563eb";
  return riskColor(d.risk_score);
}

function riskColor(score: number): string {
  if (score >= 90) return "#ef4444";
  if (score >= 70) return "#f97316";
  if (score >= 50) return "#eab308";
  if (score >= 30) return "#22c55e";
  return "#64748b";
}
