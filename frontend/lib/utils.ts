import { clsx, type ClassValue } from "clsx";

export function cn(...inputs: ClassValue[]) {
  return clsx(inputs);
}

// Coerce API values (may arrive as strings from JSON decimals) to number
function n(value: number | string | null | undefined): number | null {
  if (value == null) return null;
  const v = Number(value);
  return isNaN(v) ? null : v;
}

export function fmt(value: number | string | null | undefined): string {
  const v = n(value);
  if (v == null) return "—";
  if (v >= 1_000_000_000) return `$${(v / 1_000_000_000).toFixed(1)}B`;
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`;
  if (v >= 1_000) return `$${(v / 1_000).toFixed(1)}K`;
  return `$${v.toFixed(2)}`;
}

export function fmtNum(value: number | string | null | undefined): string {
  const v = n(value);
  if (v == null) return "—";
  return v.toLocaleString();
}

export function riskColor(score: number | string | null | undefined): string {
  const v = n(score);
  if (v == null) return "text-slate-400";
  if (v >= 90) return "text-red-400";
  if (v >= 70) return "text-orange-400";
  if (v >= 50) return "text-yellow-400";
  return "text-slate-400";
}

export function riskBadge(score: number | string | null | undefined): string {
  const v = n(score);
  if (v == null) return "bg-slate-800 text-slate-400";
  if (v >= 90) return "bg-red-500/10 text-red-400 border border-red-500/20";
  if (v >= 70) return "bg-orange-500/10 text-orange-400 border border-orange-500/20";
  if (v >= 50) return "bg-yellow-500/10 text-yellow-400 border border-yellow-500/20";
  return "bg-slate-800 text-slate-400";
}

export function sevColor(severity: string): string {
  if (severity === "critical") return "text-red-400";
  if (severity === "high") return "text-orange-400";
  return "text-slate-400";
}

export function sevBorder(severity: string): string {
  if (severity === "critical") return "border-red-500/30 bg-red-500/5";
  if (severity === "high") return "border-orange-500/30 bg-orange-500/5";
  return "border-slate-700 bg-slate-800/50";
}

export function statusBadge(status: string): string {
  switch (status) {
    case "open": return "bg-blue-500/10 text-blue-400 border border-blue-500/20";
    case "under_review": return "bg-yellow-500/10 text-yellow-400 border border-yellow-500/20";
    case "closed": return "bg-slate-700 text-slate-400";
    case "referred": return "bg-purple-500/10 text-purple-400 border border-purple-500/20";
    default: return "bg-slate-700 text-slate-400";
  }
}

export function flagEmoji(type: string): string {
  const map: Record<string, string> = {
    billing_volume: "💰",
    service_pattern: "📋",
    beneficiary_volume: "👥",
    statistical_outlier: "📊",
    cost_per_patient: "💸",
    service_intensity: "⚡",
    em_upcoding: "🔺",
  };
  return map[type] ?? "🚩";
}

export function providerName(p: { name_first?: string | null; name_last?: string | null }): string {
  return [p.name_first, p.name_last].filter(Boolean).join(" ") || "Unknown";
}
