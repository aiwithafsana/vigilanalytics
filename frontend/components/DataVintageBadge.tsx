"use client";

/**
 * DataVintageBadge — shows how fresh the scoring + LEIE data is.
 *
 * Required by the methodology doc (§8 Limitations and Required Verification):
 * every page that displays a risk score must surface data freshness so users
 * can independently verify what window the score covers.
 *
 * Two display modes:
 *   variant="compact"  — single-line tooltip badge (header / sidebar)
 *   variant="full"     — multi-line panel (provider detail / case page)
 */
import { useEffect, useState } from "react";
import { getDataVintage, type DataVintage } from "@/lib/api";

interface Props {
  variant?: "compact" | "full";
  className?: string;
}

function relativeTime(iso: string | null | undefined): string {
  if (!iso) return "never";
  const ms = Date.now() - new Date(iso).getTime();
  if (ms < 0) return "just now";
  const min = Math.floor(ms / 60_000);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const d = Math.floor(hr / 24);
  if (d < 30) return `${d}d ago`;
  const mo = Math.floor(d / 30);
  if (mo < 12) return `${mo}mo ago`;
  const yr = Math.floor(d / 365);
  return `${yr}y ago`;
}

function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toISOString().slice(0, 10);
}

export default function DataVintageBadge({ variant = "compact", className = "" }: Props) {
  const [v, setV] = useState<DataVintage | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    getDataVintage()
      .then((d) => alive && setV(d))
      .catch((e) => alive && setError(e?.message ?? "Failed to load"));
    return () => {
      alive = false;
    };
  }, []);

  if (error) {
    return (
      <div className={`text-xs text-red-600 ${className}`} title={error}>
        Data vintage unavailable
      </div>
    );
  }

  if (!v) {
    return <div className={`text-xs text-neutral-400 ${className}`}>Loading vintage…</div>;
  }

  if (variant === "compact") {
    const tooltip =
      `Scoring data: CMS Part B ${v.scoring_data_year} (through ${v.scoring_data_through}).\n` +
      `Last scored: ${formatDate(v.providers_last_scored_at)}\n` +
      `LEIE refreshed: ${formatDate(v.leie_last_refreshed_at)} (${relativeTime(v.leie_last_refreshed_at)})\n` +
      `Model version: ${v.model_version}`;
    return (
      <div
        className={`text-xs text-neutral-500 hover:text-neutral-700 cursor-help ${className}`}
        title={tooltip}
      >
        <span className="inline-block w-2 h-2 rounded-full bg-emerald-500 mr-1.5 align-middle" />
        Data: CMS {v.scoring_data_year} · LEIE {relativeTime(v.leie_last_refreshed_at)} · Model v{v.model_version}
      </div>
    );
  }

  // variant="full"
  return (
    <div className={`rounded border border-neutral-200 bg-neutral-50 px-3 py-2 text-xs ${className}`}>
      <div className="font-semibold text-neutral-700 mb-1">Data Vintage</div>
      <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-neutral-600">
        <dt>Scoring data:</dt>
        <dd>CMS Part B {v.scoring_data_year} (through {v.scoring_data_through})</dd>

        <dt>Last scored:</dt>
        <dd>{formatDate(v.providers_last_scored_at)}</dd>

        <dt>LEIE refreshed:</dt>
        <dd>
          {formatDate(v.leie_last_refreshed_at)}{" "}
          <span className="text-neutral-400">({relativeTime(v.leie_last_refreshed_at)})</span>
        </dd>

        <dt>Active LEIE matches:</dt>
        <dd>{v.leie_active_count.toLocaleString()}</dd>

        <dt>Model version:</dt>
        <dd>v{v.model_version}</dd>
      </dl>
    </div>
  );
}
