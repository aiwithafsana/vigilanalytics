"use client";

/**
 * ActiveInvestigationBanner — "currently being investigated by X" badge.
 *
 * Surfaced at the top of the provider detail page when one or more open or
 * under-review cases exist on the provider.  The intent is to prevent two
 * analysts from independently working the same target without realising it —
 * the #1 friction point surfaced during user-acceptance testing.
 *
 * This is informational, not an access lock — investigators can still open
 * their own case if they want to (e.g., a state AG opening a parallel state
 * action while a federal investigator works a federal action on the same
 * provider).  The banner makes them aware so they can coordinate.
 */
import { useEffect, useState } from "react";
import Link from "next/link";
import { getProviderActiveCases, type ProviderActiveCase } from "@/lib/api";
import { FolderOpen } from "lucide-react";

interface Props {
  npi: string;
}

function daysAgo(iso: string | null): string {
  if (!iso) return "";
  const ms = Date.now() - new Date(iso).getTime();
  const d = Math.floor(ms / (1000 * 60 * 60 * 24));
  if (d < 1) return "today";
  if (d === 1) return "1 day ago";
  return `${d} days ago`;
}

export default function ActiveInvestigationBanner({ npi }: Props) {
  const [cases, setCases] = useState<ProviderActiveCase[] | null>(null);

  useEffect(() => {
    let alive = true;
    getProviderActiveCases(npi)
      .then(c => alive && setCases(c))
      .catch(() => alive && setCases([]));
    return () => { alive = false };
  }, [npi]);

  if (!cases || cases.length === 0) return null;

  // De-duplicate by primary owner — if 3 cases are all from Sarah Liu, we say so once
  const primary = cases[0];
  const otherCount = cases.length - 1;
  const ownerName = primary.assigned_to_name || primary.created_by_name || "another investigator";

  return (
    <div className="mb-4 flex items-start gap-3 rounded-xl border border-amber-500/30 bg-amber-500/[0.06] px-4 py-3">
      <FolderOpen size={16} className="text-amber-400 shrink-0 mt-0.5" />
      <div className="text-sm text-amber-100 flex-1 min-w-0">
        <div className="font-semibold mb-0.5">
          Active investigation
          {otherCount > 0 && <span className="font-normal"> · {cases.length} cases</span>}
        </div>
        <div className="text-amber-200/80">
          <span className="text-amber-100">{ownerName}</span> opened{" "}
          <Link
            href={`/cases/${primary.id}`}
            className="underline decoration-dotted hover:text-amber-50"
          >
            {primary.case_number}: {primary.title}
          </Link>{" "}
          <span className="text-amber-200/60">({daysAgo(primary.created_at)})</span>
          {primary.status === "under_review" && (
            <span className="ml-2 text-[10px] uppercase tracking-widest px-1.5 py-0.5 rounded bg-amber-500/15 border border-amber-500/30">
              Under review
            </span>
          )}
        </div>
        {otherCount > 0 && (
          <div className="mt-1.5 text-xs text-amber-200/60">
            + {otherCount} additional open case{otherCount > 1 ? "s" : ""} on this provider.
          </div>
        )}
        <div className="mt-2 text-[11px] text-amber-200/70">
          Coordinate with the case owner before opening a parallel investigation.
        </div>
      </div>
    </div>
  );
}
