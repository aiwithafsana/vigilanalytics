"use client";

/**
 * FinancialImpactCard — headline "excess billing vs peers" estimate.
 *
 * The number attorneys put in their pitch deck.  Anchored to the per-patient
 * peer-median rate, then scaled by this provider's actual patient count.  See
 * backend/app/services/financial_impact.py for the formula.
 *
 * NOT a damages calculation — the disclaimer text makes that explicit, and
 * the same disclaimer appears in the exported PDF.
 */
import { useState } from "react";
import { DollarSign, Info, X } from "lucide-react";
import type { ProviderDetail } from "@/types";

interface Props {
  provider: ProviderDetail;
}

function fmt(amount: number | null | undefined): string {
  if (amount == null) return "—";
  if (amount >= 1_000_000) return `$${(amount / 1_000_000).toFixed(1)}M`;
  if (amount >= 1_000) return `$${Math.round(amount / 1_000)}k`;
  return `$${Math.round(amount).toLocaleString()}`;
}

export default function FinancialImpactCard({ provider }: Props) {
  const [showFormula, setShowFormula] = useState(false);
  const fi = provider.financial_impact;

  // Data not yet computed — fail quietly rather than displaying a broken card.
  if (!fi || fi.method === "unavailable" || fi.excess_billing == null) {
    return null;
  }

  // No excess (provider bills at or below peer rate) — show "within range" instead
  if (fi.excess_billing === 0) {
    return (
      <div className="mb-4 rounded-xl border border-emerald-500/20 bg-emerald-500/[0.04] px-4 py-3">
        <div className="flex items-center gap-2">
          <DollarSign size={14} className="text-emerald-400" />
          <span className="text-xs uppercase tracking-widest text-emerald-300/80 font-medium">
            Billing within peer range
          </span>
        </div>
        <p className="text-xs text-slate-400 mt-1.5">
          Provider&apos;s per-patient billing does not exceed the median for{" "}
          {provider.specialty ?? "their specialty"}
          {provider.state ? ` in ${provider.state}` : ""}.
        </p>
      </div>
    );
  }

  return (
    <div className="mb-4 rounded-xl border border-orange-500/25 bg-gradient-to-br from-orange-500/[0.08] to-red-500/[0.04] px-5 py-4">
      {/* Headline */}
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <DollarSign size={14} className="text-orange-400" />
            <span className="text-[10px] uppercase tracking-widest text-orange-300/80 font-medium">
              Estimated excess billing
            </span>
          </div>
          <div className="flex items-baseline gap-3 mb-1">
            <span className="text-3xl font-black font-mono text-orange-200">
              {fi.formatted_excess ?? fmt(fi.excess_billing)}
            </span>
            <span className="text-xs text-slate-500">
              over peer median
              {provider.specialty && ` for ${provider.specialty.toLowerCase()}`}
              {provider.state && ` in ${provider.state}`}
            </span>
          </div>
          <p className="text-[11px] text-slate-400 leading-relaxed">
            If this provider had charged the median per-patient rate for their
            specialty and state, they would have billed{" "}
            <span className="font-mono text-slate-200">{fmt(fi.expected_payment)}</span>.{" "}
            They actually billed{" "}
            <span className="font-mono text-slate-200">{fmt(fi.actual_payment)}</span>.
          </p>
        </div>
        <button
          onClick={() => setShowFormula((v) => !v)}
          className="text-slate-500 hover:text-slate-300 transition shrink-0 mt-0.5"
          aria-label="Show formula and disclaimer"
        >
          <Info size={14} />
        </button>
      </div>

      {/* Disclaimer / formula popover */}
      {showFormula && (
        <div className="mt-4 pt-4 border-t border-orange-500/15 text-xs space-y-2">
          <div className="flex items-start justify-between">
            <span className="font-semibold text-amber-200">Formula &amp; disclaimer</span>
            <button
              onClick={() => setShowFormula(false)}
              className="text-slate-500 hover:text-slate-300"
            >
              <X size={12} />
            </button>
          </div>
          <code className="block text-[11px] text-slate-300 bg-black/30 rounded px-2 py-1.5 font-mono">
            expected = peer_median_payment_per_patient × beneficiaries<br />
            excess   = max(0, actual_payment − expected)
          </code>
          <p className="text-[11px] text-slate-400 leading-relaxed">{fi.disclaimer}</p>
          <ul className="text-[11px] text-slate-500 list-disc pl-4 space-y-0.5">
            <li>Peer rate used: <span className="font-mono text-slate-300">{fmt(fi.peer_ppb_used)}</span> per patient</li>
            <li>Excess per patient: <span className="font-mono text-slate-300">{fmt(fi.excess_per_bene)}</span></li>
            <li>Patients served: <span className="font-mono text-slate-300">{provider.total_beneficiaries?.toLocaleString() ?? "—"}</span></li>
          </ul>
        </div>
      )}
    </div>
  );
}
