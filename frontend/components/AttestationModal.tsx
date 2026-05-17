"use client";

/**
 * AttestationModal — required-attestation gate for sensitive actions.
 *
 * Shown before the user can:
 *   - export a PDF or CSV
 *   - mark a case as substantiated
 *   - generate an external referral package
 *
 * The user reads the methodology limitations and clicks "I acknowledge".
 * The acknowledgment is recorded in audit_log via POST /api/audit/attestation,
 * creating a permanent record of who attested before each sensitive action.
 *
 * Per methodology doc §8: this is the legal-defensibility layer that
 * documents the user understood the system's limitations before producing
 * an artefact for external use.
 */
import { useState } from "react";
import { AlertTriangle, X, Check, Loader2 } from "lucide-react";
import { recordAttestation, type AttestationAction } from "@/lib/api";

interface Props {
  open:      boolean;
  action:    AttestationAction;
  /** Target context (e.g. provider NPI or case ID) — included in audit log */
  targetId?:    string;
  targetType?:  "provider" | "case";
  /** Human-readable label of what's about to happen */
  actionLabel: string;
  onConfirm: (attestationId: number) => void;
  onCancel: () => void;
}

const ACTION_BLURBS: Record<AttestationAction, string> = {
  pdf_export:                  "exporting a provider report PDF",
  csv_export:                  "exporting provider data to CSV",
  case_outcome_substantiated:  "marking this case as substantiated",
  case_referral:               "generating an external referral package",
};

export default function AttestationModal({
  open,
  action,
  targetId,
  targetType,
  actionLabel,
  onConfirm,
  onCancel,
}: Props) {
  const [acknowledged, setAcknowledged] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (!open) return null;

  async function handleConfirm() {
    if (!acknowledged || submitting) return;
    setSubmitting(true);
    setError(null);
    try {
      const res = await recordAttestation({
        action,
        target_id:   targetId,
        target_type: targetType,
      });
      onConfirm(res.attestation_id);
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Could not record attestation";
      setError(msg);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-[#0f1624] border border-amber-500/30 rounded-xl shadow-2xl max-w-2xl w-full mx-4 max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center gap-2 px-5 py-4 border-b border-white/[0.06]">
          <AlertTriangle size={16} className="text-amber-400 shrink-0" />
          <h2 className="text-sm font-semibold text-slate-100 flex-1">
            Methodology acknowledgment required
          </h2>
          <button
            onClick={onCancel}
            disabled={submitting}
            className="text-slate-500 hover:text-slate-300 transition disabled:opacity-50"
            aria-label="Cancel"
          >
            <X size={16} />
          </button>
        </div>

        {/* Body */}
        <div className="px-5 py-4 overflow-y-auto text-sm leading-relaxed">
          <p className="text-slate-300 mb-3">
            You are about to <span className="font-semibold">{actionLabel}</span>{" "}
            ({ACTION_BLURBS[action]}).
          </p>
          <p className="text-slate-400 mb-3">
            Vigil risk scores are <strong className="text-slate-200">statistical signals</strong>{" "}
            derived from aggregate Medicare Part B billing data, not findings of fraud.
            Before this artefact can be used in any enforcement action, court filing,
            or public communication, you must independently verify the underlying
            claim records.
          </p>

          <div className="bg-amber-500/[0.06] border border-amber-500/20 rounded-lg p-3 mb-3 text-xs text-amber-100/90 space-y-1.5">
            <p className="font-semibold text-amber-200">Specifically, you acknowledge:</p>
            <ul className="list-disc list-outside pl-5 space-y-1 text-slate-300">
              <li>Risk scores reflect billing-pattern anomalies, not confirmed fraud.</li>
              <li>
                Many fraud schemes (claim-level impossibilities, beneficiary deaths,
                date-of-service violations) cannot be detected from this dataset.
              </li>
              <li>
                Independent review of underlying CMS claim records is required
                before any enforcement action.
              </li>
              <li>
                Your acknowledgment is recorded in the audit log with your user
                identity and timestamp.
              </li>
            </ul>
          </div>

          <p className="text-xs text-slate-500 mb-2">
            Full methodology, validation results, and known limitations are
            documented in <code className="text-slate-400">docs/methodology.md</code>{" "}
            (v2.1.0).
          </p>

          <label className="flex items-start gap-2 mt-4 cursor-pointer">
            <input
              type="checkbox"
              checked={acknowledged}
              onChange={(e) => setAcknowledged(e.target.checked)}
              disabled={submitting}
              className="mt-1 accent-amber-500"
            />
            <span className="text-sm text-slate-300">
              I have read and understood the methodology limitations above, and I
              acknowledge that this output is a statistical signal that requires
              independent verification before any enforcement use.
            </span>
          </label>

          {error && (
            <div className="mt-3 text-xs text-red-400">{error}</div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-2 px-5 py-3 border-t border-white/[0.06] bg-white/[0.02]">
          <button
            onClick={onCancel}
            disabled={submitting}
            className="px-4 py-1.5 text-sm rounded-lg text-slate-400 hover:bg-white/[0.04] disabled:opacity-50 transition"
          >
            Cancel
          </button>
          <button
            onClick={handleConfirm}
            disabled={!acknowledged || submitting}
            className="flex items-center gap-2 px-4 py-1.5 text-sm rounded-lg bg-amber-500/15 hover:bg-amber-500/25 border border-amber-500/30 text-amber-200 disabled:opacity-40 disabled:cursor-not-allowed transition"
          >
            {submitting ? <Loader2 size={14} className="animate-spin" /> : <Check size={14} />}
            {submitting ? "Recording…" : "I acknowledge — proceed"}
          </button>
        </div>
      </div>
    </div>
  );
}
