"use client";
import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { getCase, updateCase, addCaseNote, recordOutcome } from "@/lib/api";
import AttestationModal from "@/components/AttestationModal";
import AuditTrail from "@/components/AuditTrail";
import { fmt, statusBadge, riskBadge, providerName } from "@/lib/utils";
import { useAuth } from "@/lib/auth";
import type { Case, CaseOutcome } from "@/types";
import { ArrowLeft, Send, CheckCircle2, XCircle, AlertTriangle, Scale, FileSearch } from "lucide-react";

const STATUSES = ["open", "under_review", "closed", "referred"];

// ── Outcome config ────────────────────────────────────────────────────────────

const OUTCOME_CONFIG: Record<CaseOutcome, { label: string; icon: React.ReactNode; color: string; bg: string; border: string }> = {
  substantiated:       { label: "Substantiated",         icon: <CheckCircle2 size={14} />, color: "text-red-400",    bg: "bg-red-500/10",     border: "border-red-500/25" },
  referred_to_doj:     { label: "Referred to DOJ",        icon: <Scale size={14} />,        color: "text-orange-400", bg: "bg-orange-500/10",  border: "border-orange-500/25" },
  referred_to_state_ag:{ label: "Referred to State AG",   icon: <Scale size={14} />,        color: "text-orange-400", bg: "bg-orange-500/10",  border: "border-orange-500/25" },
  unsubstantiated:     { label: "Unsubstantiated",        icon: <XCircle size={14} />,      color: "text-slate-400",  bg: "bg-white/[0.04]",   border: "border-white/[0.08]" },
  closed_no_action:    { label: "Closed — No Action",     icon: <FileSearch size={14} />,   color: "text-slate-400",  bg: "bg-white/[0.04]",   border: "border-white/[0.08]" },
};

const OUTCOME_DESCRIPTIONS: Record<CaseOutcome, string> = {
  substantiated:        "Evidence supports fraud. Flags marked as confirmed — will inform model retraining.",
  referred_to_doj:      "Case referred to DOJ / US Attorney. Flags marked as confirmed.",
  referred_to_state_ag: "Case referred to State Attorney General. Flags marked as confirmed.",
  unsubstantiated:      "Investigation found no actionable fraud. Case closed.",
  closed_no_action:     "Case closed without referral or finding.",
};

// ── Outcome modal ─────────────────────────────────────────────────────────────

function OutcomeModal({
  caseId,
  onClose,
  onSaved,
}: {
  caseId: number;
  onClose: () => void;
  onSaved: (updated: Case) => void;
}) {
  const [outcome, setOutcome] = useState<CaseOutcome | "">("");
  const [note, setNote] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [attestOpen, setAttestOpen] = useState(false);

  // Determine if this outcome requires methodology attestation.  Substantiated
  // and referrals produce external-facing artefacts; closure / unsubstantiated
  // outcomes don't carry the same evidentiary weight.
  const requiresAttestation =
    outcome === "substantiated" ||
    outcome === "referred_to_doj" ||
    outcome === "referred_to_state_ag";

  async function persistOutcome() {
    if (!outcome) return;
    setSaving(true);
    setError(null);
    try {
      const updated = await recordOutcome(caseId, outcome, note || undefined);
      onSaved(updated);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to save outcome");
    } finally {
      setSaving(false);
    }
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!outcome) return;
    if (requiresAttestation) {
      setAttestOpen(true);     // gate on methodology attestation
    } else {
      void persistOutcome();
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={onClose}>
      <div className="bg-[#0f1623] border border-white/[0.10] rounded-2xl p-6 w-full max-w-md shadow-2xl mx-4" onClick={e => e.stopPropagation()}>
        <div className="flex items-center gap-2 mb-5">
          <AlertTriangle size={16} className="text-orange-400" />
          <h2 className="text-base font-bold text-slate-100">Record Case Outcome</h2>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Outcome selector */}
          <div className="space-y-2">
            {(Object.keys(OUTCOME_CONFIG) as CaseOutcome[]).map(o => {
              const cfg = OUTCOME_CONFIG[o];
              const selected = outcome === o;
              return (
                <button
                  type="button"
                  key={o}
                  onClick={() => setOutcome(o)}
                  className={`w-full text-left rounded-xl border px-4 py-3 transition ${
                    selected
                      ? `${cfg.bg} ${cfg.border} ${cfg.color}`
                      : "bg-white/[0.02] border-white/[0.07] text-slate-400 hover:border-white/[0.12] hover:text-slate-300"
                  }`}
                >
                  <div className="flex items-center gap-2 mb-0.5">
                    <span className={selected ? cfg.color : "text-slate-600"}>{cfg.icon}</span>
                    <span className="text-sm font-medium">{cfg.label}</span>
                    {selected && <span className="ml-auto text-[10px] font-mono opacity-60">selected</span>}
                  </div>
                  {selected && (
                    <p className="text-[11px] text-slate-500 ml-5">{OUTCOME_DESCRIPTIONS[o]}</p>
                  )}
                </button>
              );
            })}
          </div>

          {/* Optional note */}
          <div>
            <label className="text-[10px] uppercase tracking-widest text-slate-500 font-medium block mb-1.5">
              Note (optional)
            </label>
            <textarea
              value={note}
              onChange={e => setNote(e.target.value)}
              placeholder="Summary of findings, referral details, or closing notes…"
              rows={3}
              className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3.5 py-2.5 text-sm text-slate-200 placeholder-slate-600 focus:outline-none focus:border-blue-500/40 transition resize-none"
            />
          </div>

          {error && <p className="text-xs text-red-400 bg-red-500/10 border border-red-500/20 rounded-lg px-3 py-2">{error}</p>}

          <div className="flex gap-2 pt-1">
            <button type="button" onClick={onClose}
              className="flex-1 text-sm text-slate-400 hover:text-slate-200 border border-white/[0.08] hover:border-white/[0.15] py-2.5 rounded-xl transition">
              Cancel
            </button>
            <button
              type="submit"
              disabled={!outcome || saving}
              className="flex-1 bg-blue-500/10 hover:bg-blue-500/20 border border-blue-500/25 text-blue-400 text-sm font-medium py-2.5 rounded-xl transition disabled:opacity-40"
            >
              {saving ? "Saving…" : "Record Outcome"}
            </button>
          </div>
        </form>
      </div>

      <AttestationModal
        open={attestOpen}
        action={outcome === "substantiated" ? "case_outcome_substantiated" : "case_referral"}
        targetId={String(caseId)}
        targetType="case"
        actionLabel={
          outcome === "substantiated"
            ? "mark this case as substantiated fraud"
            : "refer this case to an external agency"
        }
        onConfirm={() => {
          setAttestOpen(false);
          void persistOutcome();
        }}
        onCancel={() => setAttestOpen(false)}
      />
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function CaseDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { user } = useAuth();
  const [c, setC] = useState<Case | null>(null);
  const [loading, setLoading] = useState(true);
  const [note, setNote] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [showOutcomeModal, setShowOutcomeModal] = useState(false);

  async function load() {
    const data = await getCase(Number(id));
    setC(data);
  }

  useEffect(() => {
    load().catch(console.error).finally(() => setLoading(false));
  }, [id]);

  async function handleStatusChange(status: string) {
    if (!c) return;
    const updated = await updateCase(c.id, { status });
    setC(updated);
  }

  async function handleNoteSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!c || !note.trim()) return;
    setSubmitting(true);
    try {
      await addCaseNote(c.id, note.trim());
      setNote("");
      await load();
    } finally {
      setSubmitting(false);
    }
  }

  if (loading) return <AppShell><div className="p-8 text-slate-600 animate-pulse text-sm">Loading…</div></AppShell>;
  if (!c) return <AppShell><div className="p-8 text-slate-600 text-sm">Case not found.</div></AppShell>;

  const canWrite = user?.role === "admin" || user?.role === "analyst";
  const outcomeCfg = c.outcome ? OUTCOME_CONFIG[c.outcome] : null;

  return (
    <AppShell>
      {showOutcomeModal && (
        <OutcomeModal
          caseId={c.id}
          onClose={() => setShowOutcomeModal(false)}
          onSaved={updated => { setC(updated); setShowOutcomeModal(false); }}
        />
      )}

      <div className="p-8 max-w-4xl mx-auto fade-in">
        <Link href="/cases" className="flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-300 mb-6 transition w-fit">
          <ArrowLeft size={14} /> Cases
        </Link>

        {/* ── Outcome banner (shown when resolved) ─────────────────────────── */}
        {outcomeCfg && (
          <div className={`mb-6 rounded-xl border ${outcomeCfg.border} ${outcomeCfg.bg} p-4`}>
            <div className="flex items-center gap-2 mb-1">
              <span className={outcomeCfg.color}>{outcomeCfg.icon}</span>
              <span className={`text-sm font-bold ${outcomeCfg.color}`}>{outcomeCfg.label}</span>
              {c.resolved_at && (
                <span className="text-[10px] text-slate-600 font-mono ml-auto">
                  Resolved {new Date(c.resolved_at).toLocaleDateString()}
                </span>
              )}
            </div>
            {c.outcome_note && (
              <p className="text-xs text-slate-400 ml-5 leading-relaxed">{c.outcome_note}</p>
            )}
          </div>
        )}

        {/* Header */}
        <div className="flex items-start justify-between mb-6">
          <div>
            <div className="flex items-center gap-3 mb-1 flex-wrap">
              <span className="text-xs font-mono text-slate-600">{c.case_number}</span>
              <span className={`text-[10px] px-2 py-0.5 rounded font-mono uppercase ${statusBadge(c.status)}`}>
                {c.status.replace("_", " ")}
              </span>
              {outcomeCfg && (
                <span className={`text-[10px] px-2 py-0.5 rounded font-mono uppercase border ${outcomeCfg.border} ${outcomeCfg.color} ${outcomeCfg.bg}`}>
                  {outcomeCfg.label}
                </span>
              )}
            </div>
            <h1 className="text-xl font-bold text-slate-100">{c.title}</h1>
            <p className="text-sm text-slate-500 mt-1">
              Opened {new Date(c.created_at).toLocaleDateString()} · Updated {new Date(c.updated_at).toLocaleDateString()}
            </p>
          </div>
          {c.estimated_loss && (
            <div className="text-right">
              <p className="text-xs text-slate-600 uppercase tracking-widest mb-1">Est. Loss</p>
              <p className="text-xl font-bold font-mono text-red-400">{fmt(c.estimated_loss)}</p>
            </div>
          )}
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Main */}
          <div className="lg:col-span-2 space-y-5">
            {/* Provider card */}
            {c.provider && (
              <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
                <h2 className="text-xs uppercase tracking-widest text-slate-500 font-medium mb-3">Flagged Provider</h2>
                <Link href={`/providers/${c.provider.npi}`} className="flex items-center justify-between hover:bg-white/[0.03] -mx-2 px-2 py-1.5 rounded-lg transition group">
                  <div>
                    <p className="text-sm font-semibold text-slate-200 group-hover:text-white transition">
                      {providerName(c.provider)}
                      {c.provider.is_excluded && <span className="ml-2 text-[9px] bg-red-500/15 text-red-400 border border-red-500/20 px-1.5 py-0.5 rounded font-mono">LEIE</span>}
                    </p>
                    <p className="text-xs text-slate-600">{c.provider.specialty} · {c.provider.city}, {c.provider.state}</p>
                  </div>
                  <span className={`text-sm font-bold font-mono px-2.5 py-1 rounded-lg ${riskBadge(c.provider.risk_score)}`}>
                    {c.provider.risk_score ?? "—"}
                  </span>
                </Link>
              </div>
            )}

            {/* Notes */}
            {c.notes && (
              <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
                <h2 className="text-xs uppercase tracking-widest text-slate-500 font-medium mb-2">Description</h2>
                <p className="text-sm text-slate-400 leading-relaxed">{c.notes}</p>
              </div>
            )}

            {/* Timeline */}
            <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
              <h2 className="text-xs uppercase tracking-widest text-slate-500 font-medium mb-4">Notes ({c.case_notes.length})</h2>
              {c.case_notes.length === 0 ? (
                <p className="text-sm text-slate-600">No notes yet.</p>
              ) : (
                <div className="space-y-4">
                  {c.case_notes.map(n => (
                    <div key={n.id} className="flex gap-3">
                      <div className="w-6 h-6 rounded-full bg-slate-700 flex items-center justify-center text-xs font-bold text-slate-400 shrink-0 mt-0.5">
                        {(n.user_name ?? "?")[0]}
                      </div>
                      <div className="flex-1">
                        <div className="flex items-baseline gap-2 mb-1">
                          <span className="text-xs font-medium text-slate-300">{n.user_name ?? "Unknown"}</span>
                          <span className="text-[10px] text-slate-600">{new Date(n.created_at).toLocaleString()}</span>
                        </div>
                        <p className="text-sm text-slate-400 leading-relaxed">{n.content}</p>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {canWrite && (
                <form onSubmit={handleNoteSubmit} className="flex gap-2 mt-5 pt-4 border-t border-white/[0.05]">
                  <input
                    type="text"
                    value={note}
                    onChange={e => setNote(e.target.value)}
                    placeholder="Add a note…"
                    className="flex-1 bg-white/[0.04] border border-white/[0.08] rounded-lg px-3.5 py-2 text-sm text-slate-200 placeholder-slate-600 focus:outline-none focus:border-blue-500/40 transition"
                  />
                  <button type="submit" disabled={submitting || !note.trim()}
                    className="bg-blue-500/10 hover:bg-blue-500/20 border border-blue-500/20 text-blue-400 px-3 py-2 rounded-lg transition disabled:opacity-40">
                    <Send size={14} />
                  </button>
                </form>
              )}
            </div>
          </div>

          {/* Sidebar */}
          <div className="space-y-4">

            {/* Outcome recording — shown if not yet resolved */}
            {canWrite && !c.outcome && (
              <div className="bg-orange-500/[0.06] border border-orange-500/20 rounded-xl p-5">
                <h2 className="text-xs uppercase tracking-widest text-orange-500/70 font-medium mb-2">Record Outcome</h2>
                <p className="text-xs text-slate-500 mb-3 leading-relaxed">
                  Close this investigation by recording its final disposition. Substantiated outcomes confirm fraud signals and improve model accuracy.
                </p>
                <button
                  onClick={() => setShowOutcomeModal(true)}
                  className="w-full bg-orange-500/10 hover:bg-orange-500/20 border border-orange-500/25 text-orange-400 text-sm font-medium py-2 rounded-lg transition"
                >
                  Record Outcome →
                </button>
              </div>
            )}

            {/* Status */}
            {canWrite && !c.outcome && (
              <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
                <h2 className="text-xs uppercase tracking-widest text-slate-500 font-medium mb-3">Status</h2>
                <div className="space-y-1.5">
                  {STATUSES.map(s => (
                    <button
                      key={s}
                      onClick={() => handleStatusChange(s)}
                      className={`w-full text-left text-sm px-3 py-2 rounded-lg transition font-mono ${
                        c.status === s
                          ? "bg-white/[0.07] text-slate-100"
                          : "text-slate-500 hover:text-slate-300 hover:bg-white/[0.04]"
                      }`}
                    >
                      {s.replace("_", " ")}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Documents */}
            <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
              <h2 className="text-xs uppercase tracking-widest text-slate-500 font-medium mb-3">Documents ({c.documents.length})</h2>
              {c.documents.length === 0 ? (
                <p className="text-xs text-slate-600">No documents attached.</p>
              ) : (
                <div className="space-y-2">
                  {c.documents.map(d => (
                    <div key={d.id} className="text-xs text-slate-400 truncate">📎 {d.filename}</div>
                  ))}
                </div>
              )}
            </div>

            {/* Meta */}
            <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-5">
              <h2 className="text-xs uppercase tracking-widest text-slate-500 font-medium mb-3">Details</h2>
              <div className="space-y-2 text-xs">
                <div className="flex justify-between">
                  <span className="text-slate-600">State</span>
                  <span className="text-slate-300 font-mono">{c.state ?? "—"}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-600">NPI</span>
                  <span className="text-slate-300 font-mono">{c.provider_npi}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-600">Opened</span>
                  <span className="text-slate-300">{new Date(c.created_at).toLocaleDateString()}</span>
                </div>
                {c.resolved_at && (
                  <div className="flex justify-between">
                    <span className="text-slate-600">Resolved</span>
                    <span className="text-slate-300">{new Date(c.resolved_at).toLocaleDateString()}</span>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Chain-of-custody audit trail — methodology §10 */}
        <div className="mt-6">
          <AuditTrail targetType="case" targetId={String(c.id)} />
        </div>
      </div>
    </AppShell>
  );
}
