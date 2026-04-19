"use client";
import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { getCase, updateCase, addCaseNote } from "@/lib/api";
import { fmt, statusBadge, riskBadge, providerName } from "@/lib/utils";
import { useAuth } from "@/lib/auth";
import type { Case } from "@/types";
import { ArrowLeft, Send } from "lucide-react";

const STATUSES = ["open", "under_review", "closed", "referred"];

export default function CaseDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { user } = useAuth();
  const [c, setC] = useState<Case | null>(null);
  const [loading, setLoading] = useState(true);
  const [note, setNote] = useState("");
  const [submitting, setSubmitting] = useState(false);

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

  return (
    <AppShell>
      <div className="p-8 max-w-4xl mx-auto fade-in">
        <Link href="/cases" className="flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-300 mb-6 transition w-fit">
          <ArrowLeft size={14} /> Cases
        </Link>

        {/* Header */}
        <div className="flex items-start justify-between mb-6">
          <div>
            <div className="flex items-center gap-3 mb-1">
              <span className="text-xs font-mono text-slate-600">{c.case_number}</span>
              <span className={`text-[10px] px-2 py-0.5 rounded font-mono uppercase ${statusBadge(c.status)}`}>
                {c.status.replace("_", " ")}
              </span>
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
            {/* Status */}
            {canWrite && (
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
              </div>
            </div>
          </div>
        </div>
      </div>
    </AppShell>
  );
}
