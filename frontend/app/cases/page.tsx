"use client";
import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { getCases } from "@/lib/api";
import { fmt, statusBadge, providerName } from "@/lib/utils";
import type { Case } from "@/types";
import { FolderOpen } from "lucide-react";

const STATUSES = ["", "open", "under_review", "closed", "referred"];

export default function CasesPage() {
  const [items, setItems] = useState<Case[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [status, setStatus] = useState("");
  const [assignedToMe, setAssignedToMe] = useState(false);

  const load = useCallback(async (p = 1) => {
    setLoading(true);
    try {
      const res = await getCases({
        page: p, page_size: 20,
        ...(status && { status }),
        ...(assignedToMe && { assigned_to_me: true }),
      });
      setItems(res.items);
      setTotal(res.total);
      setPage(p);
    } finally {
      setLoading(false);
    }
  }, [status, assignedToMe]);

  useEffect(() => { load(1); }, [load]);

  const pages = Math.ceil(total / 20);

  return (
    <AppShell>
      <div className="p-8 max-w-5xl mx-auto fade-in">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-xl font-bold text-slate-100">Cases</h1>
            <p className="text-sm text-slate-500 mt-0.5">{total} total cases</p>
          </div>
        </div>

        {/* Filters */}
        <div className="flex flex-wrap gap-2 mb-5">
          <select value={status} onChange={e => setStatus(e.target.value)}
            className="bg-white/[0.03] border border-white/[0.07] rounded-lg px-3 py-2 text-sm text-slate-300 focus:outline-none">
            {STATUSES.map(s => <option key={s} value={s} className="bg-[#0f1623]">{s ? s.replace("_", " ") : "All Statuses"}</option>)}
          </select>
          <label className="flex items-center gap-2 text-sm text-slate-400 border border-white/[0.07] px-3 py-2 rounded-lg cursor-pointer">
            <input type="checkbox" checked={assignedToMe} onChange={e => setAssignedToMe(e.target.checked)} className="accent-blue-500" />
            Assigned to me
          </label>
        </div>

        {/* Case list */}
        <div className="space-y-2">
          {loading ? (
            <div className="text-slate-600 text-sm animate-pulse py-8 text-center">Loading…</div>
          ) : items.length === 0 ? (
            <div className="text-slate-600 text-sm py-12 text-center flex flex-col items-center gap-3">
              <FolderOpen size={32} className="text-slate-700" />
              <span>No cases yet. Open a case from a provider page.</span>
            </div>
          ) : items.map(c => (
            <Link
              key={c.id}
              href={`/cases/${c.id}`}
              className="flex items-center gap-4 bg-white/[0.02] border border-white/[0.05] hover:border-white/[0.10] hover:bg-white/[0.04] rounded-xl px-5 py-4 transition group"
            >
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-xs font-mono text-slate-600">{c.case_number}</span>
                  <span className={`text-[10px] px-2 py-0.5 rounded font-mono uppercase ${statusBadge(c.status)}`}>
                    {c.status.replace("_", " ")}
                  </span>
                </div>
                <p className="text-sm font-medium text-slate-200 group-hover:text-white transition truncate">{c.title}</p>
                {c.provider && (
                  <p className="text-xs text-slate-600 mt-0.5">{providerName(c.provider)} · {c.provider.specialty}</p>
                )}
              </div>
              <div className="text-right shrink-0">
                {c.estimated_loss && (
                  <p className="text-sm font-mono text-slate-300">{fmt(c.estimated_loss)}</p>
                )}
                <p className="text-xs text-slate-600 mt-0.5">{new Date(c.updated_at).toLocaleDateString()}</p>
              </div>
            </Link>
          ))}
        </div>

        {/* Pagination */}
        {pages > 1 && (
          <div className="flex items-center justify-between mt-5">
            <span className="text-xs text-slate-600">Page {page} of {pages}</span>
            <div className="flex gap-2">
              <button onClick={() => load(page - 1)} disabled={page === 1}
                className="text-xs text-slate-400 hover:text-slate-200 disabled:opacity-30 border border-white/[0.07] px-3 py-1.5 rounded-lg transition">
                ← Prev
              </button>
              <button onClick={() => load(page + 1)} disabled={page === pages}
                className="text-xs text-slate-400 hover:text-slate-200 disabled:opacity-30 border border-white/[0.07] px-3 py-1.5 rounded-lg transition">
                Next →
              </button>
            </div>
          </div>
        )}
      </div>
    </AppShell>
  );
}
