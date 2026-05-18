"use client";

/**
 * NetworkExplainer — the persistent "what is this page and how do I use it"
 * panel that goes ABOVE the search box.
 *
 * Solves a real UX failure: investigators who land on /network see a force-
 * directed graph and have no narrative for what they're looking at or what
 * they're supposed to do.  This panel fixes that by leading with the
 * question the page answers, the visual encoding, and three concrete
 * fraud patterns the network exposes.
 *
 * Collapsible after first read — once an investigator has used the page
 * twice, they don't need the explainer expanded.  But it's always one
 * click away if they forget what "hub" means or which colours indicate
 * suspicious sharing.
 */
import { useEffect, useState } from "react";
import { Network, ChevronDown, ChevronRight, AlertCircle, Users, Target } from "lucide-react";

interface Props {
  /** When true, panel starts collapsed.  Pass true on subsequent visits. */
  defaultCollapsed?: boolean;
}

const STORAGE_KEY = "vigil_network_explainer_collapsed";

export default function NetworkExplainer({ defaultCollapsed }: Props) {
  // Remember whether the user collapsed it last time, so we don't nag.
  const [collapsed, setCollapsed] = useState<boolean>(
    defaultCollapsed ?? false,
  );

  useEffect(() => {
    if (typeof window === "undefined") return;
    const remembered = window.localStorage.getItem(STORAGE_KEY);
    if (remembered === "true") setCollapsed(true);
  }, []);

  function toggle() {
    const next = !collapsed;
    setCollapsed(next);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(STORAGE_KEY, String(next));
    }
  }

  return (
    <div className="rounded-xl border border-blue-500/20 bg-blue-500/[0.04] overflow-hidden">
      <button
        onClick={toggle}
        className="w-full flex items-center gap-2 px-5 py-3 hover:bg-blue-500/[0.06] transition text-left"
      >
        <Network size={14} className="text-blue-400 shrink-0" />
        <span className="text-sm font-semibold text-slate-100">
          What this view is for
        </span>
        <span className="text-xs text-slate-500 ml-2 hidden sm:inline">
          — find who your suspect is operating with
        </span>
        <span className="ml-auto">
          {collapsed
            ? <ChevronRight size={14} className="text-slate-500" />
            : <ChevronDown  size={14} className="text-slate-500" />
          }
        </span>
      </button>

      {!collapsed && (
        <div className="px-5 pb-5 pt-2 space-y-4">

          {/* The "why" — one sentence */}
          <p className="text-sm text-slate-300 leading-relaxed">
            A provider&apos;s risk score tells you if <em>they</em> look suspicious.
            This view tells you <em>who else is involved</em>.  Most high-dollar Medicare
            fraud isn&apos;t a solo act — it&apos;s a small group of providers sharing
            patients in patterns that don&apos;t make clinical sense.
          </p>

          {/* The "how" — 3 steps */}
          <div>
            <div className="text-[10px] uppercase tracking-widest text-slate-500 font-medium mb-2">
              How to use it
            </div>
            <ol className="space-y-1.5 text-sm text-slate-300">
              <li>
                <span className="font-mono text-blue-400 mr-2">1.</span>
                Search for a provider you&apos;re already investigating.
              </li>
              <li>
                <span className="font-mono text-blue-400 mr-2">2.</span>
                The <strong>Insights panel</strong> tells you in plain English
                what the network shows — possible ring, hub provider,
                disproportionate sharing.
              </li>
              <li>
                <span className="font-mono text-blue-400 mr-2">3.</span>
                Click any node in the graph (or NPI chip in the insights)
                to make it the new center — drill into the suspicious
                neighbour.
              </li>
            </ol>
          </div>

          {/* The "what to look for" — 3 fraud patterns */}
          <div>
            <div className="text-[10px] uppercase tracking-widest text-slate-500 font-medium mb-2">
              Three fraud patterns this view exposes
            </div>
            <div className="grid sm:grid-cols-3 gap-3">

              <div className="rounded-lg border border-white/[0.06] bg-black/20 p-3">
                <div className="flex items-center gap-1.5 mb-1.5">
                  <Users size={12} className="text-orange-400" />
                  <span className="text-xs font-semibold text-slate-200">Kickback ring</span>
                </div>
                <p className="text-[11px] text-slate-400 leading-relaxed">
                  A small group of providers shares 80%+ of patients —
                  far above the specialty norm of 5-15%.  Someone is
                  steering patients between them.
                </p>
              </div>

              <div className="rounded-lg border border-white/[0.06] bg-black/20 p-3">
                <div className="flex items-center gap-1.5 mb-1.5">
                  <Target size={12} className="text-yellow-400" />
                  <span className="text-xs font-semibold text-slate-200">Hub operator</span>
                </div>
                <p className="text-[11px] text-slate-400 leading-relaxed">
                  One provider connects to most of the rest of the
                  cluster.  In organized schemes, that&apos;s usually the
                  person running it.
                </p>
              </div>

              <div className="rounded-lg border border-white/[0.06] bg-black/20 p-3">
                <div className="flex items-center gap-1.5 mb-1.5">
                  <AlertCircle size={12} className="text-red-400" />
                  <span className="text-xs font-semibold text-slate-200">Implausible pair</span>
                </div>
                <p className="text-[11px] text-slate-400 leading-relaxed">
                  Specialties that shouldn&apos;t share patients (e.g., dermatology
                  ↔ DME supplier) sharing dozens.  Classic DME or lab
                  kickback pattern.
                </p>
              </div>

            </div>
          </div>

          {/* The visual legend */}
          <div className="border-t border-blue-500/15 pt-3">
            <div className="text-[10px] uppercase tracking-widest text-slate-500 font-medium mb-2">
              Reading the graph
            </div>
            <ul className="grid sm:grid-cols-2 gap-x-4 gap-y-1 text-xs text-slate-400">
              <li>
                <span className="inline-block w-3 h-3 bg-red-500 rounded-full mr-2 align-middle" />
                Node colour = risk score (red = critical, orange = high, yellow = medium)
              </li>
              <li>
                <span className="inline-block w-3 h-3 bg-slate-500 rounded-full mr-2 align-middle" style={{ transform: "scale(1.4)" }} />
                Node size = number of connections (degree)
              </li>
              <li>
                <span className="inline-block w-8 h-0.5 bg-red-500 mr-2 align-middle" />
                Red edge = disproportionate patient sharing (suspicious)
              </li>
              <li>
                <span className="inline-block w-8 h-0.5 bg-slate-500 mr-2 align-middle" />
                Edge thickness = number of shared patients
              </li>
            </ul>
          </div>

        </div>
      )}
    </div>
  );
}
