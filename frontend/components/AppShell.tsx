"use client";
import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@/lib/auth";
import Sidebar from "@/components/Sidebar";
import { useAlertSocket, type AlertItem } from "@/lib/useAlertSocket";
import { ShieldAlert, X } from "lucide-react";

// ── Inline toast ──────────────────────────────────────────────────────────────

interface ToastEntry {
  id: number;
  alerts: AlertItem[];
  count: number;
}

let _tid = 0;
const TOAST_DURATION_MS = 8_000;

function severityLabel(s: number) {
  if (s === 1) return { text: "CRITICAL", cls: "text-red-400" };
  if (s === 2) return { text: "HIGH", cls: "text-orange-400" };
  return { text: "MEDIUM", cls: "text-yellow-400" };
}

function AlertToastStack({ toasts, onDismiss }: {
  toasts: ToastEntry[];
  onDismiss: (id: number) => void;
}) {
  if (toasts.length === 0) return null;

  return (
    <div className="fixed top-4 right-4 z-50 flex flex-col gap-2 pointer-events-none">
      {toasts.map((toast) => {
        const first = toast.alerts[0];
        const extra = toast.count - 1;
        const { text: sevText, cls: sevCls } = severityLabel(first.severity);

        return (
          <div
            key={toast.id}
            className="pointer-events-auto w-80 rounded-xl border border-red-500/30 bg-[#0f1624]/95 shadow-2xl backdrop-blur-sm toast-in"
          >
            {/* Header */}
            <div className="flex items-center gap-2 px-3.5 py-2.5 border-b border-white/[0.05]">
              <ShieldAlert size={14} className="text-red-400 shrink-0" />
              <span className="text-xs font-semibold text-slate-100 flex-1">
                {toast.count === 1 ? "New Fraud Alert" : `${toast.count} New Fraud Alerts`}
              </span>
              <button
                onClick={() => onDismiss(toast.id)}
                className="text-slate-500 hover:text-slate-300 transition"
                aria-label="Dismiss"
              >
                <X size={13} />
              </button>
            </div>

            {/* Body */}
            <div className="px-3.5 py-2.5 space-y-0.5">
              <div className="flex items-center justify-between gap-2">
                <span className="text-xs font-medium text-slate-200 truncate">
                  {first.provider_name}
                </span>
                <span className={`text-[10px] font-bold ${sevCls} shrink-0`}>
                  {sevText}
                </span>
              </div>
              <p className="text-[11px] text-slate-500 truncate">
                {[first.specialty, first.state].filter(Boolean).join(" · ")}
                {" · "}
                <span className="text-slate-400">Score {first.risk_score}</span>
              </p>
              {first.flag_type && (
                <p className="text-[11px] text-slate-600 truncate capitalize">
                  {first.flag_type.replace(/_/g, " ")}
                </p>
              )}
              {extra > 0 && (
                <p className="text-[11px] text-slate-600 pt-0.5">
                  +{extra} more alert{extra > 1 ? "s" : ""}
                </p>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ── AppShell ──────────────────────────────────────────────────────────────────

export default function AppShell({ children }: { children: React.ReactNode }) {
  const { user, loading } = useAuth();
  const router = useRouter();
  const { unreadCount, latestAlerts, clearCount } = useAlertSocket();
  const [toasts, setToasts] = useState<ToastEntry[]>([]);
  const prevAlerts = useRef<AlertItem[]>([]);

  useEffect(() => {
    if (!loading && !user) router.replace("/login");
  }, [user, loading, router]);

  // Show a toast whenever a new batch of alerts arrives from the server
  useEffect(() => {
    if (latestAlerts.length === 0) return;
    if (latestAlerts === prevAlerts.current) return;
    prevAlerts.current = latestAlerts;

    const id = ++_tid;
    const entry: ToastEntry = { id, alerts: latestAlerts, count: latestAlerts.length };

    setToasts((prev) => [...prev.slice(-2), entry]); // max 3 visible toasts

    const timer = setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, TOAST_DURATION_MS);

    return () => clearTimeout(timer);
  }, [latestAlerts]);

  const dismissToast = (id: number) =>
    setToasts((prev) => prev.filter((t) => t.id !== id));

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-slate-600 text-sm font-mono animate-pulse">Loading…</div>
      </div>
    );
  }

  if (!user) return null;

  return (
    <div className="flex min-h-screen">
      <Sidebar unreadCount={unreadCount} onClearAlerts={clearCount} />
      <main className="flex-1 overflow-auto">{children}</main>
      <AlertToastStack toasts={toasts} onDismiss={dismissToast} />
    </div>
  );
}
