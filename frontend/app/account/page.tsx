"use client";
/**
 * /account — per-user security settings.
 *
 * Currently exposes MFA enrollment + management.  This is the only place
 * users can set up TOTP; admins cannot enrol MFA on behalf of someone else
 * (you'd need physical access to their authenticator).
 */
import { useEffect, useState } from "react";
import AppShell from "@/components/AppShell";
import { useAuth } from "@/lib/auth";
import {
  mfaSetup, mfaActivate, mfaDisable, mfaRegenerateBackupCodes,
  getMe,
  type MfaSetup, type MfaActivateResult,
} from "@/lib/api";
import type { User } from "@/types";
import { ShieldCheck, ShieldOff, Loader2, AlertTriangle, Copy, Check } from "lucide-react";
import { QRCodeSVG } from "qrcode.react";

type Mode =
  | { kind: "idle" }                                           // viewing current state
  | { kind: "enrolling_qr";   setup: MfaSetup }                // QR shown, awaiting first TOTP
  | { kind: "enrolling_done"; result: MfaActivateResult }      // backup codes shown once
  | { kind: "disabling" }                                      // confirming disable
  | { kind: "regenerating" };                                  // confirming regenerate

export default function AccountPage() {
  const { user: authUser } = useAuth();
  const [user, setUser] = useState<User | null>(null);
  const [mode, setMode] = useState<Mode>({ kind: "idle" });
  const [code, setCode] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [copied, setCopied] = useState(false);

  // Load fresh user state — we need mfa_enabled which isn't in the auth context
  useEffect(() => {
    let alive = true;
    getMe()
      .then(u => alive && setUser(u))
      .catch(e => alive && setError(e?.message ?? "Failed to load user"));
    return () => { alive = false };
  }, []);

  async function handleStartEnrollment() {
    setError(null); setBusy(true);
    try {
      const setup = await mfaSetup();
      setMode({ kind: "enrolling_qr", setup });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Setup failed");
    } finally { setBusy(false); }
  }

  async function handleActivate(e: React.FormEvent) {
    e.preventDefault();
    setError(null); setBusy(true);
    try {
      const result = await mfaActivate(code);
      setCode("");
      setMode({ kind: "enrolling_done", result });
      // Refresh local user record
      setUser(await getMe());
    } catch (e) {
      setError(e instanceof Error ? e.message : "Activation failed");
    } finally { setBusy(false); }
  }

  async function handleDisable(e: React.FormEvent) {
    e.preventDefault();
    setError(null); setBusy(true);
    try {
      await mfaDisable(code);
      setCode("");
      setMode({ kind: "idle" });
      setUser(await getMe());
    } catch (e) {
      setError(e instanceof Error ? e.message : "Disable failed");
    } finally { setBusy(false); }
  }

  async function handleRegenerate(e: React.FormEvent) {
    e.preventDefault();
    setError(null); setBusy(true);
    try {
      const result = await mfaRegenerateBackupCodes(code);
      setCode("");
      setMode({ kind: "enrolling_done", result });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Regenerate failed");
    } finally { setBusy(false); }
  }

  function copyBackupCodes(codes: string[]) {
    void navigator.clipboard.writeText(codes.join("\n"));
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }

  return (
    <AppShell>
      <div className="p-8 max-w-2xl mx-auto fade-in">
        <h1 className="text-2xl font-bold text-slate-100 mb-1">Account security</h1>
        <p className="text-sm text-slate-500 mb-8">
          Signed in as <span className="text-slate-300 font-mono">{authUser?.email}</span>
        </p>

        {/* MFA section ──────────────────────────────────────────────────── */}
        <section className="bg-white/[0.03] border border-white/[0.07] rounded-xl p-6">
          <div className="flex items-start gap-3 mb-4">
            {user?.mfa_enabled ? (
              <ShieldCheck size={20} className="text-green-400 mt-0.5" />
            ) : (
              <ShieldOff size={20} className="text-amber-400 mt-0.5" />
            )}
            <div className="flex-1">
              <h2 className="text-base font-semibold text-slate-100">
                Two-factor authentication
              </h2>
              <p className="text-sm text-slate-500 mt-0.5">
                {user?.mfa_enabled
                  ? "Enabled. You'll be prompted for a TOTP code on every sign-in."
                  : "Not enabled. Required for accounts with sensitive data access."}
              </p>
            </div>
          </div>

          {error && (
            <div className="text-xs text-red-400 bg-red-500/10 border border-red-500/20 rounded-lg px-3 py-2 mb-3">
              {error}
            </div>
          )}

          {/* IDLE — show enable / disable buttons ─────────────────────── */}
          {mode.kind === "idle" && user && (
            <div className="flex gap-2 mt-4">
              {user.mfa_enabled ? (
                <>
                  <button
                    onClick={() => { setMode({ kind: "disabling" }); setError(null); }}
                    className="px-4 py-2 text-sm rounded-lg border border-red-500/30 bg-red-500/10 hover:bg-red-500/20 text-red-300 transition"
                  >
                    Disable MFA
                  </button>
                  <button
                    onClick={() => { setMode({ kind: "regenerating" }); setError(null); }}
                    className="px-4 py-2 text-sm rounded-lg border border-white/[0.08] bg-white/[0.03] hover:bg-white/[0.06] text-slate-300 transition"
                  >
                    Regenerate backup codes
                  </button>
                </>
              ) : (
                <button
                  onClick={handleStartEnrollment}
                  disabled={busy}
                  className="flex items-center gap-2 px-4 py-2 text-sm rounded-lg bg-gradient-to-r from-red-500 to-orange-500 hover:from-red-400 hover:to-orange-400 disabled:opacity-50 text-white font-medium transition"
                >
                  {busy && <Loader2 size={14} className="animate-spin" />}
                  Set up MFA
                </button>
              )}
            </div>
          )}

          {/* ENROLLING QR — show QR + code form ───────────────────────── */}
          {mode.kind === "enrolling_qr" && (
            <div className="mt-4 space-y-4">
              <div className="bg-white/[0.02] border border-white/[0.06] rounded-lg p-4">
                <p className="text-sm text-slate-400 mb-3">
                  Scan this QR code with Google Authenticator, Authy, 1Password, or any TOTP-compatible app.
                </p>
                <div className="flex justify-center bg-white p-4 rounded-lg w-fit mx-auto">
                  <QRCodeSVG
                    value={mode.setup.provisioning_uri}
                    size={192}
                    includeMargin={false}
                  />
                </div>
                <details className="mt-3">
                  <summary className="text-xs text-slate-500 cursor-pointer hover:text-slate-400">
                    Can&apos;t scan? Enter the secret manually
                  </summary>
                  <div className="mt-2 font-mono text-xs text-slate-300 bg-black/20 rounded p-2 break-all">
                    {mode.setup.secret}
                  </div>
                </details>
              </div>

              <form onSubmit={handleActivate} className="space-y-3">
                <label className="block text-xs font-medium text-slate-400 uppercase tracking-wider">
                  Enter the 6-digit code from your authenticator
                </label>
                <input
                  type="text"
                  inputMode="numeric"
                  autoComplete="one-time-code"
                  pattern="[0-9]{6}"
                  required
                  value={code}
                  onChange={e => setCode(e.target.value.replace(/\D/g, "").slice(0, 6))}
                  className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3.5 py-2.5 text-base text-slate-100 placeholder-slate-600 focus:outline-none focus:border-red-500/50 focus:ring-1 focus:ring-red-500/30 transition font-mono tracking-widest"
                  placeholder="123456"
                  autoFocus
                />
                <div className="flex gap-2">
                  <button
                    type="button"
                    onClick={() => { setMode({ kind: "idle" }); setCode(""); }}
                    className="flex-1 px-4 py-2 text-sm rounded-lg border border-white/[0.08] bg-white/[0.03] hover:bg-white/[0.06] text-slate-400 transition"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={busy || code.length !== 6}
                    className="flex-1 flex items-center justify-center gap-2 px-4 py-2 text-sm rounded-lg bg-gradient-to-r from-red-500 to-orange-500 hover:from-red-400 hover:to-orange-400 disabled:opacity-50 text-white font-medium transition"
                  >
                    {busy && <Loader2 size={14} className="animate-spin" />}
                    Verify and activate
                  </button>
                </div>
              </form>
            </div>
          )}

          {/* ENROLLING DONE — show backup codes once ──────────────────── */}
          {mode.kind === "enrolling_done" && (
            <div className="mt-4 space-y-3">
              <div className="bg-amber-500/[0.06] border border-amber-500/30 rounded-lg p-4">
                <div className="flex items-start gap-2 mb-2">
                  <AlertTriangle size={16} className="text-amber-400 shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-semibold text-amber-200">
                      Save these backup codes now.
                    </p>
                    <p className="text-xs text-amber-200/80 mt-1">
                      Each code works once if you lose access to your authenticator.
                      <strong className="text-amber-200"> They will not be shown again.</strong>
                    </p>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-1 font-mono text-sm text-amber-100 bg-black/30 rounded p-3 my-2">
                  {mode.result.backup_codes.map(c => (
                    <span key={c}>{c}</span>
                  ))}
                </div>
                <button
                  onClick={() => copyBackupCodes(mode.result.backup_codes)}
                  className="mt-1 inline-flex items-center gap-1.5 text-xs text-amber-200 hover:text-amber-100 transition"
                >
                  {copied ? <Check size={12} /> : <Copy size={12} />}
                  {copied ? "Copied" : "Copy all to clipboard"}
                </button>
              </div>
              <button
                onClick={() => setMode({ kind: "idle" })}
                className="w-full px-4 py-2 text-sm rounded-lg bg-white/[0.04] hover:bg-white/[0.07] border border-white/[0.08] text-slate-300 transition"
              >
                I&apos;ve saved my codes — done
              </button>
            </div>
          )}

          {/* DISABLING — confirm with TOTP ────────────────────────────── */}
          {mode.kind === "disabling" && (
            <form onSubmit={handleDisable} className="mt-4 space-y-3">
              <p className="text-sm text-slate-400">
                To confirm, enter a current TOTP code from your authenticator (or a backup code).
              </p>
              <input
                type="text"
                autoComplete="one-time-code"
                required
                value={code}
                onChange={e => setCode(e.target.value)}
                className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3.5 py-2.5 text-base text-slate-100 placeholder-slate-600 focus:outline-none focus:border-red-500/50 focus:ring-1 focus:ring-red-500/30 transition font-mono tracking-widest"
                placeholder="123456"
                autoFocus
              />
              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => { setMode({ kind: "idle" }); setCode(""); }}
                  className="flex-1 px-4 py-2 text-sm rounded-lg border border-white/[0.08] bg-white/[0.03] hover:bg-white/[0.06] text-slate-400 transition"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={busy || !code}
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2 text-sm rounded-lg border border-red-500/30 bg-red-500/10 hover:bg-red-500/20 disabled:opacity-50 text-red-300 font-medium transition"
                >
                  {busy && <Loader2 size={14} className="animate-spin" />}
                  Disable MFA
                </button>
              </div>
            </form>
          )}

          {/* REGENERATING — confirm with TOTP ─────────────────────────── */}
          {mode.kind === "regenerating" && (
            <form onSubmit={handleRegenerate} className="mt-4 space-y-3">
              <p className="text-sm text-slate-400">
                Generate a new set of 10 backup codes? Your existing codes will stop working.
                Enter your current TOTP code to confirm.
              </p>
              <input
                type="text"
                autoComplete="one-time-code"
                required
                value={code}
                onChange={e => setCode(e.target.value)}
                className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3.5 py-2.5 text-base text-slate-100 placeholder-slate-600 focus:outline-none focus:border-red-500/50 focus:ring-1 focus:ring-red-500/30 transition font-mono tracking-widest"
                placeholder="123456"
                autoFocus
              />
              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => { setMode({ kind: "idle" }); setCode(""); }}
                  className="flex-1 px-4 py-2 text-sm rounded-lg border border-white/[0.08] bg-white/[0.03] hover:bg-white/[0.06] text-slate-400 transition"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={busy || !code}
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2 text-sm rounded-lg bg-gradient-to-r from-red-500 to-orange-500 hover:from-red-400 hover:to-orange-400 disabled:opacity-50 text-white font-medium transition"
                >
                  {busy && <Loader2 size={14} className="animate-spin" />}
                  Regenerate codes
                </button>
              </div>
            </form>
          )}
        </section>
      </div>
    </AppShell>
  );
}
