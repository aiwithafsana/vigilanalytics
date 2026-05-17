"use client";
import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { login, loginMfa } from "@/lib/api";
import { useAuth } from "@/lib/auth";

type Phase =
  | { kind: "credentials" }
  | { kind: "mfa"; mfaToken: string };

export default function LoginPage() {
  const router = useRouter();
  const { user, loading, refresh } = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [mfaCode, setMfaCode] = useState("");
  const [phase, setPhase] = useState<Phase>({ kind: "credentials" });
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!loading && user) router.replace("/dashboard");
  }, [user, loading, router]);

  async function handleCredentialsSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    setSubmitting(true);
    try {
      const res = await login(email, password);
      if (res.kind === "mfa_required") {
        // Move to MFA prompt — credentials were correct
        setPhase({ kind: "mfa", mfaToken: res.challenge.mfa_token });
      } else {
        await refresh();
        router.replace("/dashboard");
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Login failed");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleMfaSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (phase.kind !== "mfa") return;
    setError("");
    setSubmitting(true);
    try {
      await loginMfa(phase.mfaToken, mfaCode);
      await refresh();
      router.replace("/dashboard");
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "MFA verification failed");
    } finally {
      setSubmitting(false);
    }
  }

  function handleCancelMfa() {
    setPhase({ kind: "credentials" });
    setMfaCode("");
    setError("");
  }

  return (
    <div className="min-h-screen flex items-center justify-center px-4">
      <div className="w-full max-w-sm fade-in">
        {/* Logo */}
        <div className="flex items-center gap-3 mb-10 justify-center">
          <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-red-500 to-orange-400 flex items-center justify-center text-[#080c16] font-black text-lg">
            V
          </div>
          <span className="text-xl font-bold tracking-widest text-slate-100">VIGIL</span>
        </div>

        <div className="bg-white/[0.03] border border-white/[0.07] rounded-2xl p-8">
          {phase.kind === "credentials" ? (
            <>
              <h1 className="text-lg font-semibold text-slate-100 mb-1">Sign in</h1>
              <p className="text-sm text-slate-500 mb-7">Medicare Fraud Intelligence Platform</p>

              <form onSubmit={handleCredentialsSubmit} className="space-y-4">
                <div>
                  <label className="block text-xs font-medium text-slate-400 mb-1.5 uppercase tracking-wider">
                    Email
                  </label>
                  <input
                    type="email"
                    required
                    value={email}
                    onChange={e => setEmail(e.target.value)}
                    className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3.5 py-2.5 text-sm text-slate-100 placeholder-slate-600 focus:outline-none focus:border-red-500/50 focus:ring-1 focus:ring-red-500/30 transition"
                    placeholder="you@agency.gov"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-400 mb-1.5 uppercase tracking-wider">
                    Password
                  </label>
                  <input
                    type="password"
                    required
                    value={password}
                    onChange={e => setPassword(e.target.value)}
                    className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3.5 py-2.5 text-sm text-slate-100 placeholder-slate-600 focus:outline-none focus:border-red-500/50 focus:ring-1 focus:ring-red-500/30 transition"
                    placeholder="••••••••"
                  />
                </div>

                {error && (
                  <p className="text-xs text-red-400 bg-red-500/10 border border-red-500/20 rounded-lg px-3 py-2">
                    {error}
                  </p>
                )}

                <button
                  type="submit"
                  disabled={submitting}
                  className="w-full bg-gradient-to-r from-red-500 to-orange-500 hover:from-red-400 hover:to-orange-400 disabled:opacity-50 text-white font-semibold py-2.5 rounded-lg text-sm transition mt-2"
                >
                  {submitting ? "Signing in…" : "Sign in"}
                </button>
              </form>
            </>
          ) : (
            <>
              <h1 className="text-lg font-semibold text-slate-100 mb-1">Two-factor authentication</h1>
              <p className="text-sm text-slate-500 mb-7">
                Enter the 6-digit code from your authenticator app, or a backup code.
              </p>

              <form onSubmit={handleMfaSubmit} className="space-y-4">
                <div>
                  <label className="block text-xs font-medium text-slate-400 mb-1.5 uppercase tracking-wider">
                    Verification code
                  </label>
                  <input
                    type="text"
                    inputMode="text"
                    autoComplete="one-time-code"
                    autoFocus
                    required
                    value={mfaCode}
                    onChange={e => setMfaCode(e.target.value)}
                    className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3.5 py-2.5 text-base text-slate-100 placeholder-slate-600 focus:outline-none focus:border-red-500/50 focus:ring-1 focus:ring-red-500/30 transition font-mono tracking-widest"
                    placeholder="123456"
                  />
                  <p className="text-[11px] text-slate-600 mt-1.5">
                    Tip: backup codes are 10 lowercase hex characters.
                  </p>
                </div>

                {error && (
                  <p className="text-xs text-red-400 bg-red-500/10 border border-red-500/20 rounded-lg px-3 py-2">
                    {error}
                  </p>
                )}

                <div className="flex gap-2">
                  <button
                    type="button"
                    onClick={handleCancelMfa}
                    disabled={submitting}
                    className="flex-1 bg-white/[0.03] hover:bg-white/[0.06] border border-white/[0.08] text-slate-400 font-medium py-2.5 rounded-lg text-sm transition disabled:opacity-50"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={submitting || !mfaCode}
                    className="flex-1 bg-gradient-to-r from-red-500 to-orange-500 hover:from-red-400 hover:to-orange-400 disabled:opacity-50 text-white font-semibold py-2.5 rounded-lg text-sm transition"
                  >
                    {submitting ? "Verifying…" : "Verify"}
                  </button>
                </div>
              </form>
            </>
          )}
        </div>

        <p className="text-center text-xs text-slate-600 mt-6">
          Authorized personnel only · All access is logged
        </p>
      </div>
    </div>
  );
}
