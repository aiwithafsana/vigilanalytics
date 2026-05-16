"use client";

import { useEffect } from "react";

interface ErrorProps {
  error: Error & { digest?: string };
  reset: () => void;
}

export default function GlobalError({ error, reset }: ErrorProps) {
  useEffect(() => {
    // Log to your error-reporting service here (e.g. Sentry)
    console.error("[Vigil] Unhandled error:", error);
  }, [error]);

  return (
    <div className="min-h-screen flex items-center justify-center bg-[#080c16]">
      <div className="max-w-md w-full mx-4 p-8 rounded-xl border border-white/[0.08] bg-white/[0.03] text-center">
        <div className="w-12 h-12 rounded-full bg-red-500/10 border border-red-500/20 flex items-center justify-center mx-auto mb-4">
          <svg
            className="w-6 h-6 text-red-400"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={1.5}
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126ZM12 15.75h.007v.008H12v-.008Z"
            />
          </svg>
        </div>

        <h2 className="text-lg font-semibold text-slate-100 mb-2">
          Something went wrong
        </h2>
        <p className="text-sm text-slate-400 mb-6">
          An unexpected error occurred. The issue has been logged.
          {error.digest && (
            <span className="block mt-1 font-mono text-xs text-slate-500">
              ID: {error.digest}
            </span>
          )}
        </p>

        <div className="flex gap-3 justify-center">
          <button
            onClick={reset}
            className="px-4 py-2 text-sm font-medium rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
          >
            Try again
          </button>
          <button
            onClick={() => (window.location.href = "/dashboard")}
            className="px-4 py-2 text-sm font-medium rounded-lg border border-white/[0.1] text-slate-300 hover:bg-white/[0.05] transition-colors"
          >
            Go to dashboard
          </button>
        </div>
      </div>
    </div>
  );
}
