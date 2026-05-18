"use client";

/**
 * ExclusionTimingPanel — interprets a provider's LEIE exclusion date in the
 * context of the scoring data year (2022).
 *
 * Why this exists
 * ---------------
 * "LEIE Excluded" on its own is ambiguous.  An investigator seeing it
 * doesn't know whether the exclusion happened BEFORE the billing year
 * we're showing (in which case every 2022 claim is a per-claim FCA
 * violation — a $50M case) or AFTER (in which case the model predicted
 * the exclusion from 2022 patterns — a methodology argument, but not
 * direct legal exposure).
 *
 * Three buckets, three completely different stories
 * --------------------------------------------------
 *   1. PREDATES billing  → per-claim FCA violation (critical)
 *   2. DURING billing    → confirmed-fraud training positive (high)
 *   3. POSTDATES billing → temporal holdout / predictive signal (info)
 *
 * Only renders when the provider is is_excluded=true and has a parseable
 * leie_date.  Mounts under the score header on the provider detail page.
 */
import { AlertOctagon, AlertTriangle, Eye } from "lucide-react";

interface Props {
  /** True when the provider is on the LEIE (any reason). */
  isExcluded:  boolean;
  /** LEIE exclusion date as stored in DB: YYYYMMDD string. */
  leieDate:    string | null | undefined;
  /** LEIE exclusion-type code (e.g. "1128a1"). */
  leieReason:  string | null | undefined;
}

/**
 * The calendar year of the Medicare Part B data Vigil is scoring against.
 * Pulled from the backend's SCORING_DATA_YEAR constant in routers/system.py.
 * When CMS publishes 2023 P07 data and we update the pipeline, change this
 * constant — the temporal-classification language flows from it.
 */
const SCORING_YEAR = 2022;

// OIG exclusion codes most directly tied to billing fraud.  See LEIE
// documentation at https://oig.hhs.gov/exclusions/authorities.asp
const BILLING_FRAUD_CODES = new Set([
  "1128a1",   // Medicare/Medicaid fraud conviction
  "1128a3",   // Felony healthcare fraud
  "1128b7",   // False claims act violation
  "1128b8",   // Significant billing irregularities
  "1128b9",   // Failure to disclose info about crimes
  "1156",     // Unnecessary / substandard items or services
]);

/**
 * Parse the YYYYMMDD LEIE date into a JS Date.  Returns null when the
 * string isn't a valid 8-digit date (LEIE sometimes uses placeholder
 * values like "00000000" that we treat as missing).
 */
function parseLeieDate(s: string | null | undefined): Date | null {
  if (!s || s.length !== 8 || !/^\d{8}$/.test(s)) return null;
  const y = Number(s.slice(0, 4));
  const m = Number(s.slice(4, 6));
  const d = Number(s.slice(6, 8));
  if (y < 1990 || y > 2100 || m < 1 || m > 12 || d < 1 || d > 31) return null;
  const dt = new Date(y, m - 1, d);
  // JS Date silently rolls invalid days into the next month; check we landed
  // on the date we asked for to catch e.g. 20250230 (Feb 30).
  if (dt.getFullYear() !== y || dt.getMonth() !== m - 1 || dt.getDate() !== d) {
    return null;
  }
  return dt;
}

function classifyTiming(date: Date, billingYear: number): "predates" | "during" | "postdates" {
  const y = date.getFullYear();
  if (y < billingYear) return "predates";
  if (y > billingYear) return "postdates";
  return "during";
}

function formatDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function yearsBetween(a: Date, b: Date): number {
  return Math.abs((b.getTime() - a.getTime()) / (365.25 * 24 * 60 * 60 * 1000));
}

export default function ExclusionTimingPanel({
  isExcluded, leieDate, leieReason,
}: Props) {
  if (!isExcluded) return null;
  const date = parseLeieDate(leieDate);
  if (!date) {
    // Excluded but no parseable date — render a minimal "verify on OIG" card
    // rather than silently dropping the info.
    return (
      <div className="rounded-xl border border-red-500/25 bg-red-500/[0.06] px-5 py-4 mb-4">
        <div className="flex items-start gap-2.5">
          <AlertOctagon size={14} className="text-red-400 shrink-0 mt-0.5" />
          <div className="flex-1">
            <div className="text-[10px] uppercase tracking-widest text-red-300 font-bold mb-1">
              LEIE Excluded
            </div>
            <p className="text-sm text-slate-200 font-medium mb-1">
              Exclusion date not on file
            </p>
            <p className="text-xs text-slate-400 leading-relaxed">
              This provider appears on the OIG LEIE but Vigil&apos;s record
              lacks a parseable exclusion date.  Verify directly on{" "}
              <a
                href="https://oig.hhs.gov/exclusions/exclusions_list.asp"
                target="_blank" rel="noreferrer"
                className="text-blue-400 hover:text-blue-300 underline underline-offset-2"
              >
                OIG&apos;s LEIE search
              </a>
              {" "}to determine the temporal relationship to 2022 billing data.
            </p>
          </div>
        </div>
      </div>
    );
  }

  const timing = classifyTiming(date, SCORING_YEAR);
  const reason = (leieReason || "").toLowerCase().trim();
  const isBillingFraud = BILLING_FRAUD_CODES.has(reason);
  const billingYearStart = new Date(SCORING_YEAR, 0, 1);
  const billingYearEnd   = new Date(SCORING_YEAR, 11, 31);
  const yearsPredating  = yearsBetween(date, billingYearStart);
  const yearsPostdating = yearsBetween(date, billingYearEnd);

  // ── Bucket 1: PREDATES ─────────────────────────────────────────────────────
  // Provider was on LEIE BEFORE 2022 but kept billing into 2022.  Every
  // claim after the exclusion date is a per-claim FCA violation.
  if (timing === "predates") {
    return (
      <div className="rounded-xl border border-red-500/40 bg-gradient-to-br from-red-500/[0.10] to-red-500/[0.04] px-5 py-4 mb-4">
        <div className="flex items-start gap-2.5">
          <AlertOctagon size={14} className="text-red-400 shrink-0 mt-0.5" />
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-[10px] uppercase tracking-widest font-bold text-red-300">
                Critical · Active FCA exposure
              </span>
            </div>
            <p className="text-sm font-semibold text-slate-100 mb-1">
              Excluded {formatDate(date)} — {yearsPredating.toFixed(1)} years
              before the {SCORING_YEAR} billing year
            </p>
            <p className="text-xs text-slate-300 leading-relaxed mb-2">
              This provider was on the OIG LEIE for at least{" "}
              <span className="font-mono text-red-300">{yearsPredating.toFixed(1)} years</span>{" "}
              before the billing data Vigil is showing.  Every Medicare claim
              submitted during {SCORING_YEAR} is a per-claim violation of the
              False Claims Act (31 U.S.C. § 3729) — current statutory damages
              are $13,946 to $27,894 per claim plus 3× actual damages.
            </p>
            {isBillingFraud && (
              <p className="text-xs text-red-200/80 leading-relaxed">
                Exclusion code {leieReason} indicates billing-related conduct.
                Cross-program continued billing after exclusion is the
                paradigmatic FCA case.
              </p>
            )}
          </div>
        </div>
      </div>
    );
  }

  // ── Bucket 2: DURING ───────────────────────────────────────────────────────
  // Provider was excluded mid-year.  Claims before the exclusion date are
  // legitimate; claims after are per-claim FCA violations.
  if (timing === "during") {
    return (
      <div className="rounded-xl border border-orange-500/35 bg-orange-500/[0.08] px-5 py-4 mb-4">
        <div className="flex items-start gap-2.5">
          <AlertTriangle size={14} className="text-orange-400 shrink-0 mt-0.5" />
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-[10px] uppercase tracking-widest font-bold text-orange-300">
                High · Confirmed-fraud training label
              </span>
            </div>
            <p className="text-sm font-semibold text-slate-100 mb-1">
              Excluded {formatDate(date)} — during the {SCORING_YEAR} billing year
            </p>
            <p className="text-xs text-slate-300 leading-relaxed">
              This provider was excluded mid-year.  Their {SCORING_YEAR} billing
              patterns are exactly what OIG cited — making this a confirmed
              training-positive in Vigil&apos;s methodology.  Claims submitted
              after {formatDate(date)} are per-claim FCA violations
              (31 U.S.C. § 3729).  Pre-exclusion claims are legitimate.
            </p>
          </div>
        </div>
      </div>
    );
  }

  // ── Bucket 3: POSTDATES ────────────────────────────────────────────────────
  // Provider was excluded AFTER the billing year.  Vigil's model predicted
  // them from 2022 patterns alone — this is the temporal-holdout validation
  // signal.  No direct FCA exposure from the 2022 data we're showing, but
  // the provider may still be billing in years CMS hasn't published yet.
  return (
    <div className="rounded-xl border border-blue-500/30 bg-blue-500/[0.06] px-5 py-4 mb-4">
      <div className="flex items-start gap-2.5">
        <Eye size={14} className="text-blue-400 shrink-0 mt-0.5" />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className="text-[10px] uppercase tracking-widest font-bold text-blue-300">
              Predictive signal · Out-of-sample validation
            </span>
          </div>
          <p className="text-sm font-semibold text-slate-100 mb-1">
            Excluded {formatDate(date)} — {yearsPostdating.toFixed(1)} years
            after the {SCORING_YEAR} billing year
          </p>
          <p className="text-xs text-slate-300 leading-relaxed mb-2">
            This provider was excluded by OIG{" "}
            <span className="font-mono text-blue-200">{yearsPostdating.toFixed(1)} years</span>{" "}
            after the {SCORING_YEAR} billing data Vigil scores against.  The model
            had NO knowledge of this exclusion during training — flagging this
            provider from {SCORING_YEAR} patterns is genuine out-of-sample
            predictive power, not memorisation of the LEIE list.
          </p>
          <p className="text-xs text-blue-200/80 leading-relaxed">
            For current FCA exposure, subpoena claim records from {date.getFullYear() - 1}–present
            to determine whether this provider continued billing post-exclusion.
            Vigil&apos;s data ends at {SCORING_YEAR}-12-31 so post-exclusion
            billing isn&apos;t visible here.
          </p>
        </div>
      </div>
    </div>
  );
}
