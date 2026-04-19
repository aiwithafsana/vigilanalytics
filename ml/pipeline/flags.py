"""
flags.py — Generate human-readable anomaly flag objects for each provider.

Flags are stored as a JSONB array in the providers table.
Each flag has: { type, severity, text }

Severity thresholds
-------------------
critical : ratio ≥ 5× peer  OR z-score ≥ 6  OR E&M ratio ≥ 0.85
high     : ratio ≥ 3× peer  OR z-score ≥ 3  OR E&M ratio ≥ 0.70

Priority / deduplication rules
-------------------------------
1. LEIE exclusion fires first — all other flags suppressed when present
2. billing_volume suppresses statistical_outlier (redundant signal)
3. Maximum 3 flags returned per provider (critical first)
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
PROC_DIR = DATA_DIR / "processed"


# ── Specialty-adjusted service intensity thresholds ───────────────────────────
# Labs, imaging, and high-volume facilities have structurally elevated
# services-per-beneficiary that is NOT anomalous for their care model.
# (critical, high) tuples — default applies to office-based physicians.
_INTENSITY_THRESHOLDS: dict[str, tuple[float, float]] = {
    "clinical laboratory":     (200, 100),
    "independent laboratory":  (200, 100),
    "pathology":               (120,  60),
    "diagnostic radiology":    (100,  50),
    "radiology":               (100,  50),
    "nuclear medicine":        ( 80,  40),
    "radiation oncology":      ( 60,  30),
    "physical therapy":        ( 50,  25),
    "physical medicine":       ( 50,  25),
    "chiropractic":            ( 50,  25),
    "optometry":               ( 40,  20),
    "ophthalmology":           ( 40,  20),
    "durable medical equipment": (200, 100),
    "home health":             ( 80,  40),
    "skilled nursing facility": (80,  40),
    "_default":                ( 15,   8),
}


def _intensity_thresholds(specialty: str) -> tuple[float, float]:
    key = (specialty or "").strip().lower()
    return _INTENSITY_THRESHOLDS.get(key, _INTENSITY_THRESHOLDS["_default"])


def _sev(value: float, critical: float, high: float) -> str | None:
    if value >= critical:
        return "critical"
    if value >= high:
        return "high"
    return None


def _fmt(v: float) -> str:
    if v >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:,.0f}"


def _deduplicate_flags(flags: list[dict]) -> list[dict]:
    """
    Remove redundant flags and cap output at 3.

    Rules:
    - If billing_volume is present, drop statistical_outlier (same signal, different scale).
    - Sort critical before high.
    - Cap at 3 total flags.
    """
    types_present = {f["type"] for f in flags}
    result = []
    for f in flags:
        # billing_volume already captures the statistical_outlier finding
        if f["type"] == "statistical_outlier" and "billing_volume" in types_present:
            continue
        result.append(f)

    sev_order = {"critical": 0, "high": 1}
    result.sort(key=lambda x: sev_order.get(x["severity"], 2))
    return result[:3]


def generate_flags(row: pd.Series) -> list[dict]:
    flags: list[dict] = []
    specialty = str(row.get("specialty") or "")

    # ── LEIE exclusion — hard override, suppresses all other flags ─────────────
    if row.get("is_excluded"):
        return [{
            "type": "leie_exclusion",
            "severity": "critical",
            "text": (
                "Provider appears on the OIG LEIE exclusion list. "
                "Medicare billing by an excluded provider constitutes a per-claim "
                "violation of the False Claims Act (31 U.S.C. § 3729)."
            ),
        }]

    # ── Opt-out billing ────────────────────────────────────────────────────────
    # Providers who opted out of Medicare are prohibited from billing Medicare directly.
    if row.get("is_opt_out"):
        total = float(row.get("total_payment") or 0)
        if total > 0:
            flags.append({
                "type": "opt_out_billing",
                "severity": "critical",
                "text": (
                    f"Provider has opted out of Medicare participation; "
                    f"{_fmt(total)} in Medicare payments recorded — "
                    "verify enrollment status and billing authority."
                ),
            })

    # ── New provider billing spike ─────────────────────────────────────────────
    months = float(row.get("months_enrolled") or 12)
    pv_for_spike = float(row.get("payment_vs_peer") or 0)
    if 0 < months < 12 and pv_for_spike >= 3:
        annualized = float(row.get("total_payment") or 0) * (12.0 / months)
        sev_spike = "critical" if pv_for_spike >= 5 else "high"
        flags.append({
            "type": "new_provider_spike",
            "severity": sev_spike,
            "text": (
                f"Provider enrolled {months:.0f} month(s) ago; billing is "
                f"{pv_for_spike:.1f}× peer median "
                f"(annualized rate: {_fmt(annualized)})."
            ),
        })

    # ── Billing volume ─────────────────────────────────────────────────────────
    pv = float(row.get("payment_vs_peer") or 0)
    sev = _sev(pv, critical=5, high=3)
    if sev:
        flags.append({
            "type": "billing_volume",
            "severity": sev,
            "text": (
                f"Total Medicare payments {pv:.1f}× above specialty peer median "
                f"({_fmt(row.get('total_payment', 0))} vs "
                f"{_fmt(row.get('peer_median_payment', 0))} median)."
            ),
        })

    # ── Service pattern ────────────────────────────────────────────────────────
    sv = float(row.get("services_vs_peer") or 0)
    sev = _sev(sv, critical=5, high=3)
    if sev:
        flags.append({
            "type": "service_pattern",
            "severity": sev,
            "text": (
                f"Service volume {sv:.1f}× above peer median "
                f"({int(row.get('total_services', 0)):,} vs "
                f"{int(row.get('peer_median_services', 0)):,} median)."
            ),
        })

    # ── Beneficiary volume ─────────────────────────────────────────────────────
    bv = float(row.get("benes_vs_peer") or 0)
    sev = _sev(bv, critical=4, high=2.5)
    if sev:
        flags.append({
            "type": "beneficiary_volume",
            "severity": sev,
            "text": (
                f"Patient volume {bv:.1f}× above peer median "
                f"({int(row.get('total_beneficiaries', 0)):,} beneficiaries)."
            ),
        })

    # ── Statistical outlier (z-score) ──────────────────────────────────────────
    zs = float(row.get("payment_zscore") or 0)
    sev = _sev(zs, critical=6, high=3)
    if sev:
        flags.append({
            "type": "statistical_outlier",
            "severity": sev,
            "text": (
                f"Payment z-score of {zs:.1f} within "
                f"{specialty or 'specialty'} peer group — "
                f"{'extreme' if sev == 'critical' else 'notable'} statistical outlier."
            ),
        })

    # ── Cost per patient ───────────────────────────────────────────────────────
    # Use pre-computed peer_median_ppb (median of per-provider ratios) when available;
    # fall back to ratio of peer medians only if the column is absent.
    ppb = float(row.get("payment_per_bene") or 0)
    peer_ppb = float(row.get("peer_median_ppb") or 0)
    if peer_ppb <= 0:
        # Legacy fallback — ratio of medians (less accurate)
        peer_ppb = float(row.get("peer_median_payment") or 1) / max(
            float(row.get("peer_median_benes") or 1), 1
        )
    ppb_ratio = ppb / peer_ppb if peer_ppb > 0 else 0
    sev = _sev(ppb_ratio, critical=4, high=2.5)
    if sev and ppb > 500:
        flags.append({
            "type": "cost_per_patient",
            "severity": sev,
            "text": (
                f"Payment per beneficiary {_fmt(ppb)} is {ppb_ratio:.1f}× "
                f"above peer median ({_fmt(peer_ppb)})."
            ),
        })

    # ── Service intensity (specialty-adjusted thresholds) ──────────────────────
    spb = float(row.get("services_per_bene") or 0)
    crit_thr, high_thr = _intensity_thresholds(specialty)
    sev = _sev(spb, critical=crit_thr, high=high_thr)
    if sev:
        flags.append({
            "type": "service_intensity",
            "severity": sev,
            "text": (
                f"{spb:.1f} services per beneficiary — "
                f"{'extreme' if sev == 'critical' else 'elevated'} service intensity "
                f"relative to {specialty or 'specialty'} norms."
            ),
        })

    # ── E&M upcoding ───────────────────────────────────────────────────────────
    em = float(row.get("em_upcoding_ratio") or 0)
    sev = _sev(em, critical=0.85, high=0.70)
    if sev:
        flags.append({
            "type": "em_upcoding",
            "severity": sev,
            "text": (
                f"E&M upcoding ratio {em:.0%} — "
                f"{em:.0%} of office visits billed at highest complexity level."
            ),
        })

    return _deduplicate_flags(flags)


def run(df: pd.DataFrame | None = None) -> pd.DataFrame:
    print("\n=== FLAGS ===")

    if df is None:
        df = pd.read_parquet(PROC_DIR / "scored.parquet")

    print(f"  Generating flags for {len(df):,} providers…")
    df = df.copy()
    df["flags"] = df.apply(generate_flags, axis=1)

    flag_counts = df["flags"].apply(len)
    print(f"  Providers with ≥1 flag : {(flag_counts >= 1).sum():,}")
    print(f"  Providers with critical: {df['flags'].apply(lambda f: any(x['severity']=='critical' for x in f)).sum():,}")
    leie_flags = df["flags"].apply(lambda f: any(x["type"] == "leie_exclusion" for x in f))
    print(f"  LEIE exclusion flags   : {leie_flags.sum():,}")

    out_path = PROC_DIR / "scored_with_flags.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  [flags] Saved → {out_path}")
    return df


if __name__ == "__main__":
    run()
