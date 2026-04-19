"""
flags.py — Generate human-readable anomaly flag objects for each provider.

Flags are stored as a JSONB array in the providers table.
Each flag has: { type, severity, text }

Severity thresholds
-------------------
critical : ratio ≥ 5× peer  OR z-score ≥ 6  OR E&M ratio ≥ 0.85
high     : ratio ≥ 3× peer  OR z-score ≥ 3  OR E&M ratio ≥ 0.70
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
PROC_DIR = DATA_DIR / "processed"


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


def generate_flags(row: pd.Series) -> list[dict]:
    flags = []

    # ── Billing volume ─────────────────────────────────────────────────────
    pv = float(row.get("payment_vs_peer") or 0)
    sev = _sev(pv, critical=5, high=3)
    if sev:
        flags.append({
            "type": "billing_volume",
            "severity": sev,
            "text": (
                f"Total Medicare payments {pv:.1f}× above specialty peer median "
                f"({_fmt(row.get('total_payment', 0))} vs {_fmt(row.get('peer_median_payment', 0))})"
            ),
        })

    # ── Service pattern ────────────────────────────────────────────────────
    sv = float(row.get("services_vs_peer") or 0)
    sev = _sev(sv, critical=5, high=3)
    if sev:
        flags.append({
            "type": "service_pattern",
            "severity": sev,
            "text": (
                f"Service volume {sv:.1f}× above peer median — possible phantom billing "
                f"({int(row.get('total_services', 0)):,} vs {int(row.get('peer_median_services', 0)):,} median)"
            ),
        })

    # ── Beneficiary volume ─────────────────────────────────────────────────
    bv = float(row.get("benes_vs_peer") or 0)
    sev = _sev(bv, critical=4, high=2.5)
    if sev:
        flags.append({
            "type": "beneficiary_volume",
            "severity": sev,
            "text": (
                f"Patient volume {bv:.1f}× above peer median "
                f"({int(row.get('total_beneficiaries', 0)):,} beneficiaries)"
            ),
        })

    # ── Statistical outlier (z-score) ──────────────────────────────────────
    zs = float(row.get("payment_zscore") or 0)
    sev = _sev(zs, critical=6, high=3)
    if sev:
        flags.append({
            "type": "statistical_outlier",
            "severity": sev,
            "text": (
                f"Payment z-score of {zs:.1f} — statistically extreme outlier "
                f"within {row.get('specialty', 'specialty')} peer group"
            ),
        })

    # ── Cost per patient ───────────────────────────────────────────────────
    ppb = float(row.get("payment_per_bene") or 0)
    peer_ppb = float(row.get("peer_median_payment") or 1) / max(float(row.get("peer_median_benes") or 1), 1)
    ppb_ratio = ppb / peer_ppb if peer_ppb > 0 else 0
    sev = _sev(ppb_ratio, critical=4, high=2.5)
    if sev and ppb > 500:
        flags.append({
            "type": "cost_per_patient",
            "severity": sev,
            "text": (
                f"Payment per beneficiary {_fmt(ppb)} is {ppb_ratio:.1f}× "
                f"above peer median ({_fmt(peer_ppb)})"
            ),
        })

    # ── Service intensity ──────────────────────────────────────────────────
    spb = float(row.get("services_per_bene") or 0)
    sev = _sev(spb, critical=15, high=8)
    if sev:
        flags.append({
            "type": "service_intensity",
            "severity": sev,
            "text": (
                f"{spb:.1f} services per beneficiary — "
                f"{'extreme' if sev == 'critical' else 'high'} service intensity"
            ),
        })

    # ── E&M upcoding ──────────────────────────────────────────────────────
    em = float(row.get("em_upcoding_ratio") or 0)
    sev = _sev(em, critical=0.85, high=0.70)
    if sev:
        flags.append({
            "type": "em_upcoding",
            "severity": sev,
            "text": (
                f"E&M upcoding ratio {em:.0%} — "
                f"{em:.0%} of office visits coded at highest complexity level"
            ),
        })

    return flags


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

    out_path = PROC_DIR / "scored_with_flags.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  [flags] Saved → {out_path}")
    return df


if __name__ == "__main__":
    run()
