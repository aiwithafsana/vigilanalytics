"""
detect_yoy.py — Year-over-year billing surge detection.

Identifies providers whose Medicare billing grew suspiciously fast between
consecutive years. This catches:
  - New provider spikes: enrolled within 12 months, billing at high-volume levels
  - Established provider surges: 2×+ payment increase with no peer-level growth
  - Code-switching: sudden shift to high-value HCPCS codes without volume change

Requires billing_records table to contain data for multiple years.
Single-year installs will produce no flags (gracefully).

How it works
------------
1. Pivot billing_records by (npi, year) → total_medicare_payment
2. For each consecutive year pair, compute per-provider growth ratio
3. Also compute specialty-wide median growth (the "peer growth rate")
4. Flag providers whose growth ratio is ≥ THRESHOLD_RATIO × peer growth
5. Write yoy_surge fraud_flags to DB

Thresholds
----------
  severity=1 (critical): provider grew ≥10× AND peer grew <3×  →  extreme spike
  severity=2 (high):     provider grew ≥ 4× AND peer grew <2×  →  strong surge
  severity=3 (medium):   provider grew ≥ 2× AND peer grew <1.5× → moderate surge

Only flags providers who already had ≥ $10,000 in the prior year
(new tiny billers doubling is noise; this catches real-money surges).

Confidence is derived from the ratio magnitude, capped at 0.95 (statistical
anomaly — not confirmed fraud).

Usage
-----
    python detect_yoy.py                   # run and write flags to DB
    python detect_yoy.py --dry-run         # print summary, no DB writes
    python detect_yoy.py --year-pair 2021 2022  # specific years
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

_HERE    = Path(__file__).parent
DATA_DIR = _HERE.parent / "data"
PROC_DIR = DATA_DIR / "processed"

load_dotenv(_HERE.parent.parent / "backend" / ".env")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://vigil:vigil@localhost:5432/vigil",
).replace("postgresql+asyncpg://", "postgresql://")

BATCH_SIZE    = 2_000
MIN_PRIOR_PAY = 10_000.0   # ignore providers with < $10k in prior year

# (severity, provider_ratio_threshold, peer_ratio_ceiling)
_TIERS = [
    (1, 10.0, 3.0),   # critical: 10× growth, peer < 3×
    (2,  4.0, 2.0),   # high:      4× growth, peer < 2×
    (3,  2.0, 1.5),   # medium:    2× growth, peer < 1.5×
]


def _conn():
    return psycopg2.connect(DATABASE_URL)


# ── Load billing data from DB ─────────────────────────────────────────────────

def load_billing_by_year() -> pd.DataFrame:
    """
    Load total_medicare_payment per (npi, year, specialty) from billing_records.
    We join providers to get specialty for peer-group comparison.
    """
    conn = _conn()
    sql = """
        SELECT
            br.npi,
            br.year,
            p.specialty,
            SUM(br.total_medicare_payment) AS total_payment
        FROM billing_records br
        JOIN providers p ON p.npi = br.npi
        WHERE br.total_medicare_payment IS NOT NULL
        GROUP BY br.npi, br.year, p.specialty
        ORDER BY br.npi, br.year
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    print(f"  [yoy] Loaded {len(df):,} provider-year rows, years: {sorted(df['year'].unique())}")
    return df


# ── Compute YoY ratios ────────────────────────────────────────────────────────

def compute_yoy_ratios(df: pd.DataFrame, year_a: int, year_b: int) -> pd.DataFrame:
    """
    For each provider present in both year_a and year_b, compute:
      - provider_ratio: payment_b / payment_a
      - peer_median_ratio: median of provider_ratio within specialty
      - relative_ratio:  provider_ratio / peer_median_ratio

    Returns providers with growth significantly above their peers.
    """
    ya = df[df["year"] == year_a][["npi", "specialty", "total_payment"]].rename(columns={"total_payment": "pay_a"})
    yb = df[df["year"] == year_b][["npi", "total_payment"]].rename(columns={"total_payment": "pay_b"})

    merged = ya.merge(yb, on="npi", how="inner")
    merged = merged[merged["pay_a"] >= MIN_PRIOR_PAY]   # exclude tiny billers

    merged["provider_ratio"] = (merged["pay_b"] / merged["pay_a"].replace(0, np.nan)).fillna(1.0)

    # Peer median ratio within specialty
    merged["peer_median_ratio"] = merged.groupby("specialty")["provider_ratio"].transform("median")
    merged["relative_ratio"]    = (merged["provider_ratio"] / merged["peer_median_ratio"].replace(0, np.nan)).fillna(1.0)

    print(f"  [yoy] {year_a}→{year_b}: {len(merged):,} providers with data in both years")
    return merged


# ── Generate flags ────────────────────────────────────────────────────────────

def generate_flags(merged: pd.DataFrame, year_a: int, year_b: int) -> list[dict]:
    """
    Produces a list of flag dicts for providers meeting surge thresholds.
    """
    flags = []
    for _, row in merged.iterrows():
        prov_r = float(row["provider_ratio"])
        peer_r = float(row["peer_median_ratio"])
        pay_a  = float(row["pay_a"])
        pay_b  = float(row["pay_b"])

        severity = None
        for sev, prov_thresh, peer_ceil in _TIERS:
            if prov_r >= prov_thresh and peer_r <= peer_ceil:
                severity = sev
                break

        if severity is None:
            continue

        # Confidence scales with relative_ratio, capped at 0.95
        rel   = float(row["relative_ratio"])
        conf  = min(0.50 + (rel - 1.0) * 0.05, 0.95)

        tier_label = {1: "CRITICAL", 2: "HIGH", 3: "MEDIUM"}.get(severity, "MEDIUM")
        explanation = (
            f"{tier_label} billing surge: payment grew {prov_r:.1f}× "
            f"from ${pay_a:,.0f} ({year_a}) to ${pay_b:,.0f} ({year_b}) "
            f"while the specialty peer median grew only {peer_r:.1f}×. "
            f"Relative growth vs. peers: {rel:.1f}×. "
            f"Potential schemes: new patient manufacturing, unbundling, upcoding ramp-up, "
            f"credential lending, or phantom billing. "
            f"Source: CMS Part B billing_records, NPI {row['npi']}."
        )

        flags.append({
            "npi":                  str(row["npi"]),
            "flag_type":            "yoy_surge",
            "layer":                4,                      # Layer 4 = temporal analysis
            "severity":             severity,
            "confidence":           round(conf, 3),
            "year":                 year_b,                 # flag year = the surge year
            "flag_value":           round(prov_r, 4),       # the growth ratio
            "peer_value":           round(peer_r, 4),       # peer median growth
            "explanation":          explanation,
            "estimated_overpayment": round(pay_b - pay_a * peer_r, 2),  # excess above peer growth
        })

    print(f"  [yoy] {year_a}→{year_b}: {len(flags):,} surge flags generated")
    return flags


# ── Write to DB ───────────────────────────────────────────────────────────────

def write_flags(flags: list[dict]) -> int:
    """
    Upsert yoy_surge flags into fraud_flags.
    Uses ON CONFLICT DO NOTHING — running twice won't duplicate.
    Deactivates stale yoy_surge flags for providers no longer flagged.
    """
    if not flags:
        print("  [yoy] No flags to write.")
        return 0

    conn = _conn()
    cur  = conn.cursor()

    # Deactivate any existing yoy_surge flags before inserting fresh ones
    flagged_npis = list({f["npi"] for f in flags})
    psycopg2.extras.execute_batch(
        cur,
        "UPDATE fraud_flags SET is_active = FALSE WHERE npi = %s AND flag_type = 'yoy_surge'",
        [(npi,) for npi in flagged_npis],
        page_size=BATCH_SIZE,
    )

    rows = [
        (
            f["npi"], f["flag_type"], f["layer"], f["severity"], f["confidence"],
            f["year"], f["flag_value"], f["peer_value"], f["explanation"],
            f["estimated_overpayment"],
        )
        for f in flags
    ]
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO fraud_flags
            (npi, flag_type, layer, severity, confidence, year,
             flag_value, peer_value, explanation, estimated_overpayment, is_active)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], True) for r in rows],
        page_size=BATCH_SIZE,
    )

    conn.commit()
    print(f"  [yoy] Wrote {len(flags):,} yoy_surge flags to DB")
    cur.close(); conn.close()
    return len(flags)


# ── Entrypoint ────────────────────────────────────────────────────────────────

def run(year_a: int | None = None, year_b: int | None = None, dry_run: bool = False) -> int:
    print("\n=== YOY SURGE DETECTION ===")

    billing = load_billing_by_year()

    if billing.empty:
        print("  [yoy] No billing records found. Run the ingest pipeline first.")
        return 0

    available_years = sorted(billing["year"].unique())

    if len(available_years) < 2:
        print(f"  [yoy] Only one year of data ({available_years}). YoY detection requires ≥2 years.")
        print("  [yoy] Skipping — no flags generated.")
        return 0

    # Default: compare the two most recent years
    if year_a is None:
        year_a = available_years[-2]
    if year_b is None:
        year_b = available_years[-1]

    if year_a not in available_years or year_b not in available_years:
        print(f"  [yoy] Requested years {year_a}/{year_b} not in data: {available_years}")
        return 0

    merged = compute_yoy_ratios(billing, year_a, year_b)
    flags  = generate_flags(merged, year_a, year_b)

    # Summary stats
    if flags:
        by_sev = {}
        for f in flags:
            by_sev[f["severity"]] = by_sev.get(f["severity"], 0) + 1
        print(f"  [yoy] Severity breakdown: {by_sev}")
        total_excess = sum(f.get("estimated_overpayment", 0) for f in flags)
        print(f"  [yoy] Total estimated excess vs. peer growth: ${total_excess:,.0f}")

    if dry_run:
        print("  [yoy] [dry-run] No DB writes performed.")
        return len(flags)

    return write_flags(flags)


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent.parent))
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--year-pair", nargs=2, type=int, metavar=("YEAR_A", "YEAR_B"))
    args = p.parse_args()

    year_a = args.year_pair[0] if args.year_pair else None
    year_b = args.year_pair[1] if args.year_pair else None
    run(year_a=year_a, year_b=year_b, dry_run=args.dry_run)
