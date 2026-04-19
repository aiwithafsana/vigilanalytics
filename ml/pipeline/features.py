"""
features.py — Feature engineering from aggregated CMS data.

For each provider computes:
  - peer_median_payment / services / benes  (by specialty)
  - payment_vs_peer, services_vs_peer, benes_vs_peer  (ratio to peer median)
  - payment_zscore  (z-score within specialty)
  - services_per_bene, payment_per_bene
  - billing_entropy, em_upcoding_ratio  (from ingest if available)

Outputs
-------
data/processed/features.parquet   — feature matrix, one row per NPI
"""

from pathlib import Path
import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).parent.parent / "data"
PROC_DIR = DATA_DIR / "processed"

# Features used as model inputs (must be finite floats)
FEATURE_COLS = [
    "payment_vs_peer",
    "services_vs_peer",
    "benes_vs_peer",
    "payment_zscore",
    "services_per_bene",
    "payment_per_bene_norm",   # log-scaled
    "total_payment_log",
    "total_services_log",
    "num_procedure_types_norm",
    "billing_entropy",
    "em_upcoding_ratio",
]


def _safe_div(a: pd.Series, b: pd.Series, fill: float = 0.0) -> pd.Series:
    return (a / b.replace(0, np.nan)).fillna(fill)


def _zscore_by_group(values: pd.Series, groups: pd.Series) -> pd.Series:
    """Compute z-score of values within each group."""
    mean = values.groupby(groups).transform("mean")
    std  = values.groupby(groups).transform("std").replace(0, np.nan)
    return ((values - mean) / std).fillna(0)


def build(providers: pd.DataFrame | None = None) -> pd.DataFrame:
    if providers is None:
        path = PROC_DIR / "providers_aggregated.parquet"
        providers = pd.read_parquet(path)

    print(f"  [features] Building features for {len(providers):,} providers…")
    df = providers.copy()

    # --- Require minimum data quality ---
    df = df[df["total_payment"] > 0]
    df = df[df["total_services"] > 0]
    df = df[df["total_beneficiaries"] > 0]

    # --- Per-bene / per-service ratios ---
    df["services_per_bene"]  = _safe_div(df["total_services"],      df["total_beneficiaries"])
    df["payment_per_bene"]   = _safe_div(df["total_payment"],       df["total_beneficiaries"])

    # --- Peer medians by specialty ---
    for col, out in [
        ("total_payment",        "peer_median_payment"),
        ("total_services",       "peer_median_services"),
        ("total_beneficiaries",  "peer_median_benes"),
    ]:
        df[out] = df.groupby("specialty")[col].transform("median")

    # --- Ratios vs peer ---
    df["payment_vs_peer"]  = _safe_div(df["total_payment"],       df["peer_median_payment"],  fill=1.0)
    df["services_vs_peer"] = _safe_div(df["total_services"],      df["peer_median_services"], fill=1.0)
    df["benes_vs_peer"]    = _safe_div(df["total_beneficiaries"], df["peer_median_benes"],    fill=1.0)

    # --- Z-scores within specialty ---
    df["payment_zscore"] = _zscore_by_group(df["total_payment"], df["specialty"])

    # --- Log-scaled features (stabilise heavy tails) ---
    df["total_payment_log"]        = np.log1p(df["total_payment"])
    df["total_services_log"]       = np.log1p(df["total_services"])
    df["payment_per_bene_norm"]    = np.log1p(df["payment_per_bene"])
    df["num_procedure_types_norm"] = np.log1p(df["num_procedure_types"].fillna(0))

    # --- Fill missing HCPCS features with group median ---
    for col in ["billing_entropy", "em_upcoding_ratio"]:
        if col not in df.columns:
            df[col] = np.nan
        group_median = df.groupby("specialty")[col].transform("median")
        df[col] = df[col].fillna(group_median).fillna(0.0)

    # --- Clip extreme ratios to reduce noise from tiny specialties ---
    for col in ["payment_vs_peer", "services_vs_peer", "benes_vs_peer"]:
        df[col] = df[col].clip(upper=50.0)

    df["payment_zscore"] = df["payment_zscore"].clip(-10, 50)

    print(f"  [features] Done. Feature columns: {FEATURE_COLS}")
    out_path = PROC_DIR / "features.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  [features] Saved → {out_path}")
    return df


if __name__ == "__main__":
    build()
