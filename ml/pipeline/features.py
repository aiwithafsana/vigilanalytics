"""
features.py — Feature engineering from aggregated CMS data.

For each provider computes:
  - peer_median_payment / services / benes  (by specialty × state, min 10 peers;
    falls back to specialty-only when state group is too small)
  - peer_median_ppb  (median of per-provider payment/bene ratios — correct peer cost)
  - payment_vs_peer, services_vs_peer, benes_vs_peer  (ratio to peer median)
  - payment_zscore  (z-score within specialty × state peer group)
  - services_per_bene, payment_per_bene
  - billing_entropy, em_upcoding_ratio  (from ingest if available)
  - is_excluded, is_opt_out, months_enrolled  (from LEIE / enrollment enrichment)

Outputs
-------
data/processed/features.parquet   — feature matrix, one row per NPI
"""

from pathlib import Path
import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).parent.parent / "data"
PROC_DIR = DATA_DIR / "processed"

# Minimum number of providers required to use a specialty×state peer group.
# Groups smaller than this fall back to specialty-only.
MIN_PEER_GROUP_SIZE = 10

# Features used as model inputs (must be finite floats).
# is_excluded / is_opt_out / months_enrolled come from LEIE + enrollment enrichment.
FEATURE_COLS = [
    "payment_vs_peer",
    "services_vs_peer",
    "benes_vs_peer",
    "payment_zscore",
    "services_per_bene",
    "payment_per_bene_norm",    # log-scaled
    "total_payment_log",
    "total_services_log",
    "num_procedure_types_norm",
    "billing_entropy",
    "em_upcoding_ratio",
    "is_excluded",              # binary: 1 = on OIG LEIE list
    "is_opt_out",               # binary: 1 = opted out of Medicare
    "months_enrolled",          # continuous: months in current enrollment period
]

# Canonical specialty mapping — normalises the 200+ raw NPPES/CMS strings
# down to a controlled vocabulary so peer groups are large enough to be meaningful.
_SPECIALTY_MAP: dict[str, str] = {
    # Family / primary care
    "family practice":                       "family medicine",
    "general practice":                      "family medicine",
    "family medicine":                       "family medicine",
    # Internal medicine
    "internal medicine":                     "internal medicine",
    "general internal medicine":             "internal medicine",
    # Cardiology variants
    "cardiology":                            "cardiology",
    "interventional cardiology":             "cardiology",
    "clinical cardiac electrophysiology":    "cardiology",
    # Oncology
    "hematology/oncology":                   "hematology/oncology",
    "medical oncology":                      "hematology/oncology",
    "hematology":                            "hematology/oncology",
    "gynecologic oncology":                  "hematology/oncology",
    # Surgical
    "general surgery":                       "general surgery",
    "orthopedic surgery":                    "orthopedic surgery",
    "neurological surgery":                  "neurosurgery",
    "neurosurgery":                          "neurosurgery",
    "thoracic surgery":                      "thoracic surgery",
    "vascular surgery":                      "vascular surgery",
    "plastic surgery":                       "plastic surgery",
    "urology":                               "urology",
    # Radiology / imaging
    "diagnostic radiology":                  "diagnostic radiology",
    "radiology":                             "diagnostic radiology",
    "interventional radiology":              "diagnostic radiology",
    "nuclear medicine":                      "nuclear medicine",
    "radiation oncology":                    "radiation oncology",
    # Labs / pathology
    "pathology":                             "pathology",
    "clinical pathology":                    "pathology",
    "anatomic pathology":                    "pathology",
    "clinical laboratory":                   "clinical laboratory",
    "independent laboratory":                "clinical laboratory",
    # Other specialists (keep as-is but normalise case)
    "psychiatry":                            "psychiatry",
    "neurology":                             "neurology",
    "dermatology":                           "dermatology",
    "gastroenterology":                      "gastroenterology",
    "nephrology":                            "nephrology",
    "pulmonology":                           "pulmonology",
    "pulmonary disease":                     "pulmonology",
    "rheumatology":                          "rheumatology",
    "endocrinology":                         "endocrinology",
    "infectious disease":                    "infectious disease",
    "physical medicine and rehabilitation":  "physical medicine",
    "physical therapy":                      "physical therapy",
    "occupational therapy":                  "occupational therapy",
    "chiropractic":                          "chiropractic",
    "optometry":                             "optometry",
    "ophthalmology":                         "ophthalmology",
    # Facilities / high-volume
    "durable medical equipment":             "durable medical equipment",
    "home health":                           "home health",
    "skilled nursing facility":              "skilled nursing facility",
    "hospice":                               "hospice",
    "ambulance":                             "ambulance",
    "ambulatory surgical center":            "ambulatory surgical center",
}


def _normalize_specialty(s: str | None) -> str:
    if not s:
        return "unknown"
    key = s.strip().lower()
    return _SPECIALTY_MAP.get(key, key)


def _safe_div(a: pd.Series, b: pd.Series, fill: float = 0.0) -> pd.Series:
    return (a / b.replace(0, np.nan)).fillna(fill)


def _zscore_by_group(values: pd.Series, groups: pd.Series) -> pd.Series:
    """Compute z-score of values within each group."""
    mean = values.groupby(groups).transform("mean")
    std  = values.groupby(groups).transform("std").replace(0, np.nan)
    return ((values - mean) / std).fillna(0)


def _peer_median_with_fallback(
    df: pd.DataFrame,
    col: str,
    fine_group: str,
    coarse_group: str,
    min_size: int = MIN_PEER_GROUP_SIZE,
) -> pd.Series:
    """
    Compute per-row peer median using fine_group (specialty × state).
    Fall back to coarse_group (specialty-only) when fine group has < min_size providers.
    """
    fine_count  = df.groupby(fine_group)[col].transform("count")
    fine_median = df.groupby(fine_group)[col].transform("median")
    coarse_median = df.groupby(coarse_group)[col].transform("median")
    return pd.Series(
        np.where(fine_count >= min_size, fine_median, coarse_median),
        index=df.index,
    )


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

    # --- Normalise specialty strings to canonical vocabulary ---
    df["specialty"] = df["specialty"].apply(_normalize_specialty)

    # --- Peer group keys ---
    state_col = df["state"].fillna("UNK").str.strip().str.upper()
    df["_peer_group_fine"]   = df["specialty"] + "|" + state_col   # specialty × state
    df["_peer_group_coarse"] = df["specialty"]                       # specialty-only fallback

    # --- Per-bene / per-service ratios ---
    df["services_per_bene"] = _safe_div(df["total_services"],  df["total_beneficiaries"])
    df["payment_per_bene"]  = _safe_div(df["total_payment"],   df["total_beneficiaries"])

    # --- Peer medians (specialty × state, fallback to specialty) ---
    for col, out in [
        ("total_payment",       "peer_median_payment"),
        ("total_services",      "peer_median_services"),
        ("total_beneficiaries", "peer_median_benes"),
    ]:
        df[out] = _peer_median_with_fallback(
            df, col, "_peer_group_fine", "_peer_group_coarse"
        )

    # --- Correct peer cost-per-patient: median of per-provider ratios ---
    # (median of payment/bene ratios ≠ median(payment) / median(benes))
    df["peer_median_ppb"] = _peer_median_with_fallback(
        df, "payment_per_bene", "_peer_group_fine", "_peer_group_coarse"
    )

    # --- Ratios vs peer ---
    df["payment_vs_peer"]  = _safe_div(df["total_payment"],       df["peer_median_payment"],  fill=1.0)
    df["services_vs_peer"] = _safe_div(df["total_services"],      df["peer_median_services"], fill=1.0)
    df["benes_vs_peer"]    = _safe_div(df["total_beneficiaries"], df["peer_median_benes"],    fill=1.0)

    # --- Z-scores within fine peer group, fall back to coarse ---
    fine_count_pay = df.groupby("_peer_group_fine")["total_payment"].transform("count")
    if (fine_count_pay >= MIN_PEER_GROUP_SIZE).any():
        df["payment_zscore"] = _zscore_by_group(df["total_payment"], df["_peer_group_fine"])
    else:
        df["payment_zscore"] = _zscore_by_group(df["total_payment"], df["_peer_group_coarse"])

    # --- Log-scaled features (stabilise heavy tails) ---
    df["total_payment_log"]        = np.log1p(df["total_payment"])
    df["total_services_log"]       = np.log1p(df["total_services"])
    df["payment_per_bene_norm"]    = np.log1p(df["payment_per_bene"])
    df["num_procedure_types_norm"] = np.log1p(df["num_procedure_types"].fillna(0))

    # --- Fill missing HCPCS features with group median ---
    for col in ["billing_entropy", "em_upcoding_ratio"]:
        if col not in df.columns:
            df[col] = np.nan
        group_median = df.groupby("_peer_group_coarse")[col].transform("median")
        df[col] = df[col].fillna(group_median).fillna(0.0)

    # --- LEIE / enrollment binary features ---
    for col, default in [("is_excluded", 0.0), ("is_opt_out", 0.0)]:
        if col not in df.columns:
            df[col] = default
        df[col] = df[col].fillna(default).astype(float)

    # months_enrolled: default 12 (full year) when not available
    if "months_enrolled" not in df.columns:
        df["months_enrolled"] = 12.0
    df["months_enrolled"] = df["months_enrolled"].fillna(12.0).clip(lower=0, upper=12).astype(float)

    # --- Clip extreme ratios (higher ceiling to preserve true outliers) ---
    for col in ["payment_vs_peer", "services_vs_peer", "benes_vs_peer"]:
        df[col] = df[col].clip(upper=100.0)  # was 50.0 — raised to preserve extreme outliers

    df["payment_zscore"] = df["payment_zscore"].clip(-10, 50)

    # --- Drop internal helper columns before saving ---
    df = df.drop(columns=["_peer_group_fine", "_peer_group_coarse"], errors="ignore")

    print(f"  [features] Done. Feature columns: {FEATURE_COLS}")
    out_path = PROC_DIR / "features.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  [features] Saved → {out_path}")
    return df


if __name__ == "__main__":
    build()
