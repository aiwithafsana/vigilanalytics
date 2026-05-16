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
# is_opt_out / months_enrolled come from LEIE + enrollment enrichment.
# NOTE: is_excluded is intentionally excluded from model inputs — it is the
# known-positive label used for XGBoost training.  Including it causes feature
# leakage: the model learns "is this person already caught?" rather than "does
# the billing pattern look anomalous?".  is_excluded is still computed and stored
# in features.parquet for post-scoring reporting (e.g. volume-specialty adjustment
# and dashboard display), but never fed to any model.
FEATURE_COLS = [
    "payment_vs_peer",
    "services_vs_peer",
    "benes_vs_peer",
    "ppb_vs_peer",              # per-patient cost vs. peer — size-invariant fraud signal
    "payment_zscore",
    "services_per_bene",
    "payment_per_bene_norm",    # log-scaled
    "payment_per_service_vs_peer",  # per-procedure revenue vs. peers (upcoding signal)
    "total_payment_log",
    "total_services_log",
    "num_procedure_types_norm",
    "billing_entropy",
    "em_upcoding_ratio",
    "hotspot_state",            # 1 = high-fraud geography (FL, TX, CA, NY, LA, MI, NJ)
    "yoy_payment_change",       # normalised 2021→2022 payment change vs. peer YoY trend
    "is_opt_out",               # binary: 1 = opted out of Medicare
    "months_enrolled",          # continuous: months in current enrollment period
    # NPPES enrichment (populated when nppes_enrichment.parquet is present)
    "is_sole_proprietor",       # binary: 1 = sole proprietor (NPPES) — over-represented in OIG actions
    "new_provider_high_volume", # binary: enumerated within 24mo AND payment_vs_peer >= 5
]

# States with persistently elevated Medicare fraud rates (OIG enforcement data)
_FRAUD_HOTSPOT_STATES = {"FL", "TX", "CA", "NY", "LA", "MI", "NJ", "IL", "GA", "MD"}

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
    "hematology/oncology":                   "hematology-oncology",
    "hematology-oncology":                   "hematology-oncology",
    "medical oncology":                      "hematology-oncology",
    "hematology":                            "hematology-oncology",
    "gynecologic oncology":                  "hematology-oncology",
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


def build(providers: pd.DataFrame | None = None,
          out_path: "Path | str | None" = None) -> pd.DataFrame:
    """Build feature matrix.  out_path overrides the default save location."""
    if providers is None:
        path = PROC_DIR / "providers_aggregated.parquet"
        providers = pd.read_parquet(path)

    print(f"  [features] Building features for {len(providers):,} providers…")
    df = providers.copy()

    # --- Join LEIE exclusion flags directly so is_excluded is accurate in features.parquet ---
    # train.py also does this join, but score.py reads features.parquet directly, so the
    # flag must be present here for inference-time scoring to work correctly.
    leie_path = PROC_DIR / "leie.parquet"
    if leie_path.exists():
        leie = pd.read_parquet(leie_path)
        leie_npis = set(leie["npi"].dropna().astype(str).unique())
        df["is_excluded"] = df["npi"].astype(str).isin(leie_npis).astype(float)
        n_excl = int(df["is_excluded"].sum())
        print(f"  [features] LEIE join: {n_excl:,} excluded providers flagged")

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

    # --- NPPES enrichment ----------------------------------------------------
    # When ingest_nppes.py has been run, nppes_enrichment.parquet is present
    # with per-NPI enumeration date and sole-proprietor flag.  Join it here.
    nppes_path = PROC_DIR / "nppes_enrichment.parquet"
    if nppes_path.exists():
        try:
            nppes = pd.read_parquet(nppes_path, columns=[
                "npi", "months_since_enumeration", "is_sole_proprietor",
            ])
            nppes["npi"] = nppes["npi"].astype(str)
            df["npi"] = df["npi"].astype(str)
            df = df.merge(nppes, on="npi", how="left")
            df["is_sole_proprietor"] = df["is_sole_proprietor"].fillna(0).astype(int)
            df["months_since_enumeration"] = df["months_since_enumeration"].fillna(
                df["months_since_enumeration"].median(),
            ).astype(float)
            n_enriched = int((df["months_since_enumeration"] > 0).sum())
            print(f"  [features] NPPES join: enriched {n_enriched:,} providers")
        except Exception as e:
            print(f"  [features] NPPES join failed ({e}) — using defaults")
            df["is_sole_proprietor"] = 0
            df["months_since_enumeration"] = 120.0   # 10y default
    else:
        df["is_sole_proprietor"] = 0
        df["months_since_enumeration"] = 120.0       # 10y default (long-established)

    # new_provider_high_volume: ≤24 months since NPI enumeration AND
    # billing ≥5× peer median.  New providers running at 5× their specialty's
    # median in their first 2 years is one of the strongest fraud signals
    # OIG enforcement actions point to.
    pv_col = df.get("payment_vs_peer", pd.Series(1.0, index=df.index))
    df["new_provider_high_volume"] = (
        (df["months_since_enumeration"] <= 24) & (pv_col >= 5.0)
    ).astype(int)

    # --- Per-patient cost vs. peer (size-invariant anomaly signal) ---
    # payment_vs_peer compares *totals*, penalising large chains for being large.
    # ppb_vs_peer compares cost *per beneficiary*, which is scale-independent and
    # is the correct fraud signal for volume-intensive specialties (labs, DME, ambulance).
    df["ppb_vs_peer"] = _safe_div(df["payment_per_bene"], df["peer_median_ppb"], fill=1.0)

    # --- Per-service payment vs. peer (upcoding signal) ---
    # Providers who receive higher Medicare payment *per claim* than specialty peers
    # may be upcoding (billing a higher complexity/cost code than warranted).
    # Distinct from ppb_vs_peer: captures per-claim inflation, not per-patient.
    df["payment_per_service"] = _safe_div(df["total_payment"], df["total_services"])
    df["peer_median_pps"] = _peer_median_with_fallback(
        df, "payment_per_service", "_peer_group_fine", "_peer_group_coarse"
    )
    df["payment_per_service_vs_peer"] = _safe_div(
        df["payment_per_service"], df["peer_median_pps"], fill=1.0
    ).clip(upper=50.0)

    # --- Geographic fraud hotspot flag ---
    # OIG enforcement actions and CMS Zone Program Integrity Contractor data
    # consistently show elevated fraud rates in these states.
    df["hotspot_state"] = (
        df["state"].str.strip().str.upper().isin(_FRAUD_HOTSPOT_STATES)
    ).astype(float)

    # --- Year-over-year payment change (2021→2022) ---
    # A sudden billing spike — large increase vs. the provider's specialty peers —
    # is one of the strongest temporal fraud signals. Computed only when prior-year
    # aggregated data is available (features_2021 or providers_aggregated_2021).
    # Providers with no prior-year record get 0.0 (no observed change).
    yoy_path = PROC_DIR / "providers_aggregated_2021.parquet"
    if yoy_path.exists() and out_path != yoy_path:
        try:
            prior = pd.read_parquet(yoy_path, columns=["npi", "total_payment"])
            prior = prior.rename(columns={"total_payment": "_pay_2021"})
            prior["npi"] = prior["npi"].astype(str)
            df = df.merge(prior, on="npi", how="left")

            # Raw YoY % change (clamped to avoid log issues)
            df["_yoy_raw"] = _safe_div(
                df["total_payment"] - df["_pay_2021"], df["_pay_2021"].clip(lower=1)
            ).clip(-1.0, 10.0).fillna(0.0)

            # Peer-median YoY change — removes specialty-wide trends
            df["_peer_yoy_median"] = df.groupby("_peer_group_coarse")["_yoy_raw"].transform("median")
            df["yoy_payment_change"] = (df["_yoy_raw"] - df["_peer_yoy_median"]).fillna(0.0).clip(-1.0, 5.0)
            df = df.drop(columns=["_pay_2021", "_yoy_raw", "_peer_yoy_median"], errors="ignore")

            n_yoy = int(df["yoy_payment_change"].ne(0).sum())
            print(f"  [features] YoY payment change: computed for {n_yoy:,} providers "
                  f"with 2021 records")
        except Exception as e:
            print(f"  [features] YoY computation failed ({e}) — filling 0.0")
            df["yoy_payment_change"] = 0.0
    else:
        df["yoy_payment_change"] = 0.0

    # --- Clip extreme ratios (higher ceiling to preserve true outliers) ---
    for col in ["payment_vs_peer", "services_vs_peer", "benes_vs_peer", "ppb_vs_peer",
                "payment_per_service_vs_peer"]:
        df[col] = df[col].clip(upper=100.0)  # was 50.0 — raised to preserve extreme outliers

    df["payment_zscore"] = df["payment_zscore"].clip(-10, 50)

    # --- Drop internal helper columns before saving ---
    df = df.drop(columns=["_peer_group_fine", "_peer_group_coarse"], errors="ignore")

    print(f"  [features] Done. Feature columns: {FEATURE_COLS}")
    if out_path is None:
        out_path = PROC_DIR / "features.parquet"
    out_path = Path(out_path)
    df.to_parquet(out_path, index=False)
    print(f"  [features] Saved → {out_path}")
    return df


if __name__ == "__main__":
    build()
