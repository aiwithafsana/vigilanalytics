"""
train.py — Train three anomaly models on the feature matrix.

Models
------
1. Isolation Forest   — unsupervised; outputs anomaly score ∈ (-1, 0]
2. XGBoost            — semi-supervised; uses TEMPORAL training labels only
                        (LEIE exclusions before 2023 = hard positives;
                         billing-pattern statistical outliers = soft positives)
3. Autoencoder        — MLP reconstruction error as anomaly signal

Temporal holdout design
-----------------------
The CMS billing data covers 2022.  LEIE exclusions are split at Jan 1 2023:

  Training positives  (hard labels, weight=2.0):
      Providers excluded BEFORE 2023.  These were sanctioned during or before
      the CMS 2022 billing period — their 2022 billing patterns reflect known
      fraud activity.

  Temporal holdout (saved to leie_holdout_npis.parquet for validate.py):
      Providers excluded 2023 or LATER who appear in 2022 CMS data.  They were
      billing in 2022 and caught afterward.  If the model flags them from 2022
      billing patterns ALONE, that is genuine out-of-sample predictive power —
      the only kind that survives Daubert challenge.

Why this matters
----------------
Using ALL current LEIE as training labels creates a circular model:
  - The model learns "who has already been sanctioned", not "who is committing
    fraud right now but hasn't been caught yet".
  - A defense expert destroys it in 60 seconds: "Your model was trained on
    government-confirmed exclusions — it cannot independently detect new fraud."

With temporal holdout:
  - Training uses only pre-2023 known cases.
  - Validation tests whether 2022 billing patterns predicted who the government
    would exclude in 2023-2026.  That is a defensible, non-circular methodology.

NOTE: is_excluded is NOT in FEATURE_COLS.  The model learns billing anomaly
patterns that correlate with eventual exclusion, not "is this person already
on a government list."
"""

import sys
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.ensemble import IsolationForest
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import RobustScaler
from sklearn.pipeline import Pipeline
import xgboost as xgb

DATA_DIR   = Path(__file__).parent.parent / "data"
PROC_DIR   = DATA_DIR / "processed"
MODELS_DIR = Path(__file__).parent.parent / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

from pipeline.features import FEATURE_COLS

# Providers excluded on or after this date form the temporal holdout.
# They billed during the CMS 2022 period and were caught afterward —
# the genuine test of predictive power.
_HOLDOUT_CUTOFF = "20230101"   # YYYYMMDD


def _load_investigator_labels() -> tuple[set[str], set[str]]:
    """
    Pull confirmed-fraud and confirmed-not-fraud labels from the case table.

    Investigator outcomes are *higher quality* than LEIE matches because they
    reflect actual case work — interviews, claim-level review, expert opinion
    — rather than just "was this provider added to a federal exclusion list."
    Surfacing these into training is what turns Vigil from a public-data
    classifier into a system that compounds value with usage.

    Returns
    -------
    (substantiated_npis, cleared_npis)
        Both sets are deduplicated and contain str NPIs.
    """
    import os

    try:
        import psycopg2
    except ImportError:
        # ML container doesn't always have psycopg2; OK to skip feedback
        print("  [train] psycopg2 unavailable — skipping investigator feedback loop")
        return set(), set()

    db_url = os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")
    if not db_url:
        print("  [train] No DATABASE_URL — skipping investigator feedback loop")
        return set(), set()

    try:
        conn = psycopg2.connect(db_url)
    except Exception as e:
        print(f"  [train] DB unavailable ({e}) — skipping investigator feedback loop")
        return set(), set()

    try:
        cur = conn.cursor()
        # Substantiated cases → confirmed fraud labels.  Closed/referred to
        # DOJ or state AG are also strong signals.
        cur.execute("""
            SELECT DISTINCT provider_npi
            FROM cases
            WHERE outcome IN ('substantiated', 'referred_to_doj', 'referred_to_state_ag')
              AND provider_npi IS NOT NULL
        """)
        substantiated = {r[0] for r in cur.fetchall() if r[0]}

        # Unsubstantiated cases → confirmed NOT fraud.  These are valuable as
        # hard negatives — the model should learn NOT to flag these patterns.
        cur.execute("""
            SELECT DISTINCT provider_npi
            FROM cases
            WHERE outcome IN ('unsubstantiated', 'closed_no_action')
              AND provider_npi IS NOT NULL
        """)
        cleared = {r[0] for r in cur.fetchall() if r[0]}
        cur.close()
    finally:
        conn.close()

    print(f"  [train] Investigator feedback: "
          f"{len(substantiated):,} substantiated, {len(cleared):,} cleared")
    return substantiated, cleared


def _load_data() -> tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    """
    Load features and build training labels using a temporal holdout split.

    Multi-year training
    -------------------
    If features_2021.parquet exists, 2021 provider billing patterns are stacked
    with 2022 data for training.  Each year's features are normalised relative
    to that year's peer groups, so the z-scores are comparable.

    Training positives from 2021:  providers who billed in 2021 and were excluded
    before 2023 — including providers excluded in 2021 or 2022 who NEVER appear
    in 2022 billing data.  This is the key payoff: many more confirmed examples.

    Holdout (validation) always uses 2022 providers only, so scored.parquet and
    the validation report remain an honest out-of-sample test on 2022 billing.

    Returns
    -------
    df  : combined training DataFrame (2021 + 2022, or 2022-only)
    X   : feature matrix for training
    y   : binary training labels
    """
    features_path = PROC_DIR / "features.parquet"
    leie_path     = PROC_DIR / "leie.parquet"

    df_2022 = pd.read_parquet(features_path)
    leie    = pd.read_parquet(leie_path)

    # ── Parse exclusion dates (format: YYYYMMDD stored as string) ────────────
    leie["excldate_clean"] = leie["excldate"].fillna("").str.strip()
    leie_dated = leie[
        leie["excldate_clean"].str.match(r"^\d{8}$", na=False)
    ].copy()

    # ── Temporal split (same cutoff for all years) ────────────────────────────
    train_npis   = set(
        leie_dated[leie_dated["excldate_clean"] <  _HOLDOUT_CUTOFF]["npi"]
        .dropna().astype(str).unique()
    )
    holdout_npis = set(
        leie_dated[leie_dated["excldate_clean"] >= _HOLDOUT_CUTOFF]["npi"]
        .dropna().astype(str).unique()
    )

    # ── Investigator feedback loop (the moat) ────────────────────────────────
    # Confirmed-fraud and confirmed-not-fraud labels from case outcomes are
    # added to the training set.  This is what turns the model from a generic
    # public-data scorer into a system that improves with usage.
    feedback_pos, feedback_neg = _load_investigator_labels()
    # Don't add feedback NPIs to training if they're in the temporal holdout —
    # protects validation integrity.
    feedback_pos -= holdout_npis
    feedback_neg -= holdout_npis
    train_npis |= feedback_pos

    # ── Label 2022 data ───────────────────────────────────────────────────────
    df_npi_str = df_2022["npi"].astype(str)
    df_2022["is_train_positive"]   = df_npi_str.isin(train_npis).astype(int)
    df_2022["is_holdout_positive"] = df_npi_str.isin(holdout_npis).astype(int)
    # is_cleared marks providers investigators have explicitly cleared.  Used
    # by train_xgboost to up-weight them as hard negatives.
    df_2022["is_cleared"]          = df_npi_str.isin(feedback_neg).astype(int)
    df_2022["data_year"]           = 2022

    # ── Discover and load all historical year features ───────────────────────
    historical_dfs = []
    historical_years_loaded = []
    for yr in [2021, 2020, 2019, 2018]:
        yr_path = PROC_DIR / f"features_{yr}.parquet"
        if not yr_path.exists():
            continue
        df_yr = pd.read_parquet(yr_path)
        df_yr_npi = df_yr["npi"].astype(str)
        df_yr["is_train_positive"]   = df_yr_npi.isin(train_npis).astype(int)
        df_yr["is_holdout_positive"] = df_yr_npi.isin(holdout_npis).astype(int)
        df_yr["is_cleared"]          = df_yr_npi.isin(feedback_neg).astype(int)
        df_yr["data_year"]           = yr
        historical_dfs.append(df_yr)
        historical_years_loaded.append(yr)
        n_pos = int(df_yr["is_train_positive"].sum())
        print(f"  [train] {yr}: {len(df_yr):,} providers, "
              f"{n_pos:,} training positives, "
              f"{int(df_yr['is_holdout_positive'].sum()):,} holdout providers")

    if historical_dfs:
        df_full = pd.concat([df_2022] + historical_dfs, ignore_index=True)
        print(f"  [train] Multi-year training: 2022 + {historical_years_loaded}")
    else:
        df_full = df_2022
        print(f"  [train] Single-year training: 2022 only "
              f"(no features_<year>.parquet found)")

    # ── CRITICAL: Drop holdout providers from training data entirely ─────────
    # Holdout providers (excluded 2023+) appear in historical years with billing
    # patterns from BEFORE their fraud became known.  Including them as
    # is_train_positive=0 actively teaches the model "these specific fraudsters
    # look normal" — a labeling error that suppresses recall.
    # They are still used for validation via leie_holdout_npis.parquet (2022 only).
    n_holdout_rows_before = int((df_full["is_holdout_positive"] == 1).sum())
    df = df_full[df_full["is_holdout_positive"] == 0].reset_index(drop=True)
    print(f"  [train] Dropped {n_holdout_rows_before:,} holdout-provider rows "
          f"from training (still used for 2022 validation)")

    n_train   = int(df["is_train_positive"].sum())
    n_holdout = int(df_2022["is_holdout_positive"].sum())   # holdout always 2022-only
    print(f"  [train] Temporal split (cutoff {_HOLDOUT_CUTOFF}):")
    print(f"    Hard positives for training  (excl < 2023): {n_train:,}  "
          f"(provider-year rows, across all training years)")
    print(f"    Holdout positives   (2022 data, excl ≥ 2023): {n_holdout:,}  "
          f"(reserved for validation only)")
    print(f"    Training rows (all years, unlabelled bulk):    "
          f"{len(df) - n_train:,}")

    # ── Save holdout NPIs for validate.py (2022-only) ─────────────────────────
    holdout_path = PROC_DIR / "leie_holdout_npis.parquet"
    pd.DataFrame({"npi": sorted(holdout_npis)}).to_parquet(holdout_path, index=False)
    print(f"  [train] Holdout NPIs saved → {holdout_path}")

    # ── Build feature matrix — is_excluded is NOT in FEATURE_COLS ────────────
    assert "is_excluded" not in FEATURE_COLS, (
        "is_excluded must not be in FEATURE_COLS — it causes circular training. "
        "See features.py for explanation."
    )
    X = df[FEATURE_COLS].fillna(0).values.astype(np.float32)
    y = df["is_train_positive"].values   # temporal labels only

    return df, X, y


# ── Isolation Forest ─────────────────────────────────────────────────────────

def train_isolation_forest(X: np.ndarray) -> Pipeline:
    print("  [train] Isolation Forest…")
    model = Pipeline([
        ("scaler", RobustScaler()),
        ("iso",    IsolationForest(
            n_estimators=200,
            contamination=0.02,   # ~2% assumed anomaly rate (CMS estimate)
            max_features=0.8,
            random_state=42,
            n_jobs=-1,
        )),
    ])
    model.fit(X)
    path = MODELS_DIR / "isolation_forest.joblib"
    joblib.dump(model, path)
    print(f"  [train] Saved → {path}")
    return model


# ── XGBoost ──────────────────────────────────────────────────────────────────

def train_xgboost(X: np.ndarray, y: np.ndarray, df: pd.DataFrame) -> xgb.XGBClassifier:
    """
    Semi-supervised training with fraud-specific positive signals.

    Hard positives (weight=5.0):
        Providers in LEIE excluded BEFORE 2023 across all training years.
        Heavily up-weighted because they are the only labels with confirmed
        ground-truth fraud, and we only have ~900 of them out of 5.8M rows.

    Soft positives (weight=0.2, fraud-specific):
        Providers showing patterns specifically associated with billing fraud,
        not just high billing volume:
          - High per-patient cost (ppb_vs_peer ≥ 5×)  — overcharging signal
          - High E&M upcoding ratio (≥ 0.7)            — billing higher-complexity
                                                          codes than warranted
        Either signal alone qualifies, but the two combined further up-weights.
        Volume-only outliers (large legitimate practices) are no longer labelled
        positive — that was the v2.0 noise drowning out the 891 hard positives.

    The holdout set (2023+ LEIE) is excluded from training entirely
    (dropped in _load_data).
    """
    print("  [train] XGBoost…")

    labels  = y.copy().astype(np.float32)
    weights = np.ones(len(y), dtype=np.float32)

    # Fraud-specific soft positives — only fire when fraud-distinct features
    # exceed thresholds, NOT when payment volume alone is high.
    ppb_col   = df["ppb_vs_peer"].fillna(1)
    em_col    = df["em_upcoding_ratio"].fillna(0)
    entropy_col = df["billing_entropy"].fillna(1.0)

    # Signal A: per-patient cost ≥ 5× peer median (size-invariant fraud signal)
    sig_ppb = ppb_col >= 5.0

    # Signal B: ≥70% of E&M visits coded at the highest complexity (level 5)
    # 2022 only — historical years have NaN here, never fire on B
    sig_em = em_col >= 0.7

    # Signal C: highly concentrated billing (low entropy) AND high volume
    # — fraudster billing only the top-reimbursed code over and over
    pv_col = df["payment_vs_peer"].fillna(1)
    sig_concentration = (entropy_col <= 0.4) & (pv_col >= 5.0)

    # Investigator-cleared providers ("we looked at this person, no fraud").
    # These never become soft positives — fire them out of consideration first.
    is_cleared = df.get("is_cleared", pd.Series(0, index=df.index)).fillna(0).astype(bool)

    soft_pos = (
        (sig_ppb | sig_em | sig_concentration)
        & (y == 0)                    # not already a hard training positive
        & (df["is_holdout_positive"] == 0)  # never label holdout as positive
        & (~is_cleared)              # never label cleared providers as positive
    )
    n_soft = int(soft_pos.sum())
    n_hard = int(y.sum())
    n_cleared = int(is_cleared.sum())
    n_ppb  = int(sig_ppb.sum())
    n_em   = int(sig_em.sum())
    n_conc = int(sig_concentration.sum())
    print(f"  [train] Hard positives (LEIE + substantiated cases): {n_hard:,}  weight=5.0")
    print(f"  [train]   ↳ ppb_vs_peer ≥ 5×:                  {n_ppb:,}")
    print(f"  [train]   ↳ E&M upcoding ≥ 70%:                {n_em:,}")
    print(f"  [train]   ↳ low entropy + 5× volume:           {n_conc:,}")
    print(f"  [train] Soft positives (fraud-specific):       {n_soft:,}  weight=0.2")
    print(f"  [train] Hard negatives (investigator-cleared): {n_cleared:,}  weight=3.0")
    print(f"  [train] Negatives (bulk unlabelled):           "
          f"{len(y) - n_hard - n_soft - n_cleared:,}")

    labels[soft_pos]  = 1
    weights[soft_pos] = 0.2
    weights[y == 1]   = 5.0
    # Hard negatives: investigator-cleared providers.  Up-weight at 3.0 so the
    # model learns NOT to flag patterns that experts have already reviewed and
    # cleared.  This is the negative-feedback half of the investigator loop.
    weights[is_cleared.values] = 3.0
    labels[is_cleared.values]  = 0    # explicit, in case any sneaked in as soft pos

    n_pos = labels.sum()
    n_neg = len(labels) - n_pos
    scale_pos_weight = n_neg / max(n_pos, 1)

    model = xgb.XGBClassifier(
        n_estimators=400,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        use_label_encoder=False,
        eval_metric="logloss",
        random_state=42,
        n_jobs=-1,
    )
    scaler   = RobustScaler()
    X_scaled = scaler.fit_transform(X)
    model.fit(X_scaled, labels, sample_weight=weights)

    path = MODELS_DIR / "xgboost.joblib"
    joblib.dump({"model": model, "scaler": scaler}, path)
    print(f"  [train] Saved → {path}")
    return model


# ── Autoencoder (MLP reconstruction) ─────────────────────────────────────────

def train_autoencoder(X: np.ndarray, df: pd.DataFrame) -> Pipeline:
    """
    Train an MLP to reconstruct normal provider feature vectors.
    High reconstruction error → anomalous.

    The scaler is fit on ALL providers for consistent inference-time scaling.
    The MLP is trained only on the bottom 90% of payment_zscore so it learns
    what normal billing looks like — anomalous providers reconstruct poorly,
    producing the high reconstruction error that signals fraud.
    """
    print("  [train] Autoencoder (MLP)…")

    scaler   = RobustScaler()
    X_scaled = scaler.fit_transform(X)

    zscore      = df["payment_zscore"].fillna(0).values
    normal_mask = zscore <= np.percentile(zscore, 90)
    X_normal    = X_scaled[normal_mask]
    print(f"  [train] Autoencoder training on {normal_mask.sum():,} normal providers "
          f"(bottom 90% payment_zscore; excluded {(~normal_mask).sum():,} outliers)")

    ae = MLPRegressor(
        hidden_layer_sizes=(64, 32, 16, 32, 64),
        activation="relu",
        solver="adam",
        max_iter=200,
        random_state=42,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=10,
        tol=1e-4,
    )
    ae.fit(X_normal, X_normal)

    path = MODELS_DIR / "autoencoder.joblib"
    joblib.dump({"model": ae, "scaler": scaler}, path)
    print(f"  [train] Saved → {path}")
    return ae


def run():
    print("\n=== TRAIN ===")
    df, X, y = _load_data()

    train_isolation_forest(X)
    train_xgboost(X, y, df)
    train_autoencoder(X, df)

    print("  [train] All models saved.")


if __name__ == "__main__":
    run()
