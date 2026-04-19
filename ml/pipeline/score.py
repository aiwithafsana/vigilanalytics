"""
score.py — Score every provider using the three trained models.

Composite risk score (0–100):
  - XGBoost probability     → 50% weight  (calibrated; raw probabilities preserved)
  - Isolation Forest score  → 30% weight  (percentile rank — no natural probability scale)
  - Autoencoder recon error → 20% weight  (normalized by max error in dataset)

XGBoost is trained with real LEIE labels as hard positives, so its output IS a
calibrated fraud probability. Converting it to a percentile rank would destroy that
calibration and guarantee someone always scores 100 even when no fraud exists.
Isolation Forest and Autoencoder produce relative anomaly scores with no natural
probability interpretation, so percentile rank / max-normalization are appropriate.

Outputs
-------
data/processed/scored.parquet   — features + all scores + composite risk_score
"""

import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.preprocessing import RobustScaler

DATA_DIR   = Path(__file__).parent.parent / "data"
PROC_DIR   = DATA_DIR / "processed"
MODELS_DIR = Path(__file__).parent.parent / "models"

from pipeline.features import FEATURE_COLS

WEIGHTS = {
    "xgboost":     0.50,   # calibrated probability — highest weight
    "iso_forest":  0.30,
    "autoencoder": 0.20,
}


def _percentile_rank_numpy(arr: np.ndarray) -> np.ndarray:
    """Convert raw scores to percentile ranks in [0, 1]."""
    n = len(arr)
    order = arr.argsort()
    ranks = np.empty(n)
    ranks[order] = np.arange(1, n + 1)
    return ranks / n


def score_xgboost(X: np.ndarray) -> np.ndarray:
    """
    Return raw calibrated fraud probabilities from XGBoost.

    Do NOT convert to percentile rank — the model was trained with real LEIE
    labels (hard positives) and sample weights, so predict_proba[:,1] already
    represents a meaningful fraud probability.  Applying percentile rank would
    guarantee someone always scores 1.0 regardless of actual risk level.
    """
    artifact = joblib.load(MODELS_DIR / "xgboost.joblib")
    X_scaled = artifact["scaler"].transform(X)
    probs = artifact["model"].predict_proba(X_scaled)[:, 1]
    return probs.astype(np.float64)


def score_isolation_forest(X: np.ndarray) -> np.ndarray:
    """
    Isolation Forest anomaly score normalized to [0, 1] via percentile rank.

    Percentile rank is appropriate here because IF produces a relative anomaly
    score with no natural probability interpretation.
    """
    artifact = joblib.load(MODELS_DIR / "isolation_forest.joblib")
    # decision_function returns negative scores for anomalies; flip so higher = more anomalous
    raw = -artifact.decision_function(X)
    return _percentile_rank_numpy(raw)


def score_autoencoder(X: np.ndarray) -> np.ndarray:
    """
    Autoencoder reconstruction error normalized by the maximum error in the dataset.

    Dividing by max (rather than percentile ranking) preserves the relative
    magnitude of anomalies — a provider with 2× the reconstruction error of the
    next-highest still scores 2× higher, not tied at the 99th percentile.
    """
    artifact = joblib.load(MODELS_DIR / "autoencoder.joblib")
    X_scaled = artifact["scaler"].transform(X)
    X_recon  = artifact["model"].predict(X_scaled)
    recon_error = np.mean((X_scaled - X_recon) ** 2, axis=1).astype(np.float64)
    max_err = recon_error.max()
    if max_err > 0:
        return recon_error / max_err
    return recon_error


def _validate_scores(df: pd.DataFrame) -> None:
    """
    Sanity-check: known LEIE-excluded providers must score materially higher than
    the general population.  Logs warnings when the model fails this basic check.

    Targets (from audit spec):
      - LEIE mean score ≥ 1.5× general population mean
      - ≥ 30 LEIE providers appear in the top-100 highest-scoring providers
    """
    if "is_excluded" not in df.columns:
        print("  [score] SKIP validation — 'is_excluded' column not present")
        return

    excluded = df[df["is_excluded"] == 1]["risk_score"]
    general  = df[df["is_excluded"] != 1]["risk_score"]

    if len(excluded) == 0:
        print("  [score] WARNING: No LEIE-excluded providers in dataset — "
              "check LEIE enrichment step")
        return

    leie_mean    = excluded.mean()
    general_mean = general.mean()
    ratio        = leie_mean / max(general_mean, 0.01)

    print(f"  [score] Validation — LEIE mean: {leie_mean:.1f}  "
          f"general mean: {general_mean:.1f}  ratio: {ratio:.2f}×")

    if ratio < 1.5:
        print(f"  [score] ⚠ WARNING: LEIE mean score ({leie_mean:.1f}) is "
              f"< 1.5× general mean ({general_mean:.1f}).  "
              "Model may not be discriminating excluded providers.  "
              "Consider retraining with updated LEIE labels.")
    else:
        print(f"  [score] ✓ PASS: LEIE providers score {ratio:.1f}× above general population")

    # Check top-100 coverage
    top_100_idx = set(df.nlargest(100, "risk_score").index)
    leie_in_top = sum(1 for idx in df[df["is_excluded"] == 1].index if idx in top_100_idx)
    leie_n      = len(excluded)
    print(f"  [score] LEIE in top-100: {leie_in_top} / {min(leie_n, 100)} "
          f"(target ≥ 30)")
    if leie_n >= 30 and leie_in_top < 30:
        print(f"  [score] ⚠ WARNING: Only {leie_in_top} LEIE providers in top 100 — "
              "low recall on known positives")


def run(df: pd.DataFrame | None = None) -> pd.DataFrame:
    print("\n=== SCORE ===")

    if df is None:
        df = pd.read_parquet(PROC_DIR / "features.parquet")

    X = df[FEATURE_COLS].fillna(0).values.astype(np.float32)
    print(f"  Scoring {len(df):,} providers…")

    iso_score = score_isolation_forest(X)
    xgb_score = score_xgboost(X)
    ae_score  = score_autoencoder(X)

    composite = (
        WEIGHTS["xgboost"]     * xgb_score +
        WEIGHTS["iso_forest"]  * iso_score +
        WEIGHTS["autoencoder"] * ae_score
    )

    df = df.copy()
    df["isolation_score"]   = np.round(iso_score,   4)
    df["xgboost_score"]     = np.round(xgb_score,   4)
    df["autoencoder_score"] = np.round(ae_score,    4)
    df["risk_score"]        = np.round(composite * 100, 2)   # 0–100

    print(f"  [score] risk_score — min={df['risk_score'].min():.1f}  "
          f"median={df['risk_score'].median():.1f}  "
          f"max={df['risk_score'].max():.1f}")

    _validate_scores(df)

    out_path = PROC_DIR / "scored.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  [score] Saved → {out_path}")
    return df


if __name__ == "__main__":
    run()
