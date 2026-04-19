"""
score.py — Score every provider using the three trained models.

Composite risk score (0–100):
  - XGBoost probability     → 40% weight
  - Isolation Forest score  → 30% weight
  - Autoencoder recon error → 30% weight

All three component scores are normalised to [0, 1] via percentile rank
before blending, so no single model can dominate due to scale differences.

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
    "xgboost":    0.40,
    "iso_forest": 0.30,
    "autoencoder": 0.30,
}


def _percentile_rank(arr: np.ndarray) -> np.ndarray:
    """Convert raw scores to percentile ranks in [0, 1]."""
    from scipy.stats import rankdata
    return rankdata(arr, method="average") / len(arr)


def _percentile_rank_numpy(arr: np.ndarray) -> np.ndarray:
    """Percentile rank without scipy."""
    n = len(arr)
    order = arr.argsort()
    ranks = np.empty(n)
    ranks[order] = np.arange(1, n + 1)
    return ranks / n


def score_isolation_forest(X: np.ndarray) -> np.ndarray:
    artifact = joblib.load(MODELS_DIR / "isolation_forest.joblib")
    # decision_function returns negative scores for anomalies; flip so higher = more anomalous
    raw = -artifact.decision_function(X)
    return _percentile_rank_numpy(raw)


def score_xgboost(X: np.ndarray) -> np.ndarray:
    artifact = joblib.load(MODELS_DIR / "xgboost.joblib")
    X_scaled = artifact["scaler"].transform(X)
    probs = artifact["model"].predict_proba(X_scaled)[:, 1]
    return _percentile_rank_numpy(probs)


def score_autoencoder(X: np.ndarray) -> np.ndarray:
    artifact = joblib.load(MODELS_DIR / "autoencoder.joblib")
    X_scaled = artifact["scaler"].transform(X)
    X_recon  = artifact["model"].predict(X_scaled)
    recon_error = np.mean((X_scaled - X_recon) ** 2, axis=1)
    return _percentile_rank_numpy(recon_error)


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
        WEIGHTS["xgboost"]    * xgb_score +
        WEIGHTS["iso_forest"] * iso_score +
        WEIGHTS["autoencoder"] * ae_score
    )

    df = df.copy()
    df["isolation_score"]   = np.round(iso_score, 4)
    df["xgboost_score"]     = np.round(xgb_score, 4)
    df["autoencoder_score"] = np.round(ae_score,  4)
    df["risk_score"]        = np.round(composite * 100, 2)   # 0–100

    out_path = PROC_DIR / "scored.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  [score] risk_score — min={df['risk_score'].min():.1f}  "
          f"median={df['risk_score'].median():.1f}  "
          f"max={df['risk_score'].max():.1f}")
    print(f"  [score] Saved → {out_path}")
    return df


if __name__ == "__main__":
    run()
