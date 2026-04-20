"""
train.py — Train three anomaly models on the feature matrix.

Models
------
1. Isolation Forest   — unsupervised; outputs anomaly score ∈ (-1, 0]
2. XGBoost            — semi-supervised; LEIE matches = positive labels,
                        high-percentile non-LEIE providers = soft positives
3. Autoencoder        — MLP reconstruction error as anomaly signal

Each model is saved to ml/models/ as a joblib artifact.
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


def _load_data() -> tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    features_path = PROC_DIR / "features.parquet"
    leie_path     = PROC_DIR / "leie.parquet"

    df   = pd.read_parquet(features_path)
    leie = pd.read_parquet(leie_path)

    leie_npis = set(leie["npi"].dropna().unique())
    df["is_excluded"] = df["npi"].isin(leie_npis).astype(int)

    X = df[FEATURE_COLS].fillna(0).values.astype(np.float32)
    y = df["is_excluded"].values

    return df, X, y


# ── Isolation Forest ─────────────────────────────────────────────────────────

def train_isolation_forest(X: np.ndarray) -> Pipeline:
    print("  [train] Isolation Forest…")
    # contamination=0.02 (2%) — was 0.05 (5%) which was too aggressive.
    # CMS estimates ~1–3% of Medicare spending is fraudulent; 5% generates
    # far too many anomaly positives and inflates composite risk scores.
    model = Pipeline([
        ("scaler", RobustScaler()),
        ("iso",    IsolationForest(
            n_estimators=200,
            contamination=0.02,   # ~2% assumed anomaly rate
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
    Semi-supervised approach:
      - LEIE matches → hard positive (label=1, weight=2.0)
      - Providers extreme on BOTH payment_zscore AND payment_vs_peer → soft positive
        (label=1, weight=0.3)
      - Rest → negative (label=0)

    Soft positive criterion requires TWO independent signals simultaneously:
      - Top 2% payment_zscore (extreme statistical outlier within specialty×state), AND
      - ≥10× peer median billing (payment_vs_peer ≥ 10)
    Payment_zscore alone marks legitimate high-volume specialists (academic medical
    centers, rural sole practitioners, procedure-heavy specialists).  Requiring both
    signals greatly reduces false positive labeling while still capturing the clearest
    non-LEIE anomalies.
    """
    print("  [train] XGBoost…")

    labels  = y.copy().astype(np.float32)
    weights = np.ones(len(y), dtype=np.float32)

    # Soft positives: must be extreme on TWO independent dimensions, not just one
    zscore_col  = df["payment_zscore"].fillna(0)
    pv_col      = df["payment_vs_peer"].fillna(1)
    threshold_z = np.percentile(zscore_col, 98)
    threshold_pv = 10.0   # billing 10× peer median

    soft_pos = (
        (zscore_col  >= threshold_z)
        & (pv_col    >= threshold_pv)
        & (y == 0)
    )
    n_soft = int(soft_pos.sum())
    print(f"  [train] Soft positives: {n_soft:,} "
          f"(top-2% zscore AND ≥10× payment_vs_peer, not LEIE)")

    labels[soft_pos]  = 1
    weights[soft_pos] = 0.3   # more skeptical than before (was 0.4)
    weights[y == 1]   = 2.0   # hard positives up-weighted

    # Class imbalance
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
    scaler = RobustScaler()
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

    IMPORTANT: the scaler is fit on ALL providers so that anomalous providers
    are still scaled consistently at inference time.  But the MLP itself is
    trained only on providers in the bottom 90% of payment_zscore so that the
    model learns what *normal* billing looks like.  When an anomalous provider
    is passed through at inference, the model will reconstruct it poorly —
    producing the high reconstruction error that signals an anomaly.

    Training on all providers (the previous bug) let the model learn anomalous
    patterns too, reducing reconstruction error for outliers and suppressing
    the anomaly signal we rely on for scoring.
    """
    print("  [train] Autoencoder (MLP)…")

    # Fit scaler on full dataset — ensures consistent scaling at inference
    scaler   = RobustScaler()
    X_scaled = scaler.fit_transform(X)

    # Filter to likely-normal providers for MLP training
    zscore   = df["payment_zscore"].fillna(0).values
    normal_mask = zscore <= np.percentile(zscore, 90)
    X_normal = X_scaled[normal_mask]
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
    ae.fit(X_normal, X_normal)   # train on normal-only; infer on full dataset

    path = MODELS_DIR / "autoencoder.joblib"
    joblib.dump({"model": ae, "scaler": scaler}, path)
    print(f"  [train] Saved → {path}")
    return ae


def run():
    print("\n=== TRAIN ===")
    df, X, y = _load_data()
    print(f"  Dataset: {len(df):,} providers, {y.sum():,} LEIE matches")

    train_isolation_forest(X)
    train_xgboost(X, y, df)
    train_autoencoder(X, df)

    print("  [train] All models saved.")


if __name__ == "__main__":
    run()
