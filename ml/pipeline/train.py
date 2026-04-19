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
    model = Pipeline([
        ("scaler", RobustScaler()),
        ("iso",    IsolationForest(
            n_estimators=200,
            contamination=0.05,   # ~5% assumed anomaly rate
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
      - LEIE matches → hard positive (label=1)
      - Top 2% by payment_zscore AND not LEIE → soft positive (label=1, weight=0.5)
      - Rest → negative (label=0)
    """
    print("  [train] XGBoost…")

    labels  = y.copy().astype(np.float32)
    weights = np.ones(len(y), dtype=np.float32)

    # Soft positives: top 2% payment_zscore, not already excluded
    threshold = np.percentile(df["payment_zscore"].fillna(0), 98)
    soft_pos  = (df["payment_zscore"].fillna(0) >= threshold) & (y == 0)
    labels[soft_pos]  = 1
    weights[soft_pos] = 0.4   # lower weight than hard positives
    weights[y == 1]   = 2.0   # up-weight known LEIE matches

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

def train_autoencoder(X: np.ndarray) -> Pipeline:
    """
    Train an MLP to reconstruct normal provider feature vectors.
    High reconstruction error → anomalous.
    Train only on providers in the bottom 90% of payment_zscore
    (i.e. likely-normal providers) so the model learns normal patterns.
    """
    print("  [train] Autoencoder (MLP)…")

    scaler  = RobustScaler()
    X_scaled = scaler.fit_transform(X)

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
    ae.fit(X_scaled, X_scaled)

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
    train_autoencoder(X)

    print("  [train] All models saved.")


if __name__ == "__main__":
    run()
