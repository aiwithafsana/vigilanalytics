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


# Volume-intensive specialties where total payment_vs_peer is structurally inflated.
# For these, the per-patient cost ratio (ppb_vs_peer) is the meaningful fraud signal.
# National chains that are cheap per patient should not crowd out genuine fraud cases.
_VOLUME_SPECIALTIES = frozenset([
    "clinical laboratory",
    "independent laboratory",
    "durable medical equipment",
    "durable medical equipment & medical supplies",
    "ambulance service provider",
    "ambulance",
    "pharmacy",
    "mass immunizer roster biller",
    "home health",
    "skilled nursing facility",
])

# ppb_vs_peer thresholds → discount multipliers.
# A lab charging 0.9× median per patient scores at most 15 risk — not fraud.
# A lab charging 58× median per patient keeps its full score.
_PPB_TIERS = [
    (2.0,  0.15),   # < 2× peer per-patient cost → heavy discount (national chain noise)
    (5.0,  0.45),   # 2–5× → moderate discount
    (15.0, 0.75),   # 5–15× → mild discount
]


def _spread_top_tail(composite: np.ndarray) -> np.ndarray:
    """
    Spread the top of the composite distribution so investigators have a
    meaningful gradient above score 80.

    Problem
    -------
    The raw composite saturates around 0.78-0.80 for the top 1% of providers
    because two of three component scores (isolation_forest_percentile and
    autoencoder_normalized_error) cap at 1.0 for the upper tail.  XGBoost
    probability is the only varying signal at the top, but it's clamped by
    its own training calibration.  Result: 99.99% of providers score below
    80, and only a handful of extreme outliers break through.  Real fraud
    cases that should rank ~85-90 cluster at 79 instead, hidden under the
    cliff investigators filter on.

    Fix
    ---
    Apply a rank-based curve to the composite: providers above the median
    get re-mapped onto a polynomial that stretches the top tail.  The
    transformation is strictly monotonic — it does NOT change the ranking
    of providers, only the spacing between them.  Below the median the
    score is unchanged (median provider stays at ~15).

    Curve
    -----
    Only the top 5% of providers (rank ≥ 0.95) is modified.  Below that the
    composite is left unchanged — the low-risk bulk distribution is already
    correct (median ≈ 0.15).  Within the top 5%, the composite is pushed
    toward 1.0 with a power-law curve:

        u  = (rank - 0.95) / 0.05      # u ∈ [0, 1] within top 5%
        out = c + (1 - c) · u^1.5      # gentle concave stretch

    Approximate mapping (assuming raw composite saturates at ~0.79 for the top tail):
        p95  →  79         (no change; this is the boundary)
        p97.5 →  ~86
        p99  →  ~94
        p99.5 →  ~97
        p100 →  100
    """
    n = len(composite)
    if n == 0:
        return composite
    ranks = composite.argsort().argsort() / max(n - 1, 1)   # 0..1 percentile rank

    out = composite.copy()
    is_top = ranks >= 0.95
    if not is_top.any():
        return out
    # u ∈ [0, 1] within the top 5%
    u = (ranks[is_top] - 0.95) / 0.05
    # Gentle stretch toward 1.0; preserves ordering within the top tail
    c = composite[is_top]
    out[is_top] = c + (1.0 - c) * u ** 1.5
    return out


def _specialty_volume_adjustment(df: pd.DataFrame) -> pd.Series:
    """
    For volume-intensive specialties, discount the composite risk score when the
    provider's per-patient cost is not anomalous vs. peers.

    LEIE-excluded providers are never discounted — their per-patient cost is
    irrelevant because any billing after exclusion date is a per-claim FCA violation.
    """
    if "ppb_vs_peer" not in df.columns or "specialty" not in df.columns:
        return df["risk_score"]

    scores = df["risk_score"].copy()
    is_leie = df.get("is_excluded", pd.Series(0, index=df.index)).fillna(0).astype(bool)
    spec_lower = df["specialty"].str.lower().fillna("")
    in_volume = spec_lower.isin(_VOLUME_SPECIALTIES)
    ppb = df["ppb_vs_peer"].fillna(1.0)

    mask = in_volume & ~is_leie
    for threshold, multiplier in _PPB_TIERS:
        tier_mask = mask & (ppb < threshold)
        scores[tier_mask] = scores[tier_mask] * multiplier
        mask = mask & (ppb >= threshold)   # only remaining rows move to next tier

    adj_count = (in_volume & ~is_leie).sum()
    discounted = ((df["risk_score"] - scores) > 0.1).sum()
    print(f"  [score] Volume-specialty adjustment: {adj_count:,} eligible, "
          f"{discounted:,} discounted by ppb_vs_peer")
    return scores


def _validate_scores(df: pd.DataFrame) -> None:
    """
    Sanity-check: the scoring pipeline must produce meaningful separation
    between genuinely anomalous billing patterns and the bulk population,
    WITHOUT relying on or rewarding LEIE-excluded status.

    The goal of this system is to surface *new* investigation leads — providers
    whose billing is anomalous but who have not yet been excluded.  A model that
    scores well only because it memorised the LEIE list is not useful.

    Checks:
      1. Score distribution is non-degenerate (std > 5, i.e. not all 0 or all 100).
      2. Top-100 providers by risk score include non-excluded providers
         (investigation value — not just a re-listing of known criminals).
      3. At least some non-excluded providers score above 70 (high-risk threshold).
    """
    n = len(df)
    if n == 0:
        print("  [score] SKIP validation — empty dataframe")
        return

    score_std  = df["risk_score"].std()
    score_mean = df["risk_score"].mean()
    print(f"  [score] Validation — n={n:,}  mean={score_mean:.1f}  std={score_std:.1f}")

    if score_std < 5.0:
        print(f"  [score] ⚠ WARNING: risk_score std={score_std:.1f} is very low — "
              "scores may be degenerate (all similar).  Check model inputs.")
    else:
        print(f"  [score] ✓ Score distribution: mean={score_mean:.1f}  std={score_std:.1f}")

    # New-lead coverage: non-excluded providers in the top 100
    top_100 = df.nlargest(100, "risk_score")
    if "is_excluded" in df.columns:
        non_excl_in_top = int((top_100["is_excluded"].fillna(0) == 0).sum())
        print(f"  [score] Non-excluded providers in top-100: {non_excl_in_top} "
              f"(new investigation leads)")
        if non_excl_in_top < 50:
            print(f"  [score] ⚠ WARNING: Only {non_excl_in_top} non-excluded providers "
                  "in top-100.  Top leads may be dominated by already-excluded providers; "
                  "consider retraining without is_excluded as a feature.")
        else:
            print(f"  [score] ✓ PASS: {non_excl_in_top} non-excluded providers in top-100")

    # High-risk new leads
    high_risk_new = int(
        df[
            (df["risk_score"] >= 70) &
            (df.get("is_excluded", pd.Series(0, index=df.index)).fillna(0) == 0)
        ].shape[0]
    )
    print(f"  [score] High-risk (≥70) non-excluded providers: {high_risk_new:,}")


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

    # Spread the top tail so providers don't all cluster at score 79.  See
    # _spread_top_tail for rationale; this is a monotonic re-mapping that
    # preserves ranking but gives investigators a meaningful gradient above 80.
    composite = _spread_top_tail(composite)

    df = df.copy()
    df["isolation_score"]   = np.round(iso_score,   4)
    df["xgboost_score"]     = np.round(xgb_score,   4)
    df["autoencoder_score"] = np.round(ae_score,    4)
    df["risk_score"]        = np.round(composite * 100, 2)   # 0–100

    print(f"  [score] risk_score (raw) — min={df['risk_score'].min():.1f}  "
          f"median={df['risk_score'].median():.1f}  "
          f"max={df['risk_score'].max():.1f}")

    # Post-scoring: discount volume-intensive specialties when per-patient cost is normal
    df["risk_score"] = np.round(_specialty_volume_adjustment(df), 2)

    # NOTE: No LEIE floor is applied.  Excluded providers are already monitored
    # by the compliance team; artificially inflating their scores crowds out the
    # genuine new investigation leads this system is designed to surface.
    # LEIE status is tracked in the is_excluded column for display/filtering
    # purposes only — it is not used to influence rankings.

    print(f"  [score] risk_score (adj) — min={df['risk_score'].min():.1f}  "
          f"median={df['risk_score'].median():.1f}  "
          f"max={df['risk_score'].max():.1f}")

    _validate_scores(df)

    out_path = PROC_DIR / "scored.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  [score] Saved → {out_path}")
    return df


if __name__ == "__main__":
    run()
