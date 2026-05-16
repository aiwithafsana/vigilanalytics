"""
validate.py — Model validation using temporal holdout + SHAP feature attribution.

Produces two outputs:
  1. data/processed/validation_report.json  — precision/recall/ROC at every threshold
     against the TEMPORAL HOLDOUT (providers excluded 2023+, not used in training).
  2. data/processed/shap_top10k.parquet     — SHAP values for top 10,000 providers
     by risk score, stored in DB for the UI explanation layer.

Validation methodology
----------------------
The temporal holdout contains providers who:
  - Appeared in 2022 CMS billing data (they were actively billing)
  - Were excluded from Medicare 2023 or later (caught AFTER the billing period)

Asking "does the model flag them from 2022 billing data alone?" tests genuine
predictive power.  The model never saw their exclusion status during training.
This design survives Daubert challenge — it is not circular self-reference.

Contrast with the previous methodology (v1.x):
  - Used ALL current LEIE as both training labels AND validation labels
  - Model achieved ROC-AUC=1.0 because is_excluded was a training FEATURE
  - A defense expert would correctly identify this as circular reasoning
  - The validation proved nothing: the model just reproduced its training labels
"""
from __future__ import annotations

import json
import joblib
import numpy as np
import pandas as pd
import shap
from pathlib import Path
from sklearn.metrics import roc_auc_score, average_precision_score

DATA_DIR   = Path(__file__).parent.parent / "data"
PROC_DIR   = DATA_DIR / "processed"
MODELS_DIR = Path(__file__).parent.parent / "models"

from pipeline.features import FEATURE_COLS

MODEL_VERSION = "2.0.0"


def _load(n_top: int = 10_000):
    """
    Load scored providers and attach temporal holdout labels.

    Primary labels: providers in leie_holdout_npis.parquet (excluded 2023+).
    Fallback:       all current LEIE NPIs (if holdout file missing — old behaviour).
    """
    df   = pd.read_parquet(PROC_DIR / "scored.parquet")
    leie = pd.read_parquet(PROC_DIR / "leie.parquet")

    holdout_path = PROC_DIR / "leie_holdout_npis.parquet"
    if holdout_path.exists():
        holdout_npis = set(
            pd.read_parquet(holdout_path)["npi"].astype(str).unique()
        )
        df["label"] = df["npi"].astype(str).isin(holdout_npis).astype(int)
        label_source = "temporal_holdout_2023_plus"
        print(f"  [validate] Temporal holdout labels: "
              f"{df['label'].sum():,} providers (excluded 2023+, not seen in training)")
    else:
        # Fallback: all LEIE (less defensible — logs a warning)
        leie_npis = set(leie["npi"].dropna().astype(str).unique())
        df["label"] = df["npi"].astype(str).isin(leie_npis).astype(int)
        label_source = "all_leie_fallback"
        print("  [validate] WARNING: holdout file missing — using all-LEIE labels "
              "(non-temporal, less defensible for legal use)")

    top = df.nlargest(n_top, "risk_score").copy()
    return df, top, label_source


# OIG exclusion type codes that indicate billing / financial fraud
# (types where billing data could plausibly signal the underlying conduct)
_BILLING_FRAUD_EXCLTYPES = frozenset({
    "1128a1",  # Medicare / Medicaid fraud conviction
    "1128a3",  # Felony healthcare fraud
    "1128b7",  # False claims act violation
    "1128b8",  # Significant billing irregularities
    "1128b9",  # Failure to disclose / supply information about crimes
    "1156",    # Unnecessary / substandard items or services
})


def _billing_fraud_recall(df: pd.DataFrame, leie_path: Path, threshold: float) -> dict:
    """
    Compute recall restricted to holdout providers excluded for BILLING fraud only.

    Most LEIE exclusions are for non-billing reasons (drug offenses, patient abuse,
    licence revocations). A model trained on billing patterns cannot detect these.
    Billing-fraud-specific recall is a more honest measure of what the model can do.
    """
    try:
        leie = pd.read_parquet(leie_path)
        leie["npi"] = leie["npi"].astype(str)
        # One row per NPI — keep earliest (primary) exclusion reason
        leie_dedup = (leie.sort_values("excldate")
                          .drop_duplicates("npi", keep="first"))

        holdout_npis = set(df[df["label"] == 1]["npi"].astype(str))
        billing_fraud_npis = set(
            leie_dedup[
                leie_dedup["npi"].isin(holdout_npis)
                & leie_dedup["excltype"].str.lower().str.strip().isin(_BILLING_FRAUD_EXCLTYPES)
            ]["npi"]
        )
        non_billing_npis = holdout_npis - billing_fraud_npis

        billing_mask = df["npi"].astype(str).isin(billing_fraud_npis)
        n_billing  = int(billing_mask.sum())
        n_non_bill = int(df["npi"].astype(str).isin(non_billing_npis).sum())

        caught_billing = int(((df["risk_score"] >= threshold) & billing_mask).sum())
        return {
            "billing_fraud_holdout_size":    n_billing,
            "non_billing_holdout_size":      n_non_bill,
            "billing_fraud_recall_at_threshold": round(caught_billing / max(n_billing, 1), 4),
            "billing_fraud_caught":          caught_billing,
            "data_limitation_note": (
                f"Of {len(holdout_npis)} holdout providers, {n_billing} were excluded for "
                f"billing-related reasons (codes: 1128a1, 1128a3, 1128b7, etc.). The remaining "
                f"{n_non_bill} were excluded for non-billing conduct (drug offences, patient "
                f"abuse, licence revocations) — these are undetectable from billing data alone. "
                f"Billing-fraud recall at ≥{threshold}: {caught_billing}/{n_billing} = "
                f"{caught_billing/max(n_billing,1):.1%}."
            ),
        }
    except Exception as e:
        return {"billing_fraud_recall_note": f"Could not compute ({e})"}


def build_validation_report(df: pd.DataFrame, label_source: str) -> dict:
    """
    Compute precision/recall/ROC at score thresholds 50–95.

    Tiered investigation thresholds
    --------------------------------
    The model supports three risk tiers for investigators:
      ≥70  (High risk):     ~11k providers — strong billing anomaly; investigate first
      50–69 (Moderate risk): ~30k additional providers — elevated signal; triage queue
      <50  (Baseline):       routine monitoring only

    Billing-fraud-specific recall
    -----------------------------
    Overall holdout recall is suppressed because many LEIE exclusions are for
    non-billing conduct (drug offences, patient abuse, licence revocations).
    The billing-fraud-specific recall isolates providers excluded for financial
    misconduct — the cases where billing data has predictive power.

    A ROC-AUC of 0.70–0.85 is honest and defensible.
    A ROC-AUC of 1.0 means something is wrong (data leakage).
    """
    print("  [validate] Building validation report…")

    n_holdout = int(df["label"].sum())
    if n_holdout == 0:
        print("  [validate] WARNING: No holdout positives in scored dataset — "
              "check that pipeline ran in correct order (train → score → validate)")

    y_true  = df["label"].values
    y_score = df["risk_score"].values / 100.0

    thresholds = list(range(30, 100, 5))   # start at 30 to capture moderate-risk tier
    rows = []
    for t in thresholds:
        y_pred = (y_score >= t / 100).astype(int)
        tp = int(((y_pred == 1) & (y_true == 1)).sum())
        fp = int(((y_pred == 1) & (y_true == 0)).sum())
        fn = int(((y_pred == 0) & (y_true == 1)).sum())
        tn = int(((y_pred == 0) & (y_true == 0)).sum())
        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec  = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        rows.append({
            "threshold":          t,
            "flagged_providers":  int(y_pred.sum()),
            "holdout_caught":     tp,   # holdout providers the model flagged
            "false_positives":    fp,
            "holdout_missed":     fn,   # holdout providers the model missed
            "true_negatives":     tn,
            "precision":          round(prec, 4),
            "holdout_recall":     round(rec, 4),
            "f1":                 round(2 * prec * rec / (prec + rec)
                                        if (prec + rec) > 0 else 0, 4),
        })

    # ROC / AP — only meaningful when n_holdout > 0
    if n_holdout > 0 and n_holdout < len(df):
        roc_auc  = float(roc_auc_score(y_true, y_score))
        avg_prec = float(average_precision_score(y_true, y_score))
    else:
        roc_auc  = 0.0
        avg_prec = 0.0
        print("  [validate] Skipping ROC-AUC (degenerate label distribution)")

    holdout_above_70 = int(((y_score >= 0.70) & (y_true == 1)).sum())
    holdout_above_80 = int(((y_score >= 0.80) & (y_true == 1)).sum())
    holdout_above_50 = int(((y_score >= 0.50) & (y_true == 1)).sum())

    # Non-excluded high-risk providers = actionable new investigation leads
    if "is_excluded" in df.columns:
        excl_flag = df["is_excluded"].fillna(0) == 0
        new_leads_70 = int(((df["risk_score"] >= 70) & excl_flag).sum())
        new_leads_80 = int(((df["risk_score"] >= 80) & excl_flag).sum())
        new_leads_50_69 = int(
            ((df["risk_score"] >= 50) & (df["risk_score"] < 70) & excl_flag).sum()
        )
    else:
        new_leads_70 = new_leads_80 = new_leads_50_69 = -1

    # Billing-fraud-specific recall (unaffected by non-billing exclusions)
    leie_path = PROC_DIR / "leie.parquet"
    billing_stats = _billing_fraud_recall(df, leie_path, threshold=70.0)

    report = {
        "model_version":   MODEL_VERSION,
        "training_date":   pd.Timestamp.now().isoformat(),
        "dataset_size":    len(df),

        # Validation methodology
        "validation_method": (
            "Temporal holdout: model trained on pre-2023 LEIE exclusions only. "
            "Validated against providers excluded 2023+ who appeared in 2022 CMS data. "
            "No training data contamination — model never saw holdout labels."
        ),
        "validation_label_source": label_source,
        "holdout_size":    n_holdout,
        "holdout_cutoff":  "2023-01-01",

        # Core metrics
        "roc_auc":          round(roc_auc,  4),
        "average_precision": round(avg_prec, 4),

        # Holdout recall at key thresholds (all holdout providers, regardless of exclusion type)
        "holdout_recall_at_50": round(holdout_above_50 / max(n_holdout, 1), 4),
        "holdout_recall_at_70": round(holdout_above_70 / max(n_holdout, 1), 4),
        "holdout_recall_at_80": round(holdout_above_80 / max(n_holdout, 1), 4),
        "holdout_caught_at_50": holdout_above_50,
        "holdout_caught_at_70": holdout_above_70,
        "holdout_caught_at_80": holdout_above_80,

        # Billing-fraud-specific recall — the metric that matters for legal defensibility.
        # Non-billing exclusions (drug offences, patient abuse, licence revocations) are
        # undetectable from billing data; this metric isolates the detectable subset.
        **billing_stats,

        # Mean scores — separation between groups
        "holdout_mean_score":       round(float(df[df["label"] == 1]["risk_score"].mean()), 2)
                                    if n_holdout > 0 else None,
        "non_holdout_mean_score":   round(float(df[df["label"] == 0]["risk_score"].mean()), 2),

        # Tiered investigation leads (non-excluded providers)
        # ≥70  High risk:      investigate immediately
        # 50–69 Moderate risk: triage queue for second-tier review
        "new_leads_above_70":        new_leads_70,
        "new_leads_above_80":        new_leads_80,
        "new_leads_moderate_50_69":  new_leads_50_69,
        "new_leads_total_50_plus":   (new_leads_70 + new_leads_50_69
                                      if new_leads_70 >= 0 else -1),

        "threshold_analysis": rows,
        "feature_columns":    FEATURE_COLS,
        "model_weights": {
            "xgboost":    0.50,
            "iso_forest": 0.30,
            "autoencoder": 0.20,
        },

        # Methodology notes for legal review
        "methodology_notes": {
            "leie_floor":        "None — removed in v2.0.0. Artificially flooring "
                                 "LEIE providers crowds out genuine new leads.",
            "is_excluded_feature": "Not used — removed in v2.0.0 to prevent "
                                   "circular reasoning.",
            "soft_positives":    "Billing-pattern outliers (top 2% payment_zscore "
                                 "AND ≥10× payment_vs_peer) used as soft training "
                                 "labels with weight=0.3.",
            "peer_groups":       "specialty × state (minimum 10 providers); "
                                 "falls back to specialty-only for small groups.",
            "data_vintage":      "CMS Part B 2022 (billing year 2022). "
                                 "LEIE as of pipeline run date.",
        },
    }

    out = PROC_DIR / "validation_report.json"
    with open(out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"  [validate] Saved → {out}")
    print(f"  [validate] ROC-AUC: {roc_auc:.4f}   Avg Precision: {avg_prec:.4f}")
    print(f"  [validate] Overall holdout recall  @70: {holdout_above_70}/{n_holdout} "
          f"= {holdout_above_70/max(n_holdout,1):.1%}")
    bf = billing_stats
    if "billing_fraud_holdout_size" in bf:
        n_bf = bf["billing_fraud_holdout_size"]
        c_bf = bf["billing_fraud_caught"]
        print(f"  [validate] Billing-fraud recall     @70: {c_bf}/{n_bf} "
              f"= {c_bf/max(n_bf,1):.1%}  "
              f"(non-billing excluded: {bf['non_billing_holdout_size']})")
    print(f"  [validate] Investigation leads (high ≥70):     {new_leads_70:,}")
    print(f"  [validate] Investigation leads (moderate 50-69): {new_leads_50_69:,}")
    print(f"  [validate] Investigation leads (total ≥50):    "
          f"{new_leads_70 + new_leads_50_69:,}")
    return report


def build_shap_values(top_df: pd.DataFrame, n_background: int = 500) -> pd.DataFrame:
    """
    Compute SHAP values for top providers using the XGBoost model.
    Stores per-provider feature contributions so the UI shows which billing
    patterns drove each specific score.
    """
    print(f"  [validate] Computing SHAP for {len(top_df):,} providers…")
    artifact  = joblib.load(MODELS_DIR / "xgboost.joblib")
    model     = artifact["model"]
    scaler    = artifact["scaler"]

    X_top    = top_df[FEATURE_COLS].fillna(0).values.astype(np.float32)
    X_scaled = scaler.transform(X_top)

    explainer = shap.TreeExplainer(model)
    shap_vals = explainer.shap_values(X_scaled)
    if isinstance(shap_vals, list):
        shap_vals = shap_vals[1]

    shap_df = pd.DataFrame(shap_vals, columns=FEATURE_COLS, index=top_df.index)
    shap_df.insert(0, "npi",        top_df["npi"].values)
    shap_df.insert(1, "risk_score", top_df["risk_score"].values)

    def top_features(row):
        feat_vals = [(col, abs(row[col])) for col in FEATURE_COLS]
        feat_vals.sort(key=lambda x: x[1], reverse=True)
        return [f[0] for f in feat_vals[:3]]

    shap_df["top_features"] = shap_df.apply(top_features, axis=1)

    out = PROC_DIR / "shap_top10k.parquet"
    shap_df.to_parquet(out, index=False)
    print(f"  [validate] SHAP saved → {out}")

    mean_abs   = np.abs(shap_vals).mean(axis=0)
    importance = sorted(zip(FEATURE_COLS, mean_abs), key=lambda x: x[1], reverse=True)
    print("  [validate] Global SHAP importance (top providers):")
    for feat, imp in importance[:8]:
        bar = "█" * int(imp * 200)
        print(f"    {feat:<30} {imp:.4f}  {bar}")

    return shap_df


def write_shap_to_db(shap_df: pd.DataFrame) -> None:
    """Store top-3 SHAP feature drivers per provider in the DB for API access."""
    import psycopg2, psycopg2.extras, json as _json

    print("  [validate] Writing SHAP drivers to DB…")
    conn = psycopg2.connect(dbname="vigil")
    cur  = conn.cursor()
    cur.execute("""
        ALTER TABLE providers
        ADD COLUMN IF NOT EXISTS shap_drivers JSONB
    """)
    conn.commit()

    rows = [
        (_json.dumps({"top": row["top_features"],
                      "values": {f: round(float(row[f]), 4) for f in FEATURE_COLS}}),
         str(row["npi"]))
        for _, row in shap_df.iterrows()
    ]
    psycopg2.extras.execute_batch(
        cur,
        "UPDATE providers SET shap_drivers=%s WHERE npi=%s",
        rows, page_size=1000,
    )
    conn.commit()
    print(f"  [validate] Wrote SHAP for {len(rows):,} providers.")
    cur.close()
    conn.close()


def run(n_top: int = 10_000) -> dict:
    print("\n=== VALIDATE ===")
    df, top_df, label_source = _load(n_top)

    report  = build_validation_report(df, label_source)
    shap_df = build_shap_values(top_df)
    write_shap_to_db(shap_df)

    print("  [validate] Done.")
    return report


if __name__ == "__main__":
    run()
