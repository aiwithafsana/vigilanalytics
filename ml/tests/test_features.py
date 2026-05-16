"""
test_features.py — Unit tests for feature engineering correctness.

These are pure-Python tests: no DB, no file I/O.
They run against the live FEATURE_COLS list and the feature-building helpers.
"""
import numpy as np
import pandas as pd
import pytest

from pipeline.features import FEATURE_COLS, build, _safe_div, _zscore_by_group


# ── FEATURE_COLS contract ─────────────────────────────────────────────────────

def test_is_excluded_not_in_feature_cols():
    """
    is_excluded must NOT appear in FEATURE_COLS.

    Including it causes feature leakage: the model would learn "is this person
    already caught?" rather than "does the billing pattern look anomalous?".
    The goal of this system is to surface *new* investigation leads.
    """
    assert "is_excluded" not in FEATURE_COLS, (
        "is_excluded is in FEATURE_COLS — this causes feature leakage. "
        "The model would memorise the LEIE list instead of learning billing anomalies."
    )


def test_feature_cols_are_unique():
    """Each feature name must appear exactly once."""
    assert len(FEATURE_COLS) == len(set(FEATURE_COLS)), (
        f"Duplicate entries in FEATURE_COLS: "
        f"{[c for c in FEATURE_COLS if FEATURE_COLS.count(c) > 1]}"
    )


def test_feature_cols_non_empty():
    """FEATURE_COLS must contain at least the core billing anomaly features."""
    required = {
        "payment_vs_peer", "services_vs_peer", "payment_zscore",
        "billing_entropy", "em_upcoding_ratio",
    }
    missing = required - set(FEATURE_COLS)
    assert not missing, f"Core feature(s) missing from FEATURE_COLS: {missing}"


# ── Helper unit tests ─────────────────────────────────────────────────────────

def test_safe_div_no_divide_by_zero():
    """_safe_div must not produce inf/nan when denominator is zero."""
    a = pd.Series([10.0, 0.0, 5.0])
    b = pd.Series([0.0, 0.0, 2.0])
    result = _safe_div(a, b, fill=0.0)
    assert not result.isnull().any(), "safe_div produced NaN"
    assert not np.isinf(result).any(), "safe_div produced Inf"
    assert result.iloc[0] == 0.0   # 10/0 → fill
    assert result.iloc[2] == 2.5   # 5/2


def test_zscore_by_group_zero_std():
    """Z-score with zero within-group std must not produce NaN/Inf."""
    values = pd.Series([5.0, 5.0, 5.0, 10.0])
    groups = pd.Series(["A", "A", "A", "B"])
    result = _zscore_by_group(values, groups)
    assert not result.isnull().any()
    assert not np.isinf(result).any()
    # Identical values → z-score = 0
    assert result.iloc[0] == 0.0
    assert result.iloc[1] == 0.0


# ── build() integration test (no file I/O) ───────────────────────────────────

def _minimal_providers_df(n: int = 30, has_leie: bool = True) -> pd.DataFrame:
    """Build a minimal in-memory DataFrame with the columns ingest would produce."""
    rng = np.random.default_rng(42)
    df = pd.DataFrame({
        "npi":                 [str(1_000_000_000 + i) for i in range(n)],
        "specialty":           ["internal medicine"] * n,
        "state":               ["CA"] * n,
        "total_payment":       rng.uniform(50_000, 500_000, n),
        "total_services":      rng.integers(200, 5_000, n).astype(float),
        "total_beneficiaries": rng.integers(50, 1_000, n).astype(float),
        "num_procedure_types": rng.integers(1, 20, n).astype(float),
        "billing_entropy":     rng.uniform(0.1, 3.0, n),
        "em_upcoding_ratio":   rng.uniform(0.0, 1.0, n),
        "is_opt_out":          rng.choice([0.0, 1.0], n),
        "months_enrolled":     rng.uniform(1.0, 12.0, n),
    })
    if has_leie:
        # Mark first 5 as LEIE-excluded (as would be produced by the LEIE join)
        df["is_excluded"] = 0.0
        df.loc[:4, "is_excluded"] = 1.0
    return df


def test_build_returns_all_feature_cols():
    """build() must produce a DataFrame containing every entry in FEATURE_COLS."""
    df_in = _minimal_providers_df(n=30)
    df_out = build(providers=df_in)
    missing = [c for c in FEATURE_COLS if c not in df_out.columns]
    assert not missing, f"build() did not produce feature columns: {missing}"


def test_build_no_nans_in_feature_cols():
    """All FEATURE_COLS values must be finite after build()."""
    df_in = _minimal_providers_df(n=30)
    df_out = build(providers=df_in)
    for col in FEATURE_COLS:
        assert not df_out[col].isnull().any(), f"NaN in feature column: {col}"
        assert not np.isinf(df_out[col]).any(), f"Inf in feature column: {col}"


def test_build_preserves_is_excluded_column():
    """
    is_excluded is still present in the output (needed for volume adjustments
    and dashboard display), but must NOT be in FEATURE_COLS.
    """
    df_in = _minimal_providers_df(n=30, has_leie=True)
    df_out = build(providers=df_in)
    assert "is_excluded" in df_out.columns, (
        "is_excluded was dropped from the output — it's needed for downstream reporting"
    )
    assert "is_excluded" not in FEATURE_COLS, (
        "is_excluded sneaked back into FEATURE_COLS"
    )
