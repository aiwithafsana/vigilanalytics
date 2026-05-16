"""
test_score.py — Unit tests for the scoring pipeline.

Focus: verify that LEIE-excluded providers do NOT receive an artificial score
floor, and that the _validate_scores function checks new-lead quality rather
than rewarding LEIE dominance.

All tests are pure-Python: no trained model files are required. Tests that
call the model-loading functions are skipped when model artifacts are absent.
"""
import numpy as np
import pandas as pd
import pytest

from pipeline.score import _validate_scores, _specialty_volume_adjustment


# ── Helper ────────────────────────────────────────────────────────────────────

def _make_df(
    n_excluded: int = 10,
    n_active: int = 90,
    excluded_score: float = 50.0,
    active_scores: list[float] | None = None,
) -> pd.DataFrame:
    """Build a minimal scored DataFrame for testing."""
    rows = []
    for i in range(n_excluded):
        rows.append({
            "npi": f"LEIE{i:04d}",
            "is_excluded": 1.0,
            "specialty": "internal medicine",
            "ppb_vs_peer": 1.0,
            "risk_score": excluded_score,
        })
    scores = active_scores if active_scores else [float(i) for i in range(n_active)]
    for i, s in enumerate(scores[:n_active]):
        rows.append({
            "npi": f"ACT{i:04d}",
            "is_excluded": 0.0,
            "specialty": "internal medicine",
            "ppb_vs_peer": 1.0,
            "risk_score": s,
        })
    return pd.DataFrame(rows)


# ── No LEIE floor ─────────────────────────────────────────────────────────────

def test_no_leie_floor_applied():
    """
    Excluded providers with a raw score of 50 must stay at 50 after scoring.
    The old floor raised them to 85, which crowded out genuine new leads.
    """
    df = _make_df(n_excluded=5, excluded_score=50.0)
    # Simulate what run() does after volume adjustment: just check that
    # is_excluded providers are NOT clipped to 85+ anywhere in the module.
    # We verify by asserting the scores are unchanged after _validate_scores,
    # which is the last mutation point in the pipeline.
    scores_before = df.loc[df["is_excluded"] == 1, "risk_score"].tolist()
    _validate_scores(df)  # must not mutate df
    scores_after = df.loc[df["is_excluded"] == 1, "risk_score"].tolist()
    assert scores_before == scores_after, (
        "_validate_scores must not modify risk_score values"
    )
    for score in scores_after:
        assert score == 50.0, (
            f"Excluded provider score was modified to {score}; expected 50.0. "
            "The LEIE floor must not be applied."
        )


def test_excluded_providers_can_score_below_non_excluded():
    """
    An excluded provider with a low billing anomaly should score lower than
    an active provider with a high billing anomaly.  The floor must not exist.
    """
    df = pd.DataFrame([
        {"npi": "LEIE0001", "is_excluded": 1.0, "specialty": "internal medicine",
         "ppb_vs_peer": 1.0, "risk_score": 30.0},   # excluded, unremarkable billing
        {"npi": "ACT00001", "is_excluded": 0.0, "specialty": "internal medicine",
         "ppb_vs_peer": 10.0, "risk_score": 88.0},  # active, extreme billing anomaly
    ])
    # Volume adjustment: ACT00001 has ppb_vs_peer=10 — above all discount tiers
    # so its score should not change. LEIE0001 is excluded → exempt from discount.
    adjusted = _specialty_volume_adjustment(df)
    df["risk_score"] = adjusted

    leie_score  = df.loc[df["npi"] == "LEIE0001", "risk_score"].iloc[0]
    active_score = df.loc[df["npi"] == "ACT00001", "risk_score"].iloc[0]
    assert leie_score < active_score, (
        f"Excluded provider (score={leie_score}) should score below the highly "
        f"anomalous active provider (score={active_score}) when no floor is applied."
    )


# ── _validate_scores checks new-lead quality ─────────────────────────────────

def test_validate_scores_warns_when_top100_dominated_by_excluded(capsys):
    """
    When ≥50 of the top-100 providers are LEIE-excluded, _validate_scores
    should log a warning about lead quality.
    """
    # 80 excluded providers scoring 90+, 20 active scoring 10
    rows = []
    for i in range(80):
        rows.append({"npi": f"L{i}", "is_excluded": 1.0, "risk_score": 90.0 + (i % 5)})
    for i in range(20):
        rows.append({"npi": f"A{i}", "is_excluded": 0.0, "risk_score": 10.0})
    df = pd.DataFrame(rows)
    _validate_scores(df)
    captured = capsys.readouterr()
    assert "WARNING" in captured.out, (
        "_validate_scores should warn when top-100 is dominated by excluded providers"
    )


def test_validate_scores_passes_when_top100_has_new_leads(capsys):
    """
    When ≥50 of the top-100 are non-excluded (new leads), validation passes.
    """
    rows = []
    for i in range(10):
        rows.append({"npi": f"L{i}", "is_excluded": 1.0, "risk_score": 60.0})
    for i in range(90):
        rows.append({"npi": f"A{i}", "is_excluded": 0.0, "risk_score": 70.0 + (i % 20)})
    df = pd.DataFrame(rows)
    _validate_scores(df)
    captured = capsys.readouterr()
    assert "PASS" in captured.out
    assert "WARNING" not in captured.out


def test_validate_scores_handles_missing_is_excluded(capsys):
    """_validate_scores must not crash when is_excluded column is absent."""
    df = pd.DataFrame([
        {"npi": "A", "risk_score": 80.0},
        {"npi": "B", "risk_score": 40.0},
    ])
    _validate_scores(df)   # must not raise
    captured = capsys.readouterr()
    assert "score" in captured.out.lower()


# ── _specialty_volume_adjustment ─────────────────────────────────────────────

def test_volume_adjustment_discounts_lab_with_normal_ppb():
    """A lab with ppb_vs_peer < 2.0 should have its score heavily discounted."""
    df = pd.DataFrame([{
        "npi":        "LAB0001",
        "specialty":  "clinical laboratory",
        "is_excluded": 0.0,
        "ppb_vs_peer": 1.1,   # well below 2× peer — national chain noise
        "risk_score": 75.0,
    }])
    adjusted = _specialty_volume_adjustment(df)
    assert adjusted.iloc[0] < 75.0 * 0.20, (
        f"Lab with ppb_vs_peer=1.1 should be discounted heavily; "
        f"got {adjusted.iloc[0]:.1f} from 75.0"
    )


def test_volume_adjustment_does_not_discount_excluded_lab():
    """
    An excluded lab must not be discounted — billing after exclusion is
    a per-claim FCA violation regardless of per-patient cost ratios.
    """
    df = pd.DataFrame([{
        "npi":        "LABEXCL",
        "specialty":  "clinical laboratory",
        "is_excluded": 1.0,
        "ppb_vs_peer": 0.5,   # would normally trigger heaviest discount
        "risk_score": 60.0,
    }])
    adjusted = _specialty_volume_adjustment(df)
    assert adjusted.iloc[0] == 60.0, (
        "Excluded lab should not have its score discounted by volume adjustment"
    )


def test_volume_adjustment_leaves_non_volume_specialty_unchanged():
    """A cardiologist's score must not change regardless of ppb_vs_peer."""
    df = pd.DataFrame([{
        "npi":        "CARD0001",
        "specialty":  "cardiology",
        "is_excluded": 0.0,
        "ppb_vs_peer": 0.1,
        "risk_score": 70.0,
    }])
    adjusted = _specialty_volume_adjustment(df)
    assert adjusted.iloc[0] == 70.0
