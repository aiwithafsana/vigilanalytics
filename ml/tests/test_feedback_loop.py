"""
test_feedback_loop.py — Investigator feedback loop into training.

Verifies the moat-building mechanic: case outcomes (substantiated/cleared)
become training labels that compound over time, making the model better
the more it's used.

The integration test here doesn't actually retrain the model — that's slow
and depends on real CMS data.  Instead it tests the LABEL-ASSIGNMENT
logic: given a synthetic set of case outcomes, do the right NPIs end up
flagged as is_investigator_positive vs is_cleared, with the right weights?
"""
from unittest.mock import patch

import pandas as pd
import pytest

from pipeline import train


# ── _load_investigator_labels behavior ────────────────────────────────────────

def test_load_returns_empty_when_no_db():
    """No DB → graceful empty sets, not a crash."""
    with patch.dict("os.environ", {"DATABASE_URL": ""}, clear=False):
        pos, neg = train._load_investigator_labels()
    assert pos == set()
    assert neg == set()


def test_load_returns_empty_when_db_unreachable(monkeypatch):
    """DB connection failure → empty sets logged, no exception propagates."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://nope:nope@localhost:1/nope")

    # Mock psycopg2.connect to raise so we don't depend on a real DB
    import psycopg2
    def _fail_connect(*a, **kw):
        raise psycopg2.OperationalError("not connected")
    monkeypatch.setattr(psycopg2, "connect", _fail_connect)

    pos, neg = train._load_investigator_labels()
    assert pos == set()
    assert neg == set()


# ── Label-source bookkeeping in _load_data ────────────────────────────────────

@pytest.fixture
def mock_features(tmp_path, monkeypatch):
    """
    Build a tiny synthetic features.parquet + leie.parquet so _load_data
    can run without the full CMS pipeline.  10 providers, NPIs '1000000001'
    through '1000000010'.
    """
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    monkeypatch.setattr(train, "PROC_DIR", proc_dir)

    npis = [f"100000000{i}" for i in range(1, 11)]
    features = pd.DataFrame({
        "npi": npis,
        "payment_vs_peer":    [1.0] * 10,
        "services_vs_peer":   [1.0] * 10,
        "benes_vs_peer":      [1.0] * 10,
        "ppb_vs_peer":        [1.0] * 10,
        "payment_per_service_vs_peer": [1.0] * 10,
        "payment_zscore":     [0.0] * 10,
        "services_per_bene":  [10.0] * 10,
        "payment_per_bene_norm": [5.0] * 10,
        "total_payment_log":  [10.0] * 10,
        "total_services_log": [5.0] * 10,
        "num_procedure_types_norm": [2.0] * 10,
        "billing_entropy":    [0.8] * 10,
        "em_upcoding_ratio":  [0.1] * 10,
        "hotspot_state":      [0.0] * 10,
        "yoy_payment_change": [0.0] * 10,
        "is_opt_out":         [0.0] * 10,
        "months_enrolled":    [12.0] * 10,
        "is_sole_proprietor":          [0.0] * 10,
        "new_provider_high_volume":    [0.0] * 10,
    })
    features.to_parquet(proc_dir / "features.parquet", index=False)

    # NPI 1000000003 is on LEIE (pre-2023 → training positive).
    # NPI 1000000007 is on LEIE (2024 → holdout positive).
    leie = pd.DataFrame({
        "npi":      ["1000000003", "1000000007"],
        "excldate": ["20200101",   "20240101"],
    })
    leie.to_parquet(proc_dir / "leie.parquet", index=False)
    return npis, proc_dir


def test_investigator_positives_get_added_to_train_set(mock_features, monkeypatch):
    """
    Provider 1000000005 has a 'substantiated' case → should appear in
    train_npis, get is_investigator_positive=1, and is_train_positive=1.
    """
    npis, _ = mock_features
    monkeypatch.setattr(
        train, "_load_investigator_labels",
        lambda: ({"1000000005"}, set()),    # substantiated, cleared
    )
    df, X, y = train._load_data()

    sub_row  = df[df["npi"] == "1000000005"].iloc[0]
    leie_row = df[df["npi"] == "1000000003"].iloc[0]
    plain    = df[df["npi"] == "1000000001"].iloc[0]

    assert int(sub_row["is_train_positive"])       == 1
    assert int(sub_row["is_investigator_positive"]) == 1
    assert int(sub_row["is_cleared"])              == 0

    # LEIE-positive is still a hard positive but NOT marked investigator
    assert int(leie_row["is_train_positive"])       == 1
    assert int(leie_row["is_investigator_positive"]) == 0

    # Bystander provider untouched
    assert int(plain["is_train_positive"]) == 0


def test_investigator_cleared_marked_as_negative(mock_features, monkeypatch):
    """
    Provider 1000000008 has 'unsubstantiated' → is_cleared=1, NOT a positive.
    """
    npis, _ = mock_features
    monkeypatch.setattr(
        train, "_load_investigator_labels",
        lambda: (set(), {"1000000008"}),
    )
    df, X, y = train._load_data()

    cleared = df[df["npi"] == "1000000008"].iloc[0]
    assert int(cleared["is_cleared"])           == 1
    assert int(cleared["is_train_positive"])    == 0


def test_holdout_providers_excluded_from_feedback(mock_features, monkeypatch):
    """
    Critical: a holdout-tagged provider must NEVER be added to training
    even if a case marks them substantiated.  Otherwise validation is
    contaminated and the model looks better than it is.

    The defensive mechanism is two-fold:
      1. _load_investigator_labels result has holdout NPIs subtracted before
         being added to train_npis
      2. _load_data() drops ALL rows where is_holdout_positive=1 from the
         returned training DataFrame (so they can't leak as negatives either)
    """
    npis, _ = mock_features
    # NPI 1000000007 is in the LEIE holdout (excldate 20240101).  Even if
    # an investigator substantiates them, training MUST exclude them.
    monkeypatch.setattr(
        train, "_load_investigator_labels",
        lambda: ({"1000000007"}, set()),
    )
    df, X, y = train._load_data()

    # The holdout provider must be entirely absent from the training DataFrame
    assert "1000000007" not in set(df["npi"].astype(str)), (
        "Holdout provider must be dropped from training, even when an "
        "investigator substantiates the case (validation integrity protection)"
    )
    # Sanity check: the dataframe IS smaller by exactly the holdout count
    assert len(df) == 9, "9 training rows expected after holdout provider dropped"


def test_substantiated_provider_in_leie_keeps_higher_priority(mock_features, monkeypatch):
    """
    A provider on BOTH LEIE pre-2023 AND substantiated should still be
    counted as investigator-positive (so it gets the 7.0 weight, not 5.0).
    """
    npis, _ = mock_features
    monkeypatch.setattr(
        train, "_load_investigator_labels",
        lambda: ({"1000000003"}, set()),    # also on LEIE
    )
    df, X, y = train._load_data()

    both = df[df["npi"] == "1000000003"].iloc[0]
    assert int(both["is_train_positive"])       == 1
    assert int(both["is_investigator_positive"]) == 1  # gets the higher weight
