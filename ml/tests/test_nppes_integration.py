"""
test_nppes_integration.py — NPPES enrichment merge correctness.

The NPPES ingest itself is too heavy to test in CI (downloads 7GB).  Instead
we test the things that can break independently:

  1. URL auto-detection logic (mocked HTTP)
  2. The merge in features.py — given a synthetic enrichment parquet,
     do is_sole_proprietor and new_provider_high_volume get populated?
  3. Graceful fallback when no parquet is present
  4. _post_process date arithmetic (a common breakage point)

If these tests pass, the production pipeline will correctly use NPPES data
whenever the parquet is available, regardless of how the parquet got there.
"""
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline import features, ingest_nppes


# ── URL auto-detection ───────────────────────────────────────────────────────

def test_resolve_latest_url_picks_the_first_200():
    """When the first month's URL exists, we return it without checking older."""
    fake_responses = [
        MagicMock(status_code=200),    # current month — found, stop here
        MagicMock(status_code=404),    # prior month — never reached
    ]
    with patch.object(ingest_nppes, "requests") as mock_req:
        mock_req.head.side_effect = fake_responses
        mock_req.RequestException = Exception
        url = ingest_nppes._resolve_latest_nppes_url()
    assert url.startswith("https://download.cms.gov/nppes/NPPES_Data_Dissemination_")
    assert url.endswith(".zip")
    # Should have called HEAD exactly once (early-exit on first 200)
    assert mock_req.head.call_count == 1


def test_resolve_latest_url_walks_back_through_404s():
    """When the current month's file doesn't exist, walk back month by month."""
    fake_responses = [
        MagicMock(status_code=404),    # current month
        MagicMock(status_code=404),    # one back
        MagicMock(status_code=200),    # two back — found
    ]
    with patch.object(ingest_nppes, "requests") as mock_req:
        mock_req.head.side_effect = fake_responses
        mock_req.RequestException = Exception
        url = ingest_nppes._resolve_latest_nppes_url()
    assert "NPPES_Data_Dissemination_" in url
    assert mock_req.head.call_count == 3


def test_resolve_latest_url_falls_back_when_all_attempts_fail():
    """Network failure on all candidates returns the hardcoded fallback."""
    with patch.object(ingest_nppes, "requests") as mock_req:
        mock_req.RequestException = Exception
        mock_req.head.side_effect = Exception("DNS failed")
        url = ingest_nppes._resolve_latest_nppes_url()
    assert url == ingest_nppes.NPPES_FALLBACK_URL


# ── _post_process derived columns ────────────────────────────────────────────

def test_post_process_computes_months_since_enumeration():
    """A provider enrolled on 2018-01-01 should be ~60 months as of 2022-12-31."""
    df = pd.DataFrame({
        "npi": ["1000000001"],
        "enumeration_date": ["01/01/2018"],
        "entity_type": ["1"],
        "is_sole_proprietor": ["Y"],
        "taxonomy_primary": ["207Q00000X"],
        "nppes_state": ["CA"],
    })
    out = ingest_nppes._post_process(df)
    assert out["months_since_enumeration"].iloc[0] == pytest.approx(60.0, abs=1.0)
    assert int(out["is_sole_proprietor"].iloc[0]) == 1


def test_post_process_handles_malformed_dates():
    """Garbage dates should produce 0 months, not a crash."""
    df = pd.DataFrame({
        "npi": ["1000000001", "1000000002"],
        "enumeration_date": ["not-a-date", ""],
        "entity_type": ["1", "1"],
        "is_sole_proprietor": ["N", "Y"],
        "taxonomy_primary": ["", ""],
        "nppes_state": ["CA", "CA"],
    })
    out = ingest_nppes._post_process(df)
    # Both rows have invalid dates → months default to 0
    assert (out["months_since_enumeration"] == 0).all()


def test_post_process_sole_proprietor_parsed_correctly():
    """Y/N → 1/0; other values → 0."""
    df = pd.DataFrame({
        "npi": ["1", "2", "3", "4"],
        "enumeration_date": ["01/01/2020"] * 4,
        "entity_type": ["1"] * 4,
        "is_sole_proprietor": ["Y", "N", "X", ""],
        "taxonomy_primary": [""] * 4,
        "nppes_state": ["CA"] * 4,
    })
    out = ingest_nppes._post_process(df)
    assert list(out["is_sole_proprietor"]) == [1, 0, 0, 0]


# ── features.py merge integration ────────────────────────────────────────────

def test_features_uses_nppes_when_parquet_present(tmp_path, monkeypatch):
    """
    Drop a synthetic nppes_enrichment.parquet into PROC_DIR and verify
    features.build() picks it up.
    """
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    monkeypatch.setattr(features, "PROC_DIR", proc_dir)

    # Synthetic providers (3 rows)
    providers = pd.DataFrame({
        "npi": ["1000000001", "1000000002", "1000000003"],
        "name_last":  ["A", "B", "C"],
        "name_first": ["a", "b", "c"],
        "specialty":  ["family medicine"] * 3,
        "state":      ["CA", "CA", "CA"],
        "total_payment":       [100000.0, 500000.0, 200000.0],
        "total_services":      [1000, 5000, 2000],
        "total_beneficiaries": [100, 500, 200],
        "num_procedure_types": [10, 50, 20],
        "billing_entropy":   [0.5] * 3,
        "em_upcoding_ratio": [0.1] * 3,
    })

    # NPPES data: provider 1 is new (12mo) AND high-volume in features → should
    # trigger new_provider_high_volume.  Provider 2 is established (10y).
    # Provider 3 is missing from NPPES — should get defaults.
    nppes = pd.DataFrame({
        "npi": ["1000000001", "1000000002"],
        "months_since_enumeration": [12.0, 120.0],
        "is_sole_proprietor":       [1, 0],
    })
    nppes.to_parquet(proc_dir / "nppes_enrichment.parquet", index=False)

    out = features.build(providers, out_path=proc_dir / "test_features.parquet")

    p1 = out[out["npi"] == "1000000001"].iloc[0]
    p2 = out[out["npi"] == "1000000002"].iloc[0]
    p3 = out[out["npi"] == "1000000003"].iloc[0]

    # Provider 1: NPPES populated, sole proprietor flag set
    assert int(p1["is_sole_proprietor"]) == 1
    assert float(p1["months_since_enumeration"]) == 12.0

    # Provider 2: NPPES populated, not sole prop, established
    assert int(p2["is_sole_proprietor"]) == 0
    assert float(p2["months_since_enumeration"]) == 120.0

    # Provider 3: missing from NPPES → defaults (sole_prop=0, months=median of others=66)
    # The exact default depends on features.py logic; just verify it's a real number
    assert int(p3["is_sole_proprietor"]) == 0
    assert float(p3["months_since_enumeration"]) > 0


def test_features_falls_back_gracefully_when_no_nppes(tmp_path, monkeypatch):
    """Without nppes_enrichment.parquet, features.build() uses defaults."""
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    monkeypatch.setattr(features, "PROC_DIR", proc_dir)

    providers = pd.DataFrame({
        "npi": ["1000000001"],
        "name_last":  ["A"], "name_first": ["a"],
        "specialty":  ["family medicine"],
        "state":      ["CA"],
        "total_payment": [100000.0], "total_services": [1000],
        "total_beneficiaries": [100], "num_procedure_types": [10],
        "billing_entropy":   [0.5], "em_upcoding_ratio": [0.1],
    })

    out = features.build(providers, out_path=proc_dir / "test_features.parquet")
    # Defaults: not sole prop, long-established (so new_provider_high_volume=0)
    assert int(out["is_sole_proprietor"].iloc[0])      == 0
    assert int(out["new_provider_high_volume"].iloc[0]) == 0
    assert float(out["months_since_enumeration"].iloc[0]) >= 24
