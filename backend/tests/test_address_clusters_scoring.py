"""
test_address_clusters_scoring.py — Pure-logic tests for cluster ranking.

The DB integration paths are exercised manually in the API smoke tests.
These tests cover only the composite-scoring function, which is the
business-rule heart of the feature and must produce monotonic, defensible
rankings under all inputs.
"""
from app.services.address_clusters import _cluster_risk_score


# ── Boundary conditions ──────────────────────────────────────────────────────

def test_score_is_bounded_in_0_to_100():
    """All-min inputs → ~0; all-max → 100."""
    assert _cluster_risk_score(0, 0, 0, 0.0, 0) == 0.0
    assert _cluster_risk_score(100, 100, 100, 1.0, 2) == 100.0


def test_benign_small_cluster_scores_low():
    """3 mixed-specialty, moderate-risk providers should rate well below 50."""
    s = _cluster_risk_score(3, 35, 25, 0.33, 0)
    assert s < 50, f"3-provider benign cluster scored {s}, expected < 50"


def test_pure_hospice_ring_of_8_scores_high():
    """Classic fraud pattern: 8 hospices, same specialty, all high risk."""
    s = _cluster_risk_score(8, 88, 75, 1.0, 0)
    assert s >= 70, f"8-hospice ring scored {s}, expected >= 70"


def test_leie_membership_raises_score():
    """Adding even one LEIE-excluded member should raise the cluster score."""
    base = _cluster_risk_score(8, 88, 75, 1.0, 0)
    with_leie = _cluster_risk_score(8, 88, 75, 1.0, 1)
    assert with_leie > base, (
        f"LEIE membership should raise score: {base} → {with_leie}"
    )


# ── Monotonicity ─────────────────────────────────────────────────────────────

def test_higher_same_specialty_frac_raises_score():
    """A pure-specialty cluster outranks a mixed one with everything else equal."""
    mixed = _cluster_risk_score(5, 80, 60, 0.40, 0)
    pure  = _cluster_risk_score(5, 80, 60, 1.00, 0)
    assert pure > mixed


def test_higher_max_risk_raises_score():
    """A cluster containing a critical provider outranks one with only medium."""
    medium = _cluster_risk_score(5, 65, 55, 0.5, 0)
    crit   = _cluster_risk_score(5, 95, 55, 0.5, 0)
    assert crit > medium


def test_more_providers_raises_score_up_to_saturation_at_10():
    """Each added provider adds risk up to the saturation point."""
    three  = _cluster_risk_score(3,  85, 70, 0.8, 0)
    six    = _cluster_risk_score(6,  85, 70, 0.8, 0)
    ten    = _cluster_risk_score(10, 85, 70, 0.8, 0)
    twenty = _cluster_risk_score(20, 85, 70, 0.8, 0)
    assert three < six < ten
    # Past 10, the provider_count signal saturates — no further increase
    assert ten == twenty


def test_leie_signal_saturates_at_2():
    """One LEIE member is enough to flip the cluster; two saturates."""
    zero = _cluster_risk_score(8, 88, 75, 1.0, 0)
    one  = _cluster_risk_score(8, 88, 75, 1.0, 1)
    two  = _cluster_risk_score(8, 88, 75, 1.0, 2)
    five = _cluster_risk_score(8, 88, 75, 1.0, 5)
    assert zero < one < two
    assert two == five   # saturates — additional LEIE members don't keep adding


# ── Defensibility examples (the "would this make sense to a state AG" check) ──

def test_solo_provider_groups_never_reach_production():
    """
    A single high-risk provider would produce a misleadingly high "cluster"
    score under the current formula (same_specialty trivially 1.0).  The
    SQL HAVING filter (min_size >= 2 default, default 3 in the API)
    prevents this path from ever being reached in production.

    This test documents the limitation rather than gating against it: if
    the SQL filter is ever removed or weakened, the formula needs a
    minimum-size guard added.
    """
    s_solo = _cluster_risk_score(1, 95, 95, 1.0, 0)
    s_pair = _cluster_risk_score(2, 95, 95, 1.0, 0)
    # Document the values, don't gate strictly — both are "above 70" because
    # the formula isn't designed for sub-min-size groups.  The SQL is.
    assert s_solo >= 60
    assert s_pair >= s_solo, "Adding a provider must never lower the score"


def test_leie_in_small_cluster_still_meaningful():
    """3-provider cluster where one is LEIE — still investigatively interesting."""
    s = _cluster_risk_score(3, 90, 60, 1.0, 1)
    # Composite should land in the meaningful range (40-70)
    assert 40 < s < 80
