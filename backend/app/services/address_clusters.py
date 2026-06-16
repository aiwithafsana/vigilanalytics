"""
address_clusters.py — Surface provider clusters that share a practice address.

Why this exists
---------------
One of the strongest Medicare-fraud patterns is the "shell entity" cluster:
multiple billing entities registered at the same physical address, often
the same person operating under different LLC names to multiply billing
without triggering volume-anomaly detection on any single NPI.  The DOJ
press releases all mention this pattern.

This service groups providers by the normalized practice address
(populated from NPPES via ingest_nppes.py) and surfaces clusters of three
or more — the threshold below which co-location is plausibly benign
(office sublets, multi-physician group practices, etc.).

What we surface vs. what we DON'T
---------------------------------
- We surface: address, member providers, max risk score, combined billing,
  same-specialty fraction, cluster "risk score"
- We do NOT surface: claims about whether the cluster IS a shell scheme.
  That requires human investigation.  Vigil flags the pattern; an
  investigator confirms it.

Data dependencies
-----------------
Requires `providers.address_normalized` to be populated.  Until ingest_nppes
runs with the address-capable COL_MAP, this service returns empty results
and a soft notice — never crashes.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


# Minimum providers at one address to qualify as a "cluster".  Below this,
# co-location is too common (small group practices, office sublets) to be
# investigatively meaningful.  3 is conservative; raise to 5 once we have
# real customer feedback.
MIN_CLUSTER_SIZE = 3

# Cap on members returned per cluster to keep API responses bounded.
MAX_MEMBERS_PER_CLUSTER = 50


@dataclass
class ClusterMember:
    """One provider within an address cluster."""
    npi:          str
    name:         str
    specialty:    str | None
    risk_score:   float | None
    is_excluded:  bool
    total_payment: float | None

    def to_dict(self) -> dict:
        return {
            "npi":           self.npi,
            "name":          self.name,
            "specialty":     self.specialty,
            "risk_score":    self.risk_score,
            "is_excluded":   self.is_excluded,
            "total_payment": self.total_payment,
        }


@dataclass
class AddressCluster:
    """A group of ≥3 providers sharing a normalized practice address."""
    address_normalized:  str
    provider_count:      int
    max_risk_score:      float
    avg_risk_score:      float
    combined_billing:    float
    leie_member_count:   int
    dominant_specialty:  str | None
    same_specialty_frac: float          # 0.0–1.0
    cluster_risk_score:  float          # composite, 0–100
    members:             list[ClusterMember] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "address_normalized":  self.address_normalized,
            "provider_count":      self.provider_count,
            "max_risk_score":      self.max_risk_score,
            "avg_risk_score":      self.avg_risk_score,
            "combined_billing":    self.combined_billing,
            "leie_member_count":   self.leie_member_count,
            "dominant_specialty":  self.dominant_specialty,
            "same_specialty_frac": self.same_specialty_frac,
            "cluster_risk_score":  self.cluster_risk_score,
            "members":             [m.to_dict() for m in self.members],
        }


def _cluster_risk_score(
    provider_count:      int,
    max_risk:            float,
    avg_risk:            float,
    same_specialty_frac: float,
    leie_member_count:   int,
) -> float:
    """
    Composite cluster-risk score on a 0-100 scale.

    Inputs are weighted to reflect the fraud-investigation thinking:
      - same_specialty_frac matters most (a strip-mall with 12 hospices
        is far more interesting than 12 different specialties)
      - max single-provider risk matters (the cluster inherits the
        worst case)
      - LEIE membership is a multiplier — even one excluded provider
        in a cluster turns it into an active FCA case
      - More providers = more red flag

    Composite (raw, 0..1):
        0.35 × same_specialty_frac
      + 0.25 × max_risk / 100
      + 0.15 × avg_risk / 100
      + 0.10 × min(provider_count, 10) / 10
      + 0.15 × min(leie_member_count, 2) / 2

    Scaled to 0..100.
    """
    raw = (
        0.35 * same_specialty_frac
        + 0.25 * (max_risk / 100.0)
        + 0.15 * (avg_risk / 100.0)
        + 0.10 * (min(provider_count, 10) / 10.0)
        + 0.15 * (min(leie_member_count, 2) / 2.0)
    )
    return round(raw * 100.0, 1)


async def list_address_clusters(
    db: AsyncSession,
    *,
    state:     str | None = None,
    specialty: str | None = None,    # ILIKE substring match
    min_size:  int       = MIN_CLUSTER_SIZE,
    limit:     int       = 50,
) -> tuple[list[AddressCluster], dict]:
    """
    Find address clusters matching the filter and return them ranked by
    cluster_risk_score descending.

    Returns:
        (clusters, meta) where meta carries pipeline-health info:
            - addressed_provider_count: how many providers have a
              normalized address (i.e., NPPES enrichment is loaded)
            - data_ready: True iff there's enough addressed data to make
              the result meaningful
    """
    # Sanity check: is address data loaded at all?
    coverage_row = (await db.execute(text(
        "SELECT COUNT(*) FILTER (WHERE address_normalized IS NOT NULL) AS addressed, "
        "       COUNT(*) AS total "
        "FROM providers"
    ))).one()
    addressed, total = coverage_row.addressed, coverage_row.total
    meta = {
        "addressed_provider_count": int(addressed),
        "total_provider_count":     int(total),
        "address_coverage":         round(addressed / max(total, 1), 4),
        "data_ready":               addressed >= 10_000,    # arbitrary floor
    }
    if not meta["data_ready"]:
        return [], meta

    # Build cluster aggregations in a single SQL query.  Filters apply at
    # the member level (provider state / specialty), then we group by
    # address and HAVING-filter to clusters meeting min_size.
    #
    # We build TWO copies of the filter — one for the unaliased reference
    # inside the CTE, and one with table aliases for the outer SELECT —
    # because mixing aliased / unaliased references is a recipe for bugs.
    bind: dict = {"min_size": min_size, "limit": limit}

    inner_parts = ["address_normalized IS NOT NULL"]
    outer_parts = ["p.address_normalized IS NOT NULL"]

    if state:
        inner_parts.append("state = :state")
        outer_parts.append("p.state = :state")
        bind["state"] = state.upper()
    if specialty:
        inner_parts.append("specialty ILIKE :specialty")
        outer_parts.append("p.specialty ILIKE :specialty")
        bind["specialty"] = f"%{specialty}%"

    inner_where = " AND ".join(inner_parts)
    outer_where = " AND ".join(outer_parts)

    # Materialise the candidate clusters first (cheap), then pull members
    # per surviving cluster in a single follow-up query.
    #
    # The CTE pattern is required because Postgres does NOT allow an
    # aggregate function (MODE() WITHIN GROUP) inside a FILTER predicate.
    # We compute the dominant specialty per address in `mode_cte`, then
    # join back to the main GROUP BY to derive same_specialty_frac.
    cluster_sql = text(f"""
        WITH spec_counts AS (
            SELECT address_normalized,
                   LOWER(specialty) AS spec,
                   COUNT(*)         AS spec_count
            FROM providers
            WHERE {inner_where}
            GROUP BY address_normalized, LOWER(specialty)
        ),
        mode_cte AS (
            SELECT address_normalized,
                   (ARRAY_AGG(spec ORDER BY spec_count DESC NULLS LAST))[1] AS dominant_specialty,
                   MAX(spec_count)                                          AS dominant_count
            FROM spec_counts
            GROUP BY address_normalized
        )
        SELECT
            p.address_normalized,
            COUNT(*)                                              AS provider_count,
            COALESCE(MAX(p.risk_score), 0)                        AS max_risk,
            COALESCE(AVG(p.risk_score), 0)                        AS avg_risk,
            COALESCE(SUM(p.total_payment), 0)                     AS combined_billing,
            SUM(CASE WHEN p.is_excluded THEN 1 ELSE 0 END)        AS leie_count,
            m.dominant_specialty,
            (m.dominant_count::float) / NULLIF(COUNT(*), 0)::float AS same_specialty_frac
        FROM providers p
        JOIN mode_cte m USING (address_normalized)
        WHERE {outer_where}
        GROUP BY p.address_normalized, m.dominant_specialty, m.dominant_count
        HAVING COUNT(*) >= :min_size
        ORDER BY COUNT(*) DESC, MAX(p.risk_score) DESC NULLS LAST
        LIMIT :limit
    """)
    raw_rows = (await db.execute(cluster_sql, bind)).mappings().all()
    if not raw_rows:
        return [], meta

    # Build cluster objects; ranking by composite score is computed in
    # Python so it stays cheap and well-tested.
    clusters_by_addr: dict[str, AddressCluster] = {}
    for r in raw_rows:
        cluster_risk = _cluster_risk_score(
            provider_count=     int(r["provider_count"]),
            max_risk=           float(r["max_risk"] or 0),
            avg_risk=           float(r["avg_risk"] or 0),
            same_specialty_frac=float(r["same_specialty_frac"] or 0),
            leie_member_count=  int(r["leie_count"] or 0),
        )
        clusters_by_addr[r["address_normalized"]] = AddressCluster(
            address_normalized=  r["address_normalized"],
            provider_count=      int(r["provider_count"]),
            max_risk_score=      float(r["max_risk"] or 0),
            avg_risk_score=      round(float(r["avg_risk"] or 0), 2),
            combined_billing=    float(r["combined_billing"] or 0),
            leie_member_count=   int(r["leie_count"] or 0),
            dominant_specialty=  r["dominant_specialty"],
            same_specialty_frac= round(float(r["same_specialty_frac"] or 0), 3),
            cluster_risk_score=  cluster_risk,
        )

    # Fetch members for all surviving clusters in one query
    member_sql = text(f"""
        SELECT
            npi,
            COALESCE(NULLIF(TRIM(COALESCE(name_first || ' ', '') || COALESCE(name_last, '')), ''), npi)
                                  AS name,
            specialty,
            risk_score,
            COALESCE(is_excluded, FALSE) AS is_excluded,
            total_payment,
            address_normalized
        FROM providers
        WHERE {inner_where}
          AND address_normalized = ANY(:addrs)
        ORDER BY risk_score DESC NULLS LAST
        LIMIT :total_members
    """)
    addrs = list(clusters_by_addr.keys())
    member_rows = (await db.execute(member_sql, {
        **bind,
        "addrs":         addrs,
        "total_members": len(addrs) * MAX_MEMBERS_PER_CLUSTER,
    })).mappings().all()

    for mr in member_rows:
        cluster = clusters_by_addr.get(mr["address_normalized"])
        if cluster is None or len(cluster.members) >= MAX_MEMBERS_PER_CLUSTER:
            continue
        cluster.members.append(ClusterMember(
            npi=           mr["npi"],
            name=          mr["name"],
            specialty=     mr["specialty"],
            risk_score=    float(mr["risk_score"]) if mr["risk_score"] is not None else None,
            is_excluded=   bool(mr["is_excluded"]),
            total_payment= float(mr["total_payment"]) if mr["total_payment"] is not None else None,
        ))

    # Final sort by cluster risk descending — investigators triage the
    # highest-risk clusters first.
    ranked = sorted(
        clusters_by_addr.values(),
        key=lambda c: c.cluster_risk_score,
        reverse=True,
    )
    return ranked, meta


async def cluster_for_provider(
    db: AsyncSession,
    npi: str,
) -> AddressCluster | None:
    """
    Return the address cluster the given provider is a member of, or None
    if either the provider has no normalized address or no qualifying
    cluster exists at their address.

    Used by the provider detail page to show "5 other providers at this
    address" callout.
    """
    addr_row = (await db.execute(
        text("SELECT address_normalized FROM providers WHERE npi = :npi"),
        {"npi": npi},
    )).one_or_none()
    if addr_row is None or addr_row[0] is None:
        return None
    addr = addr_row[0]

    clusters, _meta = await list_address_clusters(
        db,
        state=None, specialty=None,
        min_size=2,        # show even 2-provider co-location on detail page
        limit=10_000,      # ensure we'd find the cluster if it exists
    )
    for c in clusters:
        if c.address_normalized == addr:
            return c
    return None
