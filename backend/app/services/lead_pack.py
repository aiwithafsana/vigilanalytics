"""
lead_pack.py — Generate a jurisdiction-specific "lead pack" deliverable.

What is a lead pack?
--------------------
The artifact you bring to a 30-minute meeting with a state AG, MFCU
director, or FCA-firm partner.  Given a jurisdiction filter (state,
specialty), it produces:

  1. A ranked list of the top N providers by combined risk signal
  2. Per-provider context: score drivers, financial impact, exclusion
     timing, address-cluster membership, fraud flag categories
  3. A summary page with the headline number ("25 high-risk hospice
     providers in California")
  4. A PDF deliverable suitable for leaving behind after a meeting

The composite ranking weighs:
  - 50% provider risk score
  - 25% excess billing dollar amount (normalized to peer-median)
  - 15% distinct fraud-flag category count
  - 10% address-cluster membership (binary)

Caller flow
-----------
    # Backend service:
    leads = await generate_lead_pack(db, state="CA", specialty="hospice", limit=25)
    pdf_bytes = render_lead_pack_pdf(leads)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class LeadProvider:
    """One ranked provider in a lead pack."""
    npi:                  str
    name:                 str
    specialty:            str | None
    state:                str | None
    city:                 str | None
    risk_score:           float | None
    total_payment:        float | None
    is_excluded:          bool
    leie_date:            str | None
    leie_reason:          str | None
    # Computed signals
    excess_billing:       float | None
    distinct_flag_count:  int
    flag_types:           list[str] = field(default_factory=list)
    address_cluster_size: int = 0      # 0 if not in a cluster
    rank_score:           float = 0.0   # composite ranking, 0–100

    def to_dict(self) -> dict:
        return {
            "npi":                  self.npi,
            "name":                 self.name,
            "specialty":            self.specialty,
            "state":                self.state,
            "city":                 self.city,
            "risk_score":           self.risk_score,
            "total_payment":        self.total_payment,
            "is_excluded":          self.is_excluded,
            "leie_date":            self.leie_date,
            "leie_reason":          self.leie_reason,
            "excess_billing":       self.excess_billing,
            "distinct_flag_count":  self.distinct_flag_count,
            "flag_types":           self.flag_types,
            "address_cluster_size": self.address_cluster_size,
            "rank_score":           self.rank_score,
        }


@dataclass
class LeadPack:
    """A complete jurisdiction lead pack ready to render."""
    state:         str | None
    specialty:     str | None
    generated_at:  str
    total_in_jurisdiction: int           # before any score filter
    total_high_risk:       int           # ≥70
    leie_count:            int
    address_cluster_count: int            # how many of the leads are in clusters
    excess_billing_sum:    float          # sum of excess_billing across leads
    leads:                 list[LeadProvider] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "state":                 self.state,
            "specialty":             self.specialty,
            "generated_at":          self.generated_at,
            "total_in_jurisdiction": self.total_in_jurisdiction,
            "total_high_risk":       self.total_high_risk,
            "leie_count":            self.leie_count,
            "address_cluster_count": self.address_cluster_count,
            "excess_billing_sum":    self.excess_billing_sum,
            "leads":                 [lead.to_dict() for lead in self.leads],
        }


def _provider_excess_billing(
    total_payment:        float | None,
    payment_per_bene:     float | None,
    total_beneficiaries:  int | None,
    peer_median_ppb:      float | None,
    peer_median_payment:  float | None,
    peer_median_benes:    float | None,
) -> float | None:
    """
    Coarse excess-billing estimate ("but for peer median per-patient rate").
    Mirrors backend/app/services/financial_impact.py so a single provider's
    number is consistent across the UI, the PDF, and the lead pack.
    """
    if total_payment is None or peer_median_ppb is None or total_beneficiaries is None:
        # Fall back: derive peer_ppb from peer payment / peer benes
        if (peer_median_payment is not None and peer_median_benes
                and peer_median_benes > 0
                and total_payment is not None
                and total_beneficiaries is not None):
            peer_ppb = peer_median_payment / peer_median_benes
        else:
            return None
    else:
        peer_ppb = peer_median_ppb

    if peer_ppb is None or total_beneficiaries is None or total_payment is None:
        return None

    expected = peer_ppb * total_beneficiaries
    return max(0.0, total_payment - expected)


def _rank_score(
    risk_score:           float | None,
    excess_billing:       float | None,
    distinct_flag_count:  int,
    address_cluster_size: int,
    max_excess_in_pack:   float,
) -> float:
    """
    Composite 0–100 score for ranking leads in the pack.

    Weights chosen so a provider with score 90 and 8 distinct flag types
    ranks above a provider with score 95 but only 2 flag types — because
    breadth-of-signal is a stronger fraud indicator than score alone.
    """
    rs = (risk_score or 0) / 100.0     # 0..1

    # Excess billing scaled by the max in this pack so it's in 0..1
    eb_norm = (excess_billing or 0) / max(max_excess_in_pack, 1.0)
    eb_norm = min(eb_norm, 1.0)

    # Distinct flag categories — saturating at 5 (the realistic ceiling)
    flags_norm = min(distinct_flag_count, 5) / 5.0

    # Cluster membership is binary signal (in/not in)
    cluster_signal = 1.0 if address_cluster_size >= 3 else 0.0

    composite = (
        0.50 * rs
        + 0.25 * eb_norm
        + 0.15 * flags_norm
        + 0.10 * cluster_signal
    )
    return round(composite * 100.0, 1)


async def generate_lead_pack(
    db: AsyncSession,
    *,
    state:     str | None = None,
    specialty: str | None = None,
    limit:     int        = 25,
    min_score: float      = 70.0,
) -> LeadPack:
    """
    Build a ranked lead pack for the given jurisdiction filter.

    Returns a `LeadPack` with up to `limit` providers ranked by composite
    rank_score, plus jurisdiction-level summary stats for the cover page.

    Filter philosophy:
      - state/specialty narrow the population
      - min_score acts as a floor — we never put a low-risk provider
        in a "lead" pack, even if no high-risk providers exist
      - LEIE-excluded providers are always included, regardless of score
        (per-claim FCA exposure trumps statistical scoring)
    """
    # Compose the population filter
    where_parts: list[str] = []
    bind: dict = {}
    if state:
        where_parts.append("p.state = :state")
        bind["state"] = state.upper()
    if specialty:
        where_parts.append("p.specialty ILIKE :specialty")
        bind["specialty"] = f"%{specialty}%"
    where_pop = " AND ".join(where_parts) if where_parts else "TRUE"

    # ── Jurisdiction-level summary stats ──────────────────────────────────────
    summary_row = (await db.execute(text(f"""
        SELECT
            COUNT(*)                                              AS total,
            COUNT(*) FILTER (WHERE p.risk_score >= 70)            AS high_risk,
            COUNT(*) FILTER (WHERE p.is_excluded = TRUE)          AS leie
        FROM providers p
        WHERE {where_pop}
    """), bind)).one()

    # ── Pull the candidate leads ──────────────────────────────────────────────
    # We over-fetch by 2× to allow Python-side composite re-ranking without
    # missing the top tail.
    bind_leads = dict(bind)
    bind_leads["min_score"] = min_score
    bind_leads["fetch_n"]   = limit * 3

    leads_sql = text(f"""
        SELECT
            p.npi,
            COALESCE(NULLIF(TRIM(COALESCE(p.name_first || ' ', '') || COALESCE(p.name_last, '')), ''), p.npi)
                                          AS name,
            p.specialty, p.state, p.city,
            p.risk_score, p.total_payment,
            p.is_excluded, p.leie_date, p.leie_reason,
            p.payment_per_bene, p.total_beneficiaries,
            p.peer_median_ppb, p.peer_median_payment, p.peer_median_benes,
            p.address_normalized,
            (
                SELECT COUNT(DISTINCT ff.flag_type)
                FROM fraud_flags ff
                WHERE ff.npi = p.npi AND COALESCE(ff.is_active, TRUE)
            )                              AS distinct_flag_count,
            (
                SELECT ARRAY_AGG(DISTINCT ff.flag_type)
                FROM fraud_flags ff
                WHERE ff.npi = p.npi AND COALESCE(ff.is_active, TRUE)
            )                              AS flag_types
        FROM providers p
        WHERE {where_pop}
          AND (p.risk_score >= :min_score OR p.is_excluded = TRUE)
        ORDER BY p.risk_score DESC NULLS LAST
        LIMIT :fetch_n
    """)
    raw_leads = (await db.execute(leads_sql, bind_leads)).mappings().all()

    # ── Compute address-cluster size per candidate ────────────────────────────
    # Cheaper to do once for the whole set than to query per-provider.
    addrs = [r["address_normalized"] for r in raw_leads if r["address_normalized"]]
    cluster_sizes: dict[str, int] = {}
    if addrs:
        cluster_rows = (await db.execute(text("""
            SELECT address_normalized, COUNT(*) AS sz
            FROM providers
            WHERE address_normalized = ANY(:addrs)
            GROUP BY address_normalized
        """), {"addrs": list(set(addrs))})).all()
        cluster_sizes = {row[0]: int(row[1]) for row in cluster_rows}

    # ── Build LeadProvider instances ──────────────────────────────────────────
    candidates: list[LeadProvider] = []
    for r in raw_leads:
        excess = _provider_excess_billing(
            total_payment=        float(r["total_payment"]) if r["total_payment"] is not None else None,
            payment_per_bene=     float(r["payment_per_bene"]) if r["payment_per_bene"] is not None else None,
            total_beneficiaries=  int(r["total_beneficiaries"]) if r["total_beneficiaries"] is not None else None,
            peer_median_ppb=      float(r["peer_median_ppb"]) if r["peer_median_ppb"] is not None else None,
            peer_median_payment=  float(r["peer_median_payment"]) if r["peer_median_payment"] is not None else None,
            peer_median_benes=    float(r["peer_median_benes"]) if r["peer_median_benes"] is not None else None,
        )
        cluster_size = cluster_sizes.get(r["address_normalized"] or "", 0)
        candidates.append(LeadProvider(
            npi=                  r["npi"],
            name=                 r["name"],
            specialty=            r["specialty"],
            state=                r["state"],
            city=                 r["city"],
            risk_score=           float(r["risk_score"]) if r["risk_score"] is not None else None,
            total_payment=        float(r["total_payment"]) if r["total_payment"] is not None else None,
            is_excluded=          bool(r["is_excluded"]),
            leie_date=            r["leie_date"],
            leie_reason=          r["leie_reason"],
            excess_billing=       excess,
            distinct_flag_count=  int(r["distinct_flag_count"] or 0),
            flag_types=           list(r["flag_types"] or []),
            address_cluster_size= cluster_size,
        ))

    # ── Rank by composite score ───────────────────────────────────────────────
    max_excess = max((c.excess_billing or 0) for c in candidates) if candidates else 0
    for c in candidates:
        c.rank_score = _rank_score(
            risk_score=           c.risk_score,
            excess_billing=       c.excess_billing,
            distinct_flag_count=  c.distinct_flag_count,
            address_cluster_size= c.address_cluster_size,
            max_excess_in_pack=   max_excess,
        )

    candidates.sort(key=lambda c: c.rank_score, reverse=True)
    leads = candidates[:limit]

    cluster_count = sum(1 for c in leads if c.address_cluster_size >= 3)
    excess_sum    = sum((c.excess_billing or 0) for c in leads)

    return LeadPack(
        state=                 state,
        specialty=             specialty,
        generated_at=          datetime.now(timezone.utc).isoformat(),
        total_in_jurisdiction= int(summary_row.total),
        total_high_risk=       int(summary_row.high_risk),
        leie_count=            int(summary_row.leie),
        address_cluster_count= cluster_count,
        excess_billing_sum=    excess_sum,
        leads=                 leads,
    )
