"""
network.py — Referral network graph API.

GET /api/network/{npi}        — ego-network (1-hop) centered on a provider
GET /api/network/{npi}/2hop   — 2-hop neighborhood (can be large, capped at 150 nodes)
GET /api/network/search       — find providers by name/NPI to seed graph
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, or_, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.auth import get_current_user
from app.models import Provider, ReferralEdge

router = APIRouter()


def _provider_node(p: Provider, is_center: bool = False) -> dict:
    return {
        "npi": p.npi,
        "name": f"{p.name_first or ''} {p.name_last or ''}".strip() or p.name_last or p.npi,
        "specialty": p.specialty or "Unknown",
        "state": p.state or "",
        "risk_score": float(p.risk_score) if p.risk_score is not None else 0.0,
        "is_excluded": bool(p.is_excluded),
        "total_payment": float(p.total_payment) if p.total_payment is not None else 0.0,
        "flag_count": int(p.flag_count) if p.flag_count is not None else 0,
        "is_center": is_center,
    }


def _edge_dict(e: ReferralEdge) -> dict:
    return {
        "id": e.id,
        "source": e.source_npi,
        "target": e.target_npi,
        "referral_count": e.referral_count or 0,
        "shared_patients": e.shared_patients or 0,
        "total_payment": float(e.total_payment) if e.total_payment else 0.0,
        "referral_percentage": float(e.referral_percentage) if e.referral_percentage else 0.0,
        "is_suspicious": bool(e.is_suspicious),
    }


@router.get("/search")
async def search_network_providers(
    q: str = Query(..., min_length=2),
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_current_user),
):
    """Full-text search for providers to seed the graph from.
    Prioritises providers that have referral edges (i.e. appear in the network).
    """
    term = f"%{q}%"

    # Sub-select: does this NPI appear in referral_edges?
    has_edges_sub = (
        select(ReferralEdge.source_npi)
        .where(ReferralEdge.source_npi == Provider.npi)
        .exists()
    )

    result = await db.execute(
        select(Provider)
        .where(
            or_(
                Provider.npi.ilike(term),
                Provider.name_last.ilike(term),
                Provider.name_first.ilike(term),
            )
        )
        .order_by(
            has_edges_sub.desc(),            # providers with edges first
            Provider.risk_score.desc().nullslast(),
        )
        .limit(20)
    )
    providers = result.scalars().all()
    return [_provider_node(p) for p in providers]


@router.get("/{npi}")
async def get_provider_network(
    npi: str,
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_current_user),
):
    """
    1-hop ego network centered on {npi}.
    Returns nodes (center + all direct neighbors) and edges between them.
    """
    # Validate center provider exists
    center_result = await db.execute(select(Provider).where(Provider.npi == npi))
    center = center_result.scalar_one_or_none()
    if not center:
        raise HTTPException(status_code=404, detail="Provider not found")

    # Fetch edges touching this NPI — suspicious first, then by volume
    edges_result = await db.execute(
        select(ReferralEdge)
        .where(or_(ReferralEdge.source_npi == npi, ReferralEdge.target_npi == npi))
        .order_by(
            ReferralEdge.is_suspicious.desc(),
            ReferralEdge.shared_patients.desc().nullslast(),
        )
        .limit(100)
    )
    edges = edges_result.scalars().all()

    # Collect neighbor NPIs
    neighbor_npis = set()
    for e in edges:
        if e.source_npi != npi:
            neighbor_npis.add(e.source_npi)
        if e.target_npi != npi:
            neighbor_npis.add(e.target_npi)

    # Fetch neighbor providers
    nodes_map: dict[str, dict] = {npi: _provider_node(center, is_center=True)}
    if neighbor_npis:
        neighbors_result = await db.execute(
            select(Provider).where(Provider.npi.in_(neighbor_npis))
        )
        for p in neighbors_result.scalars().all():
            nodes_map[p.npi] = _provider_node(p)

    # Also include edges between neighbors (to show intra-cluster connections)
    # Hard cap to prevent large graph denial-of-service
    intra_edges_result = await db.execute(
        select(ReferralEdge).where(
            and_(
                ReferralEdge.source_npi.in_(neighbor_npis),
                ReferralEdge.target_npi.in_(neighbor_npis),
            )
        ).limit(150)
    )
    intra_edges = intra_edges_result.scalars().all()

    all_edges = list(edges) + list(intra_edges)

    return {
        "center_npi": npi,
        "nodes": list(nodes_map.values()),
        "edges": [_edge_dict(e) for e in all_edges],
        "stats": {
            "total_nodes": len(nodes_map),
            "total_edges": len(all_edges),
            "suspicious_edges": sum(1 for e in all_edges if e.is_suspicious),
        },
    }


@router.get("/{npi}/2hop")
async def get_provider_network_2hop(
    npi: str,
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_current_user),
    max_nodes: int = Query(default=100, le=150),
):
    """
    2-hop neighborhood. Capped at max_nodes for performance.
    Prioritises highest-risk neighbors at each hop.
    """
    center_result = await db.execute(select(Provider).where(Provider.npi == npi))
    center = center_result.scalar_one_or_none()
    if not center:
        raise HTTPException(status_code=404, detail="Provider not found")

    # Hop 1 — suspicious first, then by shared patient volume
    hop1_result = await db.execute(
        select(ReferralEdge).where(
            or_(ReferralEdge.source_npi == npi, ReferralEdge.target_npi == npi)
        ).order_by(
            ReferralEdge.is_suspicious.desc(),
            ReferralEdge.shared_patients.desc().nullslast(),
        ).limit(150)
    )
    hop1_edges = hop1_result.scalars().all()
    hop1_npis = set()
    for e in hop1_edges:
        hop1_npis.add(e.source_npi if e.source_npi != npi else e.target_npi)

    # Hop 2 — edges from hop1 providers (excluding center)
    hop2_edges_result = await db.execute(
        select(ReferralEdge).where(
            and_(
                or_(
                    ReferralEdge.source_npi.in_(hop1_npis),
                    ReferralEdge.target_npi.in_(hop1_npis),
                ),
                ReferralEdge.source_npi != npi,
                ReferralEdge.target_npi != npi,
            )
        ).limit(500)
    )
    hop2_edges = hop2_edges_result.scalars().all()
    hop2_npis = set()
    for e in hop2_edges:
        hop2_npis.add(e.source_npi)
        hop2_npis.add(e.target_npi)

    all_npis = ({npi} | hop1_npis | hop2_npis)

    # Fetch all providers, sorted by risk desc, cap at max_nodes
    providers_result = await db.execute(
        select(Provider)
        .where(Provider.npi.in_(all_npis))
        .order_by(Provider.risk_score.desc().nullslast())
        .limit(max_nodes)
    )
    providers = providers_result.scalars().all()
    kept_npis = {p.npi for p in providers}

    nodes_map = {
        p.npi: _provider_node(p, is_center=(p.npi == npi))
        for p in providers
    }

    # Filter edges to only kept nodes
    all_edges_raw = list(hop1_edges) + list(hop2_edges)
    filtered_edges = [
        e for e in all_edges_raw
        if e.source_npi in kept_npis and e.target_npi in kept_npis
    ]
    # Deduplicate
    seen = set()
    unique_edges = []
    for e in filtered_edges:
        if e.id not in seen:
            seen.add(e.id)
            unique_edges.append(e)

    return {
        "center_npi": npi,
        "nodes": list(nodes_map.values()),
        "edges": [_edge_dict(e) for e in unique_edges],
        "stats": {
            "total_nodes": len(nodes_map),
            "total_edges": len(unique_edges),
            "suspicious_edges": sum(1 for e in unique_edges if e.is_suspicious),
            "hop1_count": len(hop1_npis & kept_npis),
            "hop2_count": len((hop2_npis - hop1_npis - {npi}) & kept_npis),
        },
    }
