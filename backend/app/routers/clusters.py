"""
clusters.py — Provider-clustering endpoints.

Currently surfaces only address-based clusters (shell-entity / phantom-billing
pattern).  Future endpoints will add referral-network clusters and address-
plus-name-similarity fuzzy matches.

  GET /api/clusters/address?state=CA&specialty=hospice
      List address clusters matching the filter, ranked by composite risk.

  GET /api/clusters/address/by-provider/{npi}
      Return the cluster the given provider belongs to, or 404 if they're
      not in any qualifying cluster.

State-access enforcement: applied at the filter level — investigators with
state_access=['CA'] can only query state='CA' clusters.
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.database import get_db
from app.models import User
from app.services.address_clusters import (
    cluster_for_provider,
    list_address_clusters,
)

router = APIRouter()


@router.get("/address")
async def get_address_clusters(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    state:     str | None = Query(None, min_length=2, max_length=2),
    specialty: str | None = Query(None, min_length=2, max_length=80),
    min_size:  int        = Query(3, ge=2, le=20),
    limit:     int        = Query(25, ge=1, le=100),
):
    """
    List address clusters matching the filter.  Returns a list of
    AddressCluster dicts ranked by cluster_risk_score descending, plus
    pipeline-health metadata in the `meta` field.

    Response shape:
        {
          "meta":     { ... addressed_provider_count, data_ready, ... },
          "clusters": [ AddressCluster, ... ]
        }
    """
    # State-access enforcement
    allowed_states = current_user.state_access or []
    if allowed_states:
        if state is None:
            # User must specify a state they're allowed to access
            state = allowed_states[0]
        elif state.upper() not in {s.upper() for s in allowed_states}:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Access denied — state {state} not in your authorised "
                    f"states ({', '.join(allowed_states)})"
                ),
            )

    clusters, meta = await list_address_clusters(
        db,
        state=state,
        specialty=specialty,
        min_size=min_size,
        limit=limit,
    )
    return {
        "meta":     meta,
        "clusters": [c.to_dict() for c in clusters],
    }


@router.get("/address/by-provider/{npi}")
async def get_cluster_for_provider(
    npi: str,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Return the address cluster the given provider belongs to, or
    {"cluster": null} if they're not co-located with anyone we know of.

    Surfaced on the provider detail page as the "X other providers at this
    address" callout.
    """
    cluster = await cluster_for_provider(db, npi)
    return {"cluster": cluster.to_dict() if cluster else None}
