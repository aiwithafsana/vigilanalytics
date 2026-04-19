import asyncio
import csv
import io
from functools import partial
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user, require_role
from app.cache import cache
from app.database import get_db
from app.models import Provider, User, AuditLog, FraudFlag, BillingRecord, ReferralEdge
from app.schemas import (
    ProviderDetail, ProviderListResponse, ProviderSummary,
    FraudFlagOut, BillingRecordOut, ProviderMapPoint,
)
from app.services.evidence import generate_provider_pdf
from app.services.analysis import generate_analysis

router = APIRouter()

# Valid US states + DC only — excludes territories (PR, GU, VI, AS, MP),
# military addresses (AA, AE, AP), and garbage codes (XX, ZZ)
_US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}

# Whitelist of sortable columns — prevents attribute enumeration on the model
_SORTABLE_COLS = {
    "risk_score", "name_last", "specialty", "state",
    "total_payment", "total_services", "total_beneficiaries",
}
_SORT_DIRS = {"asc", "desc"}

# High-volume facility specialties that naturally exhibit patterns the ML model
# misreads as fraud: extreme services-per-patient ratios, high shared-patient counts
# with every referring physician, and large total payments. These specialties need
# entirely different fraud detection logic (duplicate billing, unbundling, test-kit
# fraud) — not the physician billing anomaly model applied here.
_FACILITY_SPECIALTIES: set[str] = {
    # Labs & pathology
    "Clinical Laboratory", "Independent Clinical Laboratory",
    "Clinical Laboratory - Independent", "Pathology",
    "Anatomical & Clinical Pathology", "Clinical Pathology",
    "Anatomic Pathology",

    # Imaging & diagnostics
    "Diagnostic Radiology", "Diagnostic Imaging",
    "Independent Diagnostic Testing Facility",
    "Portable X-ray Supplier", "Nuclear Medicine",
    "Mammography", "Radiation Oncology",  # radiation tech billing, not physician

    # DME & supply
    "Durable Medical Equipment & Medical Supplies",
    "Medical Supply Company with Orthotist",
    "Medical Supply Company with Prosthetist",
    "Pharmacy", "Specialty Pharmacy",
    "Mass Immunizer Roster Biller",

    # Transport & other facility types
    "Ambulance Service Provider",
    "Ambulatory Surgical Center",
    "Home Health Agency",
    "Hospice",
    "Skilled Nursing Facility",
    "Slide Preparation Facility",
}


@router.get("", response_model=ProviderListResponse)
async def list_providers(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    page: int = Query(1, ge=1, le=10_000),
    page_size: int = Query(50, ge=1, le=200),
    q: str | None = Query(None, max_length=100),
    state: str | None = Query(None, max_length=2),
    specialty: str | None = Query(None, max_length=100),
    is_excluded: bool | None = None,
    min_risk: float | None = Query(None, ge=0, le=100),
    max_risk: float | None = Query(None, ge=0, le=100),
    sort_by: str = Query("risk_score", pattern="^[a-z_]+$"),
    sort_dir: str = Query("desc", pattern="^(asc|desc)$"),
    physician_only: bool = Query(
        False,
        description=(
            "When true, excludes labs, imaging centers, DME suppliers, pharmacies, "
            "and other facility-type providers whose naturally high service volumes "
            "produce false-positive fraud signals under the physician billing model."
        ),
    ),
):
    # Enforce sort column whitelist
    if sort_by not in _SORTABLE_COLS:
        sort_by = "risk_score"

    query = select(Provider).where(Provider.state.in_(_US_STATES))

    # Physician-only mode: exclude facility-type specialties
    if physician_only:
        query = query.where(Provider.specialty.notin_(_FACILITY_SPECIALTIES))

    # State access filter
    allowed_states = current_user.state_access or []
    if allowed_states:
        query = query.where(Provider.state.in_(allowed_states))

    # Filters
    if q:
        query = query.where(
            or_(
                Provider.npi.ilike(f"%{q}%"),
                Provider.name_last.ilike(f"%{q}%"),
                Provider.name_first.ilike(f"%{q}%"),
                Provider.specialty.ilike(f"%{q}%"),
                Provider.city.ilike(f"%{q}%"),
            )
        )
    if state:
        query = query.where(Provider.state == state.upper())
    if specialty:
        query = query.where(Provider.specialty.ilike(f"%{specialty}%"))
    if is_excluded is not None:
        query = query.where(Provider.is_excluded == is_excluded)
    if min_risk is not None:
        query = query.where(Provider.risk_score >= min_risk)
    if max_risk is not None:
        query = query.where(Provider.risk_score <= max_risk)

    # Count
    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar_one()

    # Sort (col already validated against whitelist above)
    sort_col = getattr(Provider, sort_by)
    if sort_dir == "desc":
        query = query.order_by(sort_col.desc().nullslast())
    else:
        query = query.order_by(sort_col.asc().nullsfirst())

    # Paginate
    query = query.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(query)
    providers = result.scalars().all()

    return ProviderListResponse(
        items=[ProviderSummary.model_validate(p) for p in providers],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/export/csv")
async def export_providers_csv(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    state: str | None = None,
    is_excluded: bool | None = None,
    min_risk: float | None = None,
):
    query = select(Provider)
    allowed_states = current_user.state_access or []
    if allowed_states:
        query = query.where(Provider.state.in_(allowed_states))
    if state:
        query = query.where(Provider.state == state)
    if is_excluded is not None:
        query = query.where(Provider.is_excluded == is_excluded)
    if min_risk is not None:
        query = query.where(Provider.risk_score >= min_risk)

    query = query.order_by(Provider.risk_score.desc().nullslast()).limit(10_000)
    result = await db.execute(query)
    providers = result.scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "npi", "name_last", "name_first", "specialty", "state", "city",
        "total_payment", "total_services", "total_beneficiaries",
        "risk_score", "is_excluded", "leie_date", "leie_reason",
        "payment_vs_peer", "payment_zscore", "flag_count",
    ])
    for p in providers:
        writer.writerow([
            p.npi, p.name_last, p.name_first, p.specialty, p.state, p.city,
            p.total_payment, p.total_services, p.total_beneficiaries,
            p.risk_score, p.is_excluded, p.leie_date, p.leie_reason,
            p.payment_vs_peer, p.payment_zscore,
            len(p.flags) if p.flags else 0,
        ])

    db.add(AuditLog(
        user_id=current_user.id,
        action="export_providers_csv",
        target_type="provider",
        details={"count": len(providers), "filters": {"state": state, "min_risk": min_risk}},
    ))

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=vigil_providers.csv"},
    )


@router.get("/map", response_model=list[ProviderMapPoint])
async def get_provider_map(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    state: str | None = Query(None, max_length=2),
):
    """
    Geographic fraud density aggregated by state.
    Returns one row per state with high-risk counts, average risk score,
    total estimated losses (from fraud_flags), and exclusion counts.
    Cached for 5 minutes — data changes only after ML pipeline runs.
    """
    allowed_states = current_user.state_access or []
    cache_key = f"map:{','.join(sorted(allowed_states)) or 'all'}:{state or 'all'}"

    cached = await cache.get(cache_key)
    if cached is not None:
        return [ProviderMapPoint(**d) for d in cached]

    # ── Provider-level aggregation ──────────────────────────────────────────
    query = (
        select(
            Provider.state,
            func.count(Provider.npi).label("total_providers"),
            func.count(Provider.npi).filter(Provider.risk_tier <= 2).label("high_risk_count"),
            func.avg(Provider.risk_score).label("avg_risk_score"),
            func.count(Provider.npi).filter(Provider.is_excluded == True).label("excluded_count"),  # noqa: E712
        )
        .where(Provider.state.in_(_US_STATES))
    )
    if allowed_states:
        query = query.where(Provider.state.in_(allowed_states))
    if state:
        query = query.where(Provider.state == state.upper())
    query = query.group_by(Provider.state).order_by(
        func.count(Provider.npi).filter(Provider.risk_score >= 70).desc()
    )
    rows = (await db.execute(query)).all()

    # ── Per-state estimated overpayment from fraud_flags ───────────────────
    loss_query = (
        select(
            Provider.state,
            func.coalesce(func.sum(FraudFlag.estimated_overpayment), 0).label("total_loss"),
        )
        .join(FraudFlag, FraudFlag.npi == Provider.npi)
        .where(Provider.state.in_(_US_STATES))
        .where(FraudFlag.is_active == True)          # noqa: E712
        .where(FraudFlag.estimated_overpayment.isnot(None))
    )
    if allowed_states:
        loss_query = loss_query.where(Provider.state.in_(allowed_states))
    if state:
        loss_query = loss_query.where(Provider.state == state.upper())
    loss_query = loss_query.group_by(Provider.state)
    loss_rows = (await db.execute(loss_query)).all()
    loss_by_state = {r.state: float(r.total_loss) for r in loss_rows}

    result = [
        ProviderMapPoint(
            state=row.state or "",
            total_providers=row.total_providers or 0,
            high_risk_count=row.high_risk_count or 0,
            avg_risk_score=row.avg_risk_score,
            total_estimated_loss=loss_by_state.get(row.state),
            excluded_count=row.excluded_count or 0,
        )
        for row in rows
    ]

    await cache.set(cache_key, [p.model_dump() for p in result], ttl=300)
    return result


@router.get("/{npi}", response_model=ProviderDetail)
async def get_provider(
    npi: str,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(select(Provider).where(Provider.npi == npi))
    provider = result.scalar_one_or_none()
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")

    allowed_states = current_user.state_access or []
    if allowed_states and provider.state not in allowed_states:
        raise HTTPException(status_code=403, detail="Access denied for this state")

    db.add(AuditLog(
        user_id=current_user.id,
        action="view_provider",
        target_type="provider",
        target_id=npi,
    ))

    return ProviderDetail.model_validate(provider)


@router.get("/{npi}/billing", response_model=list[BillingRecordOut])
async def get_provider_billing(
    npi: str,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    year: int | None = Query(None, ge=2010, le=2030),
    limit: int = Query(default=100, ge=1, le=500),
):
    """
    HCPCS-level billing breakdown for a provider, ordered by total Medicare payment desc.
    Returns records from billing_records table (populated by Part B ingest pipeline).
    """
    result = await db.execute(select(Provider).where(Provider.npi == npi))
    provider = result.scalar_one_or_none()
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")

    allowed_states = current_user.state_access or []
    if allowed_states and provider.state not in allowed_states:
        raise HTTPException(status_code=403, detail="Access denied for this state")

    query = (
        select(BillingRecord)
        .where(BillingRecord.npi == npi)
        .order_by(BillingRecord.total_medicare_payment.desc().nullslast())
        .limit(limit)
    )
    if year is not None:
        query = query.where(BillingRecord.year == year)

    rows = (await db.execute(query)).scalars().all()
    return [BillingRecordOut.model_validate(r) for r in rows]


@router.get("/{npi}/flags", response_model=list[FraudFlagOut])
async def get_provider_flags(
    npi: str,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    active_only: bool = Query(default=True),
    limit: int = Query(default=50, ge=1, le=200),
):
    """
    Active fraud detection signals for a provider from the normalized fraud_flags table.
    Ordered by severity (critical first) then recency.
    """
    result = await db.execute(select(Provider).where(Provider.npi == npi))
    provider = result.scalar_one_or_none()
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")

    allowed_states = current_user.state_access or []
    if allowed_states and provider.state not in allowed_states:
        raise HTTPException(status_code=403, detail="Access denied for this state")

    query = (
        select(FraudFlag)
        .where(FraudFlag.npi == npi)
        .order_by(FraudFlag.severity.asc(), FraudFlag.created_at.desc())
        .limit(limit)
    )
    if active_only:
        query = query.where(FraudFlag.is_active == True)  # noqa: E712

    rows = (await db.execute(query)).scalars().all()
    return [FraudFlagOut.model_validate(r) for r in rows]


@router.get("/{npi}/report/pdf")
async def provider_pdf_report(
    npi: str,
    background_tasks: BackgroundTasks,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    Generate and stream a PDF investigation report.
    PDF rendering runs in a thread-pool executor so the async event loop
    stays unblocked. Audit logging is deferred to a background task so the
    response byte-stream starts as soon as the PDF is ready.
    """
    result = await db.execute(select(Provider).where(Provider.npi == npi))
    provider = result.scalar_one_or_none()
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")

    allowed_states = current_user.state_access or []
    if allowed_states and provider.state not in allowed_states:
        raise HTTPException(status_code=403, detail="Access denied for this state")

    # Run synchronous PDF generation in thread-pool — non-blocking for event loop
    loop = asyncio.get_event_loop()
    pdf_bytes: bytes = await loop.run_in_executor(
        None, partial(generate_provider_pdf, provider, current_user)
    )

    # Write audit log after the response is sent — don't block the download
    async def _audit() -> None:
        from app.database import async_session_maker
        async with async_session_maker() as audit_db:
            audit_db.add(AuditLog(
                user_id=current_user.id,
                action="export_provider_pdf",
                target_type="provider",
                target_id=npi,
            ))
            await audit_db.commit()

    background_tasks.add_task(_audit)

    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=vigil_provider_{npi}.pdf"},
    )


@router.get("/{npi}/analysis")
async def get_provider_analysis(
    npi: str,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """
    AI-generated investigative brief for a provider.

    Synthesizes fraud flags, billing records, and referral network into a
    structured document with scheme classification, named suspects, specific
    data points, and recommended investigative actions.

    Cached for 10 minutes — data only changes after ML pipeline runs.
    """
    # Cache key (analysis is expensive — multiple queries)
    cache_key = f"analysis:{npi}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # ── Fetch provider ─────────────────────────────────────────────────────────
    result = await db.execute(select(Provider).where(Provider.npi == npi))
    provider = result.scalar_one_or_none()
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")

    allowed_states = current_user.state_access or []
    if allowed_states and provider.state not in allowed_states:
        raise HTTPException(status_code=403, detail="Access denied for this state")

    # ── Fraud flags ────────────────────────────────────────────────────────────
    flags_result = await db.execute(
        select(FraudFlag)
        .where(FraudFlag.npi == npi)
        .where(FraudFlag.is_active == True)  # noqa: E712
        .order_by(FraudFlag.severity.asc(), FraudFlag.estimated_overpayment.desc().nullslast())
    )
    flags = flags_result.scalars().all()

    # ── Top billing records ────────────────────────────────────────────────────
    billing_result = await db.execute(
        select(BillingRecord)
        .where(BillingRecord.npi == npi)
        .order_by(BillingRecord.total_medicare_payment.desc().nullslast())
        .limit(15)
    )
    billing_records = billing_result.scalars().all()

    # ── Network edges (suspicious, sorted by shared patients) ─────────────────
    from sqlalchemy import or_
    edges_result = await db.execute(
        select(ReferralEdge)
        .where(or_(ReferralEdge.source_npi == npi, ReferralEdge.target_npi == npi))
        .where(ReferralEdge.is_suspicious == True)  # noqa: E712
        .order_by(ReferralEdge.shared_patients.desc().nullslast())
        .limit(20)
    )
    edges = edges_result.scalars().all()

    # ── Neighbor providers (for suspect name/score lookup) ────────────────────
    neighbor_npis: set[str] = set()
    for e in edges:
        if e.source_npi != npi:
            neighbor_npis.add(e.source_npi)
        if e.target_npi != npi:
            neighbor_npis.add(e.target_npi)

    neighbor_providers = []
    if neighbor_npis:
        neighbors_result = await db.execute(
            select(Provider).where(Provider.npi.in_(list(neighbor_npis)))
        )
        neighbor_providers = neighbors_result.scalars().all()

    # ── Generate brief ─────────────────────────────────────────────────────────
    brief = generate_analysis(
        provider=provider,
        flags=list(flags),
        billing_records=list(billing_records),
        network_edges=list(edges),
        neighbor_providers=neighbor_providers,
    )

    # Audit log
    db.add(AuditLog(
        user_id=current_user.id,
        action="view_analysis",
        target_type="provider",
        target_id=npi,
    ))

    await cache.set(cache_key, brief, ttl=600)
    return brief
