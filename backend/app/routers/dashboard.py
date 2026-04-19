from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user
from app.database import get_db
from app.models import Provider, Case, User, FraudFlag
from app.schemas import (
    DashboardResponse, DashboardStats, ProviderSummary, CaseOut, RiskDistribution, LeadItem
)

router = APIRouter()

_US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}


async def _get_top_leads(db: AsyncSession, allowed_states: list[str], limit: int = 15) -> list[LeadItem]:
    """
    Return the top investigation leads: worst active fraud_flags with provider context.
    Ordered by severity (critical first) then estimated_overpayment desc.
    One flag per provider (the worst one) to avoid repetition.
    """
    # asyncpg requires = ANY(:param) for array membership — IN :param is not supported
    allowed_clause = "AND p.state = ANY(:allowed)" if allowed_states else ""

    outer_sql = text(f"""
        WITH best_flags AS (
            SELECT DISTINCT ON (ff.npi)
                ff.id            AS flag_id,
                ff.npi,
                TRIM(COALESCE(p.name_first || ' ', '') || COALESCE(p.name_last, ff.npi))  AS name,
                p.specialty,
                p.state,
                p.city,
                p.is_excluded,
                ff.severity,
                ff.flag_type,
                ff.explanation,
                ff.estimated_overpayment,
                ff.flag_value,
                ff.peer_value,
                ff.hcpcs_code,
                p.total_payment,
                p.risk_score
            FROM fraud_flags ff
            JOIN providers p ON p.npi = ff.npi
            WHERE ff.is_active = TRUE
              AND p.state = ANY(:us_states)
              {allowed_clause}
            ORDER BY ff.npi, ff.severity ASC, ff.estimated_overpayment DESC NULLS LAST
        )
        SELECT * FROM best_flags
        ORDER BY severity ASC, estimated_overpayment DESC NULLS LAST
        LIMIT :limit
    """)

    bind = {"us_states": list(_US_STATES), "limit": limit}
    if allowed_states:
        bind["allowed"] = list(allowed_states)

    rows = (await db.execute(outer_sql, bind)).mappings().all()
    return [LeadItem(**dict(r)) for r in rows]


@router.get("", response_model=DashboardResponse)
async def dashboard(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    allowed = current_user.state_access or []

    # For state-restricted users, we must still query live (smaller dataset)
    # For unrestricted users (admin with no state filter), use pre-computed stats
    if not allowed:
        # Read from pre-computed dashboard_stats (updated by ML pipeline)
        stats_row = (await db.execute(
            text("SELECT * FROM dashboard_stats ORDER BY computed_at DESC LIMIT 1")
        )).mappings().one_or_none()

        if stats_row:
            open_cases = await db.scalar(
                select(func.count(Case.id)).where(Case.status == "open")
            )
            top_result = await db.execute(
                select(Provider)
                .order_by(Provider.risk_score.desc().nullslast())
                .limit(10)
            )
            recent_result = await db.execute(
                select(Case)
                .options(
                    selectinload(Case.provider),
                    selectinload(Case.case_notes),
                    selectinload(Case.documents),
                )
                .order_by(Case.updated_at.desc()).limit(5)
            )
            top_leads = await _get_top_leads(db, allowed)
            return DashboardResponse(
                stats=DashboardStats(
                    total_providers=stats_row["total_providers"] or 0,
                    total_payment=float(stats_row["total_payment"] or 0),
                    leie_matches=stats_row["leie_matches"] or 0,
                    open_cases=open_cases or 0,
                    high_risk_providers=stats_row["high_risk_providers"] or 0,
                    states_covered=stats_row["states_covered"] or 0,
                    new_leads=stats_row["new_leads"] or 0,
                ),
                risk_distribution=RiskDistribution(
                    critical=stats_row["critical_count"] or 0,
                    high=stats_row["high_count"] or 0,
                    medium=stats_row["medium_count"] or 0,
                    low=stats_row["low_count"] or 0,
                ),
                top_providers=[ProviderSummary.model_validate(p) for p in top_result.scalars().all()],
                recent_cases=[CaseOut.model_validate(c) for c in recent_result.scalars().all()],
                top_leads=top_leads,
            )

    # State-restricted users: query their subset live (much smaller)
    state_cond = Provider.state.in_(allowed)
    case_cond = Case.state.in_(allowed)

    from sqlalchemy import case as sql_case, and_
    agg = await db.execute(
        select(
            func.count(Provider.npi).label("total"),
            func.coalesce(func.sum(Provider.total_payment), 0).label("total_payment"),
            func.count(sql_case((Provider.is_excluded == True, 1))).label("leie"),
            func.count(sql_case((Provider.risk_score >= 70, 1))).label("high_risk"),
            func.count(sql_case((and_(Provider.risk_score >= 70, Provider.is_excluded == False), 1))).label("new_leads"),
            func.count(func.distinct(Provider.state)).label("states"),
            func.count(sql_case((Provider.risk_score >= 90, 1))).label("critical"),
            func.count(sql_case((and_(Provider.risk_score >= 70, Provider.risk_score < 90), 1))).label("high"),
            func.count(sql_case((and_(Provider.risk_score >= 50, Provider.risk_score < 70), 1))).label("medium"),
            func.count(sql_case((Provider.risk_score < 50, 1))).label("low"),
        ).where(state_cond)
    )
    row = agg.one()

    open_cases = await db.scalar(
        select(func.count(Case.id)).where(case_cond, Case.status == "open")
    )
    top_result = await db.execute(
        select(Provider).where(state_cond)
        .order_by(Provider.risk_score.desc().nullslast()).limit(10)
    )
    recent_result = await db.execute(
        select(Case)
        .where(case_cond)
        .options(
            selectinload(Case.provider),
            selectinload(Case.case_notes),
            selectinload(Case.documents),
        )
        .order_by(Case.updated_at.desc()).limit(5)
    )

    top_leads = await _get_top_leads(db, allowed)
    return DashboardResponse(
        stats=DashboardStats(
            total_providers=row.total or 0,
            total_payment=float(row.total_payment or 0),
            leie_matches=row.leie or 0,
            open_cases=open_cases or 0,
            high_risk_providers=row.high_risk or 0,
            states_covered=row.states or 0,
            new_leads=row.new_leads or 0,
        ),
        risk_distribution=RiskDistribution(
            critical=row.critical or 0,
            high=row.high or 0,
            medium=row.medium or 0,
            low=row.low or 0,
        ),
        top_providers=[ProviderSummary.model_validate(p) for p in top_result.scalars().all()],
        recent_cases=[CaseOut.model_validate(c) for c in recent_result.scalars().all()],
        top_leads=top_leads,
    )
