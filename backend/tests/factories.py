"""
factories.py — Lightweight test-object factories.

All functions accept a SQLAlchemy AsyncSession, create the object, flush
(so the primary key is available), and return it.  The caller is responsible
for calling `await db.commit()` when it wants the row visible to other
connections (e.g. the HTTP test client).
"""
from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import hash_password
from app.models import BillingRecord, Case, FraudFlag, Provider, ReferralEdge, User

# ── Counters — ensure uniqueness within a test session ───────────────────────

_user_seq = 0
_npi_seq = 2_000_000_000   # well outside real NPI ranges (10 digits)


def _next_email(prefix: str = "user") -> str:
    global _user_seq
    _user_seq += 1
    return f"{prefix}_{_user_seq}@factory.example"


def _next_npi() -> str:
    global _npi_seq
    _npi_seq += 1
    return str(_npi_seq)[:10]  # keep to 10 chars


# ── User ──────────────────────────────────────────────────────────────────────

async def create_user(
    db: AsyncSession,
    *,
    email: Optional[str] = None,
    password: str = "TestPassword123!",
    name: str = "Test User",
    role: str = "analyst",
    state_access: Optional[list[str]] = None,
    is_active: bool = True,
) -> User:
    user = User(
        id=uuid.uuid4(),
        email=email or _next_email(),
        hashed_password=await hash_password(password),
        name=name,
        role=role,
        state_access=state_access if state_access is not None else [],
        is_active=is_active,
    )
    db.add(user)
    await db.flush()
    return user


# ── Provider ──────────────────────────────────────────────────────────────────

async def create_provider(
    db: AsyncSession,
    *,
    npi: Optional[str] = None,
    name_last: str = "Smith",
    name_first: str = "Jane",
    specialty: str = "Internal Medicine",
    state: str = "CA",
    city: str = "Los Angeles",
    risk_score: float = 50.0,
    is_excluded: bool = False,
    **kwargs,
) -> Provider:
    provider = Provider(
        npi=npi or _next_npi(),
        name_last=name_last,
        name_first=name_first,
        specialty=specialty,
        state=state,
        city=city,
        total_payment=Decimal("100000.00"),
        risk_score=Decimal(str(risk_score)),
        is_excluded=is_excluded,
        flags=[],
        **kwargs,
    )
    db.add(provider)
    await db.flush()
    return provider


# ── Case ──────────────────────────────────────────────────────────────────────

async def create_case(
    db: AsyncSession,
    *,
    provider_npi: str,
    created_by: uuid.UUID,
    title: str = "Test Investigation",
    status: str = "open",
    state: str = "CA",
    **kwargs,
) -> Case:
    case = Case(
        case_number=f"VGL-{uuid.uuid4().hex[:8].upper()}",
        provider_npi=provider_npi,
        title=title,
        status=status,
        created_by=created_by,
        state=state,
        **kwargs,
    )
    db.add(case)
    await db.flush()
    return case


# ── FraudFlag ─────────────────────────────────────────────────────────────────

async def create_fraud_flag(
    db: AsyncSession,
    *,
    npi: str,
    flag_type: str = "billing_volume",
    severity: int = 2,
    confidence: float = 0.8,
    is_active: bool = True,
    explanation: str = "Provider billed 5× the peer median.",
    **kwargs,
) -> FraudFlag:
    flag = FraudFlag(
        npi=npi,
        flag_type=flag_type,
        severity=severity,
        confidence=Decimal(str(confidence)),
        is_active=is_active,
        explanation=explanation,
        **kwargs,
    )
    db.add(flag)
    await db.flush()
    return flag


# ── ReferralEdge ──────────────────────────────────────────────────────────────

async def create_referral_edge(
    db: AsyncSession,
    *,
    source_npi: str,
    target_npi: str,
    referral_count: int = 50,
    shared_patients: int = 30,
    is_suspicious: bool = False,
    **kwargs,
) -> ReferralEdge:
    edge = ReferralEdge(
        source_npi=source_npi,
        target_npi=target_npi,
        referral_count=referral_count,
        shared_patients=shared_patients,
        is_suspicious=is_suspicious,
        **kwargs,
    )
    db.add(edge)
    await db.flush()
    return edge


# ── BillingRecord ─────────────────────────────────────────────────────────────

async def create_billing_record(
    db: AsyncSession,
    *,
    npi: str,
    year: int = 2022,
    hcpcs_code: str = "99213",
    hcpcs_description: str = "Office or other outpatient visit",
    place_of_service: str = "11",
    total_beneficiaries: int = 100,
    total_services: int = 300,
    total_medicare_payment: float = 15000.00,
    **kwargs,
) -> BillingRecord:
    record = BillingRecord(
        npi=npi,
        year=year,
        hcpcs_code=hcpcs_code,
        hcpcs_description=hcpcs_description,
        place_of_service=place_of_service,
        total_beneficiaries=total_beneficiaries,
        total_services=total_services,
        total_medicare_payment=Decimal(str(total_medicare_payment)),
        **kwargs,
    )
    db.add(record)
    await db.flush()
    return record
