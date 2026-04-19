from __future__ import annotations
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID
from pydantic import BaseModel, EmailStr, Field


# ── Auth ──────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshRequest(BaseModel):
    refresh_token: str


class TokenData(BaseModel):
    user_id: UUID
    email: str
    role: str
    state_access: list[str]


# ── Users ─────────────────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    email: EmailStr
    # min_length=12 mirrors the router's _PW_RE policy (12+ chars, upper/lower/digit/special)
    password: str = Field(min_length=12)
    name: str
    role: str = Field(pattern="^(admin|analyst|viewer)$")
    state_access: list[str] = []


class UserUpdate(BaseModel):
    name: str | None = None
    role: str | None = Field(default=None, pattern="^(admin|analyst|viewer)$")
    state_access: list[str] | None = None
    is_active: bool | None = None


class UserOut(BaseModel):
    id: UUID
    email: str
    name: str
    role: str
    state_access: list[str]
    is_active: bool
    created_at: datetime
    last_login: datetime | None

    model_config = {"from_attributes": True}


# ── Providers ─────────────────────────────────────────────────────────────────

class FlagSchema(BaseModel):
    type: str
    severity: str
    text: str


class ProviderSummary(BaseModel):
    npi: str
    name_last: str | None
    name_first: str | None
    specialty: str | None
    state: str | None
    city: str | None
    total_payment: Decimal | None
    risk_score: Decimal | None
    is_excluded: bool
    flags: list[FlagSchema] = []
    flag_count: int | None = None
    data_year: int | None

    model_config = {"from_attributes": True}


class ProviderDetail(ProviderSummary):
    total_services: int | None
    total_beneficiaries: int | None
    num_procedure_types: int | None
    peer_median_payment: Decimal | None
    peer_median_services: Decimal | None
    peer_median_benes: Decimal | None
    payment_vs_peer: Decimal | None
    services_vs_peer: Decimal | None
    benes_vs_peer: Decimal | None
    payment_zscore: Decimal | None
    services_per_bene: Decimal | None
    payment_per_bene: Decimal | None
    billing_entropy: Decimal | None
    em_upcoding_ratio: Decimal | None
    xgboost_score: Decimal | None
    isolation_score: Decimal | None
    autoencoder_score: Decimal | None
    leie_date: str | None
    leie_reason: str | None
    scored_at: datetime | None


class ProviderListResponse(BaseModel):
    items: list[ProviderSummary]
    total: int
    page: int
    page_size: int


# ── Cases ─────────────────────────────────────────────────────────────────────

class CaseCreate(BaseModel):
    provider_npi: str
    title: str
    state: str | None = None
    estimated_loss: Decimal | None = None
    notes: str | None = None
    assigned_to: UUID | None = None


class CaseUpdate(BaseModel):
    title: str | None = None
    status: str | None = Field(default=None, pattern="^(open|under_review|closed|referred)$")
    assigned_to: UUID | None = None
    estimated_loss: Decimal | None = None
    notes: str | None = None


class CaseNoteCreate(BaseModel):
    content: str


class CaseNoteOut(BaseModel):
    id: int
    case_id: int
    user_id: UUID
    content: str
    created_at: datetime
    user_name: str | None = None

    model_config = {"from_attributes": True}


class CaseDocumentOut(BaseModel):
    id: int
    case_id: int
    filename: str
    file_size: int | None
    uploaded_by: UUID
    created_at: datetime

    model_config = {"from_attributes": True}


class CaseOut(BaseModel):
    id: int
    case_number: str
    provider_npi: str
    title: str
    status: str
    assigned_to: UUID | None
    state: str | None
    estimated_loss: Decimal | None
    notes: str | None
    created_by: UUID
    created_at: datetime
    updated_at: datetime
    provider: ProviderSummary | None = None
    case_notes: list[CaseNoteOut] = []
    documents: list[CaseDocumentOut] = []

    model_config = {"from_attributes": True}


class CaseListResponse(BaseModel):
    items: list[CaseOut]
    total: int
    page: int
    page_size: int


# ── Dashboard ─────────────────────────────────────────────────────────────────

class DashboardStats(BaseModel):
    total_providers: int
    total_payment: Decimal
    leie_matches: int
    open_cases: int
    high_risk_providers: int
    states_covered: int
    new_leads: int


class RiskDistribution(BaseModel):
    critical: int   # 90+
    high: int       # 70-89
    medium: int     # 50-69
    low: int        # <50


class LeadItem(BaseModel):
    """A single investigation lead — top fraud_flag with provider context."""
    flag_id: int
    npi: str
    name: str
    specialty: str | None
    state: str | None
    city: str | None
    is_excluded: bool
    severity: int          # 1=critical, 2=high, 3=medium
    flag_type: str
    explanation: str | None
    estimated_overpayment: Decimal | None
    flag_value: Decimal | None
    peer_value: Decimal | None
    hcpcs_code: str | None
    total_payment: Decimal | None
    risk_score: Decimal | None

    model_config = {"from_attributes": True}


class DashboardResponse(BaseModel):
    stats: DashboardStats
    risk_distribution: RiskDistribution
    top_providers: list[ProviderSummary]
    recent_cases: list[CaseOut]
    top_leads: list[LeadItem] = []


# ── Fraud Flags ───────────────────────────────────────────────────────────────

class FraudFlagOut(BaseModel):
    id: int
    npi: str
    flag_type: str
    layer: int | None
    severity: int | None
    confidence: Decimal | None
    year: int | None
    flag_value: Decimal | None
    peer_value: Decimal | None
    explanation: str | None
    estimated_overpayment: Decimal | None
    hcpcs_code: str | None
    is_active: bool
    reviewed_by: UUID | None
    reviewed_at: datetime | None
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Billing Records ───────────────────────────────────────────────────────────

class BillingRecordOut(BaseModel):
    id: int
    npi: str
    year: int
    hcpcs_code: str | None
    hcpcs_description: str | None
    place_of_service: str | None
    total_beneficiaries: int | None
    total_services: int | None
    total_claims: int | None
    avg_submitted_charge: Decimal | None
    avg_medicare_allowed: Decimal | None
    avg_medicare_payment: Decimal | None
    total_medicare_payment: Decimal | None

    model_config = {"from_attributes": True}


# ── Map / Geographic ──────────────────────────────────────────────────────────

class ProviderMapPoint(BaseModel):
    state: str
    total_providers: int
    high_risk_count: int
    avg_risk_score: Decimal | None
    total_estimated_loss: Decimal | None
    excluded_count: int


# ── Alerts ────────────────────────────────────────────────────────────────────

class AlertItem(BaseModel):
    flag_id: int
    npi: str
    provider_name: str | None
    specialty: str | None
    state: str | None
    risk_score: Decimal | None
    flag_type: str
    severity: int
    explanation: str | None
    estimated_overpayment: Decimal | None
    created_at: datetime


class AlertResponse(BaseModel):
    items: list[AlertItem]
    total: int
    since: datetime | None


# ── Audit ─────────────────────────────────────────────────────────────────────

class AuditLogOut(BaseModel):
    id: int
    user_id: UUID | None
    action: str
    target_type: str | None
    target_id: str | None
    details: dict[str, Any]
    ip_address: str | None
    created_at: datetime
    user_name: str | None = None

    model_config = {"from_attributes": True}


class AuditLogResponse(BaseModel):
    items: list[AuditLogOut]
    total: int
    page: int
    page_size: int
