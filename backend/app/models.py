import uuid
from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey, Index, Integer,
    Numeric, SmallInteger, String, Text, UniqueConstraint, func
)
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY, INET
from sqlalchemy.orm import relationship
from app.database import Base
from app.services.encryption import EncryptedString


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    name = Column(String(200), nullable=False)
    role = Column(String(20), nullable=False)  # admin | analyst | viewer
    state_access = Column(ARRAY(String(2)), default=[])  # [] = all states
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_login = Column(DateTime(timezone=True), nullable=True)

    # ── Security fields ────────────────────────────────────────────────────────
    # token_version: increment to immediately invalidate all issued JWTs for
    # this user (logout-all, forced re-auth after password change, account takeover response).
    token_version = Column(Integer, default=0, nullable=False)
    # failed_login_attempts / locked_until: temporary lockout after 5 bad passwords.
    failed_login_attempts = Column(Integer, default=0, nullable=False)
    locked_until = Column(DateTime(timezone=True), nullable=True)

    # ── MFA (TOTP) ─────────────────────────────────────────────────────────────
    # mfa_enabled: True only after the user has scanned the QR and verified a
    # TOTP code (i.e. they actually have the secret in their authenticator).
    # mfa_secret: base32 TOTP secret.  Column-encrypted via Fernet at rest
    # (see services/encryption.py).  Application code reads/writes plain text;
    # the EncryptedString TypeDecorator handles transparent encryption.
    # mfa_backup_codes: bcrypt-hashed one-time codes; an array of strings.  When
    # the user uses one, that hash is removed from the array.
    mfa_enabled        = Column(Boolean, default=False, nullable=False)
    mfa_secret         = Column(EncryptedString(256), nullable=True)
    mfa_backup_codes   = Column(JSONB, default=list, nullable=False)
    mfa_enrolled_at    = Column(DateTime(timezone=True), nullable=True)

    cases_assigned = relationship("Case", foreign_keys="Case.assigned_to", back_populates="assignee")
    cases_created = relationship("Case", foreign_keys="Case.created_by", back_populates="creator")
    notes = relationship("CaseNote", back_populates="user")
    documents = relationship("CaseDocument", back_populates="uploader")
    audit_logs = relationship("AuditLog", back_populates="user")

    __table_args__ = (Index("ix_users_email", "email"),)


class Provider(Base):
    __tablename__ = "providers"

    npi = Column(String(10), primary_key=True)
    name_last = Column(String(100))
    name_first = Column(String(100))
    specialty = Column(String(100))
    state = Column(String(2))
    city = Column(String(100))

    # Billing metrics
    total_services = Column(Integer)
    total_beneficiaries = Column(Integer)
    total_payment = Column(Numeric(12, 2))
    num_procedure_types = Column(Integer)

    # Peer comparisons
    peer_median_payment = Column(Numeric(12, 2))
    peer_median_services = Column(Numeric(12, 2))
    peer_median_benes = Column(Numeric(12, 2))
    payment_vs_peer = Column(Numeric(8, 2))
    services_vs_peer = Column(Numeric(8, 2))
    benes_vs_peer = Column(Numeric(8, 2))
    payment_zscore = Column(Numeric(8, 2))

    # Derived metrics
    services_per_bene = Column(Numeric(8, 2))
    payment_per_bene = Column(Numeric(12, 2))
    billing_entropy = Column(Numeric(6, 4))
    em_upcoding_ratio = Column(Numeric(4, 3))

    # ML scores
    risk_score = Column(Numeric(5, 2))
    xgboost_score = Column(Numeric(6, 4))
    isolation_score = Column(Numeric(6, 4))
    autoencoder_score = Column(Numeric(6, 4))

    # Per-patient peer comparison (size-invariant fraud signal for volume specialties)
    ppb_vs_peer = Column(Numeric(8, 4))
    peer_median_ppb = Column(Numeric(12, 2))

    # SHAP feature attribution for top-10k providers (pre-computed)
    shap_drivers = Column(JSONB, default=None)

    # Anomaly flags
    flags = Column(JSONB, default=[])

    # LEIE status
    is_excluded = Column(Boolean, default=False)
    leie_date = Column(String(10))
    leie_reason = Column(String(200))

    # Metadata
    scored_at = Column(DateTime(timezone=True))
    data_year = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Extended identity (from NPPES/PECOS enrichment)
    entity_type = Column(String(1))           # '1' = individual, '2' = organization
    taxonomy_code = Column(String(10))         # NUCC taxonomy code
    credential = Column(String(20))            # MD, DO, NP, PA, etc.
    enrollment_date = Column(Date, nullable=True)  # from PECOS
    is_opt_out = Column(Boolean, default=False)    # opted out of Medicare

    # Practice-address columns (from NPPES Business Practice Location).
    # These power the address-clustering feature — multiple providers
    # registered at the same physical address is one of the strongest
    # shell-entity / phantom-billing signals.  Backfilled by the
    # ingest_nppes pipeline; NULL until NPPES enrichment runs.
    street_address      = Column(String(200))   # e.g. "1234 W Pico Blvd Suite 200"
    practice_zip        = Column(String(10))    # 5- or 9-digit ZIP
    # Lowercased + whitespace-normalised address used for cluster lookup.
    # Indexed for fast cluster-size queries.  Computed during ingest.
    address_normalized  = Column(String(255), index=True)

    # Computed risk fields (set after scoring)
    risk_tier = Column(SmallInteger)           # 1=critical, 2=high, 3=medium, 4=low
    flag_count = Column(SmallInteger, default=0)   # count of active fraud flags

    cases = relationship("Case", back_populates="provider")
    referrals_out = relationship("ReferralEdge", foreign_keys="ReferralEdge.source_npi", back_populates="source")
    referrals_in = relationship("ReferralEdge", foreign_keys="ReferralEdge.target_npi", back_populates="target")
    fraud_flags = relationship("FraudFlag", back_populates="provider", cascade="all, delete-orphan")
    billing_records = relationship("BillingRecord", back_populates="provider", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_providers_risk_score", "risk_score"),
        Index("ix_providers_state", "state"),
        Index("ix_providers_specialty", "specialty"),
        Index("ix_providers_is_excluded", "is_excluded"),
    )

    @property
    def name(self) -> str:
        # SQLAlchemy returns plain str at runtime, but Pylance/Pyright infer
        # Column[str] from the Column(String(...)) declaration.  Coercing
        # through str(... or "") satisfies the type checker without changing
        # behaviour — SQLAlchemy's __get__ always returns the actual value.
        first = str(self.name_first or "")
        last  = str(self.name_last  or "")
        joined = f"{first} {last}".strip()
        return joined or ""


class ReferralEdge(Base):
    __tablename__ = "referral_edges"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_npi = Column(String(10), ForeignKey("providers.npi"), nullable=False)
    target_npi = Column(String(10), ForeignKey("providers.npi"), nullable=False)
    referral_count = Column(Integer)
    total_payment = Column(Numeric(12, 2))
    referral_percentage = Column(Numeric(5, 2))
    shared_patients = Column(Integer)
    is_suspicious = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    source = relationship("Provider", foreign_keys=[source_npi], back_populates="referrals_out")
    target = relationship("Provider", foreign_keys=[target_npi], back_populates="referrals_in")

    __table_args__ = (
        UniqueConstraint("source_npi", "target_npi", name="uq_referral_edges_pair"),
        Index("ix_referral_edges_source", "source_npi"),
        Index("ix_referral_edges_target", "target_npi"),
        Index("ix_referral_edges_suspicious", "is_suspicious"),
        # Partial index: only suspicious edges — makes filtering very fast
        Index("ix_referral_edges_src_suspicious", "source_npi", "is_suspicious", "shared_patients"),
    )


class Case(Base):
    __tablename__ = "cases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    case_number = Column(String(20), unique=True, nullable=False)
    provider_npi = Column(String(10), ForeignKey("providers.npi"), nullable=False)
    title = Column(String(200), nullable=False)
    status = Column(String(20), default="open")  # open | under_review | closed | referred
    assigned_to = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    state = Column(String(2))
    estimated_loss = Column(Numeric(12, 2))
    notes = Column(Text)
    created_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Outcome tracking — closes the investigator feedback loop
    # substantiated | unsubstantiated | referred_to_doj | referred_to_state_ag | closed_no_action
    outcome = Column(String(30), nullable=True)
    outcome_note = Column(Text, nullable=True)
    resolved_at = Column(DateTime(timezone=True), nullable=True)

    provider = relationship("Provider", back_populates="cases")
    assignee = relationship("User", foreign_keys=[assigned_to], back_populates="cases_assigned")
    creator = relationship("User", foreign_keys=[created_by], back_populates="cases_created")
    case_notes = relationship("CaseNote", back_populates="case", cascade="all, delete-orphan")
    documents = relationship("CaseDocument", back_populates="case", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_cases_status", "status"),
        Index("ix_cases_assigned_to", "assigned_to"),
        Index("ix_cases_state", "state"),
    )


class CaseNote(Base):
    __tablename__ = "case_notes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    case_id = Column(Integer, ForeignKey("cases.id"), nullable=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    case = relationship("Case", back_populates="case_notes")
    user = relationship("User", back_populates="notes")


class CaseDocument(Base):
    __tablename__ = "case_documents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    case_id = Column(Integer, ForeignKey("cases.id"), nullable=False)
    filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_size = Column(Integer)
    uploaded_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    case = relationship("Case", back_populates="documents")
    uploader = relationship("User", back_populates="documents")


class LeieExclusion(Base):
    __tablename__ = "leie_exclusions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    npi = Column(String(10), index=True)
    lastname = Column(String(100))
    firstname = Column(String(100))
    busname = Column(String(200))
    specialty = Column(String(100))
    excltype = Column(String(20))
    excldate = Column(String(8))
    reindate = Column(String(8))
    state = Column(String(2))
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class DashboardStats(Base):
    __tablename__ = "dashboard_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    total_providers = Column(Integer)
    total_payment = Column(Numeric(14, 2))
    leie_matches = Column(Integer)
    high_risk_providers = Column(Integer)
    states_covered = Column(Integer)
    new_leads = Column(Integer)
    critical_count = Column(Integer)
    high_count = Column(Integer)
    medium_count = Column(Integer)
    low_count = Column(Integer)
    computed_at = Column(DateTime(timezone=True), server_default=func.now())


class FraudFlag(Base):
    """
    Normalized fraud detection signals — one row per anomaly per provider.
    All detection layers write here. Replaces the JSONB flags column for new pipelines.
    """
    __tablename__ = "fraud_flags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    npi = Column(String(10), ForeignKey("providers.npi"), nullable=False)
    # flag_type values: billing_volume, upcoding, impossible_hours, wrong_specialty,
    # leie_match, opt_out_billing, referral_cluster, hub_spoke, yoy_surge, new_provider_spike, etc.
    flag_type = Column(String(30), nullable=False)
    layer = Column(SmallInteger)               # 1–5 detection layer
    severity = Column(SmallInteger)            # 1=critical, 2=high, 3=medium
    confidence = Column(Numeric(4, 3))         # 0.000–1.000
    year = Column(SmallInteger)
    flag_value = Column(Numeric)               # the anomalous value (ratio, hours, dollars)
    peer_value = Column(Numeric)               # peer median for comparison
    explanation = Column(Text)                 # plain-English SHAP/rule output for investigators
    estimated_overpayment = Column(Numeric(14, 2))
    hcpcs_code = Column(String(10))            # if flag is code-specific
    is_active = Column(Boolean, default=True)
    reviewed_by = Column(UUID(as_uuid=True), nullable=True)
    reviewed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    provider = relationship("Provider", back_populates="fraud_flags")

    __table_args__ = (
        Index("ix_fraud_flags_npi_active", "npi", "is_active"),
        Index("ix_fraud_flags_severity_created", "severity", "created_at"),
        Index("ix_fraud_flags_created_at", "created_at"),
        Index("ix_fraud_flags_flag_type", "flag_type"),
    )


class BillingRecord(Base):
    """
    Part B billing aggregates per NPI per HCPCS code per year.
    Loaded by the annual CMS Part B ingest pipeline.
    """
    __tablename__ = "billing_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    npi = Column(String(10), ForeignKey("providers.npi"), nullable=False)
    year = Column(SmallInteger, nullable=False)
    hcpcs_code = Column(String(10))
    hcpcs_description = Column(Text)
    place_of_service = Column(String(2))       # 11=office, 21=hospital, 12=home
    total_beneficiaries = Column(Integer)
    total_services = Column(Integer)
    total_claims = Column(Integer)
    avg_submitted_charge = Column(Numeric(12, 2))
    avg_medicare_allowed = Column(Numeric(12, 2))
    avg_medicare_payment = Column(Numeric(12, 2))
    total_medicare_payment = Column(Numeric(14, 2))

    provider = relationship("Provider", back_populates="billing_records")

    __table_args__ = (
        # Unique constraint enables ON CONFLICT DO NOTHING for idempotent loads
        UniqueConstraint("npi", "year", "hcpcs_code", "place_of_service",
                         name="uq_billing_records_npi_year_hcpcs_pos"),
        Index("ix_billing_records_npi_year", "npi", "year"),
        Index("ix_billing_records_year", "year"),
        Index("ix_billing_records_hcpcs", "hcpcs_code"),
    )


class PeerBenchmark(Base):
    """
    Specialty × state × HCPCS percentile benchmarks.
    Built annually after Part B ingest. Drives Layer 1 billing outlier detection.

    Two grouping strategies:
      - specialty-based (populated from CMS data immediately)
      - taxonomy_code-based (populated after NPPES enrichment)
    Detection layers prefer taxonomy_code when available, fall back to specialty.
    """
    __tablename__ = "peer_benchmarks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(SmallInteger, nullable=False)
    taxonomy_code = Column(String(10), nullable=True)  # NUCC code — populated post-NPPES
    specialty = Column(String(100), nullable=True)     # CMS specialty string — populated immediately
    state = Column(String(2))
    hcpcs_code = Column(String(10), nullable=True)     # NULL = provider-level (all codes summed)
    peer_count = Column(Integer)
    median_total_payment = Column(Numeric(14, 2))
    p90_total_payment = Column(Numeric(14, 2))
    p99_total_payment = Column(Numeric(14, 2))
    median_services_per_ben = Column(Numeric(8, 2))
    median_charge_per_service = Column(Numeric(10, 2))

    __table_args__ = (
        Index("ix_peer_benchmarks_lookup", "taxonomy_code", "state", "year", "hcpcs_code"),
        Index("ix_peer_benchmarks_specialty", "specialty", "state", "year", "hcpcs_code"),
    )


class AgentRun(Base):
    """
    Persisted record of one agent execution.

    Reproducibility is a first-class requirement: investigators must be
    able to point at a run and say "this is what we knew on YYYY-MM-DD at
    HH:MM UTC."  result_json contains the full AgentRunResult.to_dict()
    payload — every tool's findings, durations, raw responses, and the
    aggregated finding ranking at the time of the run.
    """
    __tablename__ = "agent_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    workflow      = Column(String(50), nullable=False, index=True)   # 'public_records', etc.
    target_type   = Column(String(20), nullable=False)               # 'provider'
    target_id     = Column(String(50), nullable=False, index=True)   # NPI
    status        = Column(String(20), nullable=False, default="running")  # running | succeeded | failed | partial
    started_at    = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    completed_at  = Column(DateTime(timezone=True), nullable=True)
    duration_ms   = Column(Integer, nullable=True)
    n_findings    = Column(Integer, nullable=True)
    max_severity  = Column(String(10), nullable=True)
    triggered_by_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    result_json   = Column(JSONB, nullable=True)
    error         = Column(Text, nullable=True)

    __table_args__ = (
        Index("ix_agent_runs_target", "target_type", "target_id"),
        Index("ix_agent_runs_workflow_started", "workflow", "started_at"),
    )


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    action = Column(String(50), nullable=False)
    target_type = Column(String(20))
    target_id = Column(String(50))
    details = Column(JSONB, default={})
    ip_address = Column(INET)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="audit_logs")

    __table_args__ = (
        Index("ix_audit_log_user_id", "user_id"),
        Index("ix_audit_log_created_at", "created_at"),
    )
