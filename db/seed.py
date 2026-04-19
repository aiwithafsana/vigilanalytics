"""
Dev seed script — creates an admin user and loads the 50-provider demo dataset.

Usage:
    cd vigil/backend
    python ../../db/seed.py
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.models import Base, User, Provider
from app.auth import hash_password

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://vigil:vigil@localhost:5432/vigil")

DEMO_PROVIDERS = [
    {
        "npi": "1234567890", "name_last": "SMITH", "name_first": "JOHN",
        "specialty": "Internal Medicine", "state": "FL", "city": "Miami",
        "total_services": 12450, "total_beneficiaries": 1820, "total_payment": 4850000.00,
        "num_procedure_types": 8,
        "peer_median_payment": 620000.00, "peer_median_services": 1800.00, "peer_median_benes": 450.00,
        "payment_vs_peer": 7.8, "services_vs_peer": 6.9, "benes_vs_peer": 4.0,
        "payment_zscore": 8.4, "services_per_bene": 6.84, "payment_per_bene": 2665.93,
        "billing_entropy": 0.3120, "em_upcoding_ratio": 0.890,
        "risk_score": 97.2, "xgboost_score": 0.9821, "isolation_score": 0.9234, "autoencoder_score": 0.8876,
        "flags": [
            {"type": "billing_volume", "severity": "critical",
             "text": "Total Medicare payments 7.8x above specialty peer median ($4.85M vs $620K)"},
            {"type": "service_pattern", "severity": "critical",
             "text": "Service volume 6.9x above peer median — possible phantom billing"},
            {"type": "em_upcoding", "severity": "high",
             "text": "E&M upcoding ratio 0.89 — 89% of visits coded at highest complexity level"},
        ],
        "is_excluded": True, "leie_date": "2019-03-15", "leie_reason": "Patient Abuse",
        "data_year": 2022,
    },
    {
        "npi": "9876543210", "name_last": "JOHNSON", "name_first": "MARIA",
        "specialty": "Pain Management", "state": "TX", "city": "Houston",
        "total_services": 9820, "total_beneficiaries": 1240, "total_payment": 3920000.00,
        "num_procedure_types": 5,
        "peer_median_payment": 890000.00, "peer_median_services": 2100.00, "peer_median_benes": 560.00,
        "payment_vs_peer": 4.4, "services_vs_peer": 4.7, "benes_vs_peer": 2.2,
        "payment_zscore": 6.1, "services_per_bene": 7.92, "payment_per_bene": 3161.29,
        "billing_entropy": 0.2890, "em_upcoding_ratio": 0.720,
        "risk_score": 91.8, "xgboost_score": 0.9412, "isolation_score": 0.8901, "autoencoder_score": 0.8654,
        "flags": [
            {"type": "billing_volume", "severity": "critical",
             "text": "Total payments 4.4x above pain management peer median"},
            {"type": "cost_per_patient", "severity": "high",
             "text": "Payment per beneficiary $3,161 vs peer median $1,589"},
        ],
        "is_excluded": False, "data_year": 2022,
    },
]

ADMIN_USER = {
    "email": "admin@vigil.gov",
    "password": "VigilAdmin2024!",
    "name": "System Administrator",
    "role": "admin",
    "state_access": [],
}

ANALYST_USER = {
    "email": "analyst@vigil.gov",
    "password": "VigilAnalyst2024!",
    "name": "Jane Analyst",
    "role": "analyst",
    "state_access": ["FL", "TX"],
}


async def seed():
    engine = create_async_engine(DATABASE_URL, echo=False)
    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, checkfirst=True))

    async with Session() as db:
        # Create users (idempotent — skip if email already exists)
        for u in [ADMIN_USER, ANALYST_USER]:
            result = await db.execute(select(User).where(User.email == u["email"]))
            existing = result.scalar_one_or_none()
            if existing:
                print(f"  User {u['email']} already exists, skipping.")
                continue
            user = User(
                email=u["email"],
                hashed_password=hash_password(u["password"]),
                name=u["name"],
                role=u["role"],
                state_access=u["state_access"],
            )
            db.add(user)

        # Create providers (idempotent — skip if NPI already exists)
        for p in DEMO_PROVIDERS:
            result = await db.execute(select(Provider).where(Provider.npi == p["npi"]))
            existing = result.scalar_one_or_none()
            if existing:
                print(f"  Provider {p['npi']} already exists, skipping.")
                continue
            provider = Provider(**p)
            db.add(provider)

        await db.commit()

    await engine.dispose()
    print("Seed complete.")
    print(f"  Admin:   {ADMIN_USER['email']} / {ADMIN_USER['password']}")
    print(f"  Analyst: {ANALYST_USER['email']} / {ANALYST_USER['password']}")


if __name__ == "__main__":
    asyncio.run(seed())
