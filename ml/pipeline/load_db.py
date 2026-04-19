"""
load_db.py — Upsert scored + flagged providers into PostgreSQL.

Also upserts LEIE exclusion records and cross-references providers.

Uses synchronous psycopg2 (not async SQLAlchemy) since this is a batch job.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / "backend" / ".env")

DATA_DIR = Path(__file__).parent.parent / "data"
PROC_DIR = DATA_DIR / "processed"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://vigil:vigil@localhost:5432/vigil"
).replace("postgresql+asyncpg://", "postgresql://")

BATCH_SIZE = 2_000


def _conn():
    return psycopg2.connect(DATABASE_URL)


def _clean(v):
    """Convert numpy/nan types to Python-native for psycopg2."""
    if v is None:
        return None
    # Handle numpy scalars before Python float check — np.float32 is NOT a subclass of float
    if isinstance(v, np.floating):
        result = float(v)
        return None if np.isnan(result) else result
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.bool_):
        return bool(v)
    # Python native float NaN
    if isinstance(v, float) and np.isnan(v):
        return None
    return v


def upsert_providers(df: pd.DataFrame, data_year: int = 2022):
    print(f"\n  [load_db] Upserting {len(df):,} providers…")
    conn = _conn()
    cur = conn.cursor()
    now = datetime.now(timezone.utc)

    sql = """
    INSERT INTO providers (
        npi, name_last, name_first, specialty, state, city,
        total_services, total_beneficiaries, total_payment, num_procedure_types,
        peer_median_payment, peer_median_services, peer_median_benes,
        payment_vs_peer, services_vs_peer, benes_vs_peer,
        payment_zscore, services_per_bene, payment_per_bene,
        billing_entropy, em_upcoding_ratio,
        risk_score, xgboost_score, isolation_score, autoencoder_score,
        flags, data_year, scored_at, created_at, updated_at
    ) VALUES %s
    ON CONFLICT (npi) DO UPDATE SET
        name_last            = EXCLUDED.name_last,
        name_first           = EXCLUDED.name_first,
        specialty            = EXCLUDED.specialty,
        state                = EXCLUDED.state,
        city                 = EXCLUDED.city,
        total_services       = EXCLUDED.total_services,
        total_beneficiaries  = EXCLUDED.total_beneficiaries,
        total_payment        = EXCLUDED.total_payment,
        num_procedure_types  = EXCLUDED.num_procedure_types,
        peer_median_payment  = EXCLUDED.peer_median_payment,
        peer_median_services = EXCLUDED.peer_median_services,
        peer_median_benes    = EXCLUDED.peer_median_benes,
        payment_vs_peer      = EXCLUDED.payment_vs_peer,
        services_vs_peer     = EXCLUDED.services_vs_peer,
        benes_vs_peer        = EXCLUDED.benes_vs_peer,
        payment_zscore       = EXCLUDED.payment_zscore,
        services_per_bene    = EXCLUDED.services_per_bene,
        payment_per_bene     = EXCLUDED.payment_per_bene,
        billing_entropy      = EXCLUDED.billing_entropy,
        em_upcoding_ratio    = EXCLUDED.em_upcoding_ratio,
        risk_score           = EXCLUDED.risk_score,
        xgboost_score        = EXCLUDED.xgboost_score,
        isolation_score      = EXCLUDED.isolation_score,
        autoencoder_score    = EXCLUDED.autoencoder_score,
        flags                = EXCLUDED.flags,
        data_year            = EXCLUDED.data_year,
        scored_at            = EXCLUDED.scored_at,
        updated_at           = EXCLUDED.updated_at
    """

    def _row(r):
        flags = r.get("flags", [])
        # Parquet stores flag arrays as numpy object arrays — convert to list first
        if hasattr(flags, "tolist"):
            flags = flags.tolist()
        if not isinstance(flags, list):
            flags = []
        return (
            str(r["npi"]),
            _clean(r.get("name_last")),
            _clean(r.get("name_first")),
            _clean(r.get("specialty")),
            _clean(r.get("state")),
            _clean(r.get("city")),
            _clean(r.get("total_services")),
            _clean(r.get("total_beneficiaries")),
            _clean(r.get("total_payment")),
            _clean(r.get("num_procedure_types")),
            _clean(r.get("peer_median_payment")),
            _clean(r.get("peer_median_services")),
            _clean(r.get("peer_median_benes")),
            _clean(r.get("payment_vs_peer")),
            _clean(r.get("services_vs_peer")),
            _clean(r.get("benes_vs_peer")),
            _clean(r.get("payment_zscore")),
            _clean(r.get("services_per_bene")),
            _clean(r.get("payment_per_bene")),
            _clean(r.get("billing_entropy")),
            _clean(r.get("em_upcoding_ratio")),
            _clean(r.get("risk_score")),
            _clean(r.get("xgboost_score")),
            _clean(r.get("isolation_score")),
            _clean(r.get("autoencoder_score")),
            json.dumps(flags),
            data_year,
            now,
            now,
            now,
        )

    rows = [_row(r) for _, r in df.iterrows()]
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        psycopg2.extras.execute_values(cur, sql, batch, template=None, page_size=BATCH_SIZE)
        conn.commit()
        total += len(batch)
        print(f"    {total:,} / {len(rows):,} upserted", end="\r")

    print(f"\n  [load_db] Providers upserted: {total:,}")
    cur.close()
    conn.close()


def upsert_leie(leie: pd.DataFrame):
    print(f"\n  [load_db] Upserting {len(leie):,} LEIE records…")
    conn = _conn()
    cur = conn.cursor()

    # Clear and reload (LEIE is a full replacement each run)
    cur.execute("TRUNCATE leie_exclusions RESTART IDENTITY")

    sql = """
    INSERT INTO leie_exclusions
        (npi, lastname, firstname, busname, specialty, excltype, excldate, reindate, state)
    VALUES %s
    """
    rows = [
        (
            _clean(r.get("npi")),
            _clean(r.get("lastname")),
            _clean(r.get("firstname")),
            _clean(r.get("busname")),
            _clean(r.get("specialty")),
            _clean(r.get("excltype")),
            _clean(r.get("excldate")),
            _clean(r.get("reindate")),
            _clean(r.get("state")),
        )
        for _, r in leie.iterrows()
    ]
    psycopg2.extras.execute_values(cur, sql, rows, page_size=BATCH_SIZE)
    conn.commit()
    print(f"  [load_db] LEIE records loaded: {len(rows):,}")
    cur.close()
    conn.close()


def mark_exclusions(leie: pd.DataFrame):
    """Set is_excluded + leie_date + leie_reason on providers that have an NPI in LEIE."""
    print("\n  [load_db] Marking excluded providers…")
    conn = _conn()
    cur = conn.cursor()

    leie_with_npi = leie[leie["npi"].str.match(r"^\d{10}$", na=False)].copy()

    cur.execute("UPDATE providers SET is_excluded = FALSE, leie_date = NULL, leie_reason = NULL")

    sql = """
    UPDATE providers SET
        is_excluded = TRUE,
        leie_date   = data.excldate,
        leie_reason = data.excltype
    FROM (VALUES %s) AS data(npi, excldate, excltype)
    WHERE providers.npi = data.npi
    """
    rows = [
        (_clean(r["npi"]), _clean(r.get("excldate")), _clean(r.get("excltype")))
        for _, r in leie_with_npi.iterrows()
    ]
    psycopg2.extras.execute_values(cur, sql, rows, page_size=BATCH_SIZE)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM providers WHERE is_excluded = TRUE")
    count = cur.fetchone()[0]
    print(f"  [load_db] Providers marked excluded: {count:,}")
    cur.close()
    conn.close()


def update_dashboard_stats():
    """Compute and insert a fresh dashboard_stats row after providers are loaded."""
    print("\n  [load_db] Computing dashboard stats…")
    conn = _conn()
    cur = conn.cursor()

    # Ensure the table exists (created by SQLAlchemy models on first run)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dashboard_stats (
            id              SERIAL PRIMARY KEY,
            total_providers INTEGER,
            total_payment   NUMERIC(14, 2),
            leie_matches    INTEGER,
            high_risk_providers INTEGER,
            states_covered  INTEGER,
            new_leads       INTEGER,
            critical_count  INTEGER,
            high_count      INTEGER,
            medium_count    INTEGER,
            low_count       INTEGER,
            computed_at     TIMESTAMPTZ DEFAULT now()
        )
    """)

    cur.execute("""
        INSERT INTO dashboard_stats (
            total_providers, total_payment, leie_matches,
            high_risk_providers, states_covered, new_leads,
            critical_count, high_count, medium_count, low_count
        )
        SELECT
            COUNT(*),
            COALESCE(SUM(total_payment), 0),
            COUNT(*) FILTER (WHERE is_excluded = TRUE),
            COUNT(*) FILTER (WHERE risk_score >= 70),
            COUNT(DISTINCT state),
            COUNT(*) FILTER (WHERE risk_score >= 70 AND is_excluded = FALSE),
            COUNT(*) FILTER (WHERE risk_score >= 90),
            COUNT(*) FILTER (WHERE risk_score >= 70 AND risk_score < 90),
            COUNT(*) FILTER (WHERE risk_score >= 50 AND risk_score < 70),
            COUNT(*) FILTER (WHERE risk_score < 50)
        FROM providers
    """)
    conn.commit()

    cur.execute("SELECT total_providers, leie_matches, high_risk_providers FROM dashboard_stats ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    print(f"  [load_db] Stats: {row[0]:,} providers, {row[1]:,} LEIE, {row[2]:,} high-risk")
    cur.close()
    conn.close()


def run():
    print("\n=== LOAD DB ===")
    df   = pd.read_parquet(PROC_DIR / "scored_with_flags.parquet")
    leie = pd.read_parquet(PROC_DIR / "leie.parquet")

    upsert_providers(df)
    upsert_leie(leie)
    mark_exclusions(leie)
    update_dashboard_stats()
    print("\n  [load_db] Done.")


if __name__ == "__main__":
    run()
