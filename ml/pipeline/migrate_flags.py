"""
migrate_flags.py — Migrate existing JSONB flags → normalized fraud_flags table.

Reads the `flags` JSONB column on providers and inserts equivalent rows into
the `fraud_flags` table. Also backfills `risk_tier` and `flag_count`.

Run ONCE after applying db/migrations/001_architecture_v2.sql.

Usage:
    python -m ml.pipeline.migrate_flags [--dry-run] [--batch-size 500]
"""

import argparse
import logging
import os

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Map string severity → integer
_SEV_MAP = {"critical": 1, "high": 2, "medium": 3, "low": 3}

# Map JSONB flag type strings → normalized flag_type strings
_TYPE_MAP = {
    "billing_volume": "billing_volume",
    "upcoding": "upcoding",
    "leie": "leie_match",
    "leie_match": "leie_match",
    "opt_out": "opt_out_billing",
    "impossible_hours": "impossible_hours",
    "wrong_specialty": "wrong_specialty",
    "referral_cluster": "referral_cluster",
    "hub_spoke": "hub_spoke",
    "yoy_surge": "yoy_surge",
    "new_provider_spike": "new_provider_spike",
}


def parse_args():
    p = argparse.ArgumentParser(description="Migrate JSONB flags to normalized fraud_flags table")
    p.add_argument("--dsn", default=None)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-existing", action="store_true", default=True,
                   help="Skip NPIs that already have rows in fraud_flags (default: True)")
    return p.parse_args()


def run(args):
    dsn = args.dsn or os.environ.get("DATABASE_URL", "postgresql://vigil:vigil@localhost:5432/vigil")
    dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Optionally skip providers that already have fraud_flags rows
    existing_npis: set = set()
    if args.skip_existing:
        cur.execute("SELECT DISTINCT npi FROM fraud_flags")
        existing_npis = {r["npi"] for r in cur.fetchall()}
        log.info("Skipping %d NPIs already in fraud_flags", len(existing_npis))

    # Pull providers with non-empty JSONB flags
    cur.execute("""
        SELECT npi, flags, risk_score
        FROM providers
        WHERE flags IS NOT NULL
          AND jsonb_typeof(flags) = 'array'
          AND jsonb_array_length(flags) > 0
        ORDER BY npi
    """)
    providers = cur.fetchall()
    log.info("Found %d providers with JSONB flags to migrate", len(providers))

    total_flags = 0
    batch = []

    for prow in providers:
        npi = prow["npi"]
        if npi in existing_npis:
            continue

        flags_json = prow["flags"]
        if not isinstance(flags_json, list):
            continue  # psycopg2 auto-decodes JSONB

        for flag in flags_json:
            if not isinstance(flag, dict):
                continue

            raw_type = (flag.get("type") or "").lower().replace(" ", "_")
            flag_type = _TYPE_MAP.get(raw_type, raw_type[:30])

            raw_sev = (flag.get("severity") or "medium").lower()
            severity = _SEV_MAP.get(raw_sev, 3)

            explanation = flag.get("text") or flag.get("explanation") or None

            batch.append({
                "npi": npi,
                "flag_type": flag_type,
                "layer": 1,    # Layer unknown for legacy JSONB flags
                "severity": severity,
                "confidence": None,
                "year": None,
                "flag_value": None,
                "peer_value": None,
                "explanation": explanation,
                "estimated_overpayment": None,
                "hcpcs_code": None,
            })

        if len(batch) >= args.batch_size:
            if not args.dry_run:
                _flush(cur, batch)
                conn.commit()
            total_flags += len(batch)
            batch = []

    # Flush remainder
    if batch:
        if not args.dry_run:
            _flush(cur, batch)
            conn.commit()
        total_flags += len(batch)

    log.info("Migrated %d flags to fraud_flags", total_flags)

    # Backfill risk_tier and flag_count
    if not args.dry_run:
        log.info("Backfilling risk_tier and flag_count on providers…")
        cur.execute("""
            UPDATE providers SET
                risk_tier = CASE
                    WHEN risk_score >= 90 THEN 1
                    WHEN risk_score >= 70 THEN 2
                    WHEN risk_score >= 50 THEN 3
                    ELSE 4
                END
            WHERE risk_tier IS NULL AND risk_score IS NOT NULL
        """)
        log.info("  Updated risk_tier for %d providers", cur.rowcount)

        cur.execute("""
            UPDATE providers p SET
                flag_count = (
                    SELECT COUNT(*) FROM fraud_flags ff
                    WHERE ff.npi = p.npi AND ff.is_active = TRUE
                )
        """)
        log.info("  Updated flag_count for %d providers", cur.rowcount)

        conn.commit()

    log.info("✓ Migration complete")
    cur.close()
    conn.close()


def _flush(cur, batch):
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO fraud_flags
          (npi, flag_type, layer, severity, confidence, year,
           flag_value, peer_value, explanation, estimated_overpayment,
           hcpcs_code, is_active, created_at)
        VALUES
          (%(npi)s, %(flag_type)s, %(layer)s, %(severity)s, %(confidence)s, %(year)s,
           %(flag_value)s, %(peer_value)s, %(explanation)s, %(estimated_overpayment)s,
           %(hcpcs_code)s, TRUE, NOW())
    """, batch, page_size=500)


if __name__ == "__main__":
    run(parse_args())
