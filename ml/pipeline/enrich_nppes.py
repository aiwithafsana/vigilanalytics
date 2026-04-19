"""
enrich_nppes.py — Enrich providers with NPPES NPI Registry data.

Reads the CMS NPPES weekly export and adds taxonomy_code, credential,
enrollment_date, entity_type, and address fields to existing provider rows.

Download NPPES data from:
  https://download.cms.gov/nppes/NPI_Files.html

Usage:
    python -m ml.pipeline.enrich_nppes --file /data/npidata_pfile_20240101-20240107.csv

Column mapping (from NPPES export format):
  NPI                               → npi
  Entity Type Code                  → entity_type  ('1'=individual, '2'=org)
  Provider Credential Text          → credential
  Healthcare Provider Taxonomy Code_1 → taxonomy_code
  Provider Enumeration Date         → enrollment_date
  Provider First Line Business Practice Location Address → address_line
"""

import argparse
import csv
import logging
import os

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Enrich providers with NPPES data")
    p.add_argument("--file", required=True, help="Path to NPPES CSV export file")
    p.add_argument("--dsn", default=None)
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def run(args):
    dsn = args.dsn or os.environ.get("DATABASE_URL", "postgresql://vigil:vigil@localhost:5432/vigil")
    dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Load existing NPI set to skip unknown providers
    cur.execute("SELECT npi FROM providers")
    known_npis = {r["npi"] for r in cur.fetchall()}
    log.info("Loaded %d known NPIs from database", len(known_npis))

    batch = []
    total_updated = 0
    skipped = 0

    with open(args.file, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader):
            npi = (row.get("NPI") or "").strip()
            if npi not in known_npis:
                skipped += 1
                continue

            entity_type = (row.get("Entity Type Code") or "").strip() or None
            credential = (row.get("Provider Credential Text") or "").strip()[:20] or None
            taxonomy_code = (row.get("Healthcare Provider Taxonomy Code_1") or "").strip()[:10] or None

            enroll_str = (row.get("Provider Enumeration Date") or "").strip()
            enrollment_date = None
            if enroll_str:
                try:
                    from datetime import datetime
                    enrollment_date = datetime.strptime(enroll_str, "%m/%d/%Y").date().isoformat()
                except ValueError:
                    pass

            batch.append({
                "npi": npi,
                "entity_type": entity_type,
                "credential": credential,
                "taxonomy_code": taxonomy_code,
                "enrollment_date": enrollment_date,
            })

            if len(batch) >= args.batch_size:
                if not args.dry_run:
                    _flush(cur, batch)
                    conn.commit()
                total_updated += len(batch)
                batch = []

                if i % 100_000 == 0:
                    log.info("  Processed %dk rows, %d updated, %d skipped", i // 1000, total_updated, skipped)

    # Flush remainder
    if batch:
        if not args.dry_run:
            _flush(cur, batch)
            conn.commit()
        total_updated += len(batch)

    log.info("NPPES enrichment complete: %d providers updated, %d skipped (not in DB)", total_updated, skipped)

    cur.close()
    conn.close()


def _flush(cur, batch):
    psycopg2.extras.execute_batch(cur, """
        UPDATE providers SET
            entity_type    = COALESCE(%(entity_type)s, entity_type),
            credential     = COALESCE(%(credential)s, credential),
            taxonomy_code  = COALESCE(%(taxonomy_code)s, taxonomy_code),
            enrollment_date = COALESCE(%(enrollment_date)s::date, enrollment_date)
        WHERE npi = %(npi)s
    """, batch, page_size=500)


if __name__ == "__main__":
    run(parse_args())
