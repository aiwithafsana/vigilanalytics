"""
backfill_addresses.py — Copy NPPES practice addresses into the providers table.

Run AFTER `python -m pipeline.ingest_nppes` has produced
``nppes_enrichment.parquet`` with address columns.

What this does
--------------
For every provider in our DB that we have an NPPES address for, set:
  - street_address      (e.g., "1234 W Pico Blvd Ste 200")
  - practice_zip        (5 digits)
  - address_normalized  (cluster lookup key)

The address columns power the address-clustering feature
(backend/app/services/address_clusters.py).  Until this script runs,
clustering returns empty + meta(data_ready=False).

Why a separate script (vs. baking into load_db)
-----------------------------------------------
- Idempotent: safe to re-run after NPPES refreshes without touching the
  scoring pipeline
- Fast: ~1.2M UPDATEs in batches of 2000 → 2-3 minutes
- Decoupled: doesn't require re-running the ML pipeline to apply a
  schema change

Usage
-----
    python -m pipeline.backfill_addresses

    # Dry-run (count what would change, no writes):
    python -m pipeline.backfill_addresses --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

DATA_DIR = Path(__file__).parent.parent / "data"
PROC_DIR = DATA_DIR / "processed"
NPPES_PARQUET = PROC_DIR / "nppes_enrichment.parquet"

# Match the URL convention in load_db.py and refresh_leie.py
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://vigil:vigil@localhost:5432/vigil",
).replace("postgresql+asyncpg://", "postgresql://")

BATCH_SIZE = 2_000

# These are the columns ingest_nppes.py writes when run with the
# address-capable COL_MAP.  Their absence indicates a stale parquet from
# a pre-address ingest — we tell the user to re-run ingest_nppes.
REQUIRED_PARQUET_COLS = {"npi", "street_address", "practice_zip", "address_normalized"}


def _conn():
    return psycopg2.connect(DATABASE_URL)


def run(dry_run: bool = False) -> None:
    if not NPPES_PARQUET.exists():
        print(
            f"  [backfill_addresses] ✗ {NPPES_PARQUET} not found.  "
            f"Run `python -m pipeline.ingest_nppes` first.",
            file=sys.stderr,
        )
        sys.exit(1)

    df = pd.read_parquet(NPPES_PARQUET, columns=None)
    missing = REQUIRED_PARQUET_COLS - set(df.columns)
    if missing:
        print(
            f"  [backfill_addresses] ✗ NPPES parquet is missing columns: "
            f"{sorted(missing)}.  Re-run ingest_nppes (the COL_MAP changed in "
            f"the address-clustering PR).",
            file=sys.stderr,
        )
        sys.exit(1)

    # Filter to rows that actually have addresses (NPPES sometimes has rows
    # with NPI but null address fields — those are deactivated providers).
    df = df[df["address_normalized"].notna()].copy()
    print(f"  [backfill_addresses] {len(df):,} providers with normalized addresses")

    if dry_run:
        # How many would update vs. how many already match
        conn = _conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM providers WHERE address_normalized IS NOT NULL
        """)
        already_set = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM providers")
        total = cur.fetchone()[0]
        cur.close(); conn.close()
        print(f"  [backfill_addresses] DRY RUN — would attempt {len(df):,} updates")
        print(f"  [backfill_addresses] Currently {already_set:,} of {total:,} providers have an address set")
        return

    # Build the update batch.  We use execute_batch (vs. execute_values) so
    # the UPDATE matches on the existing npi PK without an INSERT path.
    rows = [
        (
            (r.street_address or None),
            (r.practice_zip   or None),
            (r.address_normalized or None),
            str(r.npi),
        )
        for r in df.itertuples(index=False)
    ]

    conn = _conn()
    cur  = conn.cursor()
    print(f"  [backfill_addresses] writing {len(rows):,} updates in batches of {BATCH_SIZE:,}…")
    psycopg2.extras.execute_batch(
        cur,
        """
        UPDATE providers
        SET street_address     = %s,
            practice_zip       = %s,
            address_normalized = %s,
            updated_at         = NOW()
        WHERE npi = %s
        """,
        rows,
        page_size=BATCH_SIZE,
    )
    conn.commit()

    # ── Verify ────────────────────────────────────────────────────────────────
    cur.execute(
        "SELECT COUNT(*) FROM providers WHERE address_normalized IS NOT NULL"
    )
    addressed = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM providers")
    total = cur.fetchone()[0]
    coverage = addressed / max(total, 1)
    print(f"  [backfill_addresses] ✓ done.  Coverage: {addressed:,}/{total:,} ({coverage:.1%})")
    if coverage < 0.5:
        print(f"  [backfill_addresses] ⚠ Coverage under 50% — NPPES file may be stale "
              f"or your DB has providers NPPES doesn't know about")

    # Cluster-readiness check: how many clusters of ≥3 we'd find without filters
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT address_normalized
            FROM providers
            WHERE address_normalized IS NOT NULL
            GROUP BY address_normalized
            HAVING COUNT(*) >= 3
        ) c
    """)
    cluster_count = cur.fetchone()[0]
    print(f"  [backfill_addresses] Address clusters with ≥3 providers: {cluster_count:,}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Copy NPPES addresses into the providers table")
    p.add_argument("--dry-run", action="store_true", help="Count without writing")
    args = p.parse_args()
    run(dry_run=args.dry_run)
