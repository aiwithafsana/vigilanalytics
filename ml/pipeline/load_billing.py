"""
load_billing.py — Load CMS Part B HCPCS-level billing data into billing_records + peer_benchmarks.

Downloads MUP_PHY_R25_P05_V20_D23_Prov_Svc.csv (~1.5 GB) if not already cached,
then streams it into billing_records (one row per NPI × HCPCS code × place_of_service).

After loading, computes peer_benchmarks by taxonomy_code × state × HCPCS code
using PostgreSQL percentile functions — this drives Layer 1 outlier detection.

Usage:
    # Full run (downloads file if needed):
    python -m ml.pipeline.load_billing

    # Use a file you already downloaded:
    python -m ml.pipeline.load_billing --file /path/to/MUP_PHY_R25_P05_V20_D23_Prov_Svc.csv

    # Skip download, use cached path, dry-run to preview:
    python -m ml.pipeline.load_billing --no-download --dry-run

    # Truncate existing billing_records before loading (safe re-run):
    python -m ml.pipeline.load_billing --truncate

CMS column → billing_records mapping:
    Rndrng_NPI            → npi
    HCPCS_Cd              → hcpcs_code
    HCPCS_Desc            → hcpcs_description
    Place_Of_Srvc         → place_of_service  ('O'=Office, 'F'=Facility)
    Tot_Benes             → total_beneficiaries
    Tot_Srvcs             → total_services
    Avg_Sbmtd_Chrg        → avg_submitted_charge
    Avg_Mdcr_Alowd_Amt    → avg_medicare_allowed
    Avg_Mdcr_Pymt_Amt     → avg_medicare_payment
    Tot_Srvcs×Avg_Mdcr_Pymt_Amt → total_medicare_payment  (computed)
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
RAW_DIR  = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# CMS Part B 2022 HCPCS-level file (~1.5 GB, ~9 M rows)
CMS_HCPCS_URL = (
    "https://data.cms.gov/sites/default/files/2025-04/"
    "e3f823f8-db5b-4cc7-ba04-e7ae92b99757/MUP_PHY_R25_P05_V20_D23_Prov_Svc.csv"
)
DEFAULT_CACHE_PATH = RAW_DIR / "cms_part_b_2022_by_hcpcs.csv"


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Load CMS HCPCS billing data → billing_records + peer_benchmarks")
    p.add_argument("--file", default=None, help="Path to HCPCS CSV (skips download)")
    p.add_argument("--no-download", action="store_true", help="Use cached file, error if missing")
    p.add_argument("--year", type=int, default=2022, help="Data year (default: 2022)")
    p.add_argument("--batch-size", type=int, default=2000, help="Rows per DB batch (default: 2000)")
    p.add_argument("--chunk-size", type=int, default=100_000, help="CSV rows per pandas chunk (default: 100k)")
    p.add_argument("--truncate", action="store_true", help="Truncate billing_records before loading")
    p.add_argument("--skip-benchmarks", action="store_true", help="Skip peer_benchmarks computation")
    p.add_argument("--dry-run", action="store_true", help="Parse and validate — no writes")
    p.add_argument("--dsn", default=None)
    return p.parse_args()


# ── Download ───────────────────────────────────────────────────────────────────

def download_hcpcs_file(dest: Path) -> Path:
    """Stream-download the CMS HCPCS file with a progress indicator."""
    if dest.exists():
        size_mb = dest.stat().st_size / 1_048_576
        log.info("Using cached HCPCS file: %s (%.0f MB)", dest, size_mb)
        return dest

    import requests
    log.info("Downloading CMS HCPCS file (~1.5 GB) → %s", dest)
    log.info("  URL: %s", CMS_HCPCS_URL)

    try:
        with requests.get(CMS_HCPCS_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            downloaded = 0
            last_pct = -1
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):  # 1 MB chunks
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = int(downloaded / total * 100)
                        if pct >= last_pct + 10:
                            log.info("  Download progress: %d%% (%.0f / %.0f MB)",
                                     pct, downloaded / 1_048_576, total / 1_048_576)
                            last_pct = pct
        log.info("Download complete: %.0f MB", dest.stat().st_size / 1_048_576)
    except Exception as e:
        if dest.exists():
            dest.unlink()
        raise RuntimeError(f"Download failed: {e}") from e

    return dest


# ── Column detection ───────────────────────────────────────────────────────────

def detect_columns(raw_cols: list[str]) -> dict:
    """
    Map raw CMS column names (any capitalisation) to our internal keys.
    Returns a dict like {'npi': 'rndrng_npi', 'hcpcs': 'hcpcs_cd', ...}
    Raises ValueError if required columns are missing.
    """
    cols = {c.strip().lower() for c in raw_cols}

    def find(*candidates):
        for c in candidates:
            if c in cols:
                return c
        return None

    mapping = {
        "npi":       find("rndrng_npi", "npi"),
        "hcpcs":     find("hcpcs_cd", "hcpcs_code"),
        "desc":      find("hcpcs_desc", "hcpcs_description"),
        "pos":       find("place_of_srvc", "place_of_service"),
        "benes":     find("tot_benes", "total_beneficiaries", "bene_unique_cnt"),
        "srvcs":     find("tot_srvcs", "total_services"),
        "charges":   find("avg_sbmtd_chrg", "avg_submitted_charge"),
        "allowed":   find("avg_mdcr_alowd_amt", "avg_medicare_allowed"),
        "payment":   find("avg_mdcr_pymt_amt", "avg_medicare_payment"),
    }

    required = ["npi", "hcpcs", "srvcs", "payment"]
    missing = [k for k in required if mapping[k] is None]
    if missing:
        sample = list(cols)[:20]
        raise ValueError(
            f"Cannot find required columns {missing}.\n"
            f"  Available columns (first 20): {sample}"
        )

    return mapping


# ── Core load ──────────────────────────────────────────────────────────────────

def load_billing_records(
    cur,
    csv_path: Path,
    year: int,
    known_npis: set,
    batch_size: int,
    chunk_size: int,
    dry_run: bool,
) -> int:
    """
    Stream-parse the HCPCS CSV and batch-insert into billing_records.
    Returns total rows inserted.
    """
    import pandas as pd

    total_inserted = 0
    total_skipped = 0
    col_map = None
    batch = []
    t_start = time.time()

    log.info("Streaming %s …", csv_path.name)
    reader = pd.read_csv(
        csv_path,
        dtype=str,
        low_memory=False,
        chunksize=chunk_size,
        na_values=["", "N/A", "NA", "#N/A"],
        keep_default_na=True,
    )

    for chunk_idx, chunk in enumerate(reader):
        # Normalise column names once on first chunk
        chunk.columns = [c.strip().lower() for c in chunk.columns]
        if col_map is None:
            col_map = detect_columns(list(chunk.columns))
            log.info("Column mapping: %s", col_map)

        # Filter to known NPIs immediately (cuts memory ~90%)
        npi_col = col_map["npi"]
        chunk = chunk[chunk[npi_col].isin(known_npis)].copy()
        if chunk.empty:
            continue

        # Extract and coerce columns
        npis      = chunk[npi_col].str.strip()
        hcpcs     = chunk[col_map["hcpcs"]].str.strip().str.upper() if col_map["hcpcs"] else None
        desc      = chunk[col_map["desc"]].str.strip().str[:300] if col_map["desc"] else None
        pos       = chunk[col_map["pos"]].str.strip().str[:2] if col_map["pos"] else None
        benes     = pd.to_numeric(chunk[col_map["benes"]], errors="coerce") if col_map["benes"] else None
        srvcs     = pd.to_numeric(chunk[col_map["srvcs"]], errors="coerce")
        charges   = pd.to_numeric(chunk[col_map["charges"]], errors="coerce") if col_map["charges"] else None
        allowed   = pd.to_numeric(chunk[col_map["allowed"]], errors="coerce") if col_map["allowed"] else None
        payment   = pd.to_numeric(chunk[col_map["payment"]], errors="coerce")

        # Compute total_medicare_payment = services × avg_payment
        total_pay = srvcs * payment

        for i in range(len(chunk)):
            s = float(srvcs.iloc[i]) if srvcs is not None else None
            p = float(payment.iloc[i]) if payment is not None else None
            tp = float(total_pay.iloc[i]) if total_pay is not None else None

            # Skip rows with no meaningful payment data
            if tp is None or tp <= 0:
                total_skipped += 1
                continue

            batch.append({
                "npi":                   npis.iloc[i],
                "year":                  year,
                "hcpcs_code":            hcpcs.iloc[i] if hcpcs is not None else None,
                "hcpcs_description":     desc.iloc[i] if desc is not None else None,
                "place_of_service":      pos.iloc[i] if pos is not None else None,
                "total_beneficiaries":   int(benes.iloc[i]) if benes is not None and pd.notna(benes.iloc[i]) else None,
                "total_services":        int(s) if s and not pd.isna(s) else None,
                "total_claims":          None,   # not in HCPCS-level file
                "avg_submitted_charge":  round(float(charges.iloc[i]), 2) if charges is not None and pd.notna(charges.iloc[i]) else None,
                "avg_medicare_allowed":  round(float(allowed.iloc[i]), 2) if allowed is not None and pd.notna(allowed.iloc[i]) else None,
                "avg_medicare_payment":  round(p, 2) if p else None,
                "total_medicare_payment": round(tp, 2) if tp else None,
            })

            if len(batch) >= batch_size:
                if not dry_run:
                    _flush_billing(cur, batch)
                total_inserted += len(batch)
                batch = []

        elapsed = time.time() - t_start
        rate = total_inserted / elapsed if elapsed > 0 else 0
        log.info("  Chunk %d: %d inserted, %d skipped, %.0f rows/s",
                 chunk_idx + 1, total_inserted, total_skipped, rate)

    # Flush remainder
    if batch:
        if not dry_run:
            _flush_billing(cur, batch)
        total_inserted += len(batch)

    log.info("billing_records load complete: %d rows inserted, %d skipped (zero payment)",
             total_inserted, total_skipped)
    return total_inserted


def _flush_billing(cur, batch: list):
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO billing_records
          (npi, year, hcpcs_code, hcpcs_description, place_of_service,
           total_beneficiaries, total_services, total_claims,
           avg_submitted_charge, avg_medicare_allowed, avg_medicare_payment,
           total_medicare_payment)
        VALUES
          (%(npi)s, %(year)s, %(hcpcs_code)s, %(hcpcs_description)s, %(place_of_service)s,
           %(total_beneficiaries)s, %(total_services)s, %(total_claims)s,
           %(avg_submitted_charge)s, %(avg_medicare_allowed)s, %(avg_medicare_payment)s,
           %(total_medicare_payment)s)
        ON CONFLICT (npi, year, hcpcs_code, place_of_service) DO NOTHING
    """, batch, page_size=500)


# ── Peer benchmarks ────────────────────────────────────────────────────────────

def compute_peer_benchmarks(cur, year: int, dry_run: bool):
    """
    Build peer_benchmarks from billing_records × providers.

    Groups by specialty × state (available immediately from CMS data).
    Once NPPES enrichment populates taxonomy_code, a separate run will also
    produce taxonomy_code-based rows.

    Two sets:
      1. HCPCS-level: specialty × state × hcpcs_code  — per-procedure peer medians
      2. Provider-level: specialty × state, hcpcs_code=NULL — total billing peer medians
    """
    log.info("Computing peer_benchmarks for year %d…", year)

    # ── HCPCS-level benchmarks (specialty × state × hcpcs_code) ──────────────
    hcpcs_sql = """
        INSERT INTO peer_benchmarks
          (year, specialty, state, hcpcs_code,
           peer_count,
           median_total_payment, p90_total_payment, p99_total_payment,
           median_services_per_ben, median_charge_per_service)
        SELECT
            %s                                              AS year,
            p.specialty,
            p.state,
            br.hcpcs_code,
            COUNT(DISTINCT br.npi)                          AS peer_count,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY br.total_medicare_payment)
                                                            AS median_total_payment,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY br.total_medicare_payment)
                                                            AS p90_total_payment,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY br.total_medicare_payment)
                                                            AS p99_total_payment,
            PERCENTILE_CONT(0.50) WITHIN GROUP (
                ORDER BY CASE WHEN br.total_beneficiaries > 0
                         THEN br.total_services::numeric / br.total_beneficiaries
                         ELSE NULL END
            )                                               AS median_services_per_ben,
            PERCENTILE_CONT(0.50) WITHIN GROUP (
                ORDER BY br.avg_submitted_charge
            )                                               AS median_charge_per_service
        FROM billing_records br
        JOIN providers p ON p.npi = br.npi
        WHERE br.year = %s
          AND p.specialty IS NOT NULL
          AND p.state IS NOT NULL
          AND br.hcpcs_code IS NOT NULL
        GROUP BY p.specialty, p.state, br.hcpcs_code
        HAVING COUNT(DISTINCT br.npi) >= 5
    """

    # ── Provider-level benchmarks (hcpcs_code = NULL) ────────────────────────
    provider_sql = """
        INSERT INTO peer_benchmarks
          (year, specialty, state, hcpcs_code,
           peer_count,
           median_total_payment, p90_total_payment, p99_total_payment,
           median_services_per_ben, median_charge_per_service)
        SELECT
            %s                                              AS year,
            p.specialty,
            p.state,
            NULL                                            AS hcpcs_code,
            COUNT(DISTINCT p.npi)                           AS peer_count,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY p.total_payment)
                                                            AS median_total_payment,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY p.total_payment)
                                                            AS p90_total_payment,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY p.total_payment)
                                                            AS p99_total_payment,
            PERCENTILE_CONT(0.50) WITHIN GROUP (
                ORDER BY CASE WHEN p.total_beneficiaries > 0
                         THEN p.total_services::numeric / p.total_beneficiaries
                         ELSE NULL END
            )                                               AS median_services_per_ben,
            NULL                                            AS median_charge_per_service
        FROM providers p
        WHERE p.specialty IS NOT NULL
          AND p.state IS NOT NULL
          AND p.total_payment > 0
        GROUP BY p.specialty, p.state
        HAVING COUNT(DISTINCT p.npi) >= 5
    """

    if dry_run:
        log.info("  DRY-RUN: would run HCPCS-level and provider-level benchmark queries")
        return

    log.info("  Clearing peer_benchmarks for year %d…", year)
    cur.execute("DELETE FROM peer_benchmarks WHERE year = %s", (year,))
    log.info("  Deleted %d stale rows", cur.rowcount)

    log.info("  Computing HCPCS-level benchmarks by specialty × state (may take 2–3 min)…")
    cur.execute(hcpcs_sql, (year, year))
    log.info("  Inserted %d HCPCS-level benchmark rows", cur.rowcount)

    log.info("  Computing provider-level benchmarks by specialty × state…")
    cur.execute(provider_sql, (year,))
    log.info("  Inserted %d provider-level benchmark rows", cur.rowcount)


# ── Entry point ────────────────────────────────────────────────────────────────

def run(args):
    dsn = args.dsn or os.environ.get("DATABASE_URL", "postgresql://vigil:vigil@localhost:5432/vigil")
    dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")

    # ── Resolve CSV path ──────────────────────────────────────────────────────
    if args.file:
        csv_path = Path(args.file)
        if not csv_path.exists():
            log.error("File not found: %s", csv_path)
            sys.exit(1)
        log.info("Using provided file: %s", csv_path)
    elif args.no_download:
        csv_path = DEFAULT_CACHE_PATH
        if not csv_path.exists():
            log.error("Cached file not found at %s — remove --no-download to fetch it", csv_path)
            sys.exit(1)
        log.info("Using cached file: %s", csv_path)
    else:
        csv_path = download_hcpcs_file(DEFAULT_CACHE_PATH)

    # ── Connect ───────────────────────────────────────────────────────────────
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # ── Load known NPIs ───────────────────────────────────────────────────
        log.info("Loading known NPIs from providers table…")
        cur.execute("SELECT npi FROM providers")
        known_npis = {r["npi"] for r in cur.fetchall()}
        log.info("  %d known NPIs loaded", len(known_npis))

        # ── Optionally truncate ───────────────────────────────────────────────
        if args.truncate and not args.dry_run:
            log.info("Truncating billing_records for year %d…", args.year)
            cur.execute("DELETE FROM billing_records WHERE year = %s", (args.year,))
            log.info("  Deleted %d existing rows", cur.rowcount)
            conn.commit()

        # ── Check if already loaded ───────────────────────────────────────────
        cur.execute("SELECT COUNT(*) AS n FROM billing_records WHERE year = %s", (args.year,))
        existing = cur.fetchone()["n"]
        if existing > 0 and not args.truncate:
            log.warning(
                "billing_records already has %d rows for year %d. "
                "Use --truncate to reload, or --skip-benchmarks to just recompute benchmarks.",
                existing, args.year
            )
            if not args.skip_benchmarks:
                compute_peer_benchmarks(cur, args.year, args.dry_run)
                if not args.dry_run:
                    conn.commit()
                    log.info("peer_benchmarks updated for existing billing data.")
            return

        # ── Load billing records ──────────────────────────────────────────────
        inserted = load_billing_records(
            cur=cur,
            csv_path=csv_path,
            year=args.year,
            known_npis=known_npis,
            batch_size=args.batch_size,
            chunk_size=args.chunk_size,
            dry_run=args.dry_run,
        )

        if not args.dry_run:
            conn.commit()
            log.info("billing_records committed: %d rows for year %d", inserted, args.year)

            # ── Verify ────────────────────────────────────────────────────────
            cur.execute("SELECT COUNT(*) AS n, COUNT(DISTINCT npi) AS npis FROM billing_records WHERE year = %s",
                        (args.year,))
            row = cur.fetchone()
            log.info("  Verification: %d rows, %d distinct NPIs", row["n"], row["npis"])

        # ── Peer benchmarks ───────────────────────────────────────────────────
        if not args.skip_benchmarks:
            compute_peer_benchmarks(cur, args.year, args.dry_run)
            if not args.dry_run:
                conn.commit()

                cur.execute("SELECT COUNT(*) AS n FROM peer_benchmarks WHERE year = %s", (args.year,))
                n = cur.fetchone()["n"]
                log.info("peer_benchmarks: %d rows for year %d", n, args.year)

        # ── Update flag_count on providers ────────────────────────────────────
        if not args.dry_run:
            log.info("Refreshing flag_count on providers…")
            cur.execute("""
                UPDATE providers p SET
                    flag_count = (
                        SELECT COUNT(*) FROM fraud_flags ff
                        WHERE ff.npi = p.npi AND ff.is_active = TRUE
                    )
                WHERE EXISTS (
                    SELECT 1 FROM billing_records br WHERE br.npi = p.npi AND br.year = %s
                )
            """, (args.year,))
            log.info("  Updated flag_count for %d providers", cur.rowcount)
            conn.commit()

        log.info("✓ Done — billing_records and peer_benchmarks loaded for year %d", args.year)

    except Exception:
        conn.rollback()
        log.exception("Load failed — rolled back")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run(parse_args())
