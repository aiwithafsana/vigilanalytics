"""
load_shared_patients.py — Load CMS Physician Shared Patient Patterns into referral_edges.

Downloads the 2015 NBER Physician Shared Patient file (~250 MB zipped) if not cached,
then populates referral_edges with real provider-pair connections.

The 90-day shared patient file (pspp2015_90.zip) contains pairs of providers that
shared ≥11 Medicare beneficiaries within a 90-day window. This is the most reliable
proxy for referral relationships in public data — the 2022 version was retired by CMS
and never publicly released.

Columns in source file:
    NPI_1           → source_npi  (the referring provider by convention)
    NPI_2           → target_npi
    Pair_Count      → referral_count  (times the pair appeared in shared-patient patterns)
    Bene_Count      → shared_patients (unique beneficiaries shared)
    Same_Day_Count  → same-day services (indicator of co-treatment vs referral)

Suspicious edge criteria (any one triggers):
    - Both providers have ≥1 active fraud flag (Layer 1+ detection hit)
    - Either provider is LEIE-excluded (still billing despite sanction)
    - Pair_Count is in the top 1% for their specialty pair (disproportionate volume)

Usage:
    python -m ml.pipeline.load_shared_patients           # full run
    python -m ml.pipeline.load_shared_patients --file /path/to/pspp2015_90.zip
    python -m ml.pipeline.load_shared_patients --truncate --batch-size 5000
    python -m ml.pipeline.load_shared_patients --dry-run
"""

import argparse
import io
import itertools
import logging
import os
import sys
import time
import zipfile
from pathlib import Path

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
RAW_DIR  = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# NBER 2015 Physician Shared Patient Patterns — 90-day window
NBER_URL       = "https://data.nber.org/physician-shared-patient-patterns/2015/pspp2015_90.zip"
DEFAULT_CACHE  = RAW_DIR / "pspp2015_90.zip"


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Load NBER shared-patient patterns → referral_edges"
    )
    p.add_argument("--file",        default=None, help="Path to pspp2015_90.zip (skips download)")
    p.add_argument("--batch-size",  type=int, default=5000, help="DB insert batch size (default: 5000)")
    p.add_argument("--truncate",    action="store_true", help="DELETE existing referral_edges before load")
    p.add_argument("--dry-run",     action="store_true", help="Parse only — no DB writes")
    p.add_argument("--min-benes",   type=int, default=11,  help="Minimum shared beneficiaries (default: 11, matches CMS suppression)")
    p.add_argument("--dsn",         default=None)
    return p.parse_args()


# ── Download ───────────────────────────────────────────────────────────────────

def download_zip(dest: Path) -> Path:
    """Stream-download the NBER zip file."""
    if dest.exists():
        size_mb = dest.stat().st_size / 1_048_576
        log.info("Using cached zip: %s (%.0f MB)", dest, size_mb)
        return dest

    try:
        import requests
    except ImportError:
        raise RuntimeError("pip install requests")

    log.info("Downloading NBER shared patient data (~250 MB) → %s", dest)
    log.info("  URL: %s", NBER_URL)

    try:
        with requests.get(NBER_URL, stream=True, timeout=300) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            downloaded = 0
            last_pct = -1
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = int(downloaded / total * 100)
                        if pct >= last_pct + 10:
                            log.info("  %d%% (%.0f / %.0f MB)",
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
    Map raw column names to internal keys.
    The NBER file has been observed with multiple capitalisation styles.
    """
    cols = {c.strip().lower(): c.strip() for c in raw_cols}

    def find(*candidates):
        for c in candidates:
            if c.lower() in cols:
                return cols[c.lower()]
        return None

    mapping = {
        "npi1":       find("npi_1", "npi1", "referring_npi"),
        "npi2":       find("npi_2", "npi2", "rendering_npi"),
        "pair_count": find("pair_count", "paircount", "count"),
        "bene_count": find("bene_count", "benecount", "uniq_bene_cnt"),
        "same_day":   find("same_day_count", "samedaycount", "same_day_cnt"),
    }

    required = ["npi1", "npi2", "pair_count", "bene_count"]
    missing  = [k for k in required if mapping[k] is None]
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}\n"
            f"  Available: {list(raw_cols[:20])}"
        )
    return mapping


# ── Suspicious-edge detection ──────────────────────────────────────────────────

def build_suspicious_sets(cur) -> tuple[set, set, float]:
    """
    Returns:
        flagged_npis   — set of NPIs with ≥1 active fraud flag
        excluded_npis  — set of NPIs that are LEIE-excluded
        p99_pair_count — 99th-percentile pair_count (used for volume outliers)
    """
    log.info("Building suspicious-provider sets …")

    cur.execute("SELECT DISTINCT npi FROM fraud_flags WHERE is_active = TRUE")
    flagged_npis = {row[0] for row in cur.fetchall()}
    log.info("  Providers with active flags: %s", f"{len(flagged_npis):,}")

    cur.execute("SELECT DISTINCT npi FROM providers WHERE is_excluded = TRUE")
    excluded_npis = {row[0] for row in cur.fetchall()}
    log.info("  LEIE-excluded providers: %s", f"{len(excluded_npis):,}")

    # We'll compute p99 of pair_count from the loaded data to flag volume outliers.
    # Return 0 here; computed after streaming.
    return flagged_npis, excluded_npis


# ── Referral percentage: per-provider denominator ─────────────────────────────

def compute_referral_pcts(cur) -> None:
    """
    For each source_npi, compute referral_percentage =
        referral_count / SUM(referral_count) over all its outgoing edges × 100.
    This shows what fraction of a provider's referral activity goes to each partner.
    """
    log.info("Computing referral_percentage for each source NPI …")
    cur.execute("""
        UPDATE referral_edges re
        SET referral_percentage = ROUND(
            100.0 * re.referral_count
            / NULLIF(sub.total_referrals, 0),
            2
        )
        FROM (
            SELECT source_npi, SUM(referral_count) AS total_referrals
            FROM referral_edges
            GROUP BY source_npi
        ) sub
        WHERE re.source_npi = sub.source_npi
    """)
    log.info("  referral_percentage updated.")


# ── Main load ──────────────────────────────────────────────────────────────────

def load_shared_patients(
    cur,
    zip_path: Path,
    known_npis: set,
    flagged_npis: set,
    excluded_npis: set,
    batch_size: int,
    min_benes: int,
    dry_run: bool,
) -> int:
    """
    Stream-parse the zip CSV and batch-insert into referral_edges.
    Returns total rows inserted.
    """
    import csv

    total_inserted = 0
    total_skipped  = 0
    total_rows     = 0
    col_map        = None
    batch          = []
    t_start        = time.time()

    log.info("Opening zip: %s", zip_path)
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith((".csv", ".txt", ".tsv"))]
        if not csv_names:
            raise ValueError(f"No CSV/TXT found in {zip_path}. Contents: {zf.namelist()}")
        csv_name = csv_names[0]
        log.info("Reading: %s", csv_name)

        with zf.open(csv_name) as raw_bytes:
            # Wrap in text decoder — NBER files are plain ASCII
            text_stream = io.TextIOWrapper(raw_bytes, encoding="ascii", errors="replace")

            # Detect whether the file has a header.
            # NBER 2015 file is headerless — first field is a 10-digit NPI.
            first_line = text_stream.readline().strip()
            has_header = not first_line.split(",")[0].strip().isdigit()

            FIELDNAMES = ["NPI_1", "NPI_2", "Pair_Count", "Bene_Count", "Same_Day_Count"]

            if has_header:
                # Headered file: let DictReader use the header row
                reader = csv.DictReader(
                    itertools.chain([first_line + "\n"], text_stream)
                )
            else:
                # Headerless: inject our known column names, put first data row back
                reader = csv.DictReader(
                    itertools.chain([first_line + "\n"], text_stream),
                    fieldnames=FIELDNAMES,
                )

            for row in reader:
                total_rows += 1

                if col_map is None:
                    col_map = detect_columns(list(row.keys()))
                    log.info("Column mapping: %s", col_map)

                npi1 = row[col_map["npi1"]].strip().zfill(10)
                npi2 = row[col_map["npi2"]].strip().zfill(10)

                # Both providers must be in our database
                if npi1 not in known_npis or npi2 not in known_npis:
                    total_skipped += 1
                    continue

                # Skip self-loops
                if npi1 == npi2:
                    total_skipped += 1
                    continue

                try:
                    pair_count = int(float(row[col_map["pair_count"]]))
                    bene_count = int(float(row[col_map["bene_count"]]))
                except (ValueError, TypeError):
                    total_skipped += 1
                    continue

                # CMS suppresses pairs with <11 benes; honour the same threshold
                if bene_count < min_benes:
                    total_skipped += 1
                    continue

                same_day = 0
                if col_map.get("same_day") and row.get(col_map["same_day"]):
                    try:
                        same_day = int(float(row[col_map["same_day"]]))
                    except (ValueError, TypeError):
                        same_day = 0

                # Suspicious if both have active flags OR either is LEIE-excluded
                is_suspicious = (
                    (npi1 in flagged_npis and npi2 in flagged_npis)
                    or npi1 in excluded_npis
                    or npi2 in excluded_npis
                )

                batch.append((
                    npi1,           # source_npi
                    npi2,           # target_npi
                    pair_count,     # referral_count
                    bene_count,     # shared_patients
                    is_suspicious,  # is_suspicious
                ))

                if len(batch) >= batch_size:
                    if not dry_run:
                        _insert_batch(cur, batch)
                    total_inserted += len(batch)
                    batch = []

                    elapsed = time.time() - t_start
                    rate = total_rows / elapsed
                    log.info(
                        "  %s rows read | %s inserted | %s skipped | %.0f rows/sec",
                        f"{total_rows:,}", f"{total_inserted:,}", f"{total_skipped:,}", rate
                    )

    # Flush remainder
    if batch:
        if not dry_run:
            _insert_batch(cur, batch)
        total_inserted += len(batch)

    elapsed = time.time() - t_start
    log.info(
        "Done: %s total rows | %s inserted | %s skipped | %.1fs",
        f"{total_rows:,}", f"{total_inserted:,}", f"{total_skipped:,}", elapsed
    )
    return total_inserted


def _insert_batch(cur, batch: list) -> None:
    psycopg2.extras.execute_batch(
        cur,
        """
        INSERT INTO referral_edges
            (source_npi, target_npi, referral_count, shared_patients, is_suspicious)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        batch,
        page_size=1000,
    )


# ── Volume-outlier suspicious flag (post-load) ────────────────────────────────

def flag_volume_outliers(cur) -> None:
    """
    Mark edges in the top 1% of pair_count for their specialty-pair as suspicious.
    These disproportionate volumes can indicate kickback or steering arrangements.
    """
    log.info("Flagging top-1%% pair_count outliers as suspicious …")
    cur.execute("""
        WITH ranked AS (
            SELECT re.id,
                   PERCENT_RANK() OVER (
                       PARTITION BY p1.specialty, p2.specialty
                       ORDER BY re.referral_count
                   ) AS prank
            FROM referral_edges re
            JOIN providers p1 ON p1.npi = re.source_npi
            JOIN providers p2 ON p2.npi = re.target_npi
        )
        UPDATE referral_edges
        SET is_suspicious = TRUE
        WHERE id IN (SELECT id FROM ranked WHERE prank >= 0.99)
    """)
    rows = cur.rowcount
    log.info("  Flagged %s volume-outlier edges.", f"{rows:,}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    dsn = args.dsn or os.environ.get(
        "DATABASE_URL", "postgresql://vigil:vigil@localhost:5432/vigil"
    )
    dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")

    # Resolve zip path
    if args.file:
        zip_path = Path(args.file)
        if not zip_path.exists():
            log.error("File not found: %s", zip_path)
            sys.exit(1)
    else:
        zip_path = download_zip(DEFAULT_CACHE)

    log.info("Connecting to database …")
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # Load known NPIs into memory (fast membership test)
        log.info("Loading known NPIs …")
        cur.execute("SELECT npi FROM providers")
        known_npis = {row[0] for row in cur.fetchall()}
        log.info("  %s providers in database", f"{len(known_npis):,}")

        if not known_npis:
            log.error("No providers in database — run ingest first.")
            sys.exit(1)

        # Build suspicious-provider sets
        flagged_npis, excluded_npis = build_suspicious_sets(cur)

        # Optionally clear synthetic data
        if args.truncate and not args.dry_run:
            log.info("Truncating referral_edges …")
            cur.execute("DELETE FROM referral_edges")
            deleted = cur.rowcount
            log.info("  Deleted %s existing rows.", f"{deleted:,}")

        # Load the pairs
        total = load_shared_patients(
            cur=cur,
            zip_path=zip_path,
            known_npis=known_npis,
            flagged_npis=flagged_npis,
            excluded_npis=excluded_npis,
            batch_size=args.batch_size,
            min_benes=args.min_benes,
            dry_run=args.dry_run,
        )

        if not args.dry_run:
            # ── Commit the inserts first (makes rows visible, avoids giant txn) ──
            conn.commit()
            log.info("Committed. Total referral edges loaded: %s", f"{total:,}")

            # ── Post-processing in separate transactions ────────────────────────
            # Separate transactions keep each step independently rollback-able
            # and avoid holding a huge uncommitted dataset during heavy computation.

            log.info("Starting post-processing (separate transaction) …")
            conn2 = psycopg2.connect(dsn)
            conn2.autocommit = False
            cur2 = conn2.cursor()
            try:
                compute_referral_pcts(cur2)
                conn2.commit()
                log.info("referral_percentage committed.")

                flag_volume_outliers(cur2)
                conn2.commit()
                log.info("volume outlier flags committed.")
            except Exception:
                conn2.rollback()
                log.exception("Post-processing failed — inserts are still committed.")
            finally:
                cur2.close()
                conn2.close()

            cur = conn.cursor()  # re-open for summary query below

            # Summary stats
            cur.execute("""
                SELECT
                    COUNT(*) AS total_edges,
                    COUNT(DISTINCT source_npi) AS source_providers,
                    COUNT(DISTINCT target_npi) AS target_providers,
                    SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END) AS suspicious_edges,
                    MAX(referral_count) AS max_pair_count,
                    MAX(shared_patients) AS max_bene_count
                FROM referral_edges
            """)
            row = cur.fetchone()
            log.info("─" * 60)
            log.info("Referral edges summary:")
            log.info("  Total edges:        %s", f"{row[0]:,}")
            log.info("  Unique source NPIs: %s", f"{row[1]:,}")
            log.info("  Unique target NPIs: %s", f"{row[2]:,}")
            log.info("  Suspicious edges:   %s", f"{row[3]:,}")
            log.info("  Max pair_count:     %s", f"{row[4]:,}")
            log.info("  Max bene_count:     %s", f"{row[5]:,}")
            log.info("─" * 60)
        else:
            conn.rollback()
            log.info("Dry run complete — no changes written.")

    except Exception:
        conn.rollback()
        log.exception("Fatal error — rolling back")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
