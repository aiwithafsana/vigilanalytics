"""
detect_layer5.py — Temporal & Trend Signals (Layer 5)

Detects year-over-year billing surges and new-provider billing spikes.
Requires at least 2 years of billing_records data.

Usage:
    python -m ml.pipeline.detect_layer5 --curr-year 2023 --prev-year 2022

Flags generated:
  - yoy_surge: billing increased >= 3× YoY with no specialty change
  - new_provider_spike: enrolled < 18 months ago, already billing in top 5% of specialty
"""

import argparse
import logging
import os

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Layer 5: Temporal & trend anomaly detection")
    p.add_argument("--curr-year", type=int, default=2023)
    p.add_argument("--prev-year", type=int, default=2022)
    p.add_argument("--min-yoy-ratio", type=float, default=3.0, help="Minimum YoY billing ratio to flag (default 3×)")
    p.add_argument("--min-total", type=float, default=100_000, help="Minimum current-year total to consider (default $100K)")
    p.add_argument("--enrollment-months", type=int, default=18, help="Max enrollment age in months for new-provider spike (default 18)")
    p.add_argument("--dsn", default=None)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def run(args):
    dsn = args.dsn or os.environ.get("DATABASE_URL", "postgresql://vigil:vigil@localhost:5432/vigil")
    dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # ── Pre-flight: verify required tables exist ──────────────────────────
        # Layer 5 depends on billing_records and peer_benchmarks.  These are
        # populated by load_db.py and the peer benchmark pipeline respectively.
        # If they're absent we fail loudly rather than silently returning 0 flags.
        required_tables = ["billing_records", "peer_benchmarks", "fraud_flags", "providers"]
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = ANY(%s)
        """, (required_tables,))
        found = {row[0] for row in cur.fetchall()}
        missing = set(required_tables) - found
        if missing:
            raise RuntimeError(
                f"Layer 5 requires database tables that are not present: {sorted(missing)}. "
                f"Run load_db.py and the peer benchmark pipeline first. "
                f"Use --dry-run to test without writing."
            )
        log.info("Pre-flight: all required tables present (%s)", ", ".join(sorted(required_tables)))

        flags = []

        # ── YoY Surge Detection ───────────────────────────────────────────────
        log.info("Layer 5: YoY surge detection (%d vs %d)", args.curr_year, args.prev_year)

        cur.execute("""
            SELECT
                curr.npi,
                curr.total AS curr_total,
                prev.total AS prev_total,
                curr.total / NULLIF(prev.total, 0) AS yoy_ratio,
                p.name_last, p.name_first, p.specialty, p.state
            FROM (
                SELECT npi, SUM(total_medicare_payment) AS total
                FROM billing_records WHERE year = %s
                GROUP BY npi
            ) curr
            JOIN (
                SELECT npi, SUM(total_medicare_payment) AS total
                FROM billing_records WHERE year = %s
                GROUP BY npi
            ) prev ON curr.npi = prev.npi
            JOIN providers p ON curr.npi = p.npi
            WHERE curr.total / NULLIF(prev.total, 0) >= %s
              AND curr.total >= %s
            ORDER BY curr.total / NULLIF(prev.total, 0) DESC
        """, (args.curr_year, args.prev_year, args.min_yoy_ratio, args.min_total))

        surges = cur.fetchall()
        log.info("  Found %d YoY surge candidates", len(surges))

        for row in surges:
            ratio = float(row["yoy_ratio"] or 0)
            curr = float(row["curr_total"] or 0)
            prev = float(row["prev_total"] or 0)

            severity = 1 if ratio >= 5 else (2 if ratio >= 3 else 3)
            confidence = min(0.4 + ratio / 20.0, 0.95)

            pname = f"{row['name_first'] or ''} {row['name_last'] or ''}".strip() or row["npi"]
            explanation = (
                f"{ratio:.1f}× billing increase year-over-year: "
                f"${curr:,.0f} in {args.curr_year} vs ${prev:,.0f} in {args.prev_year} "
                f"({row['specialty'] or 'Unknown specialty'}, {row['state'] or '??'}). "
                f"No specialty or location change recorded."
            )

            flags.append({
                "npi": row["npi"],
                "flag_type": "yoy_surge",
                "layer": 5,
                "severity": severity,
                "confidence": round(confidence, 3),
                "year": args.curr_year,
                "flag_value": round(ratio, 2),
                "peer_value": None,
                "explanation": explanation,
                "estimated_overpayment": round(curr - prev, 2),
            })

        # ── New Provider Spike Detection ──────────────────────────────────────
        log.info("Layer 5: New provider spike detection (enrolled < %d months)", args.enrollment_months)

        cur.execute("""
            SELECT
                p.npi,
                p.name_last,
                p.name_first,
                p.specialty,
                p.state,
                p.enrollment_date,
                EXTRACT(MONTH FROM AGE(NOW(), p.enrollment_date)) AS months_enrolled,
                br.total,
                pb.p90_total_payment
            FROM providers p
            JOIN (
                SELECT npi, SUM(total_medicare_payment) AS total
                FROM billing_records WHERE year = %s GROUP BY npi
            ) br ON p.npi = br.npi
            JOIN peer_benchmarks pb
              ON pb.taxonomy_code = p.taxonomy_code
             AND pb.state = p.state
             AND pb.year = %s
             AND pb.hcpcs_code IS NULL
            WHERE
                p.enrollment_date IS NOT NULL
                AND p.enrollment_date >= NOW() - INTERVAL '1 month' * %s
                AND br.total >= pb.p90_total_payment
                AND pb.peer_count >= 10
        """, (args.curr_year, args.curr_year, args.enrollment_months))

        spikes = cur.fetchall()
        log.info("  Found %d new-provider spike candidates", len(spikes))

        for row in spikes:
            months = int(row["months_enrolled"] or 0)
            total = float(row["total"] or 0)
            p90 = float(row["p90_total_payment"] or 0)

            explanation = (
                f"Provider enrolled {months} months ago and is already billing in the top 10% "
                f"of {row['specialty'] or 'their specialty'} in {row['state'] or '??'}. "
                f"Billed ${total:,.0f} vs peer P90 of ${p90:,.0f}."
            )

            flags.append({
                "npi": row["npi"],
                "flag_type": "new_provider_spike",
                "layer": 5,
                "severity": 2,
                "confidence": 0.80,
                "year": args.curr_year,
                "flag_value": round(months, 0),
                "peer_value": round(p90, 2),
                "explanation": explanation,
                "estimated_overpayment": round(total - p90, 2),
            })

        log.info("Layer 5: Generated %d total flags (%d YoY surge + %d new-provider spike)",
                 len(flags), len(surges), len(spikes))

        if args.dry_run:
            for f in flags[:5]:
                log.info("  DRY-RUN: %s", f)
            log.info("  (dry-run — no writes)")
            return

        # Deactivate stale Layer 5 flags
        cur.execute("""
            UPDATE fraud_flags SET is_active = FALSE
            WHERE layer = 5 AND year = %s AND is_active = TRUE
        """, (args.curr_year,))
        log.info("  Deactivated %d stale Layer 5 flags", cur.rowcount)

        if flags:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO fraud_flags
                  (npi, flag_type, layer, severity, confidence, year,
                   flag_value, peer_value, explanation, estimated_overpayment, is_active, created_at)
                VALUES
                  (%(npi)s, %(flag_type)s, %(layer)s, %(severity)s, %(confidence)s, %(year)s,
                   %(flag_value)s, %(peer_value)s, %(explanation)s, %(estimated_overpayment)s,
                   TRUE, NOW())
            """, flags, page_size=500)

        conn.commit()
        log.info("  ✓ Inserted %d fraud_flags", len(flags))

    except Exception:
        conn.rollback()
        log.exception("Layer 5 detection failed — rolled back")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run(parse_args())
