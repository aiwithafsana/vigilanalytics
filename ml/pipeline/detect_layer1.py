"""
detect_layer1.py — Billing Volume & Code-Level Anomaly Detection (Layer 1)

Compares each provider's billing against their specialty+state peer group using
the real CMS-derived peer_benchmarks table.  Produces two types of fraud_flags:

  billing_volume (hcpcs_code=NULL) — provider's TOTAL billing >= N× peer median
  billing_volume (hcpcs_code set)  — specific HCPCS code billed >= N× peer median

Both detections run entirely inside PostgreSQL using JOIN+aggregate SQL —
no Python-side row iteration, so even 9M+ billing records run in minutes.

After flagging, backfills risk_tier on all affected providers.

Usage:
    python -m ml.pipeline.detect_layer1 --year 2022
    python -m ml.pipeline.detect_layer1 --year 2022 --min-ratio 15 --dry-run

Severity thresholds (same for provider-level and code-level):
    severity 1 (critical) : ratio >= 50×
    severity 2 (high)     : ratio >= 20×
    severity 3 (medium)   : ratio >= --min-ratio (default 10×)
"""

import argparse
import logging
import os

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Layer 1: Billing volume anomaly detection")
    p.add_argument("--year", type=int, default=2022)
    p.add_argument("--min-ratio", type=float, default=10.0,
                   help="Min billing ratio vs peer median to flag (default 10×)")
    p.add_argument("--min-peers", type=int, default=10,
                   help="Min providers in peer group to use benchmark (default 10)")
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
        log.info("Layer 1: year=%d  min_ratio=%.0f×  min_peers=%d",
                 args.year, args.min_ratio, args.min_peers)

        # Quick sanity check
        cur.execute("SELECT COUNT(*) AS n FROM peer_benchmarks WHERE year = %s", (args.year,))
        n_bench = cur.fetchone()["n"]
        if n_bench == 0:
            log.error("No peer_benchmarks found for year %d — run load_billing.py first", args.year)
            return

        cur.execute("SELECT COUNT(*) AS n FROM billing_records WHERE year = %s", (args.year,))
        n_billing = cur.fetchone()["n"]
        log.info("  peer_benchmarks: %d rows  |  billing_records: %d rows", n_bench, n_billing)

        # ══════════════════════════════════════════════════════════════════════
        # PART 1 — Provider-level total billing outliers (pure SQL)
        # ══════════════════════════════════════════════════════════════════════
        log.info("  Part 1: Provider-level total billing outliers…")

        part1_sql = """
            WITH provider_totals AS (
                SELECT
                    br.npi,
                    SUM(br.total_medicare_payment)  AS total_billed,
                    p.specialty,
                    p.state,
                    p.name_last,
                    p.name_first
                FROM billing_records br
                JOIN providers p ON br.npi = p.npi
                WHERE br.year = %(year)s
                  AND p.specialty IS NOT NULL
                  AND p.state IS NOT NULL
                GROUP BY br.npi, p.specialty, p.state, p.name_last, p.name_first
            ),
            flagged AS (
                SELECT
                    pt.npi,
                    pt.total_billed,
                    pt.specialty,
                    pt.state,
                    pt.name_last,
                    pt.name_first,
                    pb.median_total_payment  AS median,
                    pb.p90_total_payment     AS p90,
                    pb.peer_count,
                    pt.total_billed / NULLIF(pb.median_total_payment, 0) AS ratio
                FROM provider_totals pt
                JOIN peer_benchmarks pb
                  ON pb.specialty = pt.specialty
                 AND pb.state = pt.state
                 AND pb.year = %(year)s
                 AND pb.hcpcs_code IS NULL
                WHERE pb.peer_count >= %(min_peers)s
                  AND pb.median_total_payment > 0
                  AND pt.total_billed / NULLIF(pb.median_total_payment, 0) >= %(min_ratio)s
            )
            SELECT
                npi, specialty, state, name_last, name_first,
                total_billed, median, p90, peer_count,
                ROUND(ratio::numeric, 2)  AS ratio,
                CASE WHEN ratio >= 50 THEN 1
                     WHEN ratio >= 20 THEN 2
                     ELSE 3 END           AS severity,
                ROUND(LEAST(0.5 + ratio / 200.0, 0.99)::numeric, 3) AS confidence,
                GREATEST(total_billed - p90, 0)  AS est_overpay
            FROM flagged
            ORDER BY ratio DESC
        """

        cur.execute(part1_sql, {
            "year": args.year,
            "min_ratio": args.min_ratio,
            "min_peers": args.min_peers,
        })
        part1_rows = cur.fetchall()
        log.info("    → %d provider-level flags", len(part1_rows))

        if args.dry_run and part1_rows:
            for r in part1_rows[:3]:
                log.info("    SAMPLE sev=%d %.0f× — %s %s (%s, %s) $%s vs $%s median",
                         r["severity"], float(r["ratio"]),
                         r["name_first"] or "", r["name_last"] or r["npi"],
                         r["specialty"], r["state"],
                         f"{float(r['total_billed']):,.0f}",
                         f"{float(r['median']):,.0f}")

        part1_flags = []
        for r in part1_rows:
            total  = float(r["total_billed"])
            median = float(r["median"])
            p90    = float(r["p90"] or median)
            ratio  = float(r["ratio"])
            part1_flags.append({
                "npi":        r["npi"],
                "flag_type":  "billing_volume",
                "layer":      1,
                "severity":   int(r["severity"]),
                "confidence": float(r["confidence"]),
                "year":       args.year,
                "flag_value": ratio,
                "peer_value": round(median, 2),
                "explanation": (
                    f"Billed {ratio:.0f}× the median for {r['specialty']} in {r['state']}: "
                    f"${total:,.0f} vs ${median:,.0f} peer median "
                    f"(peer group: {r['peer_count']:,} providers, P90: ${p90:,.0f}). "
                    f"Estimated excess above P90: ${max(total - p90, 0):,.0f}."
                ),
                "estimated_overpayment": round(max(total - p90, 0), 2),
                "hcpcs_code": None,
            })

        # ══════════════════════════════════════════════════════════════════════
        # PART 2 — HCPCS code-level billing outliers (pure SQL)
        # ══════════════════════════════════════════════════════════════════════
        log.info("  Part 2: HCPCS code-level billing outliers (SQL)…")

        part2_sql = """
            WITH flagged AS (
                SELECT
                    br.npi,
                    br.hcpcs_code,
                    br.hcpcs_description,
                    br.total_medicare_payment   AS billed,
                    br.total_services,
                    br.total_beneficiaries,
                    p.specialty,
                    p.state,
                    pb.median_total_payment     AS median,
                    pb.p90_total_payment        AS p90,
                    pb.peer_count,
                    br.total_medicare_payment
                        / NULLIF(pb.median_total_payment, 0) AS ratio
                FROM billing_records br
                JOIN providers p ON br.npi = p.npi
                JOIN peer_benchmarks pb
                  ON pb.specialty   = p.specialty
                 AND pb.state       = p.state
                 AND pb.hcpcs_code  = br.hcpcs_code
                 AND pb.year        = %(year)s
                WHERE br.year = %(year)s
                  AND p.specialty IS NOT NULL
                  AND p.state IS NOT NULL
                  AND br.total_medicare_payment > 0
                  AND pb.peer_count >= %(min_peers)s
                  AND pb.median_total_payment > 0
                  AND br.total_medicare_payment
                        / NULLIF(pb.median_total_payment, 0) >= %(min_ratio)s
            )
            SELECT
                npi, hcpcs_code, hcpcs_description,
                specialty, state, peer_count,
                billed, median, p90,
                ROUND(ratio::numeric, 2)  AS ratio,
                CASE WHEN ratio >= 50 THEN 1
                     WHEN ratio >= 20 THEN 2
                     ELSE 3 END           AS severity,
                ROUND(LEAST(0.5 + ratio / 200.0, 0.99)::numeric, 3) AS confidence,
                GREATEST(billed - p90, 0) AS est_overpay
            FROM flagged
            ORDER BY ratio DESC
        """

        cur.execute(part2_sql, {
            "year": args.year,
            "min_ratio": args.min_ratio,
            "min_peers": args.min_peers,
        })
        part2_rows = cur.fetchall()
        log.info("    → %d HCPCS-level flags", len(part2_rows))

        if args.dry_run and part2_rows:
            for r in part2_rows[:3]:
                log.info("    SAMPLE sev=%d %.0f× — %s | %s (%s, %s) $%s vs $%s median",
                         r["severity"], float(r["ratio"]),
                         r["npi"], r["hcpcs_code"],
                         r["specialty"], r["state"],
                         f"{float(r['billed']):,.0f}",
                         f"{float(r['median']):,.0f}")

        part2_flags = []
        for r in part2_rows:
            billed = float(r["billed"])
            median = float(r["median"])
            p90    = float(r["p90"] or median)
            ratio  = float(r["ratio"])
            desc   = (r["hcpcs_description"] or r["hcpcs_code"] or "")[:80]
            part2_flags.append({
                "npi":        r["npi"],
                "flag_type":  "billing_volume",
                "layer":      1,
                "severity":   int(r["severity"]),
                "confidence": float(r["confidence"]),
                "year":       args.year,
                "flag_value": ratio,
                "peer_value": round(median, 2),
                "explanation": (
                    f"Billed {r['hcpcs_code']} ({desc}) at {ratio:.0f}× the peer median "
                    f"for {r['specialty']} in {r['state']}: "
                    f"${billed:,.0f} vs ${median:,.0f} peer median "
                    f"(peer group: {r['peer_count']:,} providers). "
                    f"Estimated excess above P90: ${max(billed - p90, 0):,.0f}."
                ),
                "estimated_overpayment": round(max(billed - p90, 0), 2),
                "hcpcs_code": r["hcpcs_code"],
            })

        all_flags  = part1_flags + part2_flags
        all_npis   = list({f["npi"] for f in all_flags})
        log.info("  Total: %d flags across %d distinct providers",
                 len(all_flags), len(all_npis))

        if args.dry_run:
            log.info("  (dry-run — no writes)")
            return

        # ══════════════════════════════════════════════════════════════════════
        # WRITE
        # ══════════════════════════════════════════════════════════════════════

        # Deactivate stale Layer 1 flags for this year
        cur.execute("""
            UPDATE fraud_flags SET is_active = FALSE
            WHERE layer = 1 AND year = %s AND is_active = TRUE
        """, (args.year,))
        log.info("  Deactivated %d stale Layer 1 flags", cur.rowcount)

        # Insert in batches
        if all_flags:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO fraud_flags
                  (npi, flag_type, layer, severity, confidence, year,
                   flag_value, peer_value, explanation, estimated_overpayment,
                   hcpcs_code, is_active, created_at)
                VALUES
                  (%(npi)s, %(flag_type)s, %(layer)s, %(severity)s, %(confidence)s, %(year)s,
                   %(flag_value)s, %(peer_value)s, %(explanation)s, %(estimated_overpayment)s,
                   %(hcpcs_code)s, TRUE, NOW())
            """, all_flags, page_size=500)
            log.info("  Inserted %d fraud_flags", len(all_flags))

        # Update flag_count on affected providers
        if all_npis:
            cur.execute("""
                UPDATE providers p
                SET flag_count = (
                    SELECT COUNT(*) FROM fraud_flags ff
                    WHERE ff.npi = p.npi AND ff.is_active = TRUE
                )
                WHERE npi = ANY(%s)
            """, (all_npis,))
            log.info("  Updated flag_count for %d providers", cur.rowcount)

            # Update risk_tier based on worst active flag
            cur.execute("""
                UPDATE providers p
                SET risk_tier = CASE
                    WHEN EXISTS (SELECT 1 FROM fraud_flags ff
                                 WHERE ff.npi = p.npi AND ff.is_active = TRUE AND ff.severity = 1)
                         THEN 1
                    WHEN EXISTS (SELECT 1 FROM fraud_flags ff
                                 WHERE ff.npi = p.npi AND ff.is_active = TRUE AND ff.severity = 2)
                         THEN 2
                    WHEN EXISTS (SELECT 1 FROM fraud_flags ff
                                 WHERE ff.npi = p.npi AND ff.is_active = TRUE AND ff.severity = 3)
                         THEN 3
                    ELSE COALESCE(risk_tier, 4)
                END
                WHERE npi = ANY(%s)
            """, (all_npis,))
            log.info("  Updated risk_tier for %d providers", cur.rowcount)

        conn.commit()

        # Summary
        cur.execute("""
            SELECT severity, COUNT(*) AS n
            FROM fraud_flags
            WHERE layer = 1 AND year = %s AND is_active = TRUE
            GROUP BY severity ORDER BY severity
        """, (args.year,))
        bd = {r["severity"]: r["n"] for r in cur.fetchall()}
        log.info("  ✓ Layer 1 complete — critical: %d  high: %d  medium: %d",
                 bd.get(1, 0), bd.get(2, 0), bd.get(3, 0))

        cur.execute("""
            SELECT risk_tier, COUNT(*) AS n
            FROM providers WHERE risk_tier IS NOT NULL
            GROUP BY risk_tier ORDER BY risk_tier
        """)
        tiers = {r["risk_tier"]: r["n"] for r in cur.fetchall()}
        log.info("  Provider risk tiers — critical: %s  high: %s  medium: %s  low: %s",
                 f"{tiers.get(1,0):,}", f"{tiers.get(2,0):,}",
                 f"{tiers.get(3,0):,}", f"{tiers.get(4,0):,}")

    except Exception:
        conn.rollback()
        log.exception("Layer 1 detection failed — rolled back")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run(parse_args())
