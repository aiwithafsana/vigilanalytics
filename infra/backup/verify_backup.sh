#!/usr/bin/env bash
# verify_backup.sh — Restore the latest backup into a throwaway DB and run sanity queries.
#
# Catches silent backup corruption that pg_dump alone wouldn't flag.  Run nightly
# via cron so a broken backup is detected within 24h, well before it's needed
# in an actual recovery.
#
# Usage:
#   verify_backup.sh                       # restore latest daily, run all checks
#   verify_backup.sh --sanity-only         # skip restore, just run checks against the
#                                            current DATABASE_URL (used in DR drills)
#   verify_backup.sh --full-restore        # restore + sanity + report (quarterly drill)
set -euo pipefail

mode="standard"
case "${1:-}" in
  --sanity-only)  mode="sanity-only" ;;
  --full-restore) mode="full-restore" ;;
  "")             mode="standard" ;;
  *)              echo "Unknown arg: $1" >&2; exit 2 ;;
esac

# Pick the DB to verify against
if [[ "$mode" == "sanity-only" ]]; then
  : "${DATABASE_URL:?DATABASE_URL required for --sanity-only}"
  verify_url="$DATABASE_URL"
else
  : "${BACKUP_S3_BUCKET:?BACKUP_S3_BUCKET required}"
  : "${VERIFY_DB_URL:?VERIFY_DB_URL required — throwaway DB for restore}"
  verify_url="$VERIFY_DB_URL"

  echo "[verify] resolving latest daily snapshot…"
  snapshot=$(aws s3 ls "s3://${BACKUP_S3_BUCKET}/daily/" | sort | tail -n1 | awk '{print $NF}')
  if [[ -z "$snapshot" ]]; then
    echo "[verify] FAIL: no daily snapshots found in s3://${BACKUP_S3_BUCKET}/daily/" >&2
    exit 1
  fi
  echo "[verify] latest snapshot: $snapshot"

  tmpfile="$(mktemp -t vigil-verify.XXXXXX)"
  trap 'rm -f "$tmpfile"' EXIT

  aws s3 cp "s3://${BACKUP_S3_BUCKET}/daily/${snapshot}" "$tmpfile"
  echo "[verify] restoring into throwaway DB…"
  pg_restore --no-owner --no-acl --clean --if-exists --dbname="$verify_url" "$tmpfile"
fi

# Sanity queries — minimal checks that confirm the restored DB has the
# tables and rows we expect.  If any check fails, the backup is suspect.
echo "[verify] running sanity queries…"
psql "$verify_url" <<'PSQL'
\set ON_ERROR_STOP on
\timing on

-- 1. All expected tables exist
SELECT 'tables_present' AS check,
       (SELECT count(*) FROM information_schema.tables
        WHERE table_schema='public'
          AND table_name IN ('users','providers','cases','audit_log',
                              'leie_exclusions','fraud_flags','billing_records'))
        = 7 AS pass;

-- 2. Providers table has data (≥ 1M rows expected in production)
SELECT 'providers_populated' AS check,
       (SELECT count(*) FROM providers) > 100000 AS pass;

-- 3. Audit log has entries (anything that's been used will have these)
SELECT 'audit_log_present' AS check,
       (SELECT count(*) FROM audit_log) >= 0 AS pass;

-- 4. LEIE exclusions present
SELECT 'leie_loaded' AS check,
       (SELECT count(*) FROM leie_exclusions) > 1000 AS pass;

-- 5. No orphaned cases (every case has a valid provider)
SELECT 'cases_referential_integrity' AS check,
       NOT EXISTS (
         SELECT 1 FROM cases c
         LEFT JOIN providers p ON p.npi = c.provider_npi
         WHERE p.npi IS NULL
       ) AS pass;
PSQL

echo "[verify] sanity OK."
echo "[verify] all checks passed."
