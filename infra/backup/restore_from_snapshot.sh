#!/usr/bin/env bash
# restore_from_snapshot.sh — Restore a Vigil DB snapshot into a target database.
#
# Usage:
#   restore_from_snapshot.sh --snapshot s3://bucket/key.dump --target-url postgres://…
#   restore_from_snapshot.sh --latest-daily          --target-url postgres://…
#   restore_from_snapshot.sh --target-time "2026-05-10 14:30:00 UTC" \
#                            --target-url postgres://…
#
# The --target-time mode performs point-in-time recovery using WAL files
# (only available on AWS RDS or a self-hosted instance with WAL archiving).
#
# WARNING: this script will OVERWRITE the target database.  Always restore
# into a fresh DB, then promote to primary after verification.
set -euo pipefail

snapshot=""
target_time=""
target_url=""
use_latest_daily=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot)         snapshot="$2"; shift 2 ;;
    --latest-daily)     use_latest_daily=1; shift ;;
    --target-time)      target_time="$2"; shift 2 ;;
    --target-url)       target_url="$2"; shift 2 ;;
    -h|--help)
      grep '^# ' "$0" | sed 's/^# //'
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$target_url" ]]; then
  echo "Error: --target-url is required" >&2
  exit 2
fi

# Mode 1: PITR (only valid with AWS RDS or WAL-archived host)
if [[ -n "$target_time" ]]; then
  echo "[restore] PITR mode — target time: $target_time"
  if ! command -v aws >/dev/null; then
    echo "PITR requires AWS CLI" >&2
    exit 1
  fi
  : "${RDS_SOURCE_INSTANCE:?RDS_SOURCE_INSTANCE env var required for PITR}"
  : "${RDS_TARGET_INSTANCE:?RDS_TARGET_INSTANCE env var required for PITR}"
  aws rds restore-db-instance-to-point-in-time \
      --source-db-instance-identifier "$RDS_SOURCE_INSTANCE" \
      --target-db-instance-identifier "$RDS_TARGET_INSTANCE" \
      --restore-time "$target_time" \
      --no-publicly-accessible
  echo "[restore] PITR initiated.  Monitor via:"
  echo "  aws rds describe-db-instances --db-instance-identifier $RDS_TARGET_INSTANCE"
  echo "[restore] When status is 'available', point app traffic at the new instance."
  exit 0
fi

# Mode 2: Snapshot restore
: "${BACKUP_S3_BUCKET:?BACKUP_S3_BUCKET env var is required for snapshot restore}"

if [[ "$use_latest_daily" -eq 1 ]]; then
  echo "[restore] resolving latest daily snapshot…"
  snapshot=$(aws s3 ls "s3://${BACKUP_S3_BUCKET}/daily/" | sort | tail -n1 | awk '{print $NF}')
  snapshot="s3://${BACKUP_S3_BUCKET}/daily/${snapshot}"
fi

if [[ -z "$snapshot" ]]; then
  echo "Error: must specify --snapshot or --latest-daily" >&2
  exit 2
fi

echo "[restore] snapshot: $snapshot"
echo "[restore] target:   $target_url"
echo
read -r -p "This will OVERWRITE the target database.  Continue? [yes/no] " confirm
if [[ "$confirm" != "yes" ]]; then
  echo "[restore] aborted by user"
  exit 1
fi

tmpfile="$(mktemp -t vigil-restore.XXXXXX)"
trap 'rm -f "$tmpfile"' EXIT

echo "[restore] downloading snapshot…"
aws s3 cp "$snapshot" "$tmpfile"

echo "[restore] restoring…"
pg_restore \
    --no-owner \
    --no-acl \
    --clean \
    --if-exists \
    --dbname="$target_url" \
    "$tmpfile"

echo "[restore] complete."
echo "[restore] run verify_backup.sh against the target to sanity-check."
