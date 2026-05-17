#!/usr/bin/env bash
# pg_dump.sh — Create an encrypted snapshot of the Vigil database.
#
# Usage:
#   pg_dump.sh                       # daily snapshot, retained 35 days
#   pg_dump.sh --monthly             # monthly archive, retained 7 years
#   pg_dump.sh --ad-hoc <label>      # ad-hoc snapshot tagged with <label>
#
# Required environment variables:
#   DATABASE_URL          postgresql://...   source DB
#   BACKUP_S3_BUCKET      vigil-db-backups   destination bucket
#   BACKUP_KMS_KEY_ID     arn:aws:kms:...    KMS key for at-rest encryption
#                                            (MUST be different from prod DB key)
#
# The KMS key requirement enforces the key-segregation principle from
# docs/data_handling_policy.md §4.3 — compromise of the production DB key
# must not yield access to backups.
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"
: "${BACKUP_S3_BUCKET:?BACKUP_S3_BUCKET is required}"
: "${BACKUP_KMS_KEY_ID:?BACKUP_KMS_KEY_ID is required}"

mode="daily"
label=""
case "${1:-}" in
  --monthly)  mode="monthly" ;;
  --ad-hoc)
    mode="ad-hoc"
    label="${2:?--ad-hoc requires a <label>}"
    ;;
  "")         mode="daily" ;;
  *)
    echo "Unknown argument: $1" >&2
    exit 2
    ;;
esac

ts="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
host="$(hostname -s)"
case "$mode" in
  monthly) key="monthly/vigil-${ts}.dump" ;;
  ad-hoc)  key="ad-hoc/${label}-${ts}.dump" ;;
  daily)   key="daily/vigil-${host}-${ts}.dump" ;;
esac

tmpfile="$(mktemp -t vigil-dump.XXXXXX)"
trap 'rm -f "$tmpfile"' EXIT

echo "[backup] mode=$mode key=$key host=$host"
echo "[backup] dumping…"

# pg_dump in custom format — supports parallel restore, smaller than plaintext.
pg_dump \
    --format=custom \
    --no-owner \
    --no-acl \
    --compress=6 \
    --file="$tmpfile" \
    "$DATABASE_URL"

size_bytes=$(stat -f%z "$tmpfile" 2>/dev/null || stat -c%s "$tmpfile")
echo "[backup] dump complete: ${size_bytes} bytes"

echo "[backup] uploading to s3://${BACKUP_S3_BUCKET}/${key}"
aws s3 cp \
    "$tmpfile" \
    "s3://${BACKUP_S3_BUCKET}/${key}" \
    --sse aws:kms \
    --sse-kms-key-id "$BACKUP_KMS_KEY_ID" \
    --metadata "mode=${mode},host=${host},timestamp=${ts}"

echo "[backup] done."

# Apply retention policy based on mode.  Object lifecycle on the S3 bucket
# should also enforce these — this script-level cleanup is a safety net.
case "$mode" in
  daily)
    cutoff=$(date -u -d '35 days ago' +%Y-%m-%d 2>/dev/null || date -u -v-35d +%Y-%m-%d)
    aws s3 ls "s3://${BACKUP_S3_BUCKET}/daily/" | \
      awk -v cutoff="$cutoff" '$1 < cutoff {print $NF}' | \
      while read -r f; do
        echo "[backup] purging daily/${f} (older than 35 days)"
        aws s3 rm "s3://${BACKUP_S3_BUCKET}/daily/${f}"
      done
    ;;
  monthly)
    # 7-year retention is enforced by S3 lifecycle policy; do nothing here.
    ;;
esac
