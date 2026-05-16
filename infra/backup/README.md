# Vigil — Database Backup and Point-in-Time Recovery

This directory contains the backup and disaster-recovery configuration for
the Vigil Postgres database.

## Recovery objectives

Per `docs/data_handling_policy.md` §7:

| Metric | Target |
|---|---|
| RPO (data loss tolerance) | 15 minutes |
| RTO (downtime tolerance)  | 4 hours |

## Backup strategy

Three concurrent layers:

1. **Continuous WAL archive** — every transaction is shipped to S3 within seconds.
   Enables point-in-time recovery to any moment in the last 35 days.
2. **Daily snapshot** — full DB snapshot taken at 03:00 UTC, retained 35 days.
3. **Monthly archive** — first daily snapshot of each calendar month, retained 7 years
   (matches the audit-log retention requirement for FCA statute of limitations).

All three are encrypted with a separate KMS key from the production database
encryption key (key-segregation requirement, §4.3).

## Files

- `pg_dump.sh` — one-shot snapshot script.  Used for daily snapshots and
  ad-hoc backups.  Run from a host with `pg_dump` 16+ installed.
- `restore_from_snapshot.sh` — restore a snapshot into a fresh DB.  Used in
  DR drills and after a confirmed data-loss event.
- `verify_backup.sh` — restore the latest snapshot into a throwaway DB and
  run sanity queries.  Run nightly to detect silent backup corruption.

## Local development

For dev, no backup is needed — the database is disposable.  The scripts here
are for staging and production.

## Production deployment

The recommended production setup uses **AWS RDS for Postgres** with managed
backups.  This file documents the equivalent self-hosted setup for
environments where managed Postgres is not available (some govt-cloud
deployments).

### AWS RDS setup (preferred)

```hcl
resource "aws_db_instance" "vigil" {
  identifier              = "vigil-prod"
  engine                  = "postgres"
  engine_version          = "16.4"
  instance_class          = "db.t4g.medium"
  allocated_storage       = 100
  storage_encrypted       = true
  kms_key_id              = aws_kms_key.vigil_db.arn

  # Backup configuration — meets RPO/RTO targets in §7
  backup_retention_period = 35           # days; supports daily restore
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:04:00-sun:05:00"

  # Monthly long-term archives via AWS Backup; separate KMS key
  copy_tags_to_snapshot   = true
  deletion_protection     = true
  skip_final_snapshot     = false

  performance_insights_enabled = true
}

# 7-year monthly archive — separate KMS key for key-segregation
resource "aws_backup_plan" "vigil_monthly" {
  name = "vigil-monthly-archive"
  rule {
    rule_name           = "monthly-7yr"
    target_vault_name   = aws_backup_vault.vigil_archive.name
    schedule            = "cron(0 5 1 * ? *)"   # 1st of month, 05:00 UTC
    lifecycle {
      cold_storage_after = 90
      delete_after       = 2555                  # 7 years
    }
  }
}
```

### Self-hosted setup

For environments without RDS (FedRAMP-restricted regions, certain govt
clouds), configure the Postgres host with WAL archiving:

```ini
# postgresql.conf
archive_mode = on
archive_command = 'aws s3 cp %p s3://vigil-wal-archive/$(hostname)/%f --sse aws:kms --sse-kms-key-id <KMS_KEY_ARN>'
wal_level = replica
archive_timeout = 60        # ensure WAL ships at least every minute
```

Run `pg_dump.sh` via cron at 03:00 UTC for daily snapshots.

## Drills

Per §7, run a quarterly DR drill:

```bash
./verify_backup.sh --full-restore
```

Document the actual RPO/RTO achieved.  Update the operations runbook if
either metric exceeds its target.

## Incident response

If a confirmed data-loss event occurs:

1. Page on-call via the SEV-1 channel (see incident-response playbook).
2. Determine the recovery target time (just before the corrupting event).
3. Run `restore_from_snapshot.sh --target-time "<YYYY-MM-DD HH:MM:SS UTC>"`.
4. Verify restored data with `verify_backup.sh --no-restore --sanity-only`.
5. Promote the restored database to primary.
6. Resume operations.
7. Begin post-mortem within 14 days.
