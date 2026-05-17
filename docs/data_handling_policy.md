# Vigil Data Handling, Retention, and Incident Response Policy

**Version:** 1.0
**Effective:** 2026-05-10
**Owner:** Vigil security officer
**Review cadence:** annually, or after any incident
**Classification:** For external review by counsel and prospective customers

> This document is a starting framework, **not** a substitute for review by qualified
> counsel and a security professional before deploying Vigil to a paying customer.
> Sections marked `[TODO: legal]` require attorney attestation before customer use.

---

## 1. Purpose and scope

This policy governs how Vigil ingests, stores, processes, transmits, retains,
and destroys data used for Medicare fraud detection. It covers:

- **Public source data** — CMS Medicare Part B Public Use File, OIG LEIE
- **Customer-generated data** — case files, investigator notes, case outcomes,
  uploaded documents, audit logs
- **Operational data** — user accounts, authentication tokens, MFA secrets,
  backup-code hashes

It does **not** govern:

- Underlying CMS claim records that customers obtain via their own subpoena or DUA
- Customer-side data the customer pulls into Vigil from external sources
  (PDMP, state Medicaid, etc.) — those are governed by the customer's own policies

---

## 2. Data classification

| Tier | Examples | Storage requirements |
|---|---|---|
| **Public** | CMS Part B PUF rows, LEIE exclusions, computed risk scores | Standard at-rest encryption on host volume |
| **Customer-confidential** | Case files, investigator notes, case outcomes, uploaded documents, audit logs | At-rest encryption + access logs |
| **Authentication secrets** | Hashed passwords, MFA secrets (column-encrypted in production), backup-code hashes, JWT signing key | At-rest encryption + KMS/HSM for keys + no plaintext logging |
| **Personally identifiable information (PII)** | Investigator email, name, IP address | At-rest encryption + minimization (only what's required) |

Vigil **does not** store any PHI (Protected Health Information) about Medicare
beneficiaries. Underlying claim records remain with CMS and are not transmitted to
Vigil. This is a deliberate design choice to reduce HIPAA scope.

---

## 3. Data residency

- All production data is stored within the United States.
- The production environment runs in AWS US-East-1 (Virginia) or US-East-2 (Ohio).
- No customer data crosses US borders for any reason — including backup, log
  aggregation, error reporting, or analytics.
- AWS region pinning is enforced at the Terraform / IaC level (see infra repo).
- Use of any non-US-region service (including AI/LLM APIs) is **prohibited** for
  endpoints that process customer-confidential data. Public data analysis may
  use US-only AI services with an audit-logged audit trail.

---

## 4. Encryption

### 4.1 In transit

- All HTTP traffic served via TLS 1.3 (minimum TLS 1.2) with HSTS enabled.
- Certificate issued by Let's Encrypt or AWS Certificate Manager.
- WebSocket connections use the same TLS termination as HTTP.
- Internal service-to-service communication (backend ↔ Postgres) over the AWS
  VPC private network; if cross-AZ, TLS-encrypted.

### 4.2 At rest

- **Database** — AES-256 at the storage volume layer (AWS EBS encrypted by default).
- **Authentication secrets** — `mfa_secret` and `mfa_backup_codes` are
  column-level encrypted via AWS KMS envelope encryption in production.
  Development environments may use plaintext for ease of debugging — see
  methodology doc §10.
- **Document uploads** — AES-256 at the object-store level (AWS S3 SSE-KMS).

### 4.3 Key management

- JWT signing key — rotated quarterly; old keys retained for `refresh_token_expire_days`
  + 7 days, then destroyed.
- MFA encryption key — managed in AWS KMS; rotation policy `aws:rotation_enabled = true`.
- TLS private key — managed by AWS Certificate Manager; auto-renewed.

---

## 5. Access controls

### 5.1 Authentication

- Email + password (minimum 12 characters, mixed case, digit, special).
- Mandatory TOTP MFA for all production accounts after 30-day grace period.
- Account lockout after 5 failed attempts (15-minute lock).
- Session inactivity timeout: `[TODO: policy decision — recommend 15 min for admin, 30 min for analyst]`.

### 5.2 Authorisation

- Three roles: `admin`, `analyst`, `viewer`.
- State-level data partitioning — analysts see only providers in states listed
  in their `state_access` array.
- Admin operations require admin role; admin actions are audit-logged.
- No anonymous access. All endpoints except `/api/health` and `/api/ready` require authentication.

### 5.3 Audit

- Every login (success and failure) is logged.
- Every write operation (POST/PUT/PATCH/DELETE) is logged via middleware.
- Every PDF/CSV export is logged with target, user, IP, methodology version.
- Every case-outcome change is logged with attestation reference.
- Audit logs are append-only — no UPDATE or DELETE permitted on `audit_log` table.

---

## 6. Retention

| Data type | Retention | Reason |
|---|---|---|
| Audit logs | 7 years | False Claims Act statute of limitations (31 USC §3731) |
| Case files (resolved) | 7 years from resolution | Same |
| Provider scores | Current + 1 prior year | Investigator may need historical comparison |
| User accounts | Until deactivation + 90 days, then anonymised | Forensic availability |
| Authentication failures | 90 days | SOC monitoring window |
| MFA secrets | Until disabled by user | Required for active TOTP |
| LEIE snapshots | Indefinite (small data) | Reconstructing historical state for litigation |
| Backups | 35 days rolling + monthly archives for 7 years | RTO/RPO + retention parity |

After retention expiry, data is **cryptographically erased** (KMS key destruction
for column-encrypted data) and the underlying row is purged via scheduled job.

---

## 7. Backup and disaster recovery

- **RPO** (recovery point objective): 15 minutes
- **RTO** (recovery time objective): 4 hours

Implementation:

- Postgres point-in-time recovery enabled with continuous WAL archive to S3.
- Daily snapshot retained for 35 days.
- Monthly snapshot retained for 7 years (compliance with retention policy §6).
- Backups encrypted with separate KMS key from production (key-segregation).
- Quarterly DR drill: restore from backup into isolated environment, verify
  data integrity, document RPO/RTO actually achieved.

---

## 8. Incident response

### 8.1 Severity classification

| Severity | Definition | Response time |
|---|---|---|
| **SEV-1** | Data breach (confirmed or suspected), unauthorized access to customer data, ransomware, system compromise | 1 hour to incident commander, 4 hours to customer notification |
| **SEV-2** | Authentication bypass, privilege escalation, MFA bypass, audit-log tampering | 4 hours |
| **SEV-3** | Service outage, degraded performance, single-user access issue | 24 hours |
| **SEV-4** | Cosmetic issue, documentation error | 5 business days |

### 8.2 Incident response playbook

On detection (alert, customer report, or internal observation) of SEV-1 or SEV-2:

1. **Contain** — disable affected accounts; revoke active JWTs by incrementing
   `token_version` on affected user rows; rotate JWT signing key if compromise of
   signing key is suspected.
2. **Preserve** — snapshot audit logs, application logs, and database to
   immutable storage; do not modify any data under investigation.
3. **Notify** — incident commander → security officer → CEO → customer (within
   SLA), in that order.
4. **Investigate** — review audit logs, application logs, AWS CloudTrail, VPC
   flow logs. Document timeline.
5. **Remediate** — patch the underlying cause, deploy, verify fix in
   production.
6. **Post-mortem** — within 14 days, publish a customer-facing post-mortem
   with timeline, root cause, remediation, and prevention steps.

### 8.3 Breach notification

Per the operating jurisdiction's laws (typically state breach-notification
statutes + HIPAA Breach Notification Rule when applicable):

- **Affected individuals** — written notice within 60 days, regardless of state.
- **State AG** — per state law (varies 14-60 days).
- **HHS-OCR** — if PHI involved, within 60 days (HIPAA §164.408). Vigil
  does not store PHI in normal operation, but if a breach somehow involved a
  customer's uploaded PHI document, the customer is the covered entity for
  notification purposes and Vigil acts as their business associate.

`[TODO: legal — finalize breach-notification thresholds and templates with state-by-state counsel before first government customer.]`

---

## 9. Third-party data flows

Vigil's source data and external dependencies, with destinations:

| Data flow | Source | Destination | Frequency | Encryption |
|---|---|---|---|---|
| CMS Part B PUF download | data.cms.gov (HTTPS) | Vigil ingest service | Annual | TLS in transit, AES-256 at rest |
| LEIE refresh | oig.hhs.gov (HTTPS) | Vigil refresh service | Weekly | TLS in transit, AES-256 at rest |
| Customer email notifications | Vigil API | AWS SES (us-east-1) | Per event | TLS, no PHI in email body |
| Error monitoring | Vigil API | `[TODO: choose vendor]` | Per error | TLS, no customer data in error payloads |

No data flows leave the United States. No customer data is shared with
third-party AI providers in normal operation. The AI-generated "investigative
narrative" feature operates on aggregated, de-identified billing data only and
runs against a US-region LLM endpoint with audit logging of every call.

---

## 10. Subprocessor list

Current subprocessors (entities that may process customer data on Vigil's behalf):

| Subprocessor | Service | Data category | Location |
|---|---|---|---|
| Amazon Web Services | Hosting, storage, KMS, networking | All tiers | US-East |
| Let's Encrypt or AWS ACM | TLS certificates | None (cert metadata only) | US |
| AWS SES | Transactional email | User email addresses only | US-East |

Adding a subprocessor that processes customer-confidential or
authentication-secret data requires customer notice and security review.

---

## 11. Customer data return and destruction

Upon contract termination:

- Customer is notified 30 days before data destruction.
- Customer may request a final export of their case files, notes, and audit
  logs in a portable format (JSON + CSV).
- 30 days after termination notice, all customer-confidential data is
  cryptographically erased.
- A destruction certificate is issued to the customer.

Public data (provider records, LEIE) is **not** customer data and is not
destroyed — it remains in Vigil for other customers.

---

## 12. Acceptable use

Customers and their users agree that Vigil is used solely for:

- Authorized fraud investigation in connection with Medicare or related
  programs (where the customer has the legal authority to conduct such
  investigation).
- Litigation support for False Claims Act actions or related civil enforcement.
- Internal compliance review by entities subject to CMS program integrity rules.

Customers **may not** use Vigil to:

- Conduct surveillance of providers not in connection with a legitimate
  investigation.
- Make individual employment, credit, insurance, or licensing decisions about
  any provider based solely on a Vigil risk score.
- Republish Vigil scores in public form without methodology context.
- Train competing fraud-detection systems on Vigil output.

---

## 13. Methodology change disclosure

Material changes to the model methodology that would alter risk scores by more
than 5% on average are disclosed to active customers at least 14 days before
deployment. The change is logged in `docs/methodology.md` §11 (change log) and
the new methodology version is surfaced via `/api/system/data-vintage`.

---

## 14. Sub-policy references

- `docs/methodology.md` — Statistical methodology and validation results
- `[TODO]` `docs/security_practices.md` — Detailed SOC operations runbook
- `[TODO]` `docs/sla.md` — Service availability and support commitments
- `[TODO]` `docs/terms_of_service.md` — Commercial terms
- `[TODO]` `docs/privacy_policy.md` — User-facing privacy notice

---

## 15. Contact

- Security incidents: `security@[domain]` (24/7 PagerDuty)
- Data subject requests: `privacy@[domain]`
- Customer support: `support@[domain]`
- Bug bounty: `[TODO: program setup]`

---

## 16. Change log

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-05-10 | Initial release |
