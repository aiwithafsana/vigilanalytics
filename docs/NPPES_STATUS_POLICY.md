# NPPES identity completeness and deactivation records

CMS documents that the public monthly file includes deactivated NPIs for which
only the NPI and deactivation date are disclosed. See the
[official NPPES readme](https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand/Downloads/Data_Dissemination_File-Readme.pdf)
and [CMS dissemination page](https://www.cms.gov/medicare/regulations-guidance/administrative-simplification/data-dissemination).

The identity normalizer must distinguish expected source suppression from
unexplained missing identities. It retains every source row in `providers` and
adds a canonical `npi_status` row keyed by the same stable record ID. No name is
invented or backfilled to make the completeness check pass.

## Exact suppression predicate

A record is `identity_suppressed=true` only if:

1. The NPI is a nonzero ten-digit identifier.
2. The deactivation date is present and parses using the source date format.
3. **Every other source column is blank**, not merely the provider name or type.

The adapter examines the complete source header, including fields not projected
into the provider table. A nameless row with a reactivation date, phone, license,
credential, enumeration date, or any other populated field remains in the
identity-completeness denominator. Malformed nonempty dates fail normalization.

## Unchanged threshold, explicit population

The threshold remains strictly less than 0.5% missing normalized names. It is
measured over records whose identity fields are not subject to this exact,
documented source suppression. The unfiltered missing-name rate is still
reported alongside the eligibility count; expected blank records are not hidden.
A vintage containing only suppressed records cannot pass the identity gate.

For the sealed February 2026 file:

| Measure | Count or rate |
| --- | ---: |
| All source/provider rows retained | 9,368,082 |
| Exact NPI/date-only deactivations | 333,563 |
| Identity records checked | 9,034,519 |
| Missing normalized identity names | 46 |
| Identity missing-name rate | 0.0005091583% |
| Required identity missing-name rate | <0.5% |
| Unfiltered normalized-name missing rate | 3.5611238245% |

The diagnostic `python -m ingest.audit_nppes --vintage 2026-02` independently
checks the sealed raw file. It found 21 unexplained *raw* missing names; the
normalization gate correctly counts an additional 25 names that become empty
after credential/punctuation cleanup, for a total of 46.

## Canonical status and downstream use

`npi_status(record_id, source, vintage, npi, status, deactivation_date,
reactivation_date, identity_suppressed)` preserves lifecycle evidence separately
from provider identity. Status is one of `deactivated_suppressed`, `deactivated`,
`reactivated`, or `no_deactivation_reported`. The last status describes the
available source fields; it is not a conclusion about clinical activity.

Suppressed provider rows retain their NPI, record ID, and empty identity fields.
They can later be linked to billing or other identities by exact NPI; empty names
must never justify a fuzzy entity merge. Deactivation alone is not a fraud label.
No raw input or raw manifest is edited.

Regression tests verify retained row counts and shared record IDs, dates, the
entire-column suppression rule, reactivation handling, and the strict 0.5%
boundary (0.5% fails; values below it pass).
