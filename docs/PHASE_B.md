# Phase B — implementation and blocked acceptance gate

Status: incomplete. Implementation and fixture tests are ready for review;
Phase B has not passed its real-data acceptance gate. No entity resolution or
later graph stages were started.

## Real-data run

`python pipeline.py --through stage1` read the sealed February 2026 NPPES full
file and stopped on the prescribed name-quality threshold:

| Check | Observed | Required |
| --- | ---: | ---: |
| NPPES provider records | 9,368,082 | All sealed source records retained |
| Empty normalized names | 333,609 | — |
| Empty normalized-name rate | 3.5611238245% | Strictly below 0.5% |
| Empty raw names | 333,584 | Reported for diagnosis |
| Missing entity type | 333,563 | Reported for diagnosis |
| Missing enumeration date | 333,563 | Reported for diagnosis |

```text
ERROR: NPPES name_norm missing rate 3.5611% violates <0.5% gate
```

The source is
`data/raw/nppes/2026-02/npidata_pfile_20050523-20260208.csv`, protected by its
vintage manifest. Stable row IDs preserve evidence for every source row.
The normalizer does not filter away nameless records or invent names to meet
the threshold. The counts suggest a broad source-data issue; this run did not
establish the individual records' deactivation status.

No canonical files were published. The temporary database was cleaned up on
failure; sealed raw inputs remain unchanged. Because the first real run could
not finish, the full-data byte-identical rerun gate remains unverified.

## Implemented behavior

- Source-specific normalizers for NPPES, LEIE, CMS Part B, shared patients,
  PECOS, and optional SAM.
- All required canonical table schemas, plus a PAC/associate-ID crosswalk so
  later ownership joins can operate entirely on canonical data.
- usaddress-based address keys, suite/unit collapse, name/credential cleanup,
  phone normalization, strict source dates, and explicit missing values.
- Exact closed list of fraud-related authorities: 1128(a)(1), 1128(a)(3), and
  1128(b)(7). SAM federal exclusions are not presumed HHS program-fraud events.
- NPPES primary taxonomy/license selection follows the primary switch.
- Part B summaries retain provider-level total payment and beneficiary counts;
  top five HCPCS use payment rank with a deterministic code tie-breaker.
- Shared patient counts use unique beneficiaries (fourth source column), not
  encounter/pair counts (third column), and retain the 2015 data year.
- PECOS supports all available enrollment/All_Owners vintages and explicitly
  transcodes legacy Windows-1252 text without changing raw bytes.
- Ordered, fixed-row-group Parquet publication and per-source row/missing-rate
  reports. No wall-clock timestamps are included in canonical data.

## Tests and gate checklist

84 tests pass on Python 3.13.15, including the original 38 acquisition tests.
Utility coverage: 11 name cases, 11 address cases, 10 phone cases, and 9 fraud
label cases. The suite builds every canonical table from sealed fixtures,
checks exact column types and representative values, and verifies identical
SHA-256 hashes on two complete fixture runs. It also proves that mutated raw
inputs, unsealed vintages, and the NPPES name-quality failure prevent publication.

- [x] Canonical normalizers and DAG command implemented.
- [x] At least eight tests each for name, address, and phone utilities.
- [x] Complete fixture pipeline produces byte-identical Parquet across two runs.
- [x] Tests prove the missing-name gate remains strict.
- [ ] Real NPPES missing-name rate below 0.5% — **failed: 3.5611%**.
- [ ] All canonical tables built from the real sealed inputs — blocked by gate.
- [ ] Two complete real-data runs produce identical output hashes — blocked.
- [x] Tested implementation and failure evidence committed for review.

Phase B can resume after an appropriate NPPES vintage is acquired or an
explicit source-data policy is agreed with the user. The build spec's rule is
to stop when a gate cannot pass, rather than weaken it; this is that stop.
