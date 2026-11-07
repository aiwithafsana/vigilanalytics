# Phase B — canonical normalization acceptance evidence

Status: Phase B is complete. Two fresh full production runs produce
byte-identical outputs. Entity resolution and graph stages have not started.

## NPPES source handling resolved

CMS publishes a special deactivation record containing only an NPI and its
deactivation date. Counting those deliberately suppressed identities as failed
provider names caused the original 3.5611% result. The
[documented lifecycle policy](NPPES_STATUS_POLICY.md) now distinguishes that
exact source record type while retaining every NPI and source row ID.

| Name-quality measure | Observed |
| --- | ---: |
| All NPPES rows retained in providers and npi_status | 9,368,082 |
| Documented NPI/date-only deactivation records | 333,563 |
| Identity records checked | 9,034,519 |
| Unexplained empty normalized identity names | 46 |
| Identity missing-name rate | 0.0005091583% |
| Required identity missing-name rate | Strictly <0.5% |
| Unfiltered missing-name rate, still reported | 3.5611238245% |

No names are invented, no raw inputs are changed, and no provider rows are
removed. A nameless record with any other populated source field—including an
unprojected credential field or reactivation date—still counts against the gate.
The tests explicitly prove that a 0.5% failure rate fails and a lower rate passes.

## Canonical output from the complete real-data build

| Table | Rows |
| --- | ---: |
| providers | 14,549,207 |
| addresses | 19,608,332 |
| exclusions | 84,093 |
| billing_summaries | 1,230,293 |
| ownerships | 830,197 |
| shared_patients | 56,040,265 |
| provider_identifiers | 3,866,739 |
| npi_status | 9,368,082 |

The build publishes 18 Parquet files, totaling 1,515,284,454 bytes. Required
canonical schemas are supplemented by the PAC/associate-ID crosswalk and NPPES
lifecycle table. All are included in determinism verification.

Inputs are the sealed February 2026 NPPES full file, current acquired LEIE,
paired 2022 Part B provider/service files, the 2015 90-day shared-patient file,
and both available PECOS vintages. SAM is explicitly optional and absent.
Normalization retains data years and vintages; acquisition time does not make
older observations current.

`PHASE_B_HASHES.manifest.json` records the complete file set, SHA-256 hashes,
byte counts, row counts, schemas, and library versions. `PHASE_B_QUALITY.manifest.json`
records the per-source missing-value and row-count reports. Raw and canonical
data remain local and gitignored; only code and acceptance evidence are pushed.

## Determinism and memory

The production command is `python pipeline.py --through stage1`. Two fresh
processes rebuilt from sealed inputs, including fresh address parsing. The
comparison passed for every file's SHA-256, byte count, row count, schema, and the
complete output file set. The source hashes are verified by the ingestion adapter
before each vintage is exposed to normalization.

The address path processes 6,572,222 distinct structured addresses. Parsing uses
four processes with one 10,000-row batch per worker in flight. The lookup joins
narrow ordinals, then completes wide rows in 4,096-row batches. Source alignment
and final row-count reconciliation guard the mapping. This completes all
19,608,332 address rows under the 3 GB DuckDB limit; no record-ID-wide global
update is retained in memory. Final rows are explicitly sorted for publication.

## Acceptance checklist

- [x] All required canonical tables build from real sealed vintages in one command.
- [x] Names, addresses and phones each have at least eight utility cases.
- [x] NPPES identity name-quality rate is below 0.5%, with expected suppression separately audited.
- [x] Every NPPES source record remains represented, with canonical lifecycle evidence.
- [x] Serial and parallel fixture runs produce byte-identical Parquet.
- [x] Address completion passes a 200,000-row, wide-ID regression under a 64 MB limit.
- [x] Source-row misalignment is rejected before address completion.
- [x] 94 pytest tests pass, including mutation and strict quality-gate failures.
- [x] Two fresh full production runs produce byte-identical Parquet across all 18 files.
- [x] Acceptance evidence versioned alongside the implementation.

The implementation remains deterministic batch processing with DuckDB, Python,
and Parquet. Enforcement authorities remain labels; no ML, entity resolution,
graph database, service, or public UI is introduced in this phase.


To reproduce the complete comparison after installing the pinned dependencies,
run this from the component root (it performs two complete builds):

```python
import hashlib
from pathlib import Path
import subprocess
import sys


def snapshot():
    result = {}
    for path in sorted(Path("data/canonical").rglob("*.parquet")):
        with path.open("rb") as stream:
            result[str(path)] = hashlib.file_digest(stream, "sha256").hexdigest()
    assert result, "No canonical outputs"
    return result


subprocess.run([sys.executable, "pipeline.py", "--through", "stage1"], check=True)
first = snapshot()
subprocess.run([sys.executable, "pipeline.py", "--through", "stage1"], check=True)
assert snapshot() == first
```
