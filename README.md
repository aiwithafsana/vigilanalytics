# Vigil National Fraud Graph

Session setup / Acceptance Gate 0 is complete. Acquisition and stages 1–5
are not implemented. `pipeline.py` deliberately fails with an explanation
until the subsequent phases implement the runner.

## Setup and verification

Python 3.11 or newer is required. Only the standard library is used at Gate 0;
pytest is the development test dependency.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install 'pytest>=8,<10'
.venv/bin/python -m pytest
```

## Sealing vintages

Fetchers in `ingest/` will call `common.manifest.seal(vintage_dir)` after
completing and validating downloads. Optional `metadata` maps relative file
paths to `fetched_at` and `source_url` strings. Fetchers must provide actual
download metadata; for manual files, the default timestamp is sealing time
and the default URL is empty. Raw files must never be modified after sealing.

The manifest contains sorted relative paths, SHA-256 values, byte counts,
fetch times, and source URLs. Re-sealing verifies the exact file inventory
without rewriting the manifest. Changed, removed, or added files raise
`ImmutabilityError`. Empty vintages and symlinks are rejected. Complete
all writes before sealing; concurrent source-file writers are unsupported.
Manifests detect accidental source changes, not malicious edits to both the
manifest and its source files.

## Gate 0 validation

- Directory skeleton and exact `CLAUDE.md` invariants created.
- Ten pytest cases pass, including same-length mutation detection and
  byte-identical repeated sealing.
- Baseline pytest was invoked before implementation: no tests existed
  (exit status 5). After implementation: 10 passed on Python 3.13.15.
- Data folders are gitignored except for their placeholder files.

Next session: Phase A acquisition. No source data was downloaded in this phase.
