# Vigil National Fraud Graph

Session setup (Gate 0) and acquisition (Phase A / Stage 0) are complete.
Stages 1–5 are not implemented. The next session is Phase B normalization.
See [Phase A acceptance evidence](docs/PHASE_A.md).

## Setup and commands

Python 3.11+ is required. Acquisition uses the standard library; pytest is the
development dependency. No graph, analytics, or ML dependencies are installed
at this phase.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install 'pytest>=8,<10'
.venv/bin/python -m pytest
.venv/bin/python pipeline.py status
.venv/bin/python pipeline.py status --verify
.venv/bin/python pipeline.py --through stage0
```

For this checkout, the existing environment is `../../work/venv` instead of
`.venv`. Run commands from this repository root. `VIGIL_DATA_ROOT` optionally
selects a separate data directory; tests always use temporary data directories.

`status` lists every registered source, its latest local vintage, and whole days
since its most recent acquisition timestamp. It reads manifests only; use
`status --verify` to rehash complete latest vintages. An unsealed newest directory
is shown explicitly. Acquisition staleness is separate from the age of the data:
fetching a 2015 dataset today does not make its observations current.

`--through stage0` verifies and reuses the latest available vintage of each
source. All four required sources must be present. Missing optional sources are
reported explicitly. Acquire new vintages with `fetch` or `import` before running
the stage. Later `--through` targets fail without running partial later stages.

## Acquisition

Each source module exposes `fetch(vintage: str) -> None` and seals a successful
acquisition. Manual sources print their official URL, expected filename/layout,
and destination when input is missing, then exit nonzero. Place files in an
unsealed vintage directory and run `fetch`, or use `import` to copy local files
without modifying their originals:

```sh
python pipeline.py import nppes --vintage 2026-02 \
  --file 'npidata_pfile_20050523-20260208.csv=/path/to/npidata_pfile_20050523-20260208.csv'
python pipeline.py fetch nppes --vintage 2026-02
python pipeline.py fetch leie --vintage 2026-11-05
```

Repeat `--file NAME=PATH` for multi-file sources. Optional
`--source-url NAME=HTTPS_URL` records a credential-free download URL; otherwise
the source landing page is recorded. Imports stage copies outside `data/raw`,
validate and seal them, then publish using exclusive creation. Existing vintages
are never replaced. An interrupted publication remains visibly unsealed and
requires review. Never edit, remove, or reuse a sealed vintage to update data.

LEIE automatically downloads `UPDATED.csv`. SAM uses `SAM_API_KEY` if present,
otherwise it requires a manual public extract. GSA publishes daily exclusions
extracts: a monthly SAM vintage captures the first day's extract of that month;
a full date selects that day's extract. It never substitutes the monthly
ENTITY extract. No key is stored in manifests, process arguments, or error text.
The SAM authenticated path is fixture-tested; no live key was available here.

HTTP transport defaults to Python `urllib` with verified TLS. This machine's
Python CA trust store could not verify the live connection; the real LEIE
acquisition succeeded with an explicit system transport:

```sh
VIGIL_HTTP_BACKEND=curl python pipeline.py fetch leie --vintage 2026-11-05
```

The curl transport requires an installed system `curl`, ignores user curlrc,
verifies certificates, restricts requests/redirects to HTTPS, and fails on
HTTP/transfer errors. It is never selected silently. `SSL_CERT_FILE` may instead
point Python at an installed CA bundle. Neither transport disables TLS checks.

## Validation and provenance

- NPPES requires the monthly full CSV and canonical-input header fields.
- Part B requires both official Provider and Provider/Service filenames with
  matching data years; a misleading renamed filename is rejected.
- Shared Patients preserves the year, interval, and headerless five-column
  layout. The unique beneficiary count is column four, distinct from pair count.
- PECOS requires enrollment/All_Owners pairs per provider class and release date;
  PPEF enrollment files are supported alongside them. CHOW imports are deferred.
- SAM requires the public V2 exclusions layout and matching extract date.

Bulk CSV validation checks headers, first 100 rows, and final newlines. It does
not claim a complete row-level audit; normalization must perform that work.
ZIPs receive a complete CRC check in addition to layout sampling. SHA-256 covers
all bytes of every acquired file.

The seal contains sorted relative paths, hashes, byte counts, fetch times, and
source URLs. Re-sealing verifies the exact inventory without changing manifest
bytes. Changed, removed, or added files raise `ImmutabilityError`; empty vintages
and symlinks are rejected. Complete all writes before sealing; concurrent source
writers are unsupported. Manifests detect accidental changes, not a coordinated
edit of both source files and their manifests.

Copied inputs also have a sealed `acquisition.manifest.json` recording the
original path, acquisition method/time, provenance URL, and digest. For legacy
local files the acquisition timestamp means transfer into this repo, while the
original download time remains explicitly unknown. Wall-clock timestamps occur
only in manifests. Raw and generated data are gitignored.
