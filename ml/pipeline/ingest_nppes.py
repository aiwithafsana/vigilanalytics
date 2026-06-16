"""
ingest_nppes.py — Stream-extract NPPES enrichment for CMS providers.

The full NPPES public file is ~7GB compressed.  We don't need most of it —
just five columns per NPI for the providers already in our CMS Part B
dataset.  This module streams the NPPES CSV in chunks, filters to our
provider set, and writes a small enrichment parquet (a few hundred MB).

Columns extracted
-----------------
- ``npi``                 — match key
- ``enumeration_date``    — NPI issuance date.  Used to compute months_enrolled
                            (new providers billing high volume = red flag).
- ``entity_type``         — 1 = individual, 2 = organization.  Already in
                            providers table but NPPES has authoritative values.
- ``taxonomy_primary``    — fine-grained provider taxonomy code (more specific
                            than CMS specialty string)
- ``is_sole_proprietor``  — Y/N — sole-proprietor practices are
                            disproportionately represented in OIG fraud actions.
- ``provider_state``      — for cross-checking CMS state matches NPPES state

Usage
-----
    # Download + ingest (~7GB download, ~30 min):
    python -m pipeline.ingest_nppes

    # Or, if you already have the CSV locally:
    python -m pipeline.ingest_nppes --local /path/to/npidata_pfile_*.csv

The output is written to data/processed/nppes_enrichment.parquet and is
consumed by features.py when present.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

DATA_DIR = Path(__file__).parent.parent / "data"
RAW_DIR  = DATA_DIR / "raw"
PROC_DIR = DATA_DIR / "processed"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

# Bulk NPPES download endpoint.  CMS publishes a monthly snapshot at:
#   https://download.cms.gov/nppes/NPPES_Data_Dissemination_<Month>_<Year>.zip
# The filename rotates each month.  We auto-detect the latest one by walking
# back month-by-month from today until we find a file that exists on the
# server.  The hardcoded fallback below is the last-known-good URL for
# environments where outbound DNS to download.cms.gov isn't permitted.
NPPES_INDEX_URL    = "https://download.cms.gov/nppes/"
NPPES_FALLBACK_URL = (
    "https://download.cms.gov/nppes/NPPES_Data_Dissemination_April_2026.zip"
)
# Used by run_pipeline.py when constructing the URL.  May 17 2026 → check for
# May 2026 file first, then April, etc., up to 6 months back.
NPPES_DOWNLOAD_URL = NPPES_FALLBACK_URL


def _resolve_latest_nppes_url() -> str:
    """
    Walk back month-by-month from today until we find an NPPES file that
    actually exists on the server.  This avoids the "URL is stale because
    the hardcoded month rotated" failure mode without requiring the operator
    to update code each month.

    Returns the resolved URL.  Falls back to the hardcoded URL if no
    candidate works (e.g. CMS site is unreachable).
    """
    from datetime import date

    today = date.today()
    candidates: list[str] = []
    for offset in range(6):
        # Walk back: this month, last month, two months ago...
        year  = today.year
        month = today.month - offset
        while month <= 0:
            month += 12
            year  -= 1
        name = date(year, month, 1).strftime("%B_%Y")   # "May_2026"
        candidates.append(
            f"{NPPES_INDEX_URL}NPPES_Data_Dissemination_{name}.zip"
        )

    for url in candidates:
        try:
            r = requests.head(url, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                print(f"  [nppes] resolved latest URL: {url}")
                return url
        except requests.RequestException:
            continue

    print(f"  [nppes] could not resolve a current URL; using fallback "
          f"{NPPES_FALLBACK_URL}")
    return NPPES_FALLBACK_URL

# The NPPES CSV is ~10GB uncompressed; chunk size of 200k rows keeps peak
# memory under ~1GB.
CHUNK_ROWS = 200_000

# Mapping from NPPES column names to our normalized names.  NPPES uses
# extremely verbose column headers; we slim them down here.
#
# Practice-location address fields are critical: they power the
# address-clustering feature that detects shell-entity / phantom-billing
# patterns ("12 hospice agencies registered at one strip-mall address").
COL_MAP = {
    "NPI":                                                "npi",
    "Provider Enumeration Date":                          "enumeration_date",
    "Entity Type Code":                                   "entity_type",
    "Healthcare Provider Taxonomy Code_1":                "taxonomy_primary",
    "Is Sole Proprietor":                                 "is_sole_proprietor",
    # Practice-location address fields (used for address clustering)
    "Provider First Line Business Practice Location Address":  "practice_addr_line1",
    "Provider Second Line Business Practice Location Address": "practice_addr_line2",
    "Provider Business Practice Location Address City Name":   "practice_city",
    "Provider Business Practice Location Address State Name":  "nppes_state",
    "Provider Business Practice Location Address Postal Code": "practice_zip",
}


def _download_nppes_zip(url: str = NPPES_DOWNLOAD_URL) -> Path:
    """Download the NPPES zip file with a progress bar."""
    dest = RAW_DIR / Path(url).name
    if dest.exists():
        print(f"  [nppes] using cached {dest.name}")
        return dest

    print(f"  [nppes] downloading {url}…")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc=dest.name,
        ) as bar:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                bar.update(len(chunk))
    return dest


def _find_data_csv(zip_path: Path) -> Path:
    """Extract the npidata_pfile_*.csv from a downloaded NPPES zip."""
    import zipfile
    extract_dir = zip_path.with_suffix("")
    extract_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            # NPPES bundles a few CSVs; we want the main npidata file.
            base = Path(name).name
            if base.startswith("npidata_pfile_") and base.endswith(".csv") \
               and "_fileheader" not in base.lower():
                target = extract_dir / base
                if not target.exists():
                    print(f"  [nppes] extracting {base} ({z.getinfo(name).file_size:,} bytes)…")
                    z.extract(name, extract_dir)
                return target
    raise FileNotFoundError(f"No npidata_pfile_*.csv found in {zip_path}")


def stream_extract(csv_path: Path, allowed_npis: set[str]) -> pd.DataFrame:
    """
    Stream the NPPES CSV in chunks, filter to ``allowed_npis``, and return
    a compact enrichment DataFrame.

    ``allowed_npis`` is the set of NPIs in our CMS providers parquet — we
    don't care about the other ~7M NPIs in NPPES (e.g., providers who don't
    bill Medicare Part B).
    """
    print(f"  [nppes] streaming {csv_path.name} (target set: "
          f"{len(allowed_npis):,} NPIs)…")
    keep_cols = list(COL_MAP.keys())
    out_chunks: list[pd.DataFrame] = []
    rows_seen = 0
    rows_kept = 0

    reader = pd.read_csv(
        csv_path,
        usecols=keep_cols,
        dtype=str,
        chunksize=CHUNK_ROWS,
        low_memory=True,
    )
    for chunk in reader:
        rows_seen += len(chunk)
        chunk = chunk.rename(columns=COL_MAP)
        chunk["npi"] = chunk["npi"].str.strip()
        chunk = chunk[chunk["npi"].isin(allowed_npis)]
        rows_kept += len(chunk)
        if len(chunk):
            out_chunks.append(chunk)
        if rows_seen % (CHUNK_ROWS * 5) == 0:
            print(f"  [nppes]   {rows_seen:>10,} rows scanned, "
                  f"{rows_kept:>8,} kept")

    print(f"  [nppes] done: {rows_seen:,} rows scanned, {rows_kept:,} kept")
    if not out_chunks:
        return pd.DataFrame(columns=list(COL_MAP.values()))
    return pd.concat(out_chunks, ignore_index=True)


def _post_process(df: pd.DataFrame) -> pd.DataFrame:
    """Type-coerce and compute derived columns."""
    if df.empty:
        return df
    # Enumeration date: MM/DD/YYYY in NPPES → pandas datetime
    df["enumeration_date"] = pd.to_datetime(
        df["enumeration_date"], errors="coerce", format="%m/%d/%Y",
    )
    # Compute months since enumeration as of 2022-12-31 (our scoring year end)
    REF_DATE = pd.Timestamp("2022-12-31")
    df["months_since_enumeration"] = (
        (REF_DATE - df["enumeration_date"]).dt.days / 30.4375
    ).clip(lower=0).fillna(0).astype(float)
    # Sole proprietor → bool (Y/N/X)
    df["is_sole_proprietor"] = (df["is_sole_proprietor"].str.upper() == "Y").astype(int)
    # entity_type already 1/2 strings — cast to int for cleanliness
    df["entity_type"] = pd.to_numeric(df["entity_type"], errors="coerce").fillna(0).astype(int)

    # ── Address normalization ─────────────────────────────────────────────
    # Build the canonical `address_normalized` key used for clustering.
    # Goal: two providers at the same physical location collapse to the
    # same key regardless of formatting differences ("Suite 200" vs
    # "STE 200", trailing periods, double spaces, etc.).
    df["street_address"] = _compose_street(df)
    df["practice_zip"]   = df["practice_zip"].fillna("").astype(str).str.strip().str[:10]
    df["address_normalized"] = (
        df["street_address"].fillna("").astype(str).map(_normalize_address)
        + "|"
        + df["practice_city"].fillna("").astype(str).map(_normalize_token)
        + "|"
        + df["nppes_state"].fillna("").astype(str).str.strip().str.upper().str[:2]
        + "|"
        + df["practice_zip"].str[:5]
    )
    # Treat an obviously-empty normalized key as None so it never clusters
    df.loc[df["street_address"].fillna("").str.len() < 4, "address_normalized"] = None
    return df


# ── Address normalization helpers ─────────────────────────────────────────────
# Designed to be conservative: we never invent addresses, only standardize
# obvious formatting variations.  False positives in clustering are far
# worse than false negatives — if we're unsure, leave the address alone.

_ADDR_SUFFIX_MAP = {
    r"\bstreet\b":    "st",
    r"\bavenue\b":    "ave",
    r"\bboulevard\b": "blvd",
    r"\broad\b":      "rd",
    r"\bdrive\b":     "dr",
    r"\bcourt\b":     "ct",
    r"\bplace\b":     "pl",
    r"\blane\b":      "ln",
    r"\bhighway\b":   "hwy",
    r"\bparkway\b":   "pkwy",
    r"\bsuite\b":     "ste",
    r"\bnumber\b":    "no",
    r"\bapartment\b": "apt",
    r"\bfloor\b":     "fl",
}
import re as _re

def _compose_street(df: pd.DataFrame) -> pd.Series:
    """Join address line 1 + line 2 with a single space, trimmed."""
    line1 = df.get("practice_addr_line1", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
    line2 = df.get("practice_addr_line2", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
    composed = line1 + (" " + line2).where(line2.ne(""), "")
    return composed.str.replace(r"\s+", " ", regex=True).str.strip()


def _normalize_token(s: str) -> str:
    """Lowercase + strip + collapse whitespace for city/state tokens."""
    if not isinstance(s, str):
        return ""
    return _re.sub(r"\s+", " ", s.lower().strip())


def _normalize_address(s: str) -> str:
    """
    Canonicalize a street address for cluster-key matching.

    Operations (all conservative):
      - lowercase
      - collapse whitespace
      - strip trailing punctuation
      - apply known suffix abbreviations (Street → st, Suite → ste, etc.)
      - remove '#' and 'no.' prefixes from unit numbers

    Returns "" for inputs too short to be a real address.
    """
    if not isinstance(s, str) or len(s) < 4:
        return ""
    out = s.lower().strip()
    out = _re.sub(r"[.,;]", " ", out)
    out = _re.sub(r"\s+", " ", out)
    for pat, repl in _ADDR_SUFFIX_MAP.items():
        out = _re.sub(pat, repl, out)
    # Remove unit-number prefix markers so "ste 200", "# 200", "no 200" collapse
    out = _re.sub(r"\b(#|no\.?)\s*", "", out)
    out = _re.sub(r"\s+", " ", out).strip()
    return out


def run(local_csv: str | None = None, url: str | None = None) -> Path:
    """
    Ingest NPPES enrichment.  If ``url`` is None, auto-resolve the latest
    monthly snapshot from CMS — this keeps the pipeline working across
    months without code changes.

    Coverage telemetry: prints what fraction of CMS Part B providers got
    NPPES data joined.  Anything under ~95% is suspicious (probable URL
    mismatch or stale snapshot).
    """
    print("\n=== INGEST: NPPES ===")

    # Load the set of NPIs we actually care about
    providers_parquet = PROC_DIR / "providers_aggregated.parquet"
    if not providers_parquet.exists():
        raise FileNotFoundError(
            f"{providers_parquet} not found — run the main ingest step first."
        )
    npi_series = pd.read_parquet(providers_parquet, columns=["npi"])["npi"]
    allowed = set(npi_series.astype(str).unique())
    n_target = len(allowed)
    print(f"  [nppes] CMS providers to enrich: {n_target:,}")

    if local_csv:
        csv_path = Path(local_csv)
        if not csv_path.exists():
            raise FileNotFoundError(f"--local file not found: {csv_path}")
    else:
        if url is None:
            url = _resolve_latest_nppes_url()
        zip_path = _download_nppes_zip(url)
        csv_path = _find_data_csv(zip_path)

    enriched = stream_extract(csv_path, allowed)
    enriched = _post_process(enriched)
    out_path = PROC_DIR / "nppes_enrichment.parquet"
    enriched.to_parquet(out_path, index=False)

    # Coverage check — a healthy run enriches ~98% of CMS providers (NPPES
    # is the authoritative source, so missing rows mean stale data or
    # deactivated NPIs).
    coverage = len(enriched) / max(n_target, 1)
    print(f"  [nppes] saved → {out_path} ({len(enriched):,} rows, "
          f"{coverage:.1%} coverage)")
    if coverage < 0.90:
        print(f"  [nppes] ⚠ low coverage ({coverage:.1%}) — likely stale "
              f"NPPES snapshot; check the URL or download a newer file")
    return out_path


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Ingest NPPES enrichment for CMS providers")
    p.add_argument("--local", help="Path to a local NPPES npidata_pfile_*.csv (skips download)")
    # Default to None so run() auto-detects the latest available snapshot URL.
    # Pass --url explicitly only when overriding for a specific historical month.
    p.add_argument("--url", default=None, help="NPPES zip URL (auto-detect if omitted)")
    args = p.parse_args()
    run(local_csv=args.local, url=args.url)
