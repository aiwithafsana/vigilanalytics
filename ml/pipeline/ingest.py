"""
ingest.py — Download and aggregate CMS Part B PUF + LEIE data.

CMS Part B Physician PUF (2022):
  ~9M rows (one row per provider × HCPCS code)
  Aggregated here to one row per NPI.

LEIE:
  ~75k excluded providers with NPI where available.

Outputs
-------
data/processed/providers_aggregated.parquet   — one row per NPI
data/processed/leie.parquet                   — raw LEIE records
data/raw/                                      — downloaded CSVs (cached)
"""

import os
import sys
import hashlib
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm

DATA_DIR = Path(__file__).parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"
PROC_DIR = DATA_DIR / "processed"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

# CMS Part B 2022 PUF — provider-level file (one row per NPI, ~200MB)
CMS_URL = (
    "https://data.cms.gov/sites/default/files/2025-11/"
    "adcd20c5-4534-43cd-8dfa-881ebe7bacfd/MUP_PHY_R25_P07_V20_D22_Prov.csv"
)

# HCPCS-level file for entropy + E&M ratio computation (~1.5GB)
CMS_HCPCS_URL = (
    "https://data.cms.gov/sites/default/files/2025-04/"
    "e3f823f8-db5b-4cc7-ba04-e7ae92b99757/MUP_PHY_R25_P05_V20_D23_Prov_Svc.csv"
)

LEIE_URL = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"

# E&M level-5 codes (highest complexity — most likely to be upcoded)
EM_LEVEL5 = {"99205", "99215", "99345", "99350"}
# All E&M visit codes
EM_ALL = {
    "99202", "99203", "99204", "99205",
    "99211", "99212", "99213", "99214", "99215",
    "99341", "99342", "99344", "99345",
    "99347", "99348", "99349", "99350",
}


def _download(url: str, dest: Path, label: str) -> Path:
    """Stream-download url → dest, with progress bar. Skips if already cached."""
    if dest.exists():
        print(f"  [cache] {label} already at {dest.name}")
        return dest

    print(f"  [download] {label}…")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=dest.name) as bar:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                bar.update(len(chunk))
    return dest


def download_cms_provider() -> Path:
    dest = RAW_DIR / "MUP_PHY_R25_P07_V20_D22_Prov.csv"
    return _download(CMS_URL, dest, "CMS Part B 2022 (by provider)")


def download_cms_hcpcs() -> Path:
    dest = RAW_DIR / "cms_part_b_2022_by_hcpcs.csv"
    return _download(CMS_HCPCS_URL, dest, "CMS Part B 2022 (by HCPCS)")


def download_leie() -> Path:
    dest = RAW_DIR / "leie_updated.csv"
    return _download(LEIE_URL, dest, "LEIE exclusions")


def aggregate_provider_file(path: Path) -> pd.DataFrame:
    """
    Load the provider-level CMS file (one row per NPI).
    Columns vary slightly by year; we normalise them here.
    """
    print("  [parse] Loading provider-level file…")
    df = pd.read_csv(path, dtype=str, low_memory=False)

    # Normalise column names (CMS changes capitalisation between years)
    df.columns = [c.strip().lower() for c in df.columns]

    col = lambda *candidates: next((c for c in candidates if c in df.columns), None)

    npi_col       = col("rndrng_npi", "npi")
    last_col      = col("rndrng_prvdr_last_org_name", "last_org_name")
    first_col     = col("rndrng_prvdr_first_name", "first_name")
    spec_col      = col("rndrng_prvdr_type", "provider_type", "specialty")
    state_col     = col("rndrng_prvdr_state_abrvtn", "state")
    city_col      = col("rndrng_prvdr_city", "city")
    svc_col       = col("tot_srvcs", "total_services")
    bene_col      = col("tot_benes", "total_beneficiaries", "bene_unique_cnt")
    pay_col       = col("tot_mdcr_pymt_amt", "medicare_payment_amt")
    proc_col      = col("tot_hcpcs_cds", "hcpcs_code_count")

    required = [npi_col, spec_col, state_col, svc_col, bene_col, pay_col]
    missing = [c for c in required if c is None]
    if missing:
        raise ValueError(f"Could not find columns: {missing}. Got: {list(df.columns[:20])}")

    out = pd.DataFrame()
    out["npi"]                  = df[npi_col].str.strip()
    out["name_last"]            = df[last_col].str.title() if last_col else ""
    out["name_first"]           = df[first_col].str.title() if first_col else ""
    out["specialty"]            = df[spec_col].str.strip() if spec_col else ""
    out["state"]                = df[state_col].str.strip() if state_col else ""
    out["city"]                 = df[city_col].str.title() if city_col else ""
    out["total_services"]       = pd.to_numeric(df[svc_col], errors="coerce")
    out["total_beneficiaries"]  = pd.to_numeric(df[bene_col], errors="coerce")
    out["total_payment"]        = pd.to_numeric(df[pay_col], errors="coerce")
    out["num_procedure_types"]  = pd.to_numeric(df[proc_col], errors="coerce") if proc_col else np.nan

    out = out.dropna(subset=["npi", "total_payment", "total_services"])
    out = out[out["npi"].str.match(r"^\d{10}$", na=False)]
    out = out[out["total_payment"] > 0]

    print(f"  [parse] {len(out):,} providers loaded")
    return out.reset_index(drop=True)


def compute_hcpcs_features(hcpcs_path: Path, npis: set) -> pd.DataFrame:
    """
    Stream through the HCPCS-level file to compute per-NPI:
      - billing_entropy   (Shannon entropy of service distribution)
      - em_upcoding_ratio (fraction of E&M visits at level 5)
    """
    print("  [parse] Computing HCPCS features (streaming)…")

    entropy_data: dict[str, dict] = {}  # npi → {hcpcs: count}
    em_data: dict[str, list] = {}       # npi → [total_em, level5_em]

    chunk_size = 500_000
    reader = pd.read_csv(hcpcs_path, dtype=str, low_memory=False, chunksize=chunk_size)

    col_map: dict = {}
    for i, chunk in enumerate(tqdm(reader, desc="HCPCS chunks")):
        if i == 0:
            cols = [c.strip().lower() for c in chunk.columns]
            chunk.columns = cols
            npi_c   = next((c for c in ["rndrng_npi", "npi"] if c in cols), None)
            hcpcs_c = next((c for c in ["hcpcs_cd", "hcpcs_code"] if c in cols), None)
            svc_c   = next((c for c in ["tot_srvcs", "total_services"] if c in cols), None)
            col_map = {"npi": npi_c, "hcpcs": hcpcs_c, "svc": svc_c}
        else:
            chunk.columns = [c.strip().lower() for c in chunk.columns]

        npi_c, hcpcs_c, svc_c = col_map["npi"], col_map["hcpcs"], col_map["svc"]
        if not all([npi_c, hcpcs_c, svc_c]):
            continue

        chunk = chunk[[npi_c, hcpcs_c, svc_c]].copy()
        chunk.columns = ["npi", "hcpcs", "svc"]
        chunk["svc"] = pd.to_numeric(chunk["svc"], errors="coerce").fillna(0)
        chunk = chunk[chunk["npi"].isin(npis)]

        for row in chunk.itertuples(index=False):
            npi, hcpcs, svc = row.npi, str(row.hcpcs).strip(), row.svc
            # entropy
            if npi not in entropy_data:
                entropy_data[npi] = {}
            entropy_data[npi][hcpcs] = entropy_data[npi].get(hcpcs, 0) + svc
            # E&M
            if hcpcs in EM_ALL:
                if npi not in em_data:
                    em_data[npi] = [0, 0]
                em_data[npi][0] += svc
                if hcpcs in EM_LEVEL5:
                    em_data[npi][1] += svc

    # Compute entropy
    records = []
    for npi, counts in entropy_data.items():
        vals = np.array(list(counts.values()), dtype=float)
        total = vals.sum()
        if total > 0:
            probs = vals / total
            entropy = float(-np.sum(probs * np.log2(probs + 1e-10)))
        else:
            entropy = 0.0
        em = em_data.get(npi, [0, 0])
        em_ratio = (em[1] / em[0]) if em[0] > 0 else 0.0
        records.append({"npi": npi, "billing_entropy": round(entropy, 4), "em_upcoding_ratio": round(em_ratio, 3)})

    return pd.DataFrame(records)


def process_leie(path: Path) -> pd.DataFrame:
    print("  [parse] Processing LEIE…")
    df = pd.read_csv(path, dtype=str, encoding="latin-1", low_memory=False)
    df.columns = [c.strip().lower() for c in df.columns]

    npi_col = next((c for c in ["npi", "npinum"] if c in df.columns), None)

    out = pd.DataFrame()
    out["npi"]       = df[npi_col].str.strip() if npi_col else ""
    out["lastname"]  = df.get("lastname", pd.Series(dtype=str)).str.title()
    out["firstname"] = df.get("firstname", pd.Series(dtype=str)).str.title()
    out["busname"]   = df.get("busname", pd.Series(dtype=str)).str.title()
    out["specialty"] = df.get("specialty", pd.Series(dtype=str))
    out["excltype"]  = df.get("excltype", pd.Series(dtype=str))
    out["excldate"]  = df.get("excldate", pd.Series(dtype=str))
    out["reindate"]  = df.get("reindate", pd.Series(dtype=str))
    out["state"]     = df.get("state", pd.Series(dtype=str))

    # Keep only records with a valid non-zero 10-digit NPI
    out = out[out["npi"].str.match(r"^\d{10}$", na=False)]
    out = out[out["npi"] != "0000000000"]
    # Remove reinstated providers (reindate is "00000000", empty, or NaN when not reinstated)
    out = out[
        out["reindate"].isna()
        | (out["reindate"].str.strip() == "")
        | (out["reindate"].str.strip() == "00000000")
    ]

    print(f"  [parse] {len(out):,} active LEIE exclusions with NPI")
    return out.reset_index(drop=True)


def run(skip_hcpcs: bool = False):
    print("\n=== INGEST ===")

    # 1. Download
    provider_path = download_cms_provider()
    leie_path     = download_leie()

    # 2. Aggregate provider file
    providers = aggregate_provider_file(provider_path)

    # 3. HCPCS-level features (entropy + E&M ratio)
    if not skip_hcpcs:
        try:
            hcpcs_path = download_cms_hcpcs()
            hcpcs_features = compute_hcpcs_features(hcpcs_path, set(providers["npi"]))
            providers = providers.merge(hcpcs_features, on="npi", how="left")
        except Exception as e:
            print(f"  [warn] HCPCS features failed ({e}) — skipping entropy/E&M columns")
            providers["billing_entropy"]   = np.nan
            providers["em_upcoding_ratio"] = np.nan
    else:
        providers["billing_entropy"]   = np.nan
        providers["em_upcoding_ratio"] = np.nan

    # 4. Save
    out_path = PROC_DIR / "providers_aggregated.parquet"
    providers.to_parquet(out_path, index=False)
    print(f"  [save] {out_path} ({len(providers):,} rows)")

    # 5. LEIE
    leie = process_leie(leie_path)
    leie_path_out = PROC_DIR / "leie.parquet"
    leie.to_parquet(leie_path_out, index=False)
    print(f"  [save] {leie_path_out} ({len(leie):,} rows)")

    return providers, leie


if __name__ == "__main__":
    skip = "--skip-hcpcs" in sys.argv
    run(skip_hcpcs=skip)
