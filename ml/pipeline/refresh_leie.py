"""
refresh_leie.py — Incremental LEIE refresh without re-running the full pipeline.

Downloads the current OIG LEIE CSV, diffs it against the DB, and updates
provider exclusion flags + risk score floor in-place. Produces a delta report
that can be surfaced in the UI as an "alert feed" of new exclusions.

Run this on a schedule (weekly recommended — OIG updates monthly):
    python refresh_leie.py

Or to just see the diff without writing:
    python refresh_leie.py --dry-run

Why incremental and not a full pipeline re-run?
  The full pipeline takes 40+ minutes (downloads 200MB CMS file, retrains models).
  Exclusion status needs to be current within days of an OIG update — waiting for
  the next scheduled full run would leave newly-excluded providers unmarked.

Output
------
  - Updates providers.is_excluded / leie_date / leie_reason
  - Applies 85.0 score floor to newly excluded providers
  - Removes exclusion marks from providers whose reinstatement date has passed
  - Upserts new records into leie_exclusions table
  - Writes data/processed/leie_delta_YYYY-MM-DD.json with the full diff
  - Prints a summary for ops monitoring
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# ── Paths ─────────────────────────────────────────────────────────────────────
_HERE     = Path(__file__).parent
DATA_DIR  = _HERE.parent / "data"
RAW_DIR   = DATA_DIR / "raw"
PROC_DIR  = DATA_DIR / "processed"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv(_HERE.parent.parent / "backend" / ".env")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://vigil:vigil@localhost:5432/vigil",
).replace("postgresql+asyncpg://", "postgresql://")

LEIE_URL     = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
LEIE_FLOOR   = 85.0
BATCH_SIZE   = 2_000


# ── Helpers ───────────────────────────────────────────────────────────────────

def _conn():
    return psycopg2.connect(DATABASE_URL)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


# ── Download ──────────────────────────────────────────────────────────────────

def download_leie_fresh() -> tuple[Path, str]:
    """
    Always re-download LEIE — it's the current snapshot, not a versioned file.
    Save alongside a .sha256 file so we can detect when OIG actually updated.
    """
    dest     = RAW_DIR / "leie_latest.csv"
    sha_file = RAW_DIR / "leie_latest.sha256"

    old_sha = sha_file.read_text().strip() if sha_file.exists() else ""

    print(f"  [leie] Downloading {LEIE_URL} …")
    r = requests.get(LEIE_URL, stream=True, timeout=120)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=1 << 20):
            f.write(chunk)

    new_sha = _sha256(dest)
    sha_file.write_text(new_sha)

    if old_sha and old_sha == new_sha:
        print("  [leie] File unchanged since last refresh (same SHA-256). No updates needed.")
    else:
        print(f"  [leie] New data detected. SHA-256: {new_sha[:16]}…")

    return dest, new_sha


# ── Parse ─────────────────────────────────────────────────────────────────────

def parse_leie(path: Path) -> pd.DataFrame:
    """
    Parse OIG LEIE CSV into a normalised DataFrame.

    OIG column names (as of 2024):
      LASTNAME, FIRSTNAME, MIDNAME, BUSNAME, GENERAL, SPECIALTY,
      UPIN, NPI, DOB, ADDRESS, CITY, STATE, ZIP,
      EXCLTYPE, EXCLDATE, REINDATE, WAIVERDATE, WVRSTATE
    """
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df.columns = [c.strip().upper() for c in df.columns]

    # Flexible column mapping — OIG has tweaked names over the years
    col = lambda *cands: next((c for c in cands if c in df.columns), None)

    npi_c      = col("NPI")
    last_c     = col("LASTNAME", "LAST_NAME")
    first_c    = col("FIRSTNAME", "FIRST_NAME")
    bus_c      = col("BUSNAME", "BUS_NAME", "BUSINESS_NAME")
    spec_c     = col("GENERAL", "SPECIALTY")
    state_c    = col("STATE")
    excltype_c = col("EXCLTYPE", "EXCL_TYPE")
    excldate_c = col("EXCLDATE", "EXCL_DATE")
    reindate_c = col("REINDATE", "REIN_DATE")

    out = pd.DataFrame()
    out["npi"]      = df[npi_c].str.strip() if npi_c else ""
    out["lastname"]  = df[last_c].str.strip().str.title() if last_c else ""
    out["firstname"] = df[first_c].str.strip().str.title() if first_c else ""
    out["busname"]   = df[bus_c].str.strip().str.title() if bus_c else ""
    out["specialty"] = df[spec_c].str.strip() if spec_c else ""
    out["state"]     = df[state_c].str.strip() if state_c else ""
    out["excltype"]  = df[excltype_c].str.strip() if excltype_c else ""
    out["excldate"]  = df[excldate_c].str.strip() if excldate_c else ""
    out["reindate"]  = df[reindate_c].str.strip() if reindate_c else ""

    print(f"  [leie] Parsed {len(out):,} total LEIE records")
    valid = out[out["npi"].str.match(r"^[12]\d{9}$", na=False)]
    print(f"  [leie] Records with real provider NPI (starts 1 or 2): {len(valid):,}")
    return out.reset_index(drop=True)


def _active_leie_npis(leie: pd.DataFrame) -> set[str]:
    """
    Active exclusions: has a real NPI, not yet reinstated.

    OIG NPI quirks:
      - Entities without individual NPIs use "0000000000" (ten zeros) — not a real NPI.
      - Real provider NPIs start with 1 or 2 per CMS NPI standards.

    OIG REINDATE quirks:
      - Active exclusions have REINDATE = "00000000" (not blank — eight zeros).
      - Reinstated providers have REINDATE = a real date like "20210415".
      - Blank / null REINDATE also means active (older records).
    """
    # Real NPI: 10 digits, first digit 1 or 2 (CMS standard)
    has_npi = leie["npi"].str.match(r"^[12]\d{9}$", na=False)
    # "00000000" = OIG null date = still active; blank/null also means active
    not_reinstated = (
        leie["reindate"].isna()
        | (leie["reindate"].str.strip() == "")
        | (leie["reindate"].str.strip() == "00000000")
    )
    return set(leie.loc[has_npi & not_reinstated, "npi"].str.strip().unique())


# ── DB diff ───────────────────────────────────────────────────────────────────

def get_current_exclusions() -> tuple[set[str], dict[str, float]]:
    """
    Returns:
      - currently_excluded_npis: set of NPIs where is_excluded = TRUE in DB
      - risk_scores:             dict npi → current risk_score
    """
    conn = _conn()
    cur  = conn.cursor()
    cur.execute("SELECT npi, risk_score FROM providers WHERE is_excluded = TRUE")
    rows = cur.fetchall()
    cur.close(); conn.close()

    excluded = {r[0] for r in rows}
    scores   = {r[0]: float(r[1]) if r[1] is not None else 0.0 for r in rows}
    return excluded, scores


# ── Apply delta ───────────────────────────────────────────────────────────────

def apply_delta(
    leie: pd.DataFrame,
    active_npis: set[str],
    current_excluded: set[str],
) -> dict:
    """
    Compute the diff and apply it to the DB.

    Returns a delta dict summarising what changed.
    """
    newly_excluded   = active_npis - current_excluded    # in new LEIE, not yet marked
    newly_reinstated = current_excluded - active_npis    # was marked, no longer in LEIE

    print(f"  [delta] Newly excluded:    {len(newly_excluded):,}")
    print(f"  [delta] Newly reinstated:  {len(newly_reinstated):,}")
    print(f"  [delta] Unchanged:         {len(active_npis & current_excluded):,}")

    conn = _conn()
    cur  = conn.cursor()

    # ── Mark newly excluded ────────────────────────────────────────────────────
    if newly_excluded:
        leie_npi_lookup = (
            leie[leie["npi"].isin(newly_excluded)]
            .drop_duplicates("npi")
            .set_index("npi")[["excldate", "excltype"]]
            .to_dict("index")
        )

        excl_rows = [
            (npi, leie_npi_lookup.get(npi, {}).get("excldate"), leie_npi_lookup.get(npi, {}).get("excltype"))
            for npi in newly_excluded
        ]

        # Update exclusion fields + apply score floor
        psycopg2.extras.execute_batch(
            cur,
            """
            UPDATE providers
            SET is_excluded  = TRUE,
                leie_date    = %s,
                leie_reason  = %s,
                risk_score   = GREATEST(risk_score, %s),
                risk_tier    = LEAST(COALESCE(risk_tier, 4), 2),
                updated_at   = NOW()
            WHERE npi = %s
            """,
            [(date, reason, LEIE_FLOOR, npi) for npi, date, reason in excl_rows],
            page_size=BATCH_SIZE,
        )
        print(f"  [delta] Score floor {LEIE_FLOOR} applied to {len(newly_excluded)} providers")

        # Insert fraud flags for newly excluded providers
        # The fraud_flags.year column is a smallint (4-digit calendar year).
        # LEIE excldate is YYYYMMDD — extract just the year (first 4 chars).
        def _excl_year(npi_: str) -> int | None:
            raw = leie_npi_lookup.get(npi_, {}).get("excldate", "") or ""
            return int(raw[:4]) if raw[:4].isdigit() else None

        flag_rows = [
            (
                npi,
                "leie_match",          # flag_type
                1,                     # layer
                1,                     # severity = critical
                1.000,                 # confidence = certain (confirmed by OIG)
                _excl_year(npi),       # year: smallint — just the 4-digit year
                1.0,                   # flag_value (binary)
                0.0,                   # peer_value
                "Provider appears on the OIG List of Excluded Individuals/Entities (LEIE). "
                "Any Medicare billing after the exclusion date is a per-claim False Claims Act "
                "violation (31 U.S.C. § 3729). Source: OIG LEIE, verified at time of refresh.",
            )
            for npi in newly_excluded
        ]
        # Only insert flags for NPIs that exist in the providers table
        # (the live LEIE contains ~83k exclusions; only a fraction billed CMS Part B)
        cur.execute("SELECT npi FROM providers WHERE npi = ANY(%s)", (list(newly_excluded),))
        npi_in_db = {r[0] for r in cur.fetchall()}
        flag_rows_filtered = [r for r in flag_rows if r[0] in npi_in_db]
        if flag_rows_filtered:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO fraud_flags
                    (npi, flag_type, layer, severity, confidence, year, flag_value, peer_value, explanation, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT DO NOTHING
                """,
                flag_rows_filtered,
                page_size=BATCH_SIZE,
            )
        print(f"  [delta] LEIE flags inserted for {len(flag_rows_filtered):,} providers "
              f"({len(newly_excluded) - len(flag_rows_filtered):,} excluded NPIs not in CMS 2022 data)")

    # ── Clear reinstated providers ─────────────────────────────────────────────
    if newly_reinstated:
        # Remove exclusion flag — score will update on next full pipeline run
        psycopg2.extras.execute_batch(
            cur,
            "UPDATE providers SET is_excluded = FALSE, leie_date = NULL, leie_reason = NULL, updated_at = NOW() WHERE npi = %s",
            [(npi,) for npi in newly_reinstated],
            page_size=BATCH_SIZE,
        )
        # Deactivate LEIE fraud flags for these providers
        psycopg2.extras.execute_batch(
            cur,
            "UPDATE fraud_flags SET is_active = FALSE WHERE npi = %s AND flag_type = 'leie_match'",
            [(npi,) for npi in newly_reinstated],
            page_size=BATCH_SIZE,
        )
        print(f"  [delta] Cleared exclusion for {len(newly_reinstated)} reinstated providers")

    conn.commit()
    cur.close(); conn.close()

    return {
        "newly_excluded":   sorted(newly_excluded),
        "newly_reinstated": sorted(newly_reinstated),
        "unchanged":        len(active_npis & current_excluded),
        "total_active":     len(active_npis),
    }


# ── Full LEIE table refresh ───────────────────────────────────────────────────

def refresh_leie_table(leie: pd.DataFrame):
    """Truncate and reload the leie_exclusions table (full replacement)."""
    conn = _conn()
    cur  = conn.cursor()
    cur.execute("TRUNCATE leie_exclusions RESTART IDENTITY")

    rows = [
        (
            r.get("npi") or None,
            r.get("lastname") or None,
            r.get("firstname") or None,
            r.get("busname") or None,
            r.get("specialty") or None,
            r.get("excltype") or None,
            r.get("excldate") or None,
            r.get("reindate") or None,
            r.get("state") or None,
        )
        for _, r in leie.iterrows()
    ]
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO leie_exclusions (npi, lastname, firstname, busname, specialty, excltype, excldate, reindate, state) VALUES %s",
        rows, page_size=BATCH_SIZE,
    )
    conn.commit()
    print(f"  [leie] Loaded {len(rows):,} records into leie_exclusions table")
    cur.close(); conn.close()


# ── Delta report ──────────────────────────────────────────────────────────────

def write_delta_report(delta: dict, sha256: str) -> Path:
    today  = datetime.now(timezone.utc).date().isoformat()
    report = {
        "refresh_date":      today,
        "leie_sha256":       sha256,
        "total_active_leie": delta["total_active"],
        "newly_excluded":    len(delta["newly_excluded"]),
        "newly_reinstated":  len(delta["newly_reinstated"]),
        "unchanged":         delta["unchanged"],
        "newly_excluded_npis":   delta["newly_excluded"][:100],   # cap for JSON size
        "newly_reinstated_npis": delta["newly_reinstated"][:100],
    }
    out = PROC_DIR / f"leie_delta_{today}.json"
    with open(out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"  [leie] Delta report → {out}")
    return out


# ── Entrypoint ────────────────────────────────────────────────────────────────

def run(dry_run: bool = False) -> dict:
    print("\n=== LEIE REFRESH ===")

    dest, sha256       = download_leie_fresh()
    leie               = parse_leie(dest)
    active_npis        = _active_leie_npis(leie)
    current_excluded, _scores = get_current_exclusions()

    newly_excluded   = active_npis - current_excluded
    newly_reinstated = current_excluded - active_npis

    print(f"\n  Summary:")
    print(f"    OIG active exclusions with NPI:  {len(active_npis):,}")
    print(f"    Currently marked in DB:          {len(current_excluded):,}")
    print(f"    New exclusions to apply:         {len(newly_excluded):,}")
    print(f"    Reinstatements to clear:         {len(newly_reinstated):,}")

    if dry_run:
        print("\n  [dry-run] No DB writes performed.")
        newly_excluded_list = sorted(newly_excluded)[:20]
        if newly_excluded_list:
            print(f"  [dry-run] First new exclusions: {newly_excluded_list}")
        return {"dry_run": True, "newly_excluded": len(newly_excluded), "newly_reinstated": len(newly_reinstated)}

    if not newly_excluded and not newly_reinstated:
        print("  [leie] No changes to apply.")
        delta = {"newly_excluded": [], "newly_reinstated": [], "unchanged": len(active_npis & current_excluded), "total_active": len(active_npis)}
    else:
        refresh_leie_table(leie)
        delta = apply_delta(leie, active_npis, current_excluded)

    report = write_delta_report(delta, sha256)

    print(f"\n  [leie] Refresh complete.")
    if delta["newly_excluded"]:
        print(f"  ACTION REQUIRED: {len(delta['newly_excluded'])} newly excluded providers")
        print(f"  → Score floor applied; LEIE flags created; investigate immediately")
    return delta


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    # Add ml/ to path for dotenv load
    sys.path.insert(0, str(Path(__file__).parent.parent))
    run(dry_run=args.dry_run)
