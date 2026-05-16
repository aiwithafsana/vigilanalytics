"""
leie_refresh.py — Native async LEIE refresh for the backend.

Downloads the OIG LEIE UPDATED.csv, diffs it against the providers table, and
applies the delta — same logic as ml/pipeline/refresh_leie.py but using the
backend's async SQLAlchemy session and stdlib csv (no pandas dependency).

Runs as a weekly background task in the FastAPI lifespan (see app.main).

Returns a delta dict with newly_excluded / newly_reinstated counts so the
result can be logged or surfaced via API.
"""
from __future__ import annotations

import csv
import io
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

LEIE_URL = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
LEIE_FLOOR = 85.0
DOWNLOAD_TIMEOUT_SEC = 180

# Real provider NPIs start with 1 or 2 per CMS NPI standards.  "0000000000" is
# the OIG placeholder for entities without an individual NPI — not a real NPI.
_NPI_RE = re.compile(r"^[12]\d{9}$")

# OIG uses "00000000" (eight zeros) as the null/empty REINDATE for active
# exclusions; blank/null REINDATE also means active in older records.
_NULL_REIN_VALUES = {"", "00000000"}


@dataclass
class LeieDelta:
    """Summary of what the refresh changed."""
    newly_excluded:    int
    newly_reinstated:  int
    unchanged:         int
    total_active_npis: int
    flags_inserted:    int       # subset of newly_excluded that exist in providers
    refreshed_at:      datetime


# ── Download + parse ──────────────────────────────────────────────────────────

async def _download_leie() -> str:
    """Download the OIG LEIE UPDATED.csv into memory.  ~30MB."""
    async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT_SEC) as client:
        r = await client.get(LEIE_URL)
        r.raise_for_status()
        return r.text


def _parse_leie_csv(text_body: str) -> list[dict]:
    """
    Parse LEIE CSV into a list of normalised dicts.

    Each row has keys: npi, lastname, firstname, busname, specialty,
    excltype, excldate, reindate, state.
    """
    reader = csv.DictReader(io.StringIO(text_body))
    rows = []
    for raw in reader:
        # Normalise column names: OIG occasionally adjusts case/spacing
        norm = {k.strip().upper(): (v or "").strip() for k, v in raw.items()}
        rows.append({
            "npi":       norm.get("NPI", ""),
            "lastname":  norm.get("LASTNAME", "").title(),
            "firstname": norm.get("FIRSTNAME", "").title(),
            "busname":   norm.get("BUSNAME", "").title(),
            "specialty": norm.get("GENERAL", "") or norm.get("SPECIALTY", ""),
            "state":     norm.get("STATE", ""),
            "excltype":  norm.get("EXCLTYPE", "") or norm.get("EXCL_TYPE", ""),
            "excldate":  norm.get("EXCLDATE", "") or norm.get("EXCL_DATE", ""),
            "reindate":  norm.get("REINDATE", "") or norm.get("REIN_DATE", ""),
        })
    return rows


def _active_leie_rows(rows: Iterable[dict]) -> list[dict]:
    """
    Filter rows to currently-active exclusions with real provider NPIs.

    Active = has a real NPI (starts 1 or 2) AND REINDATE is null/blank/00000000.
    """
    out = []
    for r in rows:
        if not _NPI_RE.match(r["npi"]):
            continue
        if r["reindate"] not in _NULL_REIN_VALUES:
            continue
        out.append(r)
    return out


# ── DB diff + apply ───────────────────────────────────────────────────────────

async def _get_current_excluded_npis(db: AsyncSession) -> set[str]:
    rows = await db.execute(text("SELECT npi FROM providers WHERE is_excluded = TRUE"))
    return {r[0] for r in rows.fetchall()}


async def _truncate_and_reload_leie_table(db: AsyncSession, all_rows: list[dict]) -> None:
    """Replace the leie_exclusions table contents with the freshly downloaded data."""
    await db.execute(text("TRUNCATE leie_exclusions RESTART IDENTITY"))
    if not all_rows:
        return
    # Bulk insert in batches to avoid oversized statements
    BATCH = 1000
    insert_sql = text("""
        INSERT INTO leie_exclusions
            (npi, lastname, firstname, busname, specialty, excltype, excldate, reindate, state)
        VALUES
            (:npi, :lastname, :firstname, :busname, :specialty, :excltype, :excldate, :reindate, :state)
    """)
    for i in range(0, len(all_rows), BATCH):
        chunk = all_rows[i:i + BATCH]
        # Coerce empty strings to NULL for cleaner data
        params = [
            {k: (v or None) for k, v in row.items()}
            for row in chunk
        ]
        await db.execute(insert_sql, params)


async def _apply_newly_excluded(
    db: AsyncSession,
    newly_excluded: set[str],
    lookup: dict[str, dict],
) -> int:
    """
    Mark newly excluded providers, apply the score floor, and insert leie_match
    fraud flags for the subset that exist in the providers table.

    Returns number of fraud_flags rows inserted.
    """
    if not newly_excluded:
        return 0

    # Update provider rows that exist in our DB
    update_sql = text("""
        UPDATE providers
        SET is_excluded = TRUE,
            leie_date   = :leie_date,
            leie_reason = :leie_reason,
            risk_score  = GREATEST(risk_score, :floor),
            risk_tier   = LEAST(COALESCE(risk_tier, 4), 2),
            updated_at  = NOW()
        WHERE npi = :npi
    """)
    update_params = [
        {
            "npi":         npi,
            "leie_date":   lookup.get(npi, {}).get("excldate") or None,
            "leie_reason": lookup.get(npi, {}).get("excltype") or None,
            "floor":       LEIE_FLOOR,
        }
        for npi in newly_excluded
    ]
    # Run as one batched statement
    await db.execute(update_sql, update_params)

    # Find which of these NPIs actually exist in providers (for fraud_flags FK)
    existing_rows = await db.execute(
        text("SELECT npi FROM providers WHERE npi = ANY(:npis)"),
        {"npis": list(newly_excluded)},
    )
    existing_npis = {r[0] for r in existing_rows.fetchall()}
    if not existing_npis:
        return 0

    flag_sql = text("""
        INSERT INTO fraud_flags
            (npi, flag_type, layer, severity, confidence, year,
             flag_value, peer_value, explanation, is_active)
        VALUES
            (:npi, 'leie_match', 1, 1, 1.000, :year,
             1.0, 0.0, :explanation, TRUE)
        ON CONFLICT DO NOTHING
    """)

    def _excl_year(npi: str) -> int | None:
        raw = (lookup.get(npi, {}).get("excldate") or "")[:4]
        return int(raw) if raw.isdigit() else None

    flag_params = [
        {
            "npi":   npi,
            "year":  _excl_year(npi),
            "explanation": (
                "Provider appears on the OIG List of Excluded Individuals/Entities "
                "(LEIE). Any Medicare billing on or after the exclusion date is a "
                "per-claim violation of the False Claims Act (31 U.S.C. § 3729). "
                "Source: OIG LEIE, verified at time of refresh."
            ),
        }
        for npi in existing_npis
    ]
    await db.execute(flag_sql, flag_params)
    return len(flag_params)


async def _apply_reinstatements(db: AsyncSession, newly_reinstated: set[str]) -> None:
    """Clear is_excluded flags for providers no longer on the active LEIE."""
    if not newly_reinstated:
        return

    # NOTE: we do NOT lower their risk_score automatically — that should be
    # recomputed by the next ML pipeline run.  We just unmark them as excluded
    # and deactivate their LEIE fraud flags.
    await db.execute(
        text("""
            UPDATE providers
            SET is_excluded = FALSE,
                leie_date   = NULL,
                leie_reason = NULL,
                updated_at  = NOW()
            WHERE npi = ANY(:npis)
        """),
        {"npis": list(newly_reinstated)},
    )
    await db.execute(
        text("""
            UPDATE fraud_flags
            SET is_active = FALSE
            WHERE npi = ANY(:npis) AND flag_type = 'leie_match'
        """),
        {"npis": list(newly_reinstated)},
    )


# ── Public entrypoint ─────────────────────────────────────────────────────────

async def refresh_leie(db: AsyncSession) -> LeieDelta:
    """
    Run a full LEIE refresh against the backend database.

    Steps:
      1. Download UPDATED.csv from oig.hhs.gov
      2. Parse, filter to active exclusions with real NPIs
      3. Diff against current providers.is_excluded set
      4. Apply newly-excluded marks (with score floor + fraud flags)
      5. Apply reinstatement clears
      6. Truncate + reload leie_exclusions table

    The caller is responsible for committing the session (or the lifespan task
    can do it).  This function performs no commits itself so the entire refresh
    is atomic.
    """
    logger.info("LEIE refresh starting")

    # 1-3. Download + parse + filter
    csv_text = await _download_leie()
    all_rows = _parse_leie_csv(csv_text)
    active   = _active_leie_rows(all_rows)
    active_npis = {r["npi"] for r in active}
    lookup = {r["npi"]: r for r in active}
    logger.info(
        "LEIE downloaded",
        extra={"total_rows": len(all_rows), "active_real_npis": len(active_npis)},
    )

    # 4. Diff
    current_excluded = await _get_current_excluded_npis(db)
    newly_excluded   = active_npis - current_excluded
    newly_reinstated = current_excluded - active_npis
    unchanged        = len(active_npis & current_excluded)

    # 5. Apply
    flags_inserted = await _apply_newly_excluded(db, newly_excluded, lookup)
    await _apply_reinstatements(db, newly_reinstated)

    # 6. Replace leie_exclusions table contents
    await _truncate_and_reload_leie_table(db, all_rows)

    delta = LeieDelta(
        newly_excluded    = len(newly_excluded),
        newly_reinstated  = len(newly_reinstated),
        unchanged         = unchanged,
        total_active_npis = len(active_npis),
        flags_inserted    = flags_inserted,
        refreshed_at      = datetime.now(timezone.utc),
    )
    logger.info(
        "LEIE refresh complete",
        extra={
            "newly_excluded":   delta.newly_excluded,
            "newly_reinstated": delta.newly_reinstated,
            "unchanged":        delta.unchanged,
            "flags_inserted":   delta.flags_inserted,
        },
    )
    return delta
