"""Inventory every source; only sealed vintages count as acquired."""
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

from common.manifest import seal
from ingest.acquisition import data_root, vintage_path
from ingest.sources import SOURCES


@dataclass(frozen=True)
class SourceStatus:
    source: str
    vintage: str
    staleness_days: int | None
    state: str
    mode: str


def statuses(*, as_of: datetime | None = None, verify: bool = False) -> list[SourceStatus]:
    now = as_of or datetime.now(timezone.utc)
    result = []
    for key, source in SOURCES.items():
        parent = data_root() / 'raw' / key
        vintages = sorted((p for p in parent.iterdir() if p.is_dir()), reverse=True) if parent.exists() else []
        if not vintages:
            result.append(SourceStatus(key, '-', None, 'MISSING' if source.required else 'OPTIONAL-MISSING', source.mode))
            continue
        # Show latest directory even when unsealed, so incomplete acquisitions
        # cannot silently hide behind a previous good vintage.
        path = vintages[0]
        vintage_path(key, path.name)
        manifest = path / 'manifest.json'
        if not manifest.exists():
            result.append(SourceStatus(key, path.name, None, 'UNSEALED', source.mode))
            continue
        payload = json.loads(manifest.read_text())
        if payload.get('version') != 1 or not payload.get('files'):
            raise ValueError(f'Invalid manifest: {manifest}')
        stamps = [datetime.fromisoformat(row['fetched_at']) for row in payload['files']]
        if any(stamp.tzinfo is None or stamp > now for stamp in stamps):
            raise ValueError(f'Invalid/future fetch timestamp: {manifest}')
        if verify:
            seal(path)
        days = (now - max(stamps)).days
        result.append(SourceStatus(key, path.name, days, 'VERIFIED' if verify else 'SEALED', source.mode))
    return result


def print_status(*, verify: bool = False) -> None:
    print(f'{"SOURCE":<18} {"VINTAGE":<12} {"DAYS SINCE FETCH":>16}  {"STATE":<17} MODE')
    for row in statuses(verify=verify):
        days = '-' if row.staleness_days is None else str(row.staleness_days)
        print(f'{row.source:<18} {row.vintage:<12} {days:>16}  {row.state:<17} {row.mode}')
