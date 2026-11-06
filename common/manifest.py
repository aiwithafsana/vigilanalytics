"""Seal raw vintages once, then verify them without modifying the manifest.

Fetchers pass metadata keyed by relative POSIX path. Manual drop-ins may omit
metadata; fetched_at then records sealing time and source_url is empty.
Callers must finish all downloads before sealing and never write concurrently.
"""
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Mapping


@dataclass(frozen=True)
class FileRecord:
    path: str
    sha256: str
    bytes: int
    fetched_at: str
    source_url: str


class ImmutabilityError(ValueError):
    """A sealed vintage no longer matches its manifest."""


def _inventory(root: Path) -> dict[str, tuple[str, int]]:
    result = {}
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise ValueError(f'Symlinks are not allowed in vintages: {path}')
        if path == root / 'manifest.json':
            continue
        if path.is_dir():
            continue
        if not path.is_file():
            raise ValueError(f'Unsupported vintage entry: {path}')
        digest = hashlib.sha256()
        size = 0
        with path.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
                size += len(chunk)
        result[path.relative_to(root).as_posix()] = (digest.hexdigest(), size)
    return result


def seal(vintage_dir: Path, *, metadata: Mapping[str, Mapping[str, str]] | None = None) -> None:
    """Hash every file into manifest.json; reject any change to a sealed vintage.

    Optional fetcher metadata supplies actual fetched_at and source_url values.
    Publication is atomic and exclusive, so an existing seal is never replaced.
    """
    root = Path(vintage_dir)
    if root.is_symlink() or not root.is_dir():
        raise ValueError(f'Expected a real vintage directory: {root}')
    manifest = root / 'manifest.json'
    inventory = _inventory(root)
    if manifest.exists():
        try:
            payload = json.loads(manifest.read_text())
            records = [FileRecord(**row) for row in payload['files']]
            if payload['version'] != 1 or len({r.path for r in records}) != len(records):
                raise ValueError('Invalid version or duplicate file paths')
            expected = {r.path: (r.sha256, r.bytes) for r in records}
        except (ValueError, TypeError, KeyError) as error:
            raise ImmutabilityError(f'Invalid manifest: {manifest}') from error
        if expected != inventory:
            changed = sorted(k for k in expected.keys() | inventory.keys()
                             if expected.get(k) != inventory.get(k))
            raise ImmutabilityError(f'Sealed vintage changed: {changed}')
        return
    if not inventory:
        raise ValueError(f'Cannot seal empty vintage: {root}')
    metadata = metadata or {}
    if set(metadata) - inventory.keys():
        raise ValueError('Metadata references files absent from the vintage')
    now = datetime.now(timezone.utc).isoformat()
    records = [FileRecord(path, digest, size,
                          metadata.get(path, {}).get('fetched_at', now),
                          metadata.get(path, {}).get('source_url', ''))
               for path, (digest, size) in sorted(inventory.items())]
    content = json.dumps({'version': 1, 'files': [asdict(r) for r in records]},
                         indent=2, sort_keys=True) + '\n'
    # A sibling temporary file cannot accidentally become part of this vintage.
    fd, name = tempfile.mkstemp(prefix='.vigil-manifest-', dir=root.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(name, manifest)
        except FileExistsError:
            seal(root)
    finally:
        Path(name).unlink(missing_ok=True)
