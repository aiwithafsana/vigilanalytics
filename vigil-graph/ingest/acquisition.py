"""Immutable publication shared by the source-specific fetchers.

All I/O touching raw vintages stays within ingest/. Large imports are staged
outside data/raw, validated, sealed, then published with exclusive creation.
"""
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import ssl
import subprocess
import tempfile
from typing import Mapping
import urllib.error
import urllib.request

from common.manifest import ImmutabilityError, seal
from ingest.sources import SOURCES
from ingest.validation import validate

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RECEIPT = 'acquisition.manifest.json'


class AcquisitionError(ValueError):
    pass


class MissingInputError(AcquisitionError):
    pass


def data_root() -> Path:
    return Path(os.environ.get('VIGIL_DATA_ROOT', str(PROJECT_ROOT / 'data')))


def vintage_path(source: str, vintage: str) -> Path:
    if source not in SOURCES or not re.fullmatch(r'\d{4}(?:-\d{2}(?:-\d{2})?)?', vintage):
        raise AcquisitionError('Use a known source and YYYY, YYYY-MM, or YYYY-MM-DD vintage')
    # Validate month/day as well as characters; no path traversal is possible.
    try:
        date.fromisoformat(vintage + {4: '-01-01', 7: '-01', 10: ''}[len(vintage)])
    except ValueError as error:
        raise AcquisitionError(f'Invalid vintage: {vintage}') from error
    path = data_root() / 'raw' / source / vintage
    for part in [data_root(), data_root() / 'raw', path.parent, path]:
        if part.is_symlink():
            raise AcquisitionError(f'Symlink in raw path: {part}')
    return path


def instructions(source: str, vintage: str) -> str:
    spec = SOURCES[source]
    return f'{source}: input required at {vintage_path(source, vintage)}\nDownload: {spec.url}\n{spec.instructions}'


def existing(source: str, vintage: str) -> bool:
    path = vintage_path(source, vintage)
    if (path / 'manifest.json').exists():
        seal(path)  # Always check immutability before considering a refetch.
        return True
    return False


def fetch_manual(source: str, vintage: str) -> None:
    if existing(source, vintage):
        print(f'{source}/{vintage}: verified sealed vintage')
        return
    path = vintage_path(source, vintage)
    if not path.exists() or not any(path.iterdir()):
        message = instructions(source, vintage)
        print(message)
        raise MissingInputError(message)
    validate(source, path, vintage)
    metadata = _metadata(path, SOURCES[source].url)
    seal(path, metadata=metadata)
    print(f'{source}/{vintage}: sealed')


def _metadata(path: Path, fallback_url: str) -> dict:
    receipt = path / RECEIPT
    if receipt.exists():
        payload = json.loads(receipt.read_text())
        metadata = {name: {'fetched_at': row['fetched_at'], 'source_url': row['source_url']}
                    for name, row in payload['files'].items()}
        metadata[RECEIPT] = {'fetched_at': payload['acquired_at'], 'source_url': fallback_url}
        return metadata
    now = datetime.now(timezone.utc).isoformat()
    return {p.relative_to(path).as_posix(): {'fetched_at': now, 'source_url': fallback_url}
            for p in path.rglob('*') if p.is_file() and p.name != 'manifest.json'}


def import_files(source: str, vintage: str, files: Mapping[str, Path], *,
                 source_urls: Mapping[str, str] | None = None,
                 fetched_at: str | None = None, method: str = 'local-copy') -> None:
    """Copy original bytes without editing originals or replacing raw files.

    For pre-existing local files, fetched_at means acquisition into this repo;
    the original download time is unknown and recorded as such in the receipt.
    Caller-supplied URLs must be credential-free public provenance URLs.
    """
    destination = vintage_path(source, vintage)
    if destination.exists():
        raise AcquisitionError(f'Refusing to replace existing vintage: {destination}')
    if not files or any(Path(n).name != n or n.startswith('.') or n in {'manifest.json', RECEIPT}
                        for n in files):
        raise AcquisitionError('Imports require safe data filenames, not manifest names')
    urls = source_urls or {}
    if set(urls) - files.keys():
        raise AcquisitionError('Source URL has no matching imported file')
    for url in urls.values():
        if not url.startswith('https://') or any(token in url.lower() for token in ['api_key=', 'token=', '@']):
            raise AcquisitionError('Use a public HTTPS provenance URL without credentials')
    scratch = PROJECT_ROOT / 'work' / 'acquisition'
    scratch.mkdir(parents=True, exist_ok=True)
    acquired_at = fetched_at or datetime.now(timezone.utc).isoformat()
    receipt = {'source': source, 'vintage': vintage, 'method': method,
               'acquired_at': acquired_at, 'files': {}}
    with tempfile.TemporaryDirectory(dir=scratch) as tmp:
        stage = Path(tmp)
        for name, original in sorted(files.items()):
            original = Path(original)
            if original.is_symlink() or not original.is_file():
                raise AcquisitionError(f'Expected a regular original file: {original}')
            print(f'{source}/{vintage}: staging {name} ({original.stat().st_size:,} bytes)', flush=True)
            before = original.stat()
            shutil.copyfile(original, stage / name)
            after = original.stat()
            if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
                raise AcquisitionError(f'Original changed during import: {original}')
            # Digest the complete copy; seal() independently hashes it again.
            with (stage / name).open('rb') as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
            receipt['files'][name] = {
                'original_path': str(original.resolve()), 'original_download_at': None,
                'fetched_at': acquired_at, 'source_url': urls.get(name, SOURCES[source].url),
                'sha256_at_acquisition': digest,
            }
        validate(source, stage, vintage)
        (stage / RECEIPT).write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
        seal(stage, metadata=_metadata(stage, SOURCES[source].url))
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive mkdir is the publication lock. Data first, manifest last.
        # An interrupted publication remains visibly unsealed; no raw cleanup.
        destination.mkdir()
        members = sorted(p for p in stage.iterdir() if p.name != 'manifest.json')
        for path in [*members, stage / 'manifest.json']:
            os.link(path, destination / path.name)
    print(f'{source}/{vintage}: sealed and published', flush=True)


def download(url: str, destination: Path) -> str:
    """Verified TLS, bounded timeout, complete transfer, no secret-bearing errors.

    SSL_CERT_FILE may point to an installed CA bundle if the Python runtime's
    default trust store is unavailable. TLS verification is never disabled.
    """
    backend = os.environ.get('VIGIL_HTTP_BACKEND', 'urllib')
    if backend == 'curl':
        # The URL is sent over stdin, keeping SAM credentials out of process
        # arguments. Ignore curlrc and require HTTPS on every redirect.
        if destination.exists():
            raise AcquisitionError('Refusing to overwrite a download destination')
        if not url.startswith('https://') or any(c in url for c in ['\\', '"', '\r', '\n']):
            raise AcquisitionError('Invalid HTTPS download URL')
        with destination.open('xb') as stream:
            try:
                result = subprocess.run(
                    ['curl', '--disable', '--config', '-', '--fail', '--location',
                     '--proto', '=https', '--proto-redir', '=https',
                     '--connect-timeout', '30', '--max-time', '300', '--silent',
                     '--user-agent', 'VigilGraph/0.1'],
                    input=f'url = "{url}"\n'.encode(), stdout=stream,
                    stderr=subprocess.PIPE, timeout=310, check=False)
            except (OSError, subprocess.TimeoutExpired):
                raise AcquisitionError('System curl transport failed; retry or use manual drop-in') from None
        if result.returncode or not destination.stat().st_size:
            raise AcquisitionError(f'System curl download failed (exit {result.returncode}); retry or use manual drop-in')
        return datetime.now(timezone.utc).isoformat()
    if backend != 'urllib':
        raise AcquisitionError('VIGIL_HTTP_BACKEND must be urllib or curl')
    request = urllib.request.Request(url, headers={'User-Agent': 'VigilGraph/0.1'})
    try:
        with urllib.request.urlopen(request, timeout=60, context=ssl.create_default_context()) as response:
            expected = response.headers.get('Content-Length')
            actual = 0
            with destination.open('xb') as stream:
                while chunk := response.read(1024 * 1024):
                    stream.write(chunk)
                    actual += len(chunk)
            if not actual or (expected is not None and actual != int(expected)):
                raise AcquisitionError('Incomplete or empty HTTP download')
    except (OSError, urllib.error.URLError) as error:
        # SAM embeds an API key in the request URL. Never propagate that URL.
        reason = 'TLS verification' if isinstance(getattr(error, 'reason', error), ssl.SSLError) else type(error).__name__
        raise AcquisitionError(f'Download failed ({reason}); retry or use manual drop-in') from None
    return datetime.now(timezone.utc).isoformat()
