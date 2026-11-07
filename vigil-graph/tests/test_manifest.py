import hashlib
import json
from pathlib import Path

import pytest

from common.manifest import ImmutabilityError, seal


@pytest.fixture
def vintage(tmp_path):
    root = tmp_path / '2026-11'
    root.mkdir()
    (root / 'source.csv').write_bytes(b'npi,name\n123,CHEN\n')
    return root


def test_seal_records_hash_size_and_fetch_metadata(vintage):
    metadata = {'source.csv': {'fetched_at': '2026-11-05T00:00:00+00:00',
                               'source_url': 'https://example.org/source.csv'}}
    seal(vintage, metadata=metadata)
    record = json.loads((vintage / 'manifest.json').read_text())['files'][0]
    assert record == dict(path='source.csv', bytes=18,
                         sha256=hashlib.sha256((vintage / 'source.csv').read_bytes()).hexdigest(),
                         **metadata['source.csv'])


def test_idempotent_manifest_bytes(vintage):
    seal(vintage)
    manifest = vintage / 'manifest.json'
    before = manifest.read_bytes(), manifest.stat().st_mtime_ns
    seal(vintage)
    assert (manifest.read_bytes(), manifest.stat().st_mtime_ns) == before


@pytest.mark.parametrize('change', ['mutate', 'add', 'remove'])
def test_rejects_changed_sealed_vintage(vintage, change):
    seal(vintage)
    before = (vintage / 'manifest.json').read_bytes()
    if change == 'mutate':
        (vintage / 'source.csv').write_bytes(b'npi,name\n123,JANE\n')
    elif change == 'add':
        (vintage / 'extra.csv').write_text('extra')
    else:
        (vintage / 'source.csv').unlink()
    with pytest.raises(ImmutabilityError, match='source.csv|extra.csv'):
        seal(vintage)
    assert (vintage / 'manifest.json').read_bytes() == before


def test_nested_paths_are_sorted(vintage):
    (vintage / 'a').mkdir()
    (vintage / 'a' / 'file.csv').write_text('data')
    seal(vintage)
    rows = json.loads((vintage / 'manifest.json').read_text())['files']
    assert [r['path'] for r in rows] == ['a/file.csv', 'source.csv']


def test_empty_vintage_fails(tmp_path):
    with pytest.raises(ValueError, match='empty'):
        seal(tmp_path)


def test_symlink_fails(vintage):
    (vintage / 'alias').symlink_to(vintage / 'source.csv')
    with pytest.raises(ValueError, match='Symlinks'):
        seal(vintage)


def test_corrupt_manifest_fails(vintage):
    (vintage / 'manifest.json').write_text('{}')
    with pytest.raises(ImmutabilityError, match='Invalid manifest'):
        seal(vintage)


def test_missing_directory_fails(tmp_path):
    with pytest.raises(ValueError, match='directory'):
        seal(tmp_path / 'missing')
