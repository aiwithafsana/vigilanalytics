from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
from urllib.error import URLError
import zipfile

import pytest

from common.manifest import ImmutabilityError
from ingest import acquisition, leie, sam
from ingest.acquisition import AcquisitionError, MissingInputError, fetch_manual, import_files, vintage_path
from ingest.registry import statuses
from ingest.validation import (LEIE_COLUMNS, NPPES_COLUMNS, PROVIDER_COLUMNS, SERVICE_COLUMNS,
                               SAM_COLUMNS, ENROLLMENT_COLUMNS, OWNER_COLUMNS, ValidationError, validate)
from pipeline import main


@pytest.fixture(autouse=True)
def isolated_data(tmp_path, monkeypatch):
    monkeypatch.setenv('VIGIL_DATA_ROOT', str(tmp_path / 'data'))
    monkeypatch.delenv('SAM_API_KEY', raising=False)
    monkeypatch.delenv('VIGIL_HTTP_BACKEND', raising=False)
    monkeypatch.setattr(acquisition, 'PROJECT_ROOT', tmp_path)


def write_csv(path, columns):
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(sorted(columns))
        writer.writerow(['value'] * len(columns))
    return path


def local_leie(tmp_path):
    return write_csv(tmp_path / 'original.csv', LEIE_COLUMNS)


def test_import_copies_original_and_idempotent_fetch_preserves_manifest(tmp_path):
    original = local_leie(tmp_path)
    before = original.read_bytes()
    import_files('leie', '2026-11', {'UPDATED.csv': original})
    root = vintage_path('leie', '2026-11')
    assert original.read_bytes() == before == (root / 'UPDATED.csv').read_bytes()
    manifest = (root / 'manifest.json').read_bytes()
    fetch_manual('leie', '2026-11')
    assert manifest == (root / 'manifest.json').read_bytes()
    receipt = json.loads((root / acquisition.RECEIPT).read_text())
    assert receipt['files']['UPDATED.csv']['original_path'] == str(original)
    assert receipt['files']['UPDATED.csv']['original_download_at'] is None
    assert statuses(verify=True)[1].state == 'VERIFIED'


def test_import_never_overwrites_a_vintage(tmp_path):
    original = local_leie(tmp_path)
    import_files('leie', '2026-11', {'UPDATED.csv': original})
    with pytest.raises(AcquisitionError, match='replace'):
        import_files('leie', '2026-11', {'UPDATED.csv': original})


def test_fetch_rejects_tampering_before_any_redownload(tmp_path, monkeypatch):
    original = local_leie(tmp_path)
    import_files('leie', '2026-11', {'UPDATED.csv': original})
    (vintage_path('leie', '2026-11') / 'UPDATED.csv').write_text('tampered')
    monkeypatch.setattr(leie, 'download', lambda *args: pytest.fail('Must not redownload'))
    with pytest.raises(ImmutabilityError):
        leie.fetch('2026-11')


def test_missing_manual_file_prints_actionable_instructions(capsys):
    with pytest.raises(MissingInputError):
        fetch_manual('nppes', '2026-02')
    output = capsys.readouterr().out
    assert 'https://download.cms.gov/nppes/NPI_Files.html' in output
    assert 'npidata_pfile' in output
    assert not vintage_path('nppes', '2026-02').exists()


def test_bad_headers_fail_without_publishing(tmp_path):
    original = write_csv(tmp_path / 'bad.csv', {'wrong'})
    with pytest.raises(ValidationError, match='header'):
        import_files('leie', '2026-11', {'UPDATED.csv': original})
    assert not vintage_path('leie', '2026-11').exists()


@pytest.mark.parametrize('vintage', ['../escape', '2026/11', '2026-13', '2026-02-30', '2026-11-01/..'])
def test_vintage_path_rejects_invalid_labels(vintage):
    with pytest.raises(AcquisitionError):
        vintage_path('nppes', vintage)


def test_nppes_requires_full_monthly_header(tmp_path):
    root = tmp_path / 'raw'
    path = write_csv(root / 'npidata_pfile_20050523-20260208.csv', NPPES_COLUMNS)
    validate('nppes', root, '2026-02')
    write_csv(path, {'NPI', 'Provider First Name'})
    with pytest.raises(ValidationError):
        validate('nppes', root, '2026-02')


def test_part_b_requires_pair_with_matching_year(tmp_path):
    root = tmp_path / 'raw'
    write_csv(root / 'MUP_PHY_R25_P07_V20_D22_Prov.csv', PROVIDER_COLUMNS)
    with pytest.raises(ValidationError, match='both'):
        validate('cms_part_b', root, '2022')
    write_csv(root / 'MUP_PHY_R25_P05_V20_D23_Prov_Svc.csv', SERVICE_COLUMNS)
    with pytest.raises(ValidationError, match='year'):
        validate('cms_part_b', root, '2022')


def test_valid_part_b_pair_seals(tmp_path):
    provider = write_csv(tmp_path / 'MUP_PHY_R25_P07_V20_D22_Prov.csv', PROVIDER_COLUMNS)
    service = write_csv(tmp_path / 'MUP_PHY_R25_P05_V20_D22_Prov_Svc.csv', SERVICE_COLUMNS)
    import_files('cms_part_b', '2022', {p.name: p for p in [provider, service]})
    assert (vintage_path('cms_part_b', '2022') / 'manifest.json').exists()


def test_shared_archive_preserves_headerless_five_column_layout(tmp_path):
    path = tmp_path / 'pspp2015_90.zip'
    with zipfile.ZipFile(path, 'w') as z:
        z.writestr('physician-shared-patient-patterns-2015-days90.txt',
                   '1000000004,1790775229,13        ,12        ,13\r\n')
    import_files('shared_patients', '2015', {path.name: path})
    assert (vintage_path('shared_patients', '2015') / path.name).read_bytes() == path.read_bytes()


@pytest.mark.parametrize('member', ['../escape.txt', 'physician-shared-patient-patterns-2014-days90.txt'])
def test_rejects_unsafe_or_mismatched_archive(tmp_path, member):
    path = tmp_path / 'pspp2015_90.zip'
    with zipfile.ZipFile(path, 'w') as z:
        z.writestr(member, '1000000004,1790775229,13,12,13\n')
    with pytest.raises(ValidationError):
        import_files('shared_patients', '2015', {path.name: path})


def test_pecos_enrollment_and_ownership_must_be_paired(tmp_path):
    root = tmp_path / 'raw'
    write_csv(root / 'HHA_All_Owners_2026.07.17.csv', OWNER_COLUMNS)
    with pytest.raises(ValidationError, match='matched'):
        validate('pecos', root, '2026-07-17')
    write_csv(root / 'HHA_Enrollments_2026.07.17.csv', ENROLLMENT_COLUMNS)
    validate('pecos', root, '2026-07-17')


def test_registry_missing_optional_unsealed_and_staleness(tmp_path):
    initial = statuses()
    assert len(initial) == 6
    assert initial[0].state == 'MISSING'
    assert initial[-1].state == 'OPTIONAL-MISSING'
    stamp = datetime.now(timezone.utc) - timedelta(days=10)
    original = local_leie(tmp_path)
    import_files('leie', '2026-10', {'UPDATED.csv': original}, fetched_at=stamp.isoformat())
    assert statuses()[1].staleness_days == 10
    vintage_path('leie', '2026-11').mkdir()
    assert statuses()[1].state == 'UNSEALED'
    assert statuses()[1].vintage == '2026-11'


def test_cli_status_and_missing_gate(capsys):
    assert main(['status']) == 0
    output = capsys.readouterr().out
    assert 'DAYS SINCE FETCH' in output
    assert all(source in output for source in ['nppes', 'leie', 'cms_part_b', 'shared_patients', 'pecos', 'sam'])
    assert main(['--through', 'stage0']) == 1
    assert 'missing required' in capsys.readouterr().err
    assert main(['--through', 'stage2']) == 1


def test_leie_automatic_download_is_sealed(tmp_path, monkeypatch):
    original = local_leie(tmp_path)
    def fake_download(url, destination):
        destination.write_bytes(original.read_bytes())
        return datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(leie, 'download', fake_download)
    leie.fetch('2026-11')
    receipt = json.loads((vintage_path('leie', '2026-11') / acquisition.RECEIPT).read_text())
    assert receipt['method'] == 'https-download'
    assert receipt['files']['UPDATED.csv']['source_url'].startswith('https://oig.hhs.gov/')


def test_network_failure_does_not_publish(tmp_path, monkeypatch):
    def broken(*args, **kwargs):
        raise URLError('https://example.org/?api_key=SECRET')
    monkeypatch.setattr(acquisition.urllib.request, 'urlopen', broken)
    with pytest.raises(AcquisitionError) as caught:
        leie.fetch('2026-11')
    assert 'SECRET' not in str(caught.value)
    assert not vintage_path('leie', '2026-11').exists()


def test_incomplete_http_response_rejected(tmp_path, monkeypatch):
    class Response(io.BytesIO):
        headers = {'Content-Length': '100'}
    monkeypatch.setattr(acquisition.urllib.request, 'urlopen', lambda *a, **k: Response(b'short'))
    with pytest.raises(AcquisitionError, match='Incomplete'):
        acquisition.download('https://example.org/file.csv', tmp_path / 'out.csv')


def test_sam_without_api_key_prints_manual_instructions(capsys):
    with pytest.raises(MissingInputError):
        sam.fetch('2026-11')
    assert 'SAM_API_KEY' in capsys.readouterr().out


def test_sam_api_key_is_not_persisted(tmp_path, monkeypatch):
    monkeypatch.setenv('SAM_API_KEY', 'SUPERSECRET')
    def fake_download(url, destination):
        assert 'api_key=SUPERSECRET' in url
        assert 'fileType=EXCLUSION' in url and 'frequency=DAILY' in url
        csv_path = write_csv(tmp_path / f'{destination.stem}.CSV', SAM_COLUMNS)
        with zipfile.ZipFile(destination, 'w') as z:
            z.write(csv_path, csv_path.name)
        return datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(sam, 'download', fake_download)
    sam.fetch('2026-11-01')
    root = vintage_path('sam', '2026-11-01')
    for name in ['manifest.json', acquisition.RECEIPT]:
        assert 'SUPERSECRET' not in (root / name).read_text()
        assert 'api_key' not in (root / name).read_text()



def test_explicit_curl_backend_uses_https_and_hides_url_from_process_args(tmp_path, monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setenv('VIGIL_HTTP_BACKEND', 'curl')
    def curl(args, **kwargs):
        assert '--disable' in args and '--proto-redir' in args
        assert 'SUPERSECRET' not in str(args)
        assert b'api_key=SUPERSECRET' in kwargs['input']
        kwargs['stdout'].write(b'data')
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(acquisition.subprocess, 'run', curl)
    path = tmp_path / 'response.zip'
    acquisition.download('https://example.org/?api_key=SUPERSECRET', path)
    assert path.read_bytes() == b'data'


def test_curl_failure_remains_a_failure_without_leaking_credentials(tmp_path, monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setenv('VIGIL_HTTP_BACKEND', 'curl')
    monkeypatch.setattr(acquisition.subprocess, 'run', lambda *a, **k: SimpleNamespace(returncode=60))
    with pytest.raises(AcquisitionError, match='exit 60'):
        acquisition.download('https://example.org/?api_key=SUPERSECRET', tmp_path / 'response.zip')


def test_wrong_nppes_vintage_rejected(tmp_path):
    root = tmp_path / 'raw'
    write_csv(root / 'npidata_pfile_20050523-20260208.csv', NPPES_COLUMNS)
    with pytest.raises(ValidationError, match='end date'):
        validate('nppes', root, '2026-03')


def test_sam_wrong_snapshot_date_rejected(tmp_path):
    root = tmp_path / 'raw'
    write_csv(root / 'SAM_Exclusions_Public_Extract_V2_22018.CSV', SAM_COLUMNS)
    with pytest.raises(ValidationError, match='date'):
        validate('sam', root, '2026-11')
