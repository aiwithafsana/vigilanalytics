"""Bounded schema checks for bulk CSVs; complete ZIP CRC verification.

CSV checks cover the header, first 100 rows, and a final newline. They are not
full row-level validation, which belongs to normalization. Hashes cover all bytes.
"""
import csv
from datetime import datetime
import io
from pathlib import Path
import re
import zipfile


class ValidationError(ValueError):
    pass


NPPES_COLUMNS = {
    'NPI', 'Entity Type Code', 'Provider Organization Name (Legal Business Name)',
    'Provider Last Name (Legal Name)', 'Provider First Name', 'Provider Enumeration Date',
    'NPI Deactivation Date', 'NPI Reactivation Date',
    'Provider First Line Business Practice Location Address',
    'Provider Second Line Business Practice Location Address',
    'Provider Business Practice Location Address City Name',
    'Provider Business Practice Location Address State Name',
    'Provider Business Practice Location Address Postal Code',
    'Provider Business Practice Location Address Telephone Number',
    'Provider First Line Business Mailing Address', 'Provider Second Line Business Mailing Address',
    'Provider Business Mailing Address City Name', 'Provider Business Mailing Address State Name',
    'Provider Business Mailing Address Postal Code', 'Healthcare Provider Taxonomy Code_1',
    'Provider License Number_1', 'Provider License Number State Code_1',
}
LEIE_COLUMNS = {'LASTNAME', 'FIRSTNAME', 'BUSNAME', 'NPI', 'EXCLTYPE', 'EXCLDATE',
                'REINDATE', 'WAIVERDATE', 'ADDRESS', 'CITY', 'STATE', 'ZIP'}
PROVIDER_COLUMNS = {'Rndrng_NPI', 'Rndrng_Prvdr_Last_Org_Name', 'Rndrng_Prvdr_First_Name',
                    'Rndrng_Prvdr_State_Abrvtn', 'Rndrng_Prvdr_Type',
                    'Tot_Srvcs', 'Tot_Benes', 'Tot_Mdcr_Pymt_Amt'}
SERVICE_COLUMNS = {'Rndrng_NPI', 'HCPCS_Cd', 'Tot_Srvcs', 'Tot_Benes', 'Avg_Mdcr_Pymt_Amt'}
OWNER_COLUMNS = {'ENROLLMENT ID', 'ASSOCIATE ID', 'ASSOCIATE ID - OWNER', 'TYPE - OWNER',
                 'ROLE TEXT - OWNER', 'ASSOCIATION DATE - OWNER', 'FIRST NAME - OWNER',
                 'LAST NAME - OWNER', 'ORGANIZATION NAME - OWNER', 'PERCENTAGE OWNERSHIP'}
ENROLLMENT_COLUMNS = {'ENROLLMENT ID', 'ASSOCIATE ID', 'NPI', 'ORGANIZATION NAME', 'STATE'}
PPEF_COLUMNS = {'NPI', 'PECOS_ASCT_CNTL_ID', 'ENRLMT_ID', 'FIRST_NAME', 'LAST_NAME', 'ORG_NAME'}
SAM_COLUMNS = {'Classification', 'Exclusion Type', 'Excluding Agency', 'Active Date', 'Termination Date'}


def _rows(stream, required: set[str], label: str) -> None:
    reader = csv.reader(stream, strict=True)
    header = next(reader, [])
    if len(set(header)) != len(header) or not required <= set(header):
        raise ValidationError(f'{label}: missing/duplicate header columns; missing={sorted(required - set(header))}')
    count = 0
    for row in reader:
        if len(row) != len(header):
            raise ValidationError(f'{label}: malformed sampled row {count + 2}')
        count += 1
        if count == 100:
            break
    if not count:
        raise ValidationError(f'{label}: no data rows')


def csv_file(path: Path, required: set[str]) -> None:
    if not path.stat().st_size:
        raise ValidationError(f'{path.name}: empty CSV')
    with path.open('rb') as stream:
        stream.seek(-1, 2)
        if stream.read(1) not in (b'\n', b'\r'):
            raise ValidationError(f'{path.name}: missing final newline (possible incomplete transfer)')
    try:
        with path.open(encoding='utf-8-sig', newline='') as stream:
            _rows(stream, required, path.name)
    except (csv.Error, UnicodeError) as error:
        raise ValidationError(f'{path.name}: invalid CSV encoding or syntax') from error


def shared_rows(stream, label: str) -> None:
    count = 0
    for row in csv.reader(stream, strict=True):
        cells = [cell.strip() for cell in row]
        if (len(cells) != 5 or not all(re.fullmatch(r'[0-9]+', c) for c in cells)
                or any(len(c) != 10 for c in cells[:2])):
            raise ValidationError(f'{label}: expected headerless NPI1,NPI2,pair_count,bene_count,same_day_count')
        count += 1
        if count == 100:
            break
    if not count:
        raise ValidationError(f'{label}: no shared patient rows')


def _zip(path: Path, required: set[str] | None = None) -> None:
    try:
        with zipfile.ZipFile(path) as archive:
            names = [n for n in archive.namelist() if not n.endswith('/')]
            if len(names) != 1:
                raise ValidationError(f'{path.name}: expected exactly one data member')
            name = names[0]
            if Path(name).name != name or name.startswith('.'):
                raise ValidationError(f'{path.name}: unsafe archive member')
            if required is None:
                match = re.fullmatch(r'physician-shared-patient-patterns-(\d{4})-days(30|60|90|180)\.txt', name, re.I)
                outer = re.fullmatch(r'pspp(\d{4})_(30|60|90|180)\.zip', path.name, re.I)
                if not match or not outer or match.groups() != outer.groups():
                    raise ValidationError(f'{path.name}: archive year/interval disagrees with member')
            elif Path(name).suffix.lower() != '.csv' or Path(name).stem.lower() != path.stem.lower():
                raise ValidationError(f'{path.name}: expected matching SAM CSV member')
            with archive.open(name) as raw, io.TextIOWrapper(raw, encoding='utf-8-sig', newline='') as stream:
                if required is None:
                    shared_rows(stream, name)
                else:
                    _rows(stream, required, name)
            if archive.testzip() is not None:
                raise ValidationError(f'{path.name}: ZIP CRC mismatch')
    except (zipfile.BadZipFile, EOFError, csv.Error, UnicodeError) as error:
        raise ValidationError(f'{path.name}: corrupt ZIP/CSV') from error


def validate(source: str, root: Path, vintage: str) -> None:
    files = sorted(p for p in root.iterdir() if p.name not in {'manifest.json', 'acquisition.manifest.json'})
    if any(p.is_symlink() or not p.is_file() or not p.stat().st_size for p in files):
        raise ValidationError('Vintage must contain nonempty regular files only')
    if source == 'nppes':
        if len(files) != 1 or not re.fullmatch(r'npidata_pfile_\d{8}-\d{8}\.csv', files[0].name, re.I):
            raise ValidationError('Expected one monthly full npidata_pfile_START-END.csv')
        end_date = datetime.strptime(files[0].stem.rsplit('-', 1)[1], '%Y%m%d').date().isoformat()
        if not end_date.startswith(vintage):
            raise ValidationError('NPPES end date disagrees with vintage')
        csv_file(files[0], NPPES_COLUMNS)
    elif source == 'leie':
        if len(files) != 1 or files[0].name != 'UPDATED.csv':
            raise ValidationError('Expected UPDATED.csv')
        csv_file(files[0], LEIE_COLUMNS)
    elif source == 'cms_part_b':
        kinds = []
        for path in files:
            match = re.fullmatch(r'MUP_PHY_.+_D(\d{2})_(Prov|Prov_Svc)\.csv', path.name, re.I)
            if not match or f'20{match[1]}' != vintage[:4]:
                raise ValidationError(f'{path.name}: filename must preserve CMS data year matching vintage {vintage}')
            kinds.append(match[2].lower())
            csv_file(path, PROVIDER_COLUMNS if kinds[-1] == 'prov' else SERVICE_COLUMNS)
        if sorted(kinds) != ['prov', 'prov_svc']:
            raise ValidationError('Expected both provider and provider/service files for one year')
    elif source == 'shared_patients':
        if len(files) != 1:
            raise ValidationError('Expected one shared patient file per vintage')
        path = files[0]
        if path.suffix.lower() == '.zip':
            _zip(path)
            match = re.fullmatch(r'pspp(\d{4})_(30|60|90|180)\.zip', path.name, re.I)
        else:
            match = re.fullmatch(r'physician-shared-patient-patterns-(\d{4})-days(30|60|90|180)\.txt', path.name, re.I)
            with path.open(encoding='utf-8-sig', newline='') as stream:
                shared_rows(stream, path.name)
        if not match or match[1] != vintage[:4]:
            raise ValidationError('Shared patient filename must match vintage data year')
    elif source == 'pecos':
        groups: dict[str, set[str]] = {}
        for path in files:
            match = re.fullmatch(r'(.+)_(All_Owners|Enrollments)_(\d{4}\.\d{2}\.\d{2})\.csv', path.name)
            if match:
                group, kind, date = match.groups()
                if date.replace('.', '-') != vintage:
                    raise ValidationError(f'{path.name}: PECOS release date disagrees with vintage')
                groups.setdefault(group, set()).add(kind)
                csv_file(path, OWNER_COLUMNS if kind == 'All_Owners' else ENROLLMENT_COLUMNS)
            elif re.fullmatch(r'PPEF_Enrollment_Extract_\d{4}\.\d{2}\.\d{2}\.csv', path.name):
                if path.name[-14:-4].replace('.', '-') != vintage:
                    raise ValidationError('PPEF date disagrees with vintage')
                csv_file(path, PPEF_COLUMNS)
            else:
                raise ValidationError(f'{path.name}: unsupported PECOS file')
        if not groups or any(kinds != {'All_Owners', 'Enrollments'} for kinds in groups.values()):
            raise ValidationError('PECOS requires matched enrollment/ownership files')
    elif source == 'sam':
        if len(files) != 1 or not re.fullmatch(r'SAM_Exclusions_Public_Extract_V2_\d{5}\.(csv|zip)', files[0].name, re.I):
            raise ValidationError('Expected a SAM public exclusions V2 extract')
        julian = files[0].stem[-5:]
        day = datetime.strptime(julian, '%y%j').date()
        if day.strftime('%y%j') != julian or not day.isoformat().startswith(vintage):
            raise ValidationError('SAM extract date disagrees with vintage')
        if files[0].suffix.lower() == '.zip':
            _zip(files[0], SAM_COLUMNS)
        else:
            csv_file(files[0], SAM_COLUMNS)
    else:
        raise ValidationError(f'Unknown source: {source}')
