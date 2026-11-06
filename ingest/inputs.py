"""The normalization boundary: only this adapter reads sealed raw bytes.

Normalizers receive DuckDB staging table names, never open raw files themselves.
All raw hashes are checked before a vintage is exposed. Physical row ordinals
are deterministic evidence keys and survive duplicate source records.
"""
from pathlib import Path
import shutil
import zipfile

from common.manifest import seal
from ingest.acquisition import data_root, vintage_path
from ingest.sources import SOURCES


def qi(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class SealedInputs:
    def __init__(self, connection, scratch: Path):
        self.connection = connection
        self.scratch = scratch
        self.scratch.mkdir(parents=True, exist_ok=True)
        self.checked = set()
        self.counter = 0

    def vintages(self, source: str) -> list[str]:
        if source not in SOURCES:
            raise ValueError(f'Unknown source {source}')
        root = data_root() / 'raw' / source
        if not root.exists():
            return []
        result = []
        for path in sorted(root.iterdir()):
            if not path.is_dir():
                raise ValueError(f'Unexpected raw source entry: {path}')
            vintage_path(source, path.name)
            if not (path / 'manifest.json').is_file():
                raise ValueError(f'Unsealed vintage cannot be normalized: {path}')
            result.append(path.name)
        return result

    def files(self, source: str, vintage: str) -> list[str]:
        root = vintage_path(source, vintage)
        if (source, vintage) not in self.checked:
            seal(root)
            self.checked.add((source, vintage))
        return sorted(p.name for p in root.iterdir() if p.name not in {'manifest.json', 'acquisition.manifest.json'})

    def csv(self, source: str, vintage: str, filename: str, *, columns: list[str] | None = None,
            header: bool = True) -> str:
        if filename not in self.files(source, vintage):
            raise ValueError(f'File absent from verified vintage: {filename}')
        path = vintage_path(source, vintage) / filename
        if path.suffix.lower() == '.zip':
            with zipfile.ZipFile(path) as archive:
                names = archive.namelist()
                if len(names) != 1 or Path(names[0]).name != names[0]:
                    raise ValueError('Expected one safe shared-patient archive member')
                target = self.scratch / f'{source}-{vintage}-{names[0]}'
                with archive.open(names[0]) as src, target.open('wb') as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
                path = target
        if source == 'pecos':
            # CMS enrollment exports contain legacy Windows-1252 bytes in
            # some releases. Validate UTF-8 in full, then explicitly transcode
            # a legacy file; no byte/row replacement or raw mutation is allowed.
            try:
                with path.open(encoding='utf-8-sig') as stream:
                    while stream.read(1024 * 1024):
                        pass
            except UnicodeDecodeError:
                target = self.scratch / f'utf8-{filename}'
                print(f'pecos/{vintage}: transcoding {filename} from Windows-1252', flush=True)
                with path.open(encoding='cp1252', newline='') as src, target.open('w', encoding='utf-8', newline='') as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
                path = target
        self.counter += 1
        name = f'input_{self.counter}'
        projection = ', '.join(qi(c) for c in columns) if columns else '*'
        options = 'header=true, all_varchar=true, delim=\',\', parallel=false, strict_mode=true'
        if not header:
            options = "header=false, columns={'npi_a':'VARCHAR','npi_b':'VARCHAR','pair_count':'VARCHAR','bene_count':'VARCHAR','same_day_count':'VARCHAR'}, delim=',', parallel=false, strict_mode=true"
        query = f'SELECT row_number() OVER ()::BIGINT AS _row, {projection} FROM read_csv({sql_literal(str(path))}, {options})'
        self.connection.execute(f'CREATE TABLE {name} AS {query}')
        return name
