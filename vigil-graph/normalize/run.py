"""Stage 1: sealed input adapters -> typed, ordered canonical Parquet tables."""
import importlib
from pathlib import Path
import tempfile

from ingest.acquisition import PROJECT_ROOT
from ingest.inputs import SealedInputs
from ingest.sources import SOURCES
from normalize import addresses
from normalize.engine import connect, publish, report


def run(output_root: Path | None = None) -> list[Path]:
    scratch=PROJECT_ROOT / 'work' / 'normalize'
    scratch.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch) as tmp:
        root=Path(tmp)
        with connect(root/'stage1.duckdb') as connection:
            addresses.initialize(connection)
            inputs=SealedInputs(connection,root/'inputs')
            for source,spec in SOURCES.items():
                vintages=inputs.vintages(source)
                if not vintages and spec.required:
                    raise ValueError(f'Missing required sealed source: {source}')
                if not vintages:
                    print(f'{source}: optional input absent',flush=True)
                for vintage in vintages:
                    print(f'Normalizing {source}/{vintage}',flush=True)
                    importlib.import_module(f'normalize.{source}').run(connection,inputs,vintage)
            addresses.finish(connection)
            report(connection,'addresses')
            # Duplicate provider IDs corrupt the later record-to-entity join.
            duplicate=connection.execute('SELECT record_id FROM providers GROUP BY 1 HAVING count(*)>1 LIMIT 1').fetchone()
            if duplicate:
                raise ValueError(f'Duplicate provider record_id: {duplicate[0]}')
            result=publish(connection,output_root)
    print(f'Stage 1 complete: {len(result)} canonical Parquet files',flush=True)
    return result
