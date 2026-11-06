from pathlib import Path
import tempfile

from ingest.acquisition import download, existing, fetch_manual, import_files, vintage_path
from ingest.sources import SOURCES


def fetch(vintage: str) -> None:
    if existing('leie', vintage):
        print(f'leie/{vintage}: verified sealed vintage')
        return
    if vintage_path('leie', vintage).exists():
        fetch_manual('leie', vintage)
        return
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / 'UPDATED.csv'
        stamp = download(SOURCES['leie'].url, path)
        import_files('leie', vintage, {path.name: path},
                     source_urls={path.name: SOURCES['leie'].url}, fetched_at=stamp, method='https-download')
