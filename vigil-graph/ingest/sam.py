"""GSA publishes daily exclusions extracts; store a snapshot each month.

API: https://open.gsa.gov/api/sam-entity-extracts-api/
No API key is ever written to manifests, logs, or errors.
"""
from datetime import date
import os
from pathlib import Path
import tempfile
from urllib.parse import urlencode

from ingest.acquisition import download, existing, fetch_manual, import_files

ENDPOINT = 'https://api.sam.gov/data-services/v1/extracts'


def fetch(vintage: str) -> None:
    if existing('sam', vintage):
        print(f'sam/{vintage}: verified sealed vintage')
        return
    from ingest.acquisition import vintage_path
    if vintage_path('sam', vintage).exists() or not os.environ.get('SAM_API_KEY'):
        fetch_manual('sam', vintage)
        return
    # Explicitly choose a daily extract, rather than mistakenly fetching the
    # monthly ENTITY extract (a different dataset).
    day = date.fromisoformat(vintage + {4: '-01-01', 7: '-01', 10: ''}[len(vintage)])
    params = {'fileType': 'EXCLUSION', 'sensitivity': 'PUBLIC', 'frequency': 'DAILY',
              'date': day.strftime('%m/%d/%Y'), 'version': 'V2'}
    public_url = ENDPOINT + '?' + urlencode(params)
    request_url = public_url + '&' + urlencode({'api_key': os.environ['SAM_API_KEY']})
    name = f'SAM_Exclusions_Public_Extract_V2_{day.strftime("%y%j")}.ZIP'
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        stamp = download(request_url, path)
        import_files('sam', vintage, {name: path}, source_urls={name: public_url},
                     fetched_at=stamp, method='https-download')
