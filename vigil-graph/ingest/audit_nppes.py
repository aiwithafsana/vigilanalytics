"""Read-only diagnostics of the documented NPI-only deactivation records."""
import json

import duckdb

from ingest.acquisition import vintage_path
from ingest.inputs import qi, sql_literal, nppes_suppression_predicate
from common.manifest import seal


def audit(vintage: str) -> dict:
    root = vintage_path('nppes', vintage)
    seal(root)
    files = sorted(root.glob('npidata_pfile_*.csv'))
    if len(files) != 1:
        raise ValueError('Expected one sealed NPPES CSV')
    source = f"read_csv({sql_literal(str(files[0]))}, header=true, all_varchar=true, parallel=false, strict_mode=true)"
    with duckdb.connect() as connection:
        connection.execute("SET threads=1; SET memory_limit='2GB'")
        names = [row[0] for row in connection.execute(f'DESCRIBE SELECT * FROM {source}').fetchall()]
        raw_name = f"CASE WHEN trim({qi('Entity Type Code')})='2' THEN {qi('Provider Organization Name (Legal Business Name)')} ELSE concat_ws(' ',{qi('Provider First Name')},{qi('Provider Last Name (Legal Name)')}) END"
        row = connection.execute(f'''WITH classified AS (
            SELECT ({nppes_suppression_predicate(names)}) AS tombstone,
                   coalesce(trim({raw_name}),'')='' AS nameless
            FROM {source})
            SELECT count(*), count(*) FILTER(WHERE tombstone),
                   count(*) FILTER(WHERE nameless),
                   count(*) FILTER(WHERE nameless AND NOT tombstone)
            FROM classified''').fetchone()
    return dict(vintage=vintage,source_rows=row[0],documented_npi_only_deactivations=row[1],
                raw_nameless_rows=row[2],raw_nameless_identity_rows=row[3],
                identity_rows=row[0]-row[1])


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vintage',required=True)
    args=parser.parse_args()
    print(json.dumps(audit(args.vintage),sort_keys=True,indent=2))
