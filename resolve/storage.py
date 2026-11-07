"""Stage 2 reads only canonical Parquet. Scratch SQL is disk backed."""
import json
import os
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def data_root():
    return Path(os.environ.get('VIGIL_DATA_ROOT', PROJECT_ROOT / 'data'))


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def connect(path, config):
    c = duckdb.connect(str(path))
    c.execute(f"SET memory_limit={literal(config.memory_limit)}")
    c.execute('SET threads=1; SET preserve_insertion_order=false')
    return c


def load(c, canonical):
    for name in ('providers', 'addresses'):
        files = sorted((Path(canonical) / name).glob('*.parquet'))
        if not files:
            raise ValueError(f'Missing canonical {name}; run Stage 1 first')
        c.read_parquet([str(p) for p in files]).create_view(name)
    if c.execute('SELECT record_id FROM providers GROUP BY 1 HAVING count(*)>1 LIMIT 1').fetchall():
        raise ValueError('Duplicate canonical provider record_id')
    if c.execute('SELECT 1 FROM providers WHERE record_id IS NULL OR record_id=\'\' LIMIT 1').fetchall():
        raise ValueError('Empty canonical provider record_id')
    c.execute('''CREATE TABLE nodes AS SELECT
        row_number() OVER(ORDER BY record_id)-1 AS node_id, *,
        coalesce(entity_type, 0)::UTINYINT AS kind FROM providers''')
    if c.execute('SELECT 1 FROM nodes WHERE kind NOT IN (0,1,2) LIMIT 1').fetchall():
        raise ValueError('Unknown canonical entity_type')
    c.execute('''CREATE TABLE node_addresses AS SELECT DISTINCT n.node_id,
        a.addr_key, a.addr_type, a.vintage, a.city, a.state
        FROM addresses a JOIN nodes n USING(record_id)
        WHERE coalesce(a.addr_key,'')<>\'\' ''')


def copy(c, query, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    c.execute(f'COPY ({query}) TO {literal(path)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)').fetchall()
    return path


def write_json(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')
