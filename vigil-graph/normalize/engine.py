"""Typed canonical schemas and deterministic DuckDB Parquet publication."""
from pathlib import Path
import json
import os

import duckdb

from common.normalization import (normalize_name, normalize_phone, normalize_address,
                                   normalize_authority, fraud_related, zip5)
from ingest.acquisition import data_root
from ingest.inputs import qi, sql_literal

SCHEMAS = {
    'provider_identifiers': [('record_id','VARCHAR'),('id_type','VARCHAR'),('id_value','VARCHAR'),('source','VARCHAR'),('vintage','VARCHAR')],
    'providers': [('record_id','VARCHAR'),('source','VARCHAR'),('vintage','VARCHAR'),('npi','VARCHAR'),
                  ('entity_type','INTEGER'),('name_raw','VARCHAR'),('name_norm','VARCHAR'),
                  ('first','VARCHAR'),('last','VARCHAR'),('org_name_norm','VARCHAR'),
                  ('specialty_code','VARCHAR'),('specialty_desc','VARCHAR'),('license_no','VARCHAR'),
                  ('license_state','VARCHAR'),('phone_norm','VARCHAR'),('enumeration_date','DATE')],
    'addresses': [('record_id','VARCHAR'),('source','VARCHAR'),('vintage','VARCHAR'),('npi','VARCHAR'),
                  ('addr_type','VARCHAR'),('addr_raw','VARCHAR'),('addr_key','VARCHAR'),('city','VARCHAR'),
                  ('state','VARCHAR'),('zip5','VARCHAR'),('lat','DOUBLE'),('lon','DOUBLE')],
    'exclusions': [('record_id','VARCHAR'),('vintage','VARCHAR'),('npi','VARCHAR'),('last','VARCHAR'),
                   ('first','VARCHAR'),('business_name','VARCHAR'),('excl_type','VARCHAR'),
                   ('excl_authority','VARCHAR'),('fraud_related','BOOLEAN'),('excl_date','DATE'),
                   ('rein_date','DATE'),('waiver_date','DATE')],
    'billing_summaries': [('npi','VARCHAR'),('year','INTEGER'),('total_services','DOUBLE'),
                         ('total_payment','DOUBLE'),('unique_beneficiaries','BIGINT'),
                         ('top_hcpcs','VARCHAR'),('state','VARCHAR'),('specialty','VARCHAR')],
    'ownerships': [('record_id','VARCHAR'),('vintage','VARCHAR'),('org_npi_or_pac','VARCHAR'),
                   ('owner_name_norm','VARCHAR'),('owner_role','VARCHAR'),('pct_ownership','DOUBLE'),
                   ('effective_date','DATE')],
    'shared_patients': [('npi_a','VARCHAR'),('npi_b','VARCHAR'),('shared_count','BIGINT'),('year','INTEGER')],
}
SORT_KEYS = {'provider_identifiers':'record_id, id_type, id_value', 'providers':'record_id', 'addresses':'record_id, addr_type',
             'exclusions':'record_id', 'billing_summaries':'npi, year',
             'ownerships':'record_id', 'shared_patients':'npi_a, npi_b, year'}


def connect(path: Path):
    connection = duckdb.connect(str(path))
    connection.execute("SET threads=1; SET memory_limit='3GB'; SET preserve_insertion_order=true")
    connection.create_function('norm_name', normalize_name, ['VARCHAR'], 'VARCHAR', null_handling='special')
    connection.create_function('norm_phone', normalize_phone, ['VARCHAR'], 'VARCHAR', null_handling='special')
    connection.create_function('norm_zip', zip5, ['VARCHAR'], 'VARCHAR', null_handling='special')
    connection.create_function('norm_authority', normalize_authority, ['VARCHAR'], 'VARCHAR', null_handling='special')
    connection.create_function('is_fraud', fraud_related, ['VARCHAR'], 'BOOLEAN', null_handling='special')
    def addr(line1, line2, city, state, postal):
        return normalize_address(dict(line1=line1,line2=line2,city=city,state=state,zip=postal))
    connection.create_function('norm_addr', addr, ['VARCHAR']*5, 'VARCHAR', null_handling='special')
    for table, columns in SCHEMAS.items():
        connection.execute(f'CREATE TABLE {table} ({", ".join(qi(k)+" "+t for k,t in columns)})')
    return connection


def text(column: str) -> str:
    return f"coalesce(trim({qi(column)}), '')"


def date_value(column: str, fmt: str) -> str:
    value = text(column)
    # Invalid nonempty dates are fatal; blank/zero sentinel dates are nullable.
    return f"CASE WHEN {value} IN ('', '00000000', '0') THEN NULL ELSE strptime({value}, '{fmt}')::DATE END"


def npi(column: str) -> str:
    value = text(column)
    # LEIE zero NPIs are explicitly missing; malformed identifiers fail loudly.
    return f"CASE WHEN {value} IN ('', '0', '0000000000') THEN '' WHEN regexp_full_match({value}, '[0-9]{{10}}') THEN {value} ELSE error('Malformed NPI') END"


def record_id(source: str, vintage: str, filename: str) -> str:
    return f"{sql_literal(source+':'+vintage+':'+filename+':')} || _row::VARCHAR"


def insert(connection, table: str, expressions: dict[str,str], relation: str) -> None:
    columns = SCHEMAS[table]
    values = [expressions.get(name, "''" if kind == 'VARCHAR' else 'NULL') for name, kind in columns]
    connection.execute(f'INSERT INTO {table} SELECT {", ".join(values)} FROM {relation}')


def report(connection, table: str, where: str = 'true') -> dict:
    columns = SCHEMAS[table]
    count = connection.execute(f'SELECT count(*) FROM {table} WHERE {where}').fetchone()[0]
    expressions = []
    for name, kind in columns:
        missing = f'{qi(name)} IS NULL'
        if kind == 'VARCHAR':
            missing += f" OR {qi(name)} = ''"
        expressions.append(f'sum(CASE WHEN {missing} THEN 1 ELSE 0 END)')
    aggregates = ', '.join(expressions)
    counts = connection.execute(f'SELECT {aggregates} FROM {table} WHERE {where}').fetchone()
    result = {'table': table, 'rows': count, 'null_rates': {name: (missing or 0)/count if count else 0.0 for (name,_), missing in zip(columns, counts)}}
    print(json.dumps(result, sort_keys=True), flush=True)
    return result


def publish(connection, output_root: Path | None = None) -> list[Path]:
    root = output_root or data_root() / 'canonical'
    result = []
    for table, columns in SCHEMAS.items():
        partition = 'vintage' if any(name=='vintage' for name,_ in columns) else 'year'
        vintages = connection.execute(f'SELECT DISTINCT {partition} FROM {table} ORDER BY 1').fetchall()
        if not vintages:
            # Optional ownership input can be absent; retain a typed empty table.
            vintages = [('empty',)]
        for (vintage,) in vintages:
            target = root / table / f'{vintage}.parquet'
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_suffix('.parquet.tmp')
            condition = 'false' if vintage=='empty' else f'{partition}={sql_literal(str(vintage))}'
            query = f'SELECT * FROM {table} WHERE {condition} ORDER BY {SORT_KEYS[table]}'
            connection.execute(f"COPY ({query}) TO {sql_literal(str(temporary))} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000, PER_THREAD_OUTPUT false)")
            os.replace(temporary, target)
            result.append(target)
    return result
