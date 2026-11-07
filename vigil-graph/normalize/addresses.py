"""Bounded-memory address normalization with stable positional source alignment."""
from collections import deque
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import os

import pandas as pd

from ingest.inputs import sql_literal, qi
from normalize.address_worker import normalize_batch
from normalize.engine import insert, SCHEMAS

PARALLEL_THRESHOLD=100_000
BATCH_SIZE=10_000


def initialize(connection):
    connection.execute('CREATE TABLE address_parts (record_id VARCHAR, addr_type VARCHAR, line1 VARCHAR, line2 VARCHAR, city VARCHAR, state VARCHAR, zip VARCHAR)')


def add(connection, relation: str, *, rid: str, source: str, vintage: str,
        npi: str, kind: str, line1: str, line2: str, city: str, state: str, postal: str):
    relation=f'(SELECT * FROM {relation} WHERE {line1} <> \'\')'
    expressions=dict(record_id=rid,source=sql_literal(source),vintage=sql_literal(vintage),
                     npi=npi,addr_type=sql_literal(kind),
                     addr_raw=f"concat_ws(', ', {line1}, {line2}, {city}, {state}, {postal})",
                     city=f'upper({city})',state=f'upper({state})',zip5=f'norm_zip({postal})')
    insert(connection,'addresses',expressions,relation)
    connection.execute(f'INSERT INTO address_parts SELECT {rid}, {sql_literal(kind)}, {line1}, {line2}, {city}, {state}, {postal} FROM {relation}')


def _parse_dictionary(connection,count,workers):
    connection.execute('ALTER TABLE address_dictionary ADD COLUMN addr_key VARCHAR')
    if count<PARALLEL_THRESHOLD or workers==1:
        connection.execute('UPDATE address_dictionary SET addr_key=norm_addr(line1,line2,city,state,zip)')
        return
    connection.execute('CREATE TABLE normalized_address_keys (address_id BIGINT, addr_key VARCHAR)')
    def chunks():
        for start in range(0,count,BATCH_SIZE):
            yield connection.execute(f'''SELECT rowid,line1,line2,city,state,zip FROM address_dictionary
                WHERE rowid>={start} AND rowid<{start+BATCH_SIZE} ORDER BY rowid''').fetchall()
    iterator=iter(chunks())
    # Keep only one batch per worker in flight. Executor.map on Python 3.11
    # eagerly queues the entire iterable and would retain millions of strings.
    with ProcessPoolExecutor(max_workers=workers,mp_context=multiprocessing.get_context('spawn')) as pool:
        pending=deque()
        for _ in range(workers):
            batch=next(iterator,None)
            if batch is not None:
                pending.append(pool.submit(normalize_batch,batch))
        completed=0
        while pending:
            rows=pending.popleft().result()
            frame=pd.DataFrame(rows,columns=['address_id','addr_key'])
            connection.register('_address_batch',frame)
            connection.execute('INSERT INTO normalized_address_keys SELECT * FROM _address_batch')
            connection.unregister('_address_batch')
            completed+=len(rows)
            if completed%100_000==0 or completed==count:
                print(f'Parsed address keys: {completed:,}/{count:,}',flush=True)
            batch=next(iterator,None)
            if batch is not None:
                pending.append(pool.submit(normalize_batch,batch))
    connection.execute('''UPDATE address_dictionary AS d SET addr_key=k.addr_key
        FROM normalized_address_keys k WHERE d.rowid=k.address_id''')
    connection.execute('DROP TABLE normalized_address_keys')


def finish(connection):
    # add() appends both tables from identical ordered source relations. Verify
    # that alignment before exploiting a streaming positional join; never rely
    # on an unchecked assumption about row positions.
    mismatch=connection.execute('''SELECT count(*) FROM addresses a POSITIONAL JOIN address_parts p
        WHERE a.record_id IS DISTINCT FROM p.record_id OR a.addr_type IS DISTINCT FROM p.addr_type
           OR a.rowid IS DISTINCT FROM p.rowid''').fetchone()[0]
    if mismatch:
        raise ValueError('Address staging lost source-row alignment')
    connection.execute('CREATE TABLE address_dictionary AS SELECT DISTINCT line1,line2,city,state,zip FROM address_parts ORDER BY ALL')
    count=connection.execute('SELECT count(*) FROM address_dictionary').fetchone()[0]
    workers=int(os.environ.get('VIGIL_ADDRESS_WORKERS',str(min(4,os.cpu_count() or 1))))
    if not 1<=workers<=8:
        raise ValueError('VIGIL_ADDRESS_WORKERS must be between 1 and 8')
    print(f'Parsing {count:,} distinct structured addresses with usaddress ({workers} workers)',flush=True)
    _parse_dictionary(connection,count,workers)
    # Resolve only narrow ordinal/key columns in the large join. Carrying wide
    # record IDs through it, or updating every destination row in one statement,
    # retains too much memory. Complete wide output in bounded ordinal batches.
    connection.execute('SET preserve_insertion_order=false')
    projection=', '.join('k.addr_key AS addr_key' if name=='addr_key' else f'a.{qi(name)}'
                         for name,_ in SCHEMAS['addresses'])
    equal=' AND '.join(f'p.{qi(name)}=d.{qi(name)}' for name in ['line1','line2','city','state','zip'])
    connection.execute(f'''CREATE TABLE address_row_keys AS SELECT p.rowid AS ordinal,d.addr_key
        FROM address_parts p JOIN address_dictionary d ON {equal} ORDER BY ordinal''')
    expected=connection.execute('SELECT count(*) FROM addresses').fetchone()[0]
    last=connection.execute('SELECT coalesce(max(rowid)+1,0) FROM addresses').fetchone()[0]
    if last!=expected:
        raise ValueError('Address staging ordinals are not contiguous')
    connection.execute('CREATE TABLE addresses_complete AS SELECT * FROM addresses WHERE false')
    for start in range(0,expected,4096):
        end=start+4096
        connection.execute(f'''INSERT INTO addresses_complete SELECT {projection}
            FROM addresses a JOIN address_row_keys k ON a.rowid=k.ordinal
            WHERE a.rowid>={start} AND a.rowid<{end} AND k.ordinal>={start} AND k.ordinal<{end}''')
        if end%1_048_576==0:
            print(f'Joined address keys: {min(end,expected):,}/{expected:,}',flush=True)
    actual=connection.execute('SELECT count(*) FROM addresses_complete').fetchone()[0]
    if actual!=expected:
        raise ValueError(f'Address completion changed row count: {expected} -> {actual}')
    connection.execute('DROP TABLE addresses')
    connection.execute('ALTER TABLE addresses_complete RENAME TO addresses')
    for table in ['address_parts','address_dictionary','address_row_keys']:
        connection.execute(f'DROP TABLE {table}')
    print(f'Address keys joined: {actual:,} rows; source-row count preserved',flush=True)
