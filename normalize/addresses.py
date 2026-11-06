"""Shared address staging; parse each distinct structured address once."""
from ingest.inputs import sql_literal
from normalize.engine import insert, text


def initialize(connection):
    connection.execute('CREATE TABLE address_parts (record_id VARCHAR, addr_type VARCHAR, line1 VARCHAR, line2 VARCHAR, city VARCHAR, state VARCHAR, zip VARCHAR)')


def add(connection, relation: str, *, rid: str, source: str, vintage: str,
        npi: str, kind: str, line1: str, line2: str, city: str, state: str, postal: str):
    relation = f'(SELECT * FROM {relation} WHERE {line1} <> \'\')'
    expressions = dict(record_id=rid, source=sql_literal(source), vintage=sql_literal(vintage),
                       npi=npi, addr_type=sql_literal(kind),
                       addr_raw=f"concat_ws(', ', {line1}, {line2}, {city}, {state}, {postal})",
                       city=f'upper({city})', state=f'upper({state})', zip5=f'norm_zip({postal})')
    insert(connection, 'addresses', expressions, relation)
    connection.execute(f'INSERT INTO address_parts SELECT {rid}, {sql_literal(kind)}, {line1}, {line2}, {city}, {state}, {postal} FROM {relation}')


def finish(connection):
    connection.execute('CREATE TABLE address_dictionary AS SELECT DISTINCT line1,line2,city,state,zip FROM address_parts ORDER BY ALL')
    count = connection.execute('SELECT count(*) FROM address_dictionary').fetchone()[0]
    print(f'Parsing {count:,} distinct structured addresses with usaddress', flush=True)
    connection.execute('ALTER TABLE address_dictionary ADD COLUMN addr_key VARCHAR')
    connection.execute('UPDATE address_dictionary SET addr_key=norm_addr(line1,line2,city,state,zip)')
    connection.execute('''UPDATE addresses AS a SET addr_key=d.addr_key
        FROM address_parts p JOIN address_dictionary d USING(line1,line2,city,state,zip)
        WHERE a.record_id=p.record_id AND a.addr_type=p.addr_type''')
