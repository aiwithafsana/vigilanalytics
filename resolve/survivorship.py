"""Deterministic golden identities; all observed addresses and NPIs survive."""
from pathlib import Path

from resolve.storage import copy


def publish(c, output):
    output = Path(output)
    c.execute('''CREATE TABLE golden_names AS SELECT * EXCLUDE(rank) FROM (
        SELECT m.entity_id,n.*,row_number() OVER(PARTITION BY entity_id ORDER BY
            CASE WHEN coalesce(name_norm,'')='' THEN 1 ELSE 0 END,
            CASE WHEN source='nppes' THEN 0 ELSE 1 END,vintage DESC,record_id) AS rank
        FROM mapping m JOIN nodes n USING(node_id,record_id)) WHERE rank=1''')
    c.execute('''CREATE TABLE golden_addresses AS SELECT * EXCLUDE(rank) FROM (
        SELECT entity_id,addr_key,addr_type,vintage,city,state,
            row_number() OVER(PARTITION BY entity_id ORDER BY
                CASE WHEN addr_type='practice' THEN 0 ELSE 1 END,vintage DESC,addr_key,city,state) AS rank
        FROM mapping JOIN node_addresses USING(node_id)) WHERE rank=1''')
    c.execute('''CREATE TABLE entity_rows AS SELECT g.entity_id,
        (SELECT max(nullif(n.kind,0)) FROM mapping m JOIN nodes n USING(node_id)
         WHERE m.entity_id=g.entity_id)::INTEGER AS entity_type,
        g.name_norm,g.name_raw,g.record_id AS name_record_id,g.source AS name_source,
        coalesce(a.addr_key,'') AS addr_key,coalesce(a.addr_type,'') AS addr_type,
        a.vintage AS address_vintage,a.city,a.state,
        (SELECT count(*) FROM mapping m WHERE m.entity_id=g.entity_id) AS member_count
        FROM golden_names g LEFT JOIN golden_addresses a USING(entity_id)''')
    queries = {
        'entities':'SELECT * FROM entity_rows ORDER BY entity_id',
        'record_to_entity':'SELECT record_id,entity_id FROM mapping ORDER BY record_id',
        'entity_addresses':'''SELECT entity_id,addr_key,min(vintage) AS first_seen,max(vintage) AS last_seen
            FROM mapping JOIN node_addresses USING(node_id) GROUP BY 1,2 ORDER BY 1,2''',
        'entity_npis':'''SELECT DISTINCT entity_id,npi FROM mapping JOIN nodes USING(node_id)
            WHERE coalesce(npi,'')<>'' ORDER BY entity_id,npi''',
    }
    return [copy(c,q,output/f'{name}.parquet') for name,q in queries.items()]
