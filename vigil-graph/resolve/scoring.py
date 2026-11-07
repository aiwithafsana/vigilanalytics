"""Rule v1. Persist each feature for a future reviewed Splink training set."""
from pathlib import Path

import jellyfish

from resolve.storage import copy


def _value(record, key):
    return (record.get(key) if isinstance(record,dict) else getattr(record,key,None)) or ''


def score_pair(a, b, blocks=None):
    get = _value
    if get(a,'npi') and get(a,'npi') == get(b,'npi'):
        return 1.0, {'npi_exact':1}
    names = get(a,'name_norm'),get(b,'name_norm')
    addr_a = set(get(a,'addr_keys') or [get(a,'addr_key')]) - {''}
    addr_b = set(get(b,'addr_keys') or [get(b,'addr_key')]) - {''}
    f = {'name':jellyfish.jaro_winkler_similarity(*names) if all(names) else 0.0,
         'addr':float(bool(addr_a & addr_b)),
         'phone':float(bool(get(a,'phone_norm')) and get(a,'phone_norm')==get(b,'phone_norm')),
         'license':float(bool(get(a,'license_no') and get(a,'license_state')) and
                         get(a,'license_no')==get(b,'license_no') and get(a,'license_state')==get(b,'license_state'))}
    score = .4*f['name'] + .2*f['addr'] + .2*f['phone'] + .2*f['license']
    if f['addr'] and not f['name'] and not f['phone'] and not f['license']:
        score = min(score,.2)
    if blocks is not None and set(blocks)=={'ADDR'} and f['name']<.9 and not f['phone'] and not f['license']:
        score = min(score,.2)
    return score, f


def score(c, output):
    c.create_function('jw_name', lambda a,b: jellyfish.jaro_winkler_similarity(a,b) if a and b else 0.0,
                      ['VARCHAR','VARCHAR'],'DOUBLE')
    c.execute('''CREATE TABLE pair_features AS SELECT p.*,
        (coalesce(a.npi,'')<>'' AND a.npi=b.npi)::INTEGER AS npi_exact,
        CASE WHEN coalesce(a.npi,'')<>'' AND a.npi=b.npi THEN NULL
             ELSE jw_name(coalesce(a.name_norm,''),coalesce(b.name_norm,'')) END AS name,
        EXISTS(SELECT 1 FROM node_addresses x JOIN node_addresses y USING(addr_key)
               WHERE x.node_id=p.left_node AND y.node_id=p.right_node)::DOUBLE AS addr,
        (coalesce(a.phone_norm,'')<>'' AND a.phone_norm=coalesce(b.phone_norm,''))::DOUBLE AS phone,
        (coalesce(a.license_no,'')<>'' AND coalesce(a.license_state,'')<>''
          AND a.license_no=coalesce(b.license_no,'') AND a.license_state=coalesce(b.license_state,''))::DOUBLE AS license
        FROM candidate_nodes p JOIN nodes a ON p.left_node=a.node_id JOIN nodes b ON p.right_node=b.node_id''')
    c.execute('''CREATE TABLE scored AS SELECT *, CASE WHEN npi_exact=1 THEN 1.0
        WHEN addr=1 AND name=0 AND phone=0 AND license=0 THEN .2
        WHEN blocks=['ADDR'] AND name<.9 AND phone=0 AND license=0 THEN least(.2,.4*name+.2*addr)
        ELSE .4*name+.2*addr+.2*phone+.2*license END AS score FROM pair_features''')
    # Feature columns are NULL for NPI-direct rows: they bypass fuzzy scoring.
    copy(c, '''SELECT a.record_id AS left_id,b.record_id AS right_id,s.blocks,s.score,s.npi_exact,
        s.name, CASE WHEN npi_exact=0 THEN addr END AS addr,
        CASE WHEN npi_exact=0 THEN phone END AS phone,
        CASE WHEN npi_exact=0 THEN license END AS license
        FROM scored s JOIN nodes a ON s.left_node=a.node_id JOIN nodes b ON s.right_node=b.node_id
        ORDER BY left_id,right_id''', Path(output)/'scored_pairs.parquet')
