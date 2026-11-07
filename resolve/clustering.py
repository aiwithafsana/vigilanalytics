"""Guarded union-find; exact NPI agreement never overrides entity-type conflicts."""
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from resolve.storage import copy, write_json


class ResolutionGateError(ValueError):
    pass


def entity_id(record_ids):
    # Unambiguous UTF-8 compact JSON array, sorted by the complete record ID.
    payload = json.dumps(sorted(record_ids), ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]


def preflight(c, output, config):
    """Exact NPI unions alone can prove the gate impossible; dump before costly fuzzy work."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    c.execute(f'''CREATE TABLE npi_alarms AS SELECT npi, count(*) AS records,
        count(DISTINCT nullif(kind,0)) AS known_types,
        count(*)>{config.max_cluster_records} AS oversized,
        count(DISTINCT nullif(kind,0))>1 AS type_conflict
        FROM nodes WHERE coalesce(npi,'')<>'' GROUP BY npi
        HAVING oversized OR type_conflict''')
    counts = c.execute('SELECT count(*), count(*) FILTER(WHERE oversized), count(*) FILTER(WHERE type_conflict) FROM npi_alarms').fetchall()[0]
    if counts[0]:
        copy(c, 'SELECT * FROM npi_alarms ORDER BY npi', output/'npi_alarms.parquet')
        copy(c, '''SELECT a.*, n.* EXCLUDE(npi) FROM npi_alarms a JOIN nodes n USING(npi)
            ORDER BY npi, record_id''', output/'cluster_review.parquet')
        write_json(output/'gate.json', {'status':'blocked', 'reason':'exact_npi_preflight',
            'npi_groups':counts[0], 'oversized_groups':counts[1], 'type_conflict_groups':counts[2],
            'policy':config.as_dict(), 'national_clustering_completed':False})
        raise ResolutionGateError(f'Exact-NPI preflight: {counts[1]} clusters exceed {config.max_cluster_records} records; '
                                  f'{counts[2]} NPIs mix individuals and organizations; see cluster_review.parquet')


class UnionFind:
    def __init__(self, kinds, max_records):
        self.parent = np.arange(len(kinds), dtype=np.int64)
        self.size = np.ones(len(kinds), dtype=np.uint32)
        self.kind = np.asarray(kinds, dtype=np.uint8).copy()
        self.max_records = max_records

    def find(self, i):
        i = int(i)
        while self.parent[i] != i:
            self.parent[i] = self.parent[self.parent[i]]
            i = int(self.parent[i])
        return i

    def union(self, a, b):
        a, b = self.find(a), self.find(b)
        if a == b:
            return
        if self.kind[a] and self.kind[b] and self.kind[a] != self.kind[b]:
            raise ResolutionGateError('Union would mix individual and organization')
        if self.size[a] + self.size[b] > self.max_records:
            raise ResolutionGateError(f'Cluster exceeds {self.max_records} records')
        a, b = min(a,b), max(a,b)  # deterministic root independent of batch boundaries
        self.parent[b] = a
        self.size[a] += self.size[b]
        self.kind[a] = self.kind[a] or self.kind[b]


def cluster(c, output, config):
    kinds = c.execute('SELECT kind FROM nodes ORDER BY node_id').fetchnumpy()['kind']
    uf = UnionFind(kinds, config.max_cluster_records)
    cursor = c.cursor()
    # NPI pairs are retained with exact-match provenance, but never fuzzy scored.
    cursor.execute(f'''SELECT left_node,right_node FROM (
        SELECT min(node_id) OVER(PARTITION BY npi) AS left_node,node_id AS right_node,1 AS exact
        FROM nodes WHERE coalesce(npi,'')<>'' QUALIFY left_node<right_node
        UNION ALL SELECT left_node,right_node,0 AS exact FROM scored
        WHERE npi_exact=0 AND score>={config.threshold})
        ORDER BY exact DESC,left_node,right_node''')
    try:
        while rows := cursor.fetchmany(config.batch_size):
            for a,b in rows:
                try:
                    uf.union(a,b)
                except ResolutionGateError:
                    roots = {uf.find(a), uf.find(b)}
                    members = [i for i in range(len(kinds)) if uf.find(i) in roots]
                    c.register('alarm_members', pd.DataFrame({'node_id':members}))
                    copy(c, '''SELECT * FROM nodes JOIN alarm_members USING(node_id)
                        ORDER BY record_id''', Path(output)/'cluster_review.parquet')
                    write_json(Path(output)/'gate.json', {'status':'blocked', 'reason':'union_alarm',
                        'left_node':a, 'right_node':b, 'records':len(members), 'policy':config.as_dict()})
                    raise
    finally:
        cursor.close()
    c.execute('CREATE TABLE membership(node_id BIGINT, root BIGINT)')
    for start in range(0,len(kinds),config.batch_size):
        end = min(start+config.batch_size,len(kinds))
        frame = pd.DataFrame({'node_id':range(start,end), 'root':[uf.find(i) for i in range(start,end)]})
        c.register('membership_batch',frame)
        c.execute('INSERT INTO membership SELECT * FROM membership_batch')
        c.unregister('membership_batch')
    c.create_function('entity_digest',entity_id,["VARCHAR[]"],'VARCHAR')
    c.execute('''CREATE TABLE entity_keys AS SELECT root,
        entity_digest(list(record_id ORDER BY record_id)) AS entity_id
        FROM membership JOIN nodes USING(node_id) GROUP BY root''')
    collision = c.execute('SELECT entity_id FROM entity_keys GROUP BY 1 HAVING count(*)>1 LIMIT 1').fetchall()
    if collision:
        raise ResolutionGateError(f'Entity ID hash collision: {collision[0]}')
    c.execute('''CREATE TABLE mapping AS SELECT record_id, node_id, entity_id
        FROM membership JOIN entity_keys USING(root) JOIN nodes USING(node_id)''')
