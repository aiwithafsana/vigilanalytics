"""Auditable union of six candidate blocks, with deterministic bounded splits."""
import math
from pathlib import Path

from resolve.storage import copy, write_json


def generate(c, output, config):
    output = Path(output)
    # JSON tuple keys avoid delimiter collisions. Blank identities get NPI blocks only.
    c.execute('''CREATE TABLE block_members AS
        SELECT DISTINCT 'NPI' AS block, to_json([npi]) AS block_key, node_id
        FROM nodes WHERE coalesce(npi,'')<>''
        UNION ALL
        SELECT DISTINCT 'ADDR', to_json([kind::VARCHAR,addr_key]), node_id
        FROM nodes JOIN node_addresses USING(node_id) WHERE kind IN (1,2) AND coalesce(name_norm,'')<>''
        UNION ALL
        SELECT 'LAST_STATE', to_json([kind::VARCHAR,last,license_state]), node_id
        FROM nodes WHERE kind=1 AND coalesce(name_norm,'')<>'' AND coalesce(last,'')<>'' AND coalesce(license_state,'')<>''
        UNION ALL
        SELECT 'PHONE', to_json([kind::VARCHAR,phone_norm]), node_id
        FROM nodes WHERE kind IN (1,2) AND coalesce(name_norm,'')<>'' AND coalesce(phone_norm,'')<>''
        UNION ALL
        SELECT 'LICENSE', to_json([kind::VARCHAR,license_no,license_state]), node_id
        FROM nodes WHERE kind IN (1,2) AND coalesce(name_norm,'')<>'' AND coalesce(license_no,'')<>'' AND coalesce(license_state,'')<>''
        UNION ALL
        SELECT DISTINCT 'ORG_STATE', to_json([kind::VARCHAR,left(org_name_norm,10),state]), node_id
        FROM nodes JOIN node_addresses USING(node_id)
        WHERE kind=2 AND coalesce(org_name_norm,'')<>'' AND coalesce(state,'')<>\'\' ''')
    c.execute('''CREATE TABLE original_block_sizes AS SELECT block,block_key,count(*) AS records,
        count(*)*(count(*)-1)//2 AS pairs FROM block_members GROUP BY 1,2''')
    # First refine oversized groups by name initial. A still-oversized refinement
    # is sliced in name/record-ID order. This reduces recall across slices, which
    # is logged; no rows are dropped and exact NPI unions are independent of splits.
    c.execute(f'''CREATE TABLE refined_members AS SELECT m.*, CASE WHEN s.pairs>{config.max_block_pairs}
        THEN left(coalesce(n.name_norm,''),1) ELSE '' END AS initial
        FROM block_members m JOIN original_block_sizes s USING(block,block_key)
        JOIN nodes n USING(node_id)''')
    max_nodes = (1 + math.isqrt(1 + 8*config.max_block_pairs)) // 2
    c.execute(f'''CREATE TABLE split_members AS SELECT block,block_key,node_id,initial,
        (row_number() OVER(PARTITION BY block,block_key,initial ORDER BY name_norm,record_id)-1)//{max_nodes} AS slice
        FROM refined_members JOIN nodes USING(node_id)''')
    c.execute('''CREATE TABLE final_block_sizes AS SELECT block,block_key,initial,slice,
        count(*) AS records,count(*)*(count(*)-1)//2 AS pairs FROM split_members GROUP BY 1,2,3,4''')
    maximum = c.execute('SELECT coalesce(max(pairs),0) FROM final_block_sizes').fetchall()[0][0]
    if maximum > config.max_block_pairs:
        raise ValueError('Block split failed to enforce pair limit')
    copy(c, 'SELECT * FROM original_block_sizes ORDER BY block,block_key', output/'block_sizes.parquet')
    copy(c, 'SELECT * FROM final_block_sizes ORDER BY block,block_key,initial,slice', output/'split_block_sizes.parquet')
    split = c.execute(f'''SELECT block,block_key,records,pairs FROM original_block_sizes
        WHERE pairs>{config.max_block_pairs} ORDER BY block,block_key''').fetchall()
    write_json(output/'block_report.json', {'max_pairs_after_splits':maximum,
        'pair_limit':config.max_block_pairs,
        'splits':[dict(zip(('block','key','records','pairs'),r)) for r in split],
        'split_policy':'name initial, then bounded slices ordered by name and record_id',
        'recall_caveat':'Pairs across split partitions are omitted; independent NPI unions retain exact links.'})
    c.execute('''CREATE TABLE candidate_nodes AS SELECT a.node_id AS left_node,b.node_id AS right_node,
        list(DISTINCT a.block ORDER BY a.block) AS blocks
        FROM split_members a JOIN split_members b
        ON a.block=b.block AND a.block_key=b.block_key AND a.initial=b.initial
        AND a.slice=b.slice AND a.node_id<b.node_id GROUP BY 1,2''')
    copy(c, '''SELECT a.record_id AS left_id,b.record_id AS right_id,p.blocks
        FROM candidate_nodes p JOIN nodes a ON a.node_id=p.left_node JOIN nodes b ON b.node_id=p.right_node
        ORDER BY left_id,right_id''', output/'candidates.parquet')
    print(f'Candidates: {c.execute("SELECT count(*) FROM candidate_nodes").fetchall()[0][0]:,}; '
          f'split {len(split):,} oversized blocks; max {maximum:,} pairs/block',flush=True)
