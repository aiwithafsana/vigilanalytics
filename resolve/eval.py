"""Reproducible weak labels plus an explicit 30-pair human review gate."""
import csv
import hashlib
import json
from pathlib import Path

from resolve.clustering import ResolutionGateError
from resolve.storage import copy, literal, write_json


def sample(c, output, count=100):
    output = Path(output)
    # Each positive NPI occurs in all three sources. Prefer a raw-name variant;
    # this evaluates exact identity linkage, not fuzzy-match recall without an NPI.
    c.execute(f'''CREATE TABLE positive_sample AS SELECT * EXCLUDE(rank) FROM (
        SELECT a.node_id AS left_node,b.node_id AS right_node,1::INTEGER AS label,
            'same NPI in NPPES, CMS and LEIE' AS label_basis, '' AS matched_city,
            row_number() OVER(PARTITION BY a.npi ORDER BY (a.name_raw<>b.name_raw) DESC,
                (a.name_norm<>b.name_norm) DESC,b.source,b.record_id) AS rank
        FROM nodes a JOIN nodes b ON a.npi=b.npi AND a.record_id<b.record_id
        WHERE a.source='cms_part_b' AND b.source IN ('leie','nppes') AND coalesce(a.npi,'')<>''
        AND a.kind=b.kind AND EXISTS(SELECT 1 FROM nodes x WHERE x.npi=a.npi AND x.source='nppes')
        AND EXISTS(SELECT 1 FROM nodes x WHERE x.npi=a.npi AND x.source='leie')
        AND NOT EXISTS(SELECT 1 FROM nodes x WHERE x.npi=a.npi AND x.kind<>0 AND x.kind<>a.kind)
        ) WHERE rank=1 ORDER BY sha256(left_node::VARCHAR || ':' || right_node::VARCHAR) LIMIT {count}''')
    # Adjacent, distinct NPIs inside each surname/city block keep this national
    # query linear in group size instead of materializing every possible pair.
    c.execute('''CREATE TABLE negative_order AS SELECT node_id,npi,last,city,
        lead(node_id) OVER(PARTITION BY last,city ORDER BY npi,node_id) AS next_node,
        lead(npi) OVER(PARTITION BY last,city ORDER BY npi,node_id) AS next_npi
        FROM (SELECT DISTINCT n.node_id,n.npi,n.last,upper(a.city) AS city
            FROM nodes n JOIN node_addresses a USING(node_id)
            WHERE n.source='nppes' AND n.kind=1 AND coalesce(n.npi,'')<>''
            AND coalesce(n.last,'')<>'' AND coalesce(a.city,'')<>\'\')''')
    c.execute(f'''CREATE TABLE negative_sample AS SELECT
        least(node_id,next_node) AS left_node,greatest(node_id,next_node) AS right_node,
        0::INTEGER AS label,'same surname and city, different NPIs (requires review)' AS label_basis, min(city) AS matched_city
        FROM negative_order WHERE next_node IS NOT NULL AND npi<>next_npi GROUP BY 1,2
        ORDER BY sha256(left_node::VARCHAR || ':' || right_node::VARCHAR) LIMIT {count}''')
    counts = c.execute('SELECT (SELECT count(*) FROM positive_sample),(SELECT count(*) FROM negative_sample)').fetchall()[0]
    if counts != (count,count):
        raise ResolutionGateError(f'Evaluation requires {count} positives and negatives; available {counts}')
    c.execute('CREATE TABLE labeled_nodes AS SELECT * FROM positive_sample UNION ALL SELECT * FROM negative_sample')
    c.execute('''CREATE TABLE labeled_sample AS SELECT sha256(a.record_id || chr(10) || b.record_id) AS pair_id,
        a.record_id AS left_id,b.record_id AS right_id,p.label,p.label_basis,p.matched_city,
        a.name_raw AS left_name,b.name_raw AS right_name,a.source AS left_source,b.source AS right_source,
        a.npi AS left_npi,b.npi AS right_npi,
        (SELECT min(city) FROM node_addresses x WHERE x.node_id=a.node_id) AS left_city,
        (SELECT min(city) FROM node_addresses x WHERE x.node_id=b.node_id) AS right_city
        FROM labeled_nodes p JOIN nodes a ON a.node_id=p.left_node JOIN nodes b ON b.node_id=p.right_node''')
    path = copy(c,'SELECT * FROM labeled_sample ORDER BY pair_id',output/'labeled_pairs.parquet')
    c.execute(f'COPY (SELECT * FROM labeled_sample ORDER BY pair_id) TO {literal(output/"labeled_pairs.csv")} (HEADER, DELIMITER \',\')')
    review = c.execute('''SELECT * FROM (SELECT *,row_number() OVER(PARTITION BY label ORDER BY pair_id) AS rank
        FROM labeled_sample) WHERE rank<=15 ORDER BY label DESC,pair_id''').fetchdf()
    review_ids = list(review['pair_id'])
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    write_json(output/'review_request.json', {'sample_sha256':digest,'pair_ids':review_ids,
        'status':'pending','instructions':'Inspect all 30 pairs in human_review.md; record corrections by pair ID. Human confirmation is required.'})
    lines = ['# Phase C: 30-pair human spot-check','',
        'These labels are generated hypotheses. Check whether each pair is the same identity. '
        'Different NPIs alone do not prove different people. Reply with corrections by row number, or confirm all 30 labels.',
        '', '| Row | Expected | Left name · source · NPI · city | Right name · source · NPI · city |',
        '|---:|---|---|---|']
    for i,r in enumerate(review.to_dict('records'),1):
        clean=lambda s:('' if s is None or str(s)=='nan' else str(s)).replace('|','/').replace('\n',' ')
        left=' · '.join(clean(r[k]) for k in ('left_name','left_source','left_npi'))+' · '+clean(r['matched_city'] or r['left_city'])
        right=' · '.join(clean(r[k]) for k in ('right_name','right_source','right_npi'))+' · '+clean(r['matched_city'] or r['right_city'])
        lines.append(f'| {i} | {"Same" if r["label"] else "Different"} | {left} | {right} |')
    lines += ['', 'Full record IDs, labels and pair IDs: [labeled_pairs.csv](labeled_pairs.csv).',
              '', f'Sample SHA-256: `{digest}`. No human review has been recorded.']
    (output/'human_review.md').write_text('\n'.join(lines)+'\n')
    return path


def load_sample(c, path):
    c.read_parquet(str(path)).create_view('labeled_sample')


def evaluate(c, output, config):
    c.execute('''CREATE TABLE evaluation_pairs AS SELECT s.*,
        (a.entity_id=b.entity_id)::INTEGER AS prediction
        FROM labeled_sample s LEFT JOIN mapping a ON s.left_id=a.record_id
        LEFT JOIN mapping b ON s.right_id=b.record_id''')
    if c.execute('SELECT count(*) FROM evaluation_pairs WHERE prediction IS NULL').fetchall()[0][0]:
        raise ResolutionGateError('Evaluation sample contains unmapped records')
    tp,fp,fn,tn = c.execute('''SELECT count(*) FILTER(WHERE label=1 AND prediction=1),
        count(*) FILTER(WHERE label=0 AND prediction=1),count(*) FILTER(WHERE label=1 AND prediction=0),
        count(*) FILTER(WHERE label=0 AND prediction=0) FROM evaluation_pairs''').fetchall()[0]
    precision = tp/(tp+fp) if tp+fp else 0.
    recall = tp/(tp+fn) if tp+fn else 0.
    result = dict(tp=tp,fp=fp,fn=fn,tn=tn,precision=precision,recall=recall,
                  passed=precision>=.98 and recall>=.85)
    copy(c,'SELECT * FROM evaluation_pairs ORDER BY pair_id',Path(output)/'evaluation_pairs.parquet')
    write_json(Path(output)/'evaluation.json',result)
    # Acceptance here means the score crosses the configured union threshold.
    for name,where,order in [('lowest_accepted',f'score>={config.threshold}','score, left_node,right_node'),
                             ('highest_rejected',f'score<{config.threshold}','score DESC,left_node,right_node')]:
        rows = c.execute(f'''SELECT a.record_id AS left_id,b.record_id AS right_id,s.score,
            s.npi_exact,s.name,s.addr,s.phone,s.license,s.blocks FROM scored s
            JOIN nodes a ON a.node_id=s.left_node JOIN nodes b ON b.node_id=s.right_node
            WHERE {where} ORDER BY {order} LIMIT 20''').fetchdf()
        rows.to_csv(Path(output)/f'{name}.csv',index=False)
        print(name+'\n'+rows.to_string(index=False),flush=True)
    print(json.dumps(result,sort_keys=True),flush=True)
    if not result['passed']:
        raise ResolutionGateError('Clustering precision/recall gate failed')
    return result


def require_review(output, review_path):
    request=json.loads((Path(output)/'review_request.json').read_text())
    if review_path is None:
        raise ResolutionGateError(f'Human review pending: {Path(output)/"human_review.md"}')
    review=json.loads(Path(review_path).read_text())
    if (review.get('sample_sha256')!=request['sample_sha256'] or
        set(review.get('pair_ids',[]))!=set(request['pair_ids']) or
        len(review.get('pair_ids',[]))!=30 or not review.get('reviewer') or
        review.get('confirmed') is not True):
        raise ResolutionGateError('Human review must attest to the exact sample and all 30 pair IDs')
    write_json(Path(output)/'human_review_attestation.json',review)
