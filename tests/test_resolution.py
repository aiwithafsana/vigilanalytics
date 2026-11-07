import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from common.normalization import normalize_name
from normalize.engine import SCHEMAS
from resolve import blocking, clustering, scoring, survivorship
from resolve import eval as evaluation
from resolve.config import ResolutionConfig
from resolve.run import run
from resolve.storage import connect, load


def canonical(root, records):
    """Small typed canonical fixtures; the resolver never needs raw inputs."""
    c=duckdb.connect()
    for table in ('providers','addresses'):
        schema=SCHEMAS[table]
        c.execute(f'CREATE TABLE {table} ('+', '.join(f'"{k}" {t}' for k,t in schema)+')')
        values=[]
        for r in records:
            base=dict(r, name_raw=r.get('name_raw',r.get('name','Robert Chen')),
                      name_norm=normalize_name(r.get('name_raw',r.get('name','Robert Chen'))),
                      entity_type=r.get('entity_type',1),vintage=r.get('vintage','2026-02'),source=r.get('source','nppes'))
            base.setdefault('last','CHEN')
            if table=='providers':
                values.append([base.get(k,'' if t=='VARCHAR' else None) for k,t in schema])
            else:
                for addr in r.get('addresses',[]):
                    row=dict(base,**addr)
                    values.append([row.get(k,'' if t=='VARCHAR' else None) for k,t in schema])
        if values:
            c.executemany(f'INSERT INTO {table} VALUES ('+','.join('?' for _ in schema)+')',values)
        dest=Path(root)/table/'fixture.parquet'
        dest.parent.mkdir(parents=True,exist_ok=True)
        c.execute(f"COPY (SELECT * FROM {table}) TO '{dest}' (FORMAT PARQUET)")
    c.close()
    return Path(root)


def record(rid,**kwargs):
    return dict(record_id=rid,**kwargs)


def chen():
    address={'addr_key':'123|MAIN ST|204|02110','addr_type':'practice','city':'BOSTON','state':'MA'}
    common=dict(license_no='A123',license_state='MA',phone_norm='2125550123',addresses=[address])
    return [record('nppes:1',name_raw='CHEN, ROBERT',npi='1234567890',**common),
            record('leie:1',source='leie',name_raw='Robert Chen MD',**common),
            record('cms:1',source='cms_part_b',npi='1234567890'),
            record('nppes:2',name_raw='ROBERT CHEN',npi='9999999999',license_no='B999',license_state='CA',phone_norm='3105550100')]


def mapping(output):
    return dict(duckdb.sql(f"SELECT * FROM read_parquet('{output}/record_to_entity.parquet')").fetchall())


def test_robert_chen_and_all_output_determinism(tmp_path):
    paths=[]
    for index,records in enumerate((chen(),list(reversed(chen())))):
        source=canonical(tmp_path/f'canonical{index}',records)
        output=tmp_path/f'resolved{index}'
        run(source,output,evaluate_sample=False)
        m=mapping(output)
        assert m['nppes:1']==m['leie:1']==m['cms:1']
        assert m['nppes:2']!=m['nppes:1']
        paths.append(output)
    for name in ('entities','entity_addresses','entity_npis','record_to_entity','candidates','scored_pairs'):
        assert (paths[0]/f'{name}.parquet').read_bytes()==(paths[1]/f'{name}.parquet').read_bytes()


def test_entity_digest_unambiguous_and_order_independent():
    assert clustering.entity_id(['b','a'])==hashlib.sha256(b'["a","b"]').hexdigest()[:16]
    assert clustering.entity_id(['ab','c'])!=clustering.entity_id(['a','bc'])


def test_scoring_exact_and_features():
    assert scoring.score_pair({'npi':'123'},{'npi':'123'})==(1.,{'npi_exact':1})
    a={'name_norm':'ROBERT CHEN','addr_key':'A','phone_norm':'P','license_no':'L','license_state':'MA'}
    assert scoring.score_pair(a,a)[0]==pytest.approx(1.)
    assert scoring.score_pair({'addr_key':'A'},{'addr_key':'A'},['ADDR'])[0]==.2
    assert scoring.score_pair({'name_norm':'ABC','addr_key':'A'},{'name_norm':'XYZ','addr_key':'A'},['ADDR'])[0]==.2
    assert scoring.score_pair({}, {})[0]==0.
    assert scoring.score_pair({'license_no':'L'},{'license_no':'L'})[1]['license']==0


def test_preflight_cluster_alarm_and_preserve_existing(tmp_path):
    source=canonical(tmp_path/'c',[record(f'r:{i:03}',npi='1234567890') for i in range(51)])
    output=tmp_path/'o';output.mkdir()
    (output/'entities.parquet').write_bytes(b'previous-success')
    with pytest.raises(clustering.ResolutionGateError,match='1 clusters exceed 50'):
        run(source,output,evaluate_sample=False)
    assert (output/'entities.parquet').read_bytes()==b'previous-success'
    assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{output}/cluster_review.parquet')").fetchall()[0][0]==51
    assert json.loads((output/'gate.json').read_text())['status']=='blocked'


def test_fuzzy_cluster_alarm_dumps_attempted_members(tmp_path):
    rows=[record(f'r:{i:03}',npi=f'{i:010}',license_no='L',license_state='MA',phone_norm='P',
        addresses=[dict(addr_key='A',addr_type='practice')]) for i in range(51)]
    source=canonical(tmp_path/'c',rows)
    with pytest.raises(clustering.ResolutionGateError,match='Cluster exceeds 50'):
        run(source,tmp_path/'o',evaluate_sample=False)
    assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{tmp_path}/o/cluster_review.parquet')").fetchall()[0][0]==51
    assert not (tmp_path/'o'/'entities.parquet').exists()


def test_type_conflict_fails_exact_npi(tmp_path):
    source=canonical(tmp_path/'c',[record('a',npi='123'),record('b',npi='123',entity_type=2)])
    with pytest.raises(clustering.ResolutionGateError,match='1 NPIs mix'):
        run(source,tmp_path/'o',evaluate_sample=False)


def test_unknown_type_cannot_bridge_person_and_org():
    uf=clustering.UnionFind([1,0,2],50)
    uf.union(0,1)
    with pytest.raises(clustering.ResolutionGateError,match='mix'):
        uf.union(1,2)


def test_no_cross_type_fuzzy_candidates(tmp_path):
    rows=[record('person',name='ACME',phone_norm='P',license_no='L',license_state='MA'),
          record('org',name='ACME',phone_norm='P',license_no='L',license_state='MA',entity_type=2)]
    run(canonical(tmp_path/'c',rows),tmp_path/'o',evaluate_sample=False)
    assert len(set(mapping(tmp_path/'o').values()))==2
    assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{tmp_path}/o/candidates.parquet')").fetchall()[0][0]==0


def test_blank_suppressed_identity_exact_only_and_survivorship(tmp_path):
    rows=[record('a',name='',entity_type=None,npi='123',vintage='2026-03'),
          record('b',name='Robert Chen',source='leie',npi='123'),
          record('c',name='',entity_type=None),record('d',name='',entity_type=None)]
    run(canonical(tmp_path/'c',rows),tmp_path/'o',evaluate_sample=False)
    m=mapping(tmp_path/'o')
    assert m['a']==m['b']
    assert m['c']!=m['d']
    name=duckdb.sql(f"SELECT name_norm FROM read_parquet('{tmp_path}/o/entities.parquet') WHERE entity_id='{m['a']}'").fetchall()[0][0]
    assert name=='ROBERT CHEN'


def test_address_survivorship_preserves_history_and_prefers_practice(tmp_path):
    rows=[record('a',name='OLD NPPES',npi='123',addresses=[dict(addr_key='P',addr_type='practice',vintage='2020'),dict(addr_key='M',addr_type='mailing',vintage='2026')]),
          record('b',name='NEW CMS',source='cms_part_b',npi='123',addresses=[dict(addr_key='P2',addr_type='practice',vintage='2022'),dict(addr_key='P',addr_type='practice',vintage='2021')])]
    run(canonical(tmp_path/'c',rows),tmp_path/'o',evaluate_sample=False)
    golden=duckdb.sql(f"SELECT name_norm,addr_key FROM read_parquet('{tmp_path}/o/entities.parquet')").fetchall()[0]
    assert golden==('OLD NPPES','P2')
    addresses=duckdb.sql(f"SELECT addr_key,first_seen,last_seen FROM read_parquet('{tmp_path}/o/entity_addresses.parquet') ORDER BY 1").fetchall()
    assert addresses==[('M','2026','2026'),('P','2020','2021'),('P2','2022','2022')]


def test_split_cap_and_candidate_dedup(tmp_path):
    rows=[record(f'r:{i:02}',phone_norm='P',addresses=[dict(addr_key='A',addr_type='practice')]) for i in range(10)]
    run(canonical(tmp_path/'c',rows),tmp_path/'o',ResolutionConfig(max_block_pairs=3),evaluate_sample=False)
    report=json.loads((tmp_path/'o'/'block_report.json').read_text())
    assert report['max_pairs_after_splits']<=3
    assert {b['block'] for b in report['splits']}=={'ADDR','PHONE'}
    pairs=duckdb.sql(f"SELECT left_id,right_id,blocks FROM read_parquet('{tmp_path}/o/candidates.parquet')").fetchall()
    assert all(a<b and blocks==['ADDR','PHONE'] for a,b,blocks in pairs)
    assert len({(a,b) for a,b,_ in pairs})==len(pairs)


def test_all_shared_addresses_score_including_non_golden(tmp_path):
    rows=[record('a',phone_norm='P',license_no='L',license_state='MA',addresses=[dict(addr_key='A'),dict(addr_key='Z')]),
          record('b',phone_norm='P',license_no='L',license_state='MA',addresses=[dict(addr_key='Z')])]
    run(canonical(tmp_path/'c',rows),tmp_path/'o',evaluate_sample=False)
    assert len(set(mapping(tmp_path/'o').values()))==1
    score=duckdb.sql(f"SELECT score,addr,name,phone,license FROM read_parquet('{tmp_path}/o/scored_pairs.parquet')").fetchall()[0]
    assert score==pytest.approx((1,1,1,1,1))


def test_configurable_threshold(tmp_path):
    rows=[record('a',phone_norm='P',license_no='L',license_state='MA'),
          record('b',phone_norm='P',license_no='L',license_state='MA')]
    source=canonical(tmp_path/'c',rows)
    run(source,tmp_path/'strict',evaluate_sample=False)
    run(source,tmp_path/'loose',ResolutionConfig(threshold=.75),evaluate_sample=False)
    assert len(set(mapping(tmp_path/'strict').values()))==2
    assert len(set(mapping(tmp_path/'loose').values()))==1


def test_null_features_do_not_produce_null_score(tmp_path):
    rows=[record('a',phone_norm='P'),record('b',phone_norm=None,addresses=[dict(addr_key='A')]),
          record('c',phone_norm='P',license_no=None,license_state=None)]
    run(canonical(tmp_path/'c',rows),tmp_path/'o',evaluate_sample=False)
    assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{tmp_path}/o/scored_pairs.parquet') WHERE score IS NULL").fetchall()[0][0]==0


def test_evaluation_sample_and_explicit_human_gate(tmp_path):
    rows=[]
    for i in range(100):
        npi=f'{i+1:010}'
        for source,name in [('nppes','CHEN, ROBERT'),('leie','Robert Chen MD'),('cms_part_b','Robert Chen')]:
            rows.append(record(f'{source}:{i:03}',name=name,npi=npi,source=source,
                               addresses=[dict(addr_key=f'A{i}',city='BOSTON')]))
    rows.append(record('nppes:extra',name='JANE CHEN',npi='9999999999',addresses=[dict(addr_key='EXTRA',city='BOSTON')]))
    source=canonical(tmp_path/'c',rows)
    with pytest.raises(clustering.ResolutionGateError,match='Human review pending'):
        run(source,tmp_path/'o')
    result=json.loads((tmp_path/'o'/'evaluation.json').read_text())
    assert result['precision']==result['recall']==1.
    request=json.loads((tmp_path/'o'/'review_request.json').read_text())
    assert len(request['pair_ids'])==30
    assert not (tmp_path/'o'/'entities.parquet').exists()
    bad=tmp_path/'bad.json';bad.write_text(json.dumps({'confirmed':True}))
    with pytest.raises(clustering.ResolutionGateError,match='exact sample'):
        evaluation.require_review(tmp_path/'o',bad)
    # Fixture-only attestation; no real-data review is synthesized by the pipeline.
    attestation=tmp_path/'review.json'
    attestation.write_text(json.dumps(dict(request,confirmed=True,reviewer='synthetic fixture')))
    run(source,tmp_path/'reviewed',review_path=attestation)
    assert json.loads((tmp_path/'reviewed'/'gate.json').read_text())['status']=='complete'


@pytest.mark.parametrize('kwargs',[{'threshold':0},{'threshold':2},{'max_cluster_records':0},{'max_block_pairs':0}])
def test_invalid_config(kwargs):
    with pytest.raises(ValueError):
        ResolutionConfig(**kwargs)


def test_exact_npi_unions_survive_block_splits(tmp_path):
    rows=[record(f'r:{i:02}',name=f'NAME {i}',npi='123') for i in range(8)]
    run(canonical(tmp_path/'c',rows),tmp_path/'o',ResolutionConfig(max_block_pairs=1),evaluate_sample=False)
    assert len(set(mapping(tmp_path/'o').values()))==1
    assert json.loads((tmp_path/'o'/'block_report.json').read_text())['max_pairs_after_splits']<=1


def test_sql_scoring_matches_reference_and_address_only_cap(tmp_path):
    rows=[record('a',name='ROBERT CHEN',phone_norm='P',license_no='L',license_state='MA',addresses=[dict(addr_key='A')]),
          record('b',name='ROBERT CHEN',phone_norm='P',license_no='L',license_state='MA',addresses=[dict(addr_key='A')]),
          record('c',name='XYZ',phone_norm=None,license_no=None,addresses=[dict(addr_key='A')])]
    source=canonical(tmp_path/'c',rows)
    output=tmp_path/'o'
    run(source,output,evaluate_sample=False)
    actual=duckdb.sql(f"SELECT left_id,right_id,blocks,score FROM read_parquet('{output}/scored_pairs.parquet')").fetchall()
    by_id={r['record_id']:dict(r,name_norm=normalize_name(r.get('name')),addr_key='A') for r in rows}
    for a,b,blocks,score in actual:
        assert score==pytest.approx(scoring.score_pair(by_id[a],by_id[b],blocks)[0])
    assert next(s for a,b,_,s in actual if (a,b)==('a','c'))==.2


def test_duplicate_canonical_id_fails_before_resolution(tmp_path):
    source=canonical(tmp_path/'c',[record('a'),record('a')])
    with pytest.raises(ValueError,match='Duplicate canonical'):
        run(source,tmp_path/'o',evaluate_sample=False)


def test_evaluation_measures_transitive_clustering_and_false_merges(tmp_path):
    config=ResolutionConfig()
    with connect(tmp_path/'eval.duckdb',config) as c:
        c.execute("CREATE TABLE mapping(record_id VARCHAR,entity_id VARCHAR)")
        c.execute("INSERT INTO mapping VALUES ('a','one'),('b','one'),('c','one'),('d','two')")
        c.execute('CREATE TABLE labeled_sample(pair_id VARCHAR,left_id VARCHAR,right_id VARCHAR,label INTEGER)')
        c.execute("INSERT INTO labeled_sample VALUES ('tp','a','c',1),('fp','a','b',0),('fn','a','d',1)")
        c.execute('CREATE TABLE scored(left_node BIGINT,right_node BIGINT,score DOUBLE,npi_exact INTEGER,name DOUBLE,addr DOUBLE,phone DOUBLE,license DOUBLE,blocks VARCHAR[])')
        c.execute('CREATE TABLE nodes(node_id BIGINT,record_id VARCHAR)')
        with pytest.raises(clustering.ResolutionGateError,match='precision/recall'):
            evaluation.evaluate(c,tmp_path,config)
    metrics=json.loads((tmp_path/'evaluation.json').read_text())
    assert metrics['tp']==metrics['fp']==metrics['fn']==1
    assert metrics['precision']==metrics['recall']==.5
