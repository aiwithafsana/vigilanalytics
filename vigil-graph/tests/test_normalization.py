from pathlib import Path
import csv
import hashlib
import json
import zipfile

import duckdb
import pytest

from common.normalization import normalize_name, normalize_phone, normalize_address, normalize_authority, fraud_related
from ingest import acquisition
from ingest.acquisition import import_files
from ingest.validation import NPPES_COLUMNS, LEIE_COLUMNS, PROVIDER_COLUMNS, SERVICE_COLUMNS
from normalize.engine import SCHEMAS
from normalize.run import run


@pytest.mark.parametrize('raw,expected',[
    ('Chen, Robert MD','ROBERT CHEN'),('Robert Chen M.D.','ROBERT CHEN'),
    ('  jane   smith DO ','JANE SMITH'),('Acme, LLC','ACME'),('ACME INC','ACME'),
    ('Acme PC','ACME'),('SMITH-JONES, JANE','JANE SMITH JONES'),('', ''),(None,''),
    ('Smith, Jane DDS','JANE SMITH'),('Drake Medical','DRAKE MEDICAL'),
])
def test_normalize_name(raw,expected):
    assert normalize_name(raw)==expected


@pytest.mark.parametrize('raw,expected',[
    ('(212) 555-0123','2125550123'),('+1 212 555 0123','2125550123'),
    ('212.555.0123','2125550123'),('212-555-0123 ext 45','2125550123'),
    ('212-555-0123 x45','2125550123'),('5550123',''),('', ''),(None,''),
    ('442071234567',''),('121255501234',''),
])
def test_normalize_phone(raw,expected):
    assert normalize_phone(raw)==expected


@pytest.mark.parametrize('raw,expected',[
    ('123 Main Street Suite 204, Boston MA 02110','123|MAIN ST|204|02110'),
    ('123 Main St Ste. 204, Boston MA 02110','123|MAIN ST|204|02110'),
    ('123 Main St #204, Boston MA 02110','123|MAIN ST|204|02110'),
    ('123 Main St, Boston MA 02110-1234','123|MAIN ST||02110'),
    ('123 North Main Avenue, Boston MA 02110','123|N MAIN AVE||02110'),
    ('PO Box 44, Boston MA 02110','44|PO BOX||02110'),
    ({'line1':'123 Main Street','line2':'Suite 204','city':'Boston','state':'MA','zip':'021101234'},'123|MAIN ST|204|02110'),
    ({'line1':'','city':'Boston','state':'MA','zip':'02110'},''),
    ('123 Main Street',''),('', ''),(None,''),
])
def test_normalize_address(raw,expected):
    assert normalize_address(raw)==expected


@pytest.mark.parametrize('authority,expected',[
    ('1128a1',True),('1128(a)(3)',True),('1128B7',True),('1128(a)(2)',False),
    ('1128b4',False),('1128b14',False),('1128b16',False),('',False),('federal debarment',False),
])
def test_closed_fraud_authority_list(authority,expected):
    assert fraud_related(authority) is expected


def write_csv(path,columns,rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=sorted(columns))
        writer.writeheader()
        writer.writerows(rows)
    return path


@pytest.fixture
def sealed_sources(tmp_path,monkeypatch):
    monkeypatch.setenv('VIGIL_DATA_ROOT',str(tmp_path/'data'))
    monkeypatch.setattr(acquisition,'PROJECT_ROOT',tmp_path)
    root=tmp_path/'originals'
    nppes_cols=set(NPPES_COLUMNS)
    nppes_cols.add('Provider Business Practice Location Address Telephone Number')
    for i in range(1,16):
        nppes_cols.update({f'Healthcare Provider Taxonomy Code_{i}',f'Provider License Number_{i}',
                          f'Provider License Number State Code_{i}',f'Healthcare Provider Primary Taxonomy Switch_{i}'})
    path=write_csv(root/'npidata_pfile_20050523-20260208.csv',nppes_cols,[{
        'NPI':'1234567890','Entity Type Code':'1','Provider Last Name (Legal Name)':'CHEN','Provider First Name':'ROBERT',
        'Provider Enumeration Date':'05/23/2005','Healthcare Provider Primary Taxonomy Switch_2':'Y',
        'Healthcare Provider Taxonomy Code_2':'207Q00000X','Provider License Number_2':'A123','Provider License Number State Code_2':'MA',
        'Provider Business Practice Location Address Telephone Number':'(212) 555-0123',
        'Provider First Line Business Practice Location Address':'123 Main St',
        'Provider Second Line Business Practice Location Address':'Suite 204',
        'Provider Business Practice Location Address City Name':'Boston',
        'Provider Business Practice Location Address State Name':'MA',
        'Provider Business Practice Location Address Postal Code':'02110',
    }])
    import_files('nppes','2026-02',{path.name:path})
    leiecols=set(LEIE_COLUMNS)|{'SPECIALTY'}
    path=write_csv(root/'UPDATED.csv',leiecols,[{
        'FIRSTNAME':'Robert','LASTNAME':'Chen MD','NPI':'1234567890','EXCLTYPE':'1128a1','EXCLDATE':'20200101','SPECIALTY':'FAMILY MEDICINE',
        'ADDRESS':'123 Main St #204','CITY':'Boston','STATE':'MA','ZIP':'02110',
    },{'FIRSTNAME':'Jane','LASTNAME':'Smith','NPI':'0000000000','EXCLTYPE':'1128b4','EXCLDATE':'20190101'}])
    import_files('leie','2026-11-05',{path.name:path})
    providercols=set(PROVIDER_COLUMNS)|{'Rndrng_Prvdr_Ent_Cd','Rndrng_Prvdr_St1','Rndrng_Prvdr_St2','Rndrng_Prvdr_City','Rndrng_Prvdr_Zip5'}
    prov=write_csv(root/'MUP_PHY_R25_P07_V20_D22_Prov.csv',providercols,[{
        'Rndrng_NPI':'1234567890','Rndrng_Prvdr_First_Name':'Robert','Rndrng_Prvdr_Last_Org_Name':'Chen',
        'Rndrng_Prvdr_Ent_Cd':'I','Rndrng_Prvdr_State_Abrvtn':'MA','Rndrng_Prvdr_Type':'Family Practice',
        'Tot_Srvcs':'10','Tot_Benes':'4','Tot_Mdcr_Pymt_Amt':'1000',
    }])
    svc=write_csv(root/'MUP_PHY_R25_P05_V20_D22_Prov_Svc.csv',SERVICE_COLUMNS,[
        {'Rndrng_NPI':'1234567890','HCPCS_Cd':'99213','Tot_Srvcs':'5','Tot_Benes':'3','Avg_Mdcr_Pymt_Amt':'100'},
        {'Rndrng_NPI':'1234567890','HCPCS_Cd':'99214','Tot_Srvcs':'5','Tot_Benes':'3','Avg_Mdcr_Pymt_Amt':'100'},
    ])
    import_files('cms_part_b','2022',{p.name:p for p in [prov,svc]})
    path=root/'pspp2015_90.zip'
    with zipfile.ZipFile(path,'w') as z:
        z.writestr('physician-shared-patient-patterns-2015-days90.txt','1234567890,2345678901,99,12,4\n')
    import_files('shared_patients','2015',{path.name:path})
    return tmp_path


def test_all_canonical_tables_types_evidence_and_byte_determinism(sealed_sources):
    paths=run(sealed_sources/'canonical-first')
    repeated=run(sealed_sources/'canonical-second')
    def hashes(items,base):
        return {str(p.relative_to(base)):hashlib.sha256(p.read_bytes()).hexdigest() for p in items}
    assert hashes(paths,sealed_sources/'canonical-first')==hashes(repeated,sealed_sources/'canonical-second')
    for table,columns in SCHEMAS.items():
        files=[p for p in paths if p.parent.name==table]
        assert files
        with duckdb.connect() as c:
            actual=c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[str(files[0])]).fetchall()
            assert [(r[0],r[1]) for r in actual]==columns
    with duckdb.connect() as c:
        nppes=c.execute('SELECT name_norm,specialty_code,license_no FROM read_parquet(?)',[str(sealed_sources/'canonical-first/providers/2026-02.parquet')]).fetchone()
        assert nppes==('ROBERT CHEN','207Q00000X','A123')
        addr=c.execute('SELECT addr_key FROM read_parquet(?) WHERE addr_type=\'practice\'',[str(sealed_sources/'canonical-first/addresses/2026-02.parquet')]).fetchone()[0]
        assert addr=='123|MAIN ST|204|02110'
        exclusions=c.execute('SELECT npi,fraud_related,rein_date FROM read_parquet(?) ORDER BY record_id',[str(sealed_sources/'canonical-first/exclusions/2026-11-05.parquet')]).fetchall()
        assert exclusions==[('1234567890',True,None),('',False,None)]
        billing=c.execute('SELECT total_payment,unique_beneficiaries,top_hcpcs FROM read_parquet(?)',[str(sealed_sources/'canonical-first/billing_summaries/2022.parquet')]).fetchone()
        assert billing==(1000.0,4,'99213,99214')
        assert c.execute('SELECT shared_count FROM read_parquet(?)',[str(sealed_sources/'canonical-first/shared_patients/2015.parquet')]).fetchone()[0]==12


def test_mutated_sealed_input_blocks_normalization(sealed_sources):
    path=sealed_sources/'data/raw/leie/2026-11-05/UPDATED.csv'
    path.write_bytes(path.read_bytes()+b'changed')
    with pytest.raises(ValueError,match='Sealed vintage changed'):
        run(sealed_sources/'canonical')
    assert not (sealed_sources/'canonical').exists()


def test_nppes_missing_name_gate_is_not_weakened(sealed_sources):
    # New sealed fixture vintage with a valid source schema but no provider name.
    root=sealed_sources/'originals'
    source=root/'npidata_pfile_20050523-20260208.csv'
    with source.open(newline='') as stream:
        reader=csv.DictReader(stream);row=next(reader);columns=reader.fieldnames
    row['Provider First Name']='';row['Provider Last Name (Legal Name)']=''
    second=write_csv(root/'npidata_pfile_20050523-20260308.csv',columns,[row])
    import_files('nppes','2026-03',{second.name:second})
    with pytest.raises(ValueError,match='violates <0.5%'):
        run(sealed_sources/'canonical')
    assert not (sealed_sources/'canonical').exists()


def test_pecos_legacy_encoding_and_canonical_pac_links(tmp_path,monkeypatch):
    from common.manifest import seal
    from ingest.inputs import SealedInputs
    from normalize.engine import connect
    from normalize import addresses, pecos
    monkeypatch.setenv('VIGIL_DATA_ROOT',str(tmp_path/'data'))
    root=tmp_path/'data/raw/pecos/2026-07-17'
    row={'ENROLLMENT ID':'E1','ASSOCIATE ID':'1111111111','ASSOCIATE ID - OWNER':'2222222222',
         'TYPE - OWNER':'I','ROLE TEXT - OWNER':'5% OR GREATER DIRECT OWNERSHIP INTEREST',
         'ASSOCIATION DATE - OWNER':'7/1/2020','FIRST NAME - OWNER':'José','LAST NAME - OWNER':'Chen',
         'ORGANIZATION NAME - OWNER':'','PERCENTAGE OWNERSHIP':'11.5','ADDRESS LINE 1 - OWNER':'',
         'ADDRESS LINE 2 - OWNER':'','CITY - OWNER':'','STATE - OWNER':'MA','ZIP CODE - OWNER':'02110'}
    path=write_csv(root/'HHA_All_Owners_2026.07.17.csv',row.keys(),[row])
    path.write_bytes(path.read_text().encode('cp1252'))
    seal(root)
    with connect(tmp_path/'stage.duckdb') as c:
        addresses.initialize(c)
        inputs=SealedInputs(c,tmp_path/'scratch')
        pecos.run(c,inputs,'2026-07-17')
        assert c.execute('SELECT name_norm,entity_type FROM providers').fetchone()==('JOSÉ CHEN',1)
        assert c.execute('SELECT org_npi_or_pac,pct_ownership FROM ownerships').fetchone()==('1111111111',11.5)
        assert c.execute('SELECT id_type,id_value FROM provider_identifiers').fetchone()==('pac','2222222222')


def test_real_input_adapter_rejects_unsealed_vintage(tmp_path,monkeypatch):
    from ingest.inputs import SealedInputs
    monkeypatch.setenv('VIGIL_DATA_ROOT',str(tmp_path/'data'))
    (tmp_path/'data/raw/nppes/2026-02').mkdir(parents=True)
    with duckdb.connect() as c:
        inputs=SealedInputs(c,tmp_path/'scratch')
        with pytest.raises(ValueError,match='Unsealed'):
            inputs.vintages('nppes')
