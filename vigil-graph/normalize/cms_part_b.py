from ingest.inputs import sql_literal
from normalize import addresses
from normalize.engine import insert, text, npi, record_id, report


def run(connection,inputs,vintage):
    files=inputs.files('cms_part_b',vintage)
    provider=next(f for f in files if f.lower().endswith('_prov.csv'))
    service=next(f for f in files if f.lower().endswith('_prov_svc.csv'))
    table=inputs.csv('cms_part_b',vintage,provider)
    rid=record_id('cms_part_b',vintage,provider)
    first=text('Rndrng_Prvdr_First_Name');last=text('Rndrng_Prvdr_Last_Org_Name')
    name=f"CASE WHEN {text('Rndrng_Prvdr_Ent_Cd')}='O' THEN {last} ELSE trim({first} || ' ' || {last}) END"
    insert(connection,'providers',dict(record_id=rid,source="'cms_part_b'",vintage=sql_literal(vintage),npi=npi('Rndrng_NPI'),
        entity_type=f"CASE {text('Rndrng_Prvdr_Ent_Cd')} WHEN 'O' THEN 2 WHEN 'I' THEN 1 ELSE NULL END",
        name_raw=name,name_norm=f'norm_name({name})',first=f'norm_name({first})',
        last=f"CASE WHEN {text('Rndrng_Prvdr_Ent_Cd')}='I' THEN norm_name({last}) ELSE '' END",
        org_name_norm=f"CASE WHEN {text('Rndrng_Prvdr_Ent_Cd')}='O' THEN norm_name({last}) ELSE '' END",
        specialty_desc=text('Rndrng_Prvdr_Type')),table)
    addresses.add(connection,table,rid=rid,source='cms_part_b',vintage=vintage,npi=npi('Rndrng_NPI'),kind='practice',
        line1=text('Rndrng_Prvdr_St1'),line2=text('Rndrng_Prvdr_St2'),city=text('Rndrng_Prvdr_City'),
        state=text('Rndrng_Prvdr_State_Abrvtn'),postal=text('Rndrng_Prvdr_Zip5'))
    services=inputs.csv('cms_part_b',vintage,service,columns=['Rndrng_NPI','HCPCS_Cd','Tot_Srvcs','Avg_Mdcr_Pymt_Amt'])
    # Rank procedures by implied payment with HCPCS as deterministic tie-breaker.
    connection.execute(f'''CREATE TABLE top_hcpcs AS
        SELECT npi, string_agg(hcpcs, ',' ORDER BY payment DESC, hcpcs) FILTER(WHERE rank<=5) AS codes
        FROM (SELECT *, row_number() OVER(PARTITION BY npi ORDER BY payment DESC, hcpcs) AS rank
            FROM (SELECT {npi('Rndrng_NPI')} AS npi, {text('HCPCS_Cd')} AS hcpcs,
                         sum({text('Tot_Srvcs')}::DOUBLE * {text('Avg_Mdcr_Pymt_Amt')}::DOUBLE) AS payment
                  FROM {services} GROUP BY 1,2)) GROUP BY npi''')
    insert(connection,'billing_summaries',dict(npi=npi('Rndrng_NPI'),year=str(int(vintage[:4])),
        total_services=f"nullif({text('Tot_Srvcs')},'')::DOUBLE",total_payment=f"nullif({text('Tot_Mdcr_Pymt_Amt')},'')::DOUBLE",
        unique_beneficiaries=f"nullif({text('Tot_Benes')},'')::BIGINT",top_hcpcs="coalesce(codes,'')",
        state=text('Rndrng_Prvdr_State_Abrvtn'),specialty=text('Rndrng_Prvdr_Type')),
        f'{table} p LEFT JOIN top_hcpcs t ON {npi("Rndrng_NPI")}=t.npi')
    for temp in [table,services,'top_hcpcs']:
        connection.execute(f'DROP TABLE {temp}')
    report(connection,'billing_summaries',f'year={int(vintage[:4])}')
    report(connection,'providers',f"source='cms_part_b' AND vintage={sql_literal(vintage)}")
