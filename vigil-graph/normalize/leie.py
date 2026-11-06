from ingest.inputs import sql_literal
from normalize import addresses
from normalize.engine import insert, text, npi, record_id, date_value, report


def run(connection, inputs, vintage):
    for filename in inputs.files('leie',vintage):
        table=inputs.csv('leie',vintage,filename)
        rid=record_id('leie',vintage,filename)
        business=text('BUSNAME');first=text('FIRSTNAME');last=text('LASTNAME')
        name=f"CASE WHEN {business}<>'' THEN {business} ELSE trim({first} || ' ' || {last}) END"
        insert(connection,'providers',dict(record_id=rid,source="'leie'",vintage=sql_literal(vintage),npi=npi('NPI'),
            entity_type=f"CASE WHEN {business}<>'' THEN 2 ELSE 1 END",name_raw=name,name_norm=f'norm_name({name})',
            first=f'norm_name({first})',last=f'norm_name({last})',org_name_norm=f'norm_name({business})',
            specialty_desc=text('SPECIALTY')),table)
        insert(connection,'exclusions',dict(record_id=rid,vintage=sql_literal(vintage),npi=npi('NPI'),
            last=f'norm_name({last})',first=f'norm_name({first})',business_name=business,
            excl_type=text('EXCLTYPE'),excl_authority=f"norm_authority({text('EXCLTYPE')})",
            fraud_related=f"is_fraud({text('EXCLTYPE')})",excl_date=date_value('EXCLDATE','%Y%m%d'),
            rein_date=date_value('REINDATE','%Y%m%d'),waiver_date=date_value('WAIVERDATE','%Y%m%d')),table)
        addresses.add(connection,table,rid=rid,source='leie',vintage=vintage,npi=npi('NPI'),kind='mailing',
            line1=text('ADDRESS'),line2="''",city=text('CITY'),state=text('STATE'),postal=text('ZIP'))
        connection.execute(f'DROP TABLE {table}')
    report(connection,'exclusions',f'vintage={sql_literal(vintage)}')
    report(connection,'providers',f"source='leie' AND vintage={sql_literal(vintage)}")
