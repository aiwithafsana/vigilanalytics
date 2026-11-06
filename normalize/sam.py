"""Public SAM identities; federal exclusions are not assumed fraud authorities.

SAM statuses are distinct from HHS 1128 program-fraud exclusions. Preserve them
as canonical enforcement records with fraud_related=false for later labeling.
"""
from ingest.inputs import sql_literal
from normalize import addresses
from normalize.engine import insert, text, npi, record_id, report


def run(connection,inputs,vintage):
    for filename in inputs.files('sam',vintage):
        table=inputs.csv('sam',vintage,filename)
        rid=record_id('sam',vintage,filename)
        first=text('First');last=text('Last');name=text('Name')
        individual=f"lower({text('Classification')})='individual'"
        insert(connection,'providers',dict(record_id=rid,source="'sam'",vintage=sql_literal(vintage),npi=npi('NPI'),
            entity_type=f'CASE WHEN {individual} THEN 1 ELSE 2 END',name_raw=name,name_norm=f'norm_name({name})',
            first=f'norm_name({first})',last=f'norm_name({last})',
            org_name_norm=f"CASE WHEN {individual} THEN '' ELSE norm_name({name}) END"),table)
        def event(column):
            value=text(column)
            return f"CASE WHEN {value} IN ('','Indefinite') THEN NULL ELSE coalesce(try_strptime({value}, ['%m/%d/%Y','%Y-%m-%d'])::DATE, error('Invalid SAM date')) END"
        insert(connection,'exclusions',dict(record_id=rid,vintage=sql_literal(vintage),npi=npi('NPI'),
            last=f'norm_name({last})',first=f'norm_name({first})',business_name=f"CASE WHEN {individual} THEN '' ELSE {name} END",
            excl_type=text('Exclusion Type'),excl_authority=text('Excluding Agency'),fraud_related='false',
            excl_date=event('Active Date'),rein_date=event('Termination Date')),table)
        addresses.add(connection,table,rid=rid,source='sam',vintage=vintage,npi=npi('NPI'),kind='mailing',
            line1=text('Address 1'),line2=text('Address 2'),city=text('City'),state=text('State / Province'),postal=text('Zip Code'))
        connection.execute(f'DROP TABLE {table}')
    report(connection,'exclusions',f'vintage={sql_literal(vintage)}')
    report(connection,'providers',f"source='sam' AND vintage={sql_literal(vintage)}")
