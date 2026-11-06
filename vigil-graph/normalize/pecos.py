from ingest.inputs import sql_literal
from normalize import addresses
from normalize.engine import insert, text, npi, record_id, date_value, report


def run(connection,inputs,vintage):
    for filename in inputs.files('pecos',vintage):
        table=inputs.csv('pecos',vintage,filename)
        rid=record_id('pecos',vintage,filename)
        if '_All_Owners_' in filename:
            first=text('FIRST NAME - OWNER');last=text('LAST NAME - OWNER');org=text('ORGANIZATION NAME - OWNER')
            name=f"CASE WHEN {text('TYPE - OWNER')}='O' THEN {org} ELSE trim({first} || ' ' || {last}) END"
            insert(connection,'providers',dict(record_id=rid,source="'pecos_owner'",vintage=sql_literal(vintage),
                entity_type=f"CASE {text('TYPE - OWNER')} WHEN 'I' THEN 1 WHEN 'O' THEN 2 ELSE NULL END",
                name_raw=name,name_norm=f'norm_name({name})',first=f'norm_name({first})',last=f'norm_name({last})',
                org_name_norm=f'norm_name({org})'),table)
            # PAC/associate IDs are deliberately retained, not coerced into NPIs.
            insert(connection,'ownerships',dict(record_id=rid,vintage=sql_literal(vintage),
                org_npi_or_pac=text('ASSOCIATE ID'),owner_name_norm=f'norm_name({name})',
                owner_role=text('ROLE TEXT - OWNER'),
                pct_ownership=f"nullif(replace({text('PERCENTAGE OWNERSHIP')},'%',''),'')::DOUBLE",
                effective_date=date_value('ASSOCIATION DATE - OWNER','%m/%d/%Y')),table)
            addresses.add(connection,table,rid=rid,source='pecos_owner',vintage=vintage,npi="''",kind='mailing',
                line1=text('ADDRESS LINE 1 - OWNER'),line2=text('ADDRESS LINE 2 - OWNER'),city=text('CITY - OWNER'),
                state=text('STATE - OWNER'),postal=text('ZIP CODE - OWNER'))
            # Extra canonical crosswalk preserves the PAC identities needed by
            # later ownership edges without reopening raw files downstream.
            connection.execute(f'''INSERT INTO provider_identifiers SELECT {rid}, 'pac', {text('ASSOCIATE ID - OWNER')}, 'pecos_owner', {sql_literal(vintage)} FROM {table}''')
        elif '_Enrollments_' in filename:
            name=text('ORGANIZATION NAME')
            insert(connection,'providers',dict(record_id=rid,source="'pecos_enrollment'",vintage=sql_literal(vintage),npi=npi('NPI'),
                entity_type='2',name_raw=name,name_norm=f'norm_name({name})',org_name_norm=f'norm_name({name})',
                specialty_code=text('PROVIDER TYPE CODE'),specialty_desc=text('PROVIDER TYPE TEXT')),table)
            addresses.add(connection,table,rid=rid,source='pecos_enrollment',vintage=vintage,npi=npi('NPI'),kind='practice',
                line1=text('ADDRESS LINE 1'),line2=text('ADDRESS LINE 2'),city=text('CITY'),state=text('STATE'),postal=text('ZIP CODE'))
            connection.execute(f'''INSERT INTO provider_identifiers SELECT {rid}, 'pac', {text('ASSOCIATE ID')}, 'pecos_enrollment', {sql_literal(vintage)} FROM {table}''')
        else:  # PPEF Enrollment Extract
            first=text('FIRST_NAME');last=text('LAST_NAME');org=text('ORG_NAME')
            name=f"CASE WHEN {org}<>'' THEN {org} ELSE trim({first} || ' ' || {last}) END"
            insert(connection,'providers',dict(record_id=rid,source="'pecos_enrollment'",vintage=sql_literal(vintage),npi=npi('NPI'),
                entity_type=f"CASE WHEN {org}<>'' THEN 2 ELSE 1 END",name_raw=name,name_norm=f'norm_name({name})',
                first=f'norm_name({first})',last=f'norm_name({last})',org_name_norm=f'norm_name({org})',
                specialty_code=text('PROVIDER_TYPE_CD'),specialty_desc=text('PROVIDER_TYPE_DESC')),table)
            connection.execute(f'''INSERT INTO provider_identifiers SELECT {rid}, 'pac', {text('PECOS_ASCT_CNTL_ID')}, 'pecos_enrollment', {sql_literal(vintage)} FROM {table}''')
        connection.execute(f'DROP TABLE {table}')
    report(connection,'ownerships',f'vintage={sql_literal(vintage)}')
    report(connection,'providers',f"source LIKE 'pecos%' AND vintage={sql_literal(vintage)}")
