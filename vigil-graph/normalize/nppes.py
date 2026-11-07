import json

from ingest.inputs import sql_literal
from normalize import addresses
from normalize.engine import insert, text, npi, record_id, date_value, report


def run(connection, inputs, vintage):
    for filename in inputs.files('nppes', vintage):
        columns = ['NPI','Entity Type Code','Provider Organization Name (Legal Business Name)',
                   'Provider Last Name (Legal Name)','Provider First Name','Provider Enumeration Date',
                   'Healthcare Provider Taxonomy Code_1','Provider License Number_1','Provider License Number State Code_1',
                   'Provider Business Practice Location Address Telephone Number',
                   'NPI Deactivation Date','NPI Reactivation Date']
        # Preserve every license/taxonomy in the source, using the primary
        # taxonomy's slot for the single canonical specialty/license fields.
        # Full source-row evidence remains accessible through record_id.
        for kind in ['Mailing','Practice Location']:
            columns += [f'Provider First Line Business {kind} Address',f'Provider Second Line Business {kind} Address',
                        f'Provider Business {kind} Address City Name',f'Provider Business {kind} Address State Name',
                        f'Provider Business {kind} Address Postal Code']
        # Use the source's declared primary taxonomy instead of assuming slot 1.
        for slot in range(2,16):
            columns += [f'Healthcare Provider Taxonomy Code_{slot}',f'Provider License Number_{slot}',f'Provider License Number State Code_{slot}']
        columns += [f'Healthcare Provider Primary Taxonomy Switch_{i}' for i in range(1,16)]
        table = inputs.csv('nppes',vintage,filename,columns=columns)
        rid=record_id('nppes',vintage,filename)
        org=text('Provider Organization Name (Legal Business Name)')
        first=text('Provider First Name');last=text('Provider Last Name (Legal Name)')
        name=f"CASE WHEN {text('Entity Type Code')}='2' THEN {org} ELSE trim({first} || ' ' || {last}) END"
        deactivation=date_value('NPI Deactivation Date','%m/%d/%Y')
        reactivation=date_value('NPI Reactivation Date','%m/%d/%Y')
        status=(f"CASE WHEN _identity_suppressed THEN 'deactivated_suppressed' "
                f"WHEN {reactivation} IS NOT NULL AND ({deactivation} IS NULL OR {reactivation}>{deactivation}) THEN 'reactivated' "
                f"WHEN {deactivation} IS NOT NULL THEN 'deactivated' ELSE 'no_deactivation_reported' END")
        insert(connection,'npi_status',dict(record_id=rid,source="'nppes'",vintage=sql_literal(vintage),
               npi=npi('NPI'),status=status,deactivation_date=deactivation,reactivation_date=reactivation,
               identity_suppressed='_identity_suppressed'),table)
        def primary(prefix):
            branches=' '.join(f"WHEN {text(f'Healthcare Provider Primary Taxonomy Switch_{i}')}='Y' THEN {text(prefix+str(i))}" for i in range(1,16))
            return f'CASE {branches} ELSE {text(prefix+"1")} END'
        insert(connection,'providers',dict(record_id=rid,source="'nppes'",vintage=sql_literal(vintage),npi=npi('NPI'),
               entity_type=f"nullif({text('Entity Type Code')}, '')::INTEGER",name_raw=name,name_norm=f'norm_name({name})',
               first=f'norm_name({first})',last=f'norm_name({last})',org_name_norm=f'norm_name({org})',
               specialty_code=primary('Healthcare Provider Taxonomy Code_'),license_no=f'upper({primary("Provider License Number_")})',
               license_state=f'upper({primary("Provider License Number State Code_")})',
               phone_norm=f"norm_phone({text('Provider Business Practice Location Address Telephone Number')})",
               enumeration_date=date_value('Provider Enumeration Date','%m/%d/%Y')),table)
        for label,kind in [('Mailing','mailing'),('Practice Location','practice')]:
            addresses.add(connection,table,rid=rid,source='nppes',vintage=vintage,npi=npi('NPI'),kind=kind,
                line1=text(f'Provider First Line Business {label} Address'),line2=text(f'Provider Second Line Business {label} Address'),
                city=text(f'Provider Business {label} Address City Name'),state=text(f'Provider Business {label} Address State Name'),
                postal=text(f'Provider Business {label} Address Postal Code'))
        connection.execute(f'DROP TABLE {table}')
    stats=report(connection,'providers',f"source='nppes' AND vintage={sql_literal(vintage)}")
    # Keep all records in providers and preserve lifecycle metadata separately.
    # CMS intentionally suppresses identity fields on NPI/date-only records;
    # measuring their name completeness as if they were disclosed is a bug.
    count, suppressed, missing = connection.execute(f'''
        SELECT count(*), count(*) FILTER(WHERE s.identity_suppressed),
               count(*) FILTER(WHERE NOT s.identity_suppressed AND p.name_norm='')
        FROM providers p JOIN npi_status s USING(record_id)
        WHERE p.source='nppes' AND p.vintage={sql_literal(vintage)}''').fetchone()
    identity_count=count-suppressed
    rate=missing/identity_count if identity_count else 1.0
    print(json.dumps(dict(source='nppes',vintage=vintage,source_rows=count,
        documented_suppressed_records=suppressed,identity_records=identity_count,
        missing_identity_names=missing,missing_identity_name_rate=rate,
        unfiltered_name_missing_rate=stats['null_rates']['name_norm'],threshold=0.005),sort_keys=True),flush=True)
    if rate >= 0.005:
        raise ValueError(f'NPPES name_norm missing rate {rate:.4%} violates <0.5% gate on identity-bearing records')
