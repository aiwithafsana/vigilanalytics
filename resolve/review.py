"""Readable evidence for a halted build; never approves or repairs identities."""
from pathlib import Path

from resolve.storage import literal


def write_alarm_report(c, output):
    output=Path(output)
    c.execute('''CREATE TEMP VIEW alarm_records AS SELECT a.*,n.* EXCLUDE(npi)
        FROM npi_alarms a JOIN nodes n USING(npi)''')
    for name,query in {
        'npi_alarms':'SELECT * FROM npi_alarms ORDER BY npi',
        'cluster_review':'SELECT * FROM alarm_records ORDER BY npi,record_id',
    }.items():
        c.execute(f'COPY ({query}) TO {literal(output/(name+".csv"))} (HEADER, DELIMITER \',\')').fetchall()
    oversized=c.execute('''SELECT npi,max(records),min(name_norm),
        string_agg(DISTINCT source,', ' ORDER BY source)
        FROM alarm_records WHERE oversized GROUP BY npi ORDER BY max(records) DESC,npi''').fetchall()
    conflicts=c.execute('''SELECT a.source,a.kind,b.source,b.kind,count(DISTINCT a.npi)
        FROM alarm_records a JOIN alarm_records b USING(npi)
        WHERE a.kind<b.kind GROUP BY 1,2,3,4 ORDER BY 5 DESC,1,2,3,4''').fetchall()
    lines=['# Phase C cluster alarm review','',
        'The build halted before candidate generation. All source records remain in canonical Parquet. '
        'These are identity conflicts and repeated observations, not findings of fraud.',
        '', '## Exact-NPI groups above 50 records','',
        '| NPI | Records | Canonical name | Sources |','|---|---:|---|---|']
    clean=lambda v:str(v).replace('|','/').replace('\n',' ')
    lines += ['| '+' | '.join(map(clean,row))+' |' for row in oversized]
    lines += ['', 'Enrollment observations may repeat one NPI. Inspect the full row evidence to distinguish '
        'repeated source observations from different identities.',
        '', '## Person/organization disagreements','',
        'Type 1 means individual; type 2 means organization. Counts below overlap.',
        '', '| Source A | Type A | Source B | Type B | Distinct NPIs |','|---|---:|---|---:|---:|']
    lines += ['| '+' | '.join(map(clean,row))+' |' for row in conflicts]
    lines += ['', '## Proposed repair and review sequence','',
        '1. Correct PECOS type inference in normalization. The current adapter treats every class-specific '
        'enrollment as an organization and infers PPEF type from whether ORG_NAME is populated. '
        'A populated organization-name field can contain an individual’s name. '
        'The code must retain the original '
        'inference and document any registry-based correction.',
        '2. Review remaining mismatched NPIs individually. Examples include an excluded individual’s name '
        'paired with a pharmacy or healthcare organization NPI. Do not transfer exclusion labels across '
        'an unresolved person/organization conflict.',
        '3. Review the large exact-NPI groups. A narrowly scoped exception, if authorized, should be bound '
        'to the exact reviewed member IDs and forbid additional fuzzy merges. The default 50-record alarm '
        'should remain in force for every unreviewed group. No exception is enabled in this implementation.',
        '4. Complete the 30-pair human spot-check, rerun national blocking/scoring/clustering, verify metrics '
        'and two-run Parquet determinism, then publish accepted identities.',
        '', 'Full row evidence: [cluster_review.csv](cluster_review.csv). '
        'Group inventory: [npi_alarms.csv](npi_alarms.csv).']
    (output/'cluster_review.md').write_text('\n'.join(lines)+'\n')
    c.execute('DROP VIEW alarm_records')
