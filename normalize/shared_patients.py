from normalize.engine import insert, npi, text, report


def run(connection,inputs,vintage):
    for filename in inputs.files('shared_patients',vintage):
        table=inputs.csv('shared_patients',vintage,filename,header=False)
        # CMS column 3 counts pairs/encounters; column 4 is unique shared people.
        # Preserve source direction; graph construction later defines symmetry.
        insert(connection,'shared_patients',dict(npi_a=npi('npi_a'),npi_b=npi('npi_b'),
               shared_count=f"{text('bene_count')}::BIGINT",year=str(int(vintage[:4]))),table)
        connection.execute(f'DROP TABLE {table}')
    report(connection,'shared_patients',f'year={int(vintage[:4])}')
