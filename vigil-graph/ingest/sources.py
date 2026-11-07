"""Source catalog: acquisition contracts and authoritative landing pages."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    key: str
    url: str
    mode: str
    required: bool = True
    instructions: str = ''


SOURCES = {
    'nppes': Source('nppes', 'https://download.cms.gov/nppes/NPI_Files.html', 'manual',
                    instructions='Extract the monthly full dissemination CSV (npidata_pfile*.csv); weekly updates are insufficient.'),
    'leie': Source('leie', 'https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv', 'auto',
                   instructions='Download the full active exclusions CSV as UPDATED.csv.'),
    'cms_part_b': Source('cms_part_b', 'https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners',
                         'manual', instructions='Provide both by-Provider and by-Provider-and-Service CSVs for the same data year. Preserve CMS filenames with DYY and Prov/Prov_Svc suffixes.'),
    'shared_patients': Source('shared_patients', 'https://www.cms.gov/Regulations-and-Guidance/Legislation/FOIA/Referral-Data-FAQs',
                              'manual', instructions='Provide one psppYYYY_DD.zip or physician-shared-patient-patterns-YYYY-daysDD.txt, preserving its year and interval.'),
    'pecos': Source('pecos', 'https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment',
                    'manual', False, 'Provide matching *_All_Owners_*.csv and *_Enrollments_*.csv pairs; PPEF_Enrollment_Extract_*.csv may also be included. CHOW files need a later acquisition contract.'),
    'sam': Source('sam', 'https://sam.gov/data-services/Exclusions/Public%20V2?privacy=Public',
                  'key-or-manual', False, 'Set SAM_API_KEY for the public daily extract, sampled as a monthly snapshot, or drop in SAM_Exclusions_Public_Extract_V2_YYDDD.ZIP/CSV.'),
}
