# Phase A — acceptance evidence

Completed November 5, 2026 (America/Los_Angeles). Stop after this phase as
required by the build spec. Gate 0 remains intact, including exact CLAUDE.md.

## Gate checklist

- [x] NPPES, LEIE, CMS Part B, Shared Patient Patterns are sealed with SHA-256 manifests.
- [x] `python pipeline.py status` prints every source, latest vintage, and staleness.
- [x] Mutation, file addition, and file removal cause re-sealing to fail.
- [x] `python pipeline.py --through stage0` completes against real sealed vintages.
- [x] All 38 tests pass on Python 3.13.15; baseline was 10 passing tests.
- [x] Implementation and evidence committed as the Phase A milestone.

## Acquired inputs

| Source | Vintage | Data files | Acquisition |
| --- | --- | ---: | --- |
| NPPES | 2026-02 | 1 | Full 330-column February dissemination CSV copied from the existing Vigil download; 11,207,185,966 bytes |
| LEIE | 2026-11-05 | 1 | Live official UPDATED.csv through the automatic fetcher with explicit verified system curl; 10,802,977 bytes |
| CMS Part B | 2022 | 2 | Local official Provider CSV, 484,670,744 bytes; freshly retrieved official 2022 Provider/Service CSV, 3,066,783,950 bytes |
| Shared Patients | 2015 | 1 | Existing pspp2015_90.zip; 566,928,093 bytes; complete ZIP CRC verified |
| PECOS | 2026-07-17 | 9 | FQHC, HHA, Hospice, RHC enrollment/All_Owners pairs plus PPEF enrollment |
| PECOS | 2026-07-31 | 4 | Hospital and SNF enrollment/All_Owners pairs |
| SAM exclusions | absent | 0 | Optional; no local extract or SAM_API_KEY available |

All originals remain in place. Copied raw inputs and manifests live under
`data/raw/<source>/<vintage>/` and are intentionally excluded from Git. Receipts
record exact original paths and public source URLs; vintage manifests record
full-file SHA-256 values. Data stays with this local checkout; Git alone does
not transport the sealed inputs.

PECOS vintages cover different provider classes. Later normalization must retain
these classes across vintages, rather than treating the latest source-level
date as a complete replacement of every earlier class. `status` shows the latest
source-level date; each vintage's manifest remains the authoritative inventory.

## Data-year mismatch corrected during acquisition

The legacy `Downloads/vigil/ml/data/raw/cms_part_b_2022_by_hcpcs.csv` was backed
by a URL ending `MUP_PHY_R25_P05_V20_D23_Prov_Svc.csv` in both legacy acquisition
scripts. The official CMS catalog identifies that distribution as calendar year
2023. It was excluded from the new 2022 vintage.

The correct 2022 service file was retrieved from the [official CMS catalog](https://data.cms.gov/data.json):

- [2022 Provider CSV](https://data.cms.gov/sites/default/files/2025-11/adcd20c5-4534-43cd-8dfa-881ebe7bacfd/MUP_PHY_R25_P07_V20_D22_Prov.csv)
- [2022 Provider/Service CSV](https://data.cms.gov/sites/default/files/2025-11/53fb2bae-4913-48dc-a6d4-d8c025906567/MUP_PHY_R25_P05_V20_D22_Prov_Svc.csv)

No legacy files or legacy pipeline code were changed.

## Verification

```text
SOURCE             VINTAGE      DAYS SINCE FETCH  STATE             MODE
nppes              2026-02                     0  SEALED            manual
leie               2026-11-05                  0  SEALED            auto
cms_part_b         2022                        0  SEALED            manual
shared_patients    2015                        0  SEALED            manual
pecos              2026-07-31                  0  SEALED            manual
sam                -                           -  OPTIONAL-MISSING  key-or-manual

Stage 0 complete: all four required sources verified and sealed.
38 passed
```

Days since fetch describe acquisition into this checkout, not data age. NPPES
observations are from February 2026; billing observations are from 2022; shared
patient observations are from 2015, using the 90-day interval. Normalization must
preserve these periods. This phase makes no finding about any real provider.

The suite also covers missing manual inputs with actionable instructions,
no-overwrite imports, schema/year checks, archive member validation, network
failure isolation, truncated transfers, and secret-free SAM provenance/errors.
No synthetic test data was used to satisfy the real acquisition gate.

## Authoritative source contracts

- [NPPES bulk files](https://download.cms.gov/nppes/NPI_Files.html)
- [OIG full LEIE CSV](https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv)
- [CMS Part B collection](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners)
- [CMS Shared Patient Patterns landing page](https://www.cms.gov/Regulations-and-Guidance/Legislation/FOIA/Referral-Data-FAQs)
- [CMS shared-patient technical requirements](https://downloads.cms.gov/foia/physician_shared_patient_patterns_technical_requirements.pdf)
- [Existing archive's NBER download location](https://data.nber.org/physician-shared-patient-patterns/2015/pspp2015_90.zip)
- [CMS enrollment/ownership collection](https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment)
- [GSA extracts API and official samples](https://open.gsa.gov/api/sam-entity-extracts-api/)

Phase B must build canonical tables exclusively from these sealed vintages and
satisfy its normalization/determinism gate before entity resolution begins.
