# Phase C / Stage 2 — implemented, national acceptance halted

The resolver is implemented. The national build **has not passed Gate C** and
has not published accepted entity tables. Stage 3 has not started.

## Run and inputs

```sh
python pipeline.py --through stage2
# After the exact-NPI data alarms are resolved and a real human has reviewed:
python pipeline.py --through stage2 --review /path/to/human_review.json
```

Stage 2 reads only the canonical `providers` and `addresses` Parquet partitions;
it does not reacquire or renormalize sources. All 14,549,207 provider records are
included. Inputs are fingerprinted in `canonical_inputs.manifest.json`.
Scratch tables use disk-backed DuckDB with a 3 GB memory limit. Candidate
pairing and scoring have been exercised on fixtures; their national scale has
not been verified because the mandatory preflight alarm stops that work.

## National preflight result

Exact NPI unions alone would produce:

- **16 groups above 50 records**, with 51–151 members.
- **593 NPIs with both individual and organization classifications**.
- **609 distinct alarm groups**, containing **3,314 provider rows**.

The 16 oversized groups are disjoint from the type-conflict groups. The largest
is NPI `1316602774`, Fast Pace Medical Clinic: one NPPES row plus 150 PECOS
enrollment rows. These are repeated enrollment observations, not evidence of
fraud. No canonical records were removed, aggregated, or altered to pass the
alarm.

The type-conflict source comparisons overlap:

| Source A / type | Source B / type | Distinct NPIs |
|---|---|---:|
| NPPES / individual | PECOS enrollment / organization | 583 |
| PECOS enrollment / individual | PECOS enrollment / organization | 524 |
| CMS Part B / individual | PECOS enrollment / organization | 502 |
| LEIE / individual | NPPES / organization | 5 |
| PECOS enrollment / individual | NPPES / organization | 5 |

The PECOS normalizer currently treats every class-specific enrollment file row
as an organization, and infers PPEF type from whether `ORG_NAME` is populated.
Canonical evidence shows that field can contain a person's name: for example,
NPI `1003808494` is Andrew Chang, classified as an individual by NPPES/CMS and as
both types by different PECOS rows. This is a normalization defect to repair
with explicit type provenance and regression coverage before accepting merges.

The remaining 10 conflicts require closer review. Some involve different names,
such as LEIE Liana Karapetyan versus NPPES ANG Health Care for NPI `1447482435`,
and LEIE Jeffrey Black versus NPPES Northside Pharmacy for NPI `1902228067`.
An NPI agreement must not automatically transfer an individual's exclusion
label to an organization across an unresolved identity conflict.

Local artifacts under `data/resolved/`:

- `gate.json`: blocked status and unchanged thresholds.
- `npi_alarms.parquet` / `.csv`: all 609 group summaries.
- `cluster_review.parquet` / `.csv`: all 3,314 source/provider records.
- `cluster_review.md`: readable group counts and proposed repair sequence.
- `labeled_pairs.parquet` / `.csv`: 100 generated positive pairs and 100
  surname/city hard negatives, with source record IDs and label provenance.
- `human_review.md`, `review_request.json`: the required 30-pair spot-check.

These source-derived row artifacts remain local and are ignored by Git. The
committed preflight manifest records their hashes, not their full contents.

## Resolver policy and outputs

Six nonempty-key block types are unioned and deduplicated. Individuals and
organizations have separate fuzzy blocks. Unclassified or blank identities can
link by exact NPI only. Each original block's count and pair count are written
to Parquet; oversized blocks are refined by name initial and then sliced in
name/record-ID order so no final block exceeds 2,000,000 pairs. The split report
explicitly records the lost cross-partition candidate coverage. Exact NPI unions
are independent of splitting, so splitting never severs an exact NPI group.

Scoring follows the requested .40 name + .20 address + .20 phone + .20 license
rule. Name similarity for an empty name is zero. Addresses compare all valid
normalized addresses, including addresses not selected for the golden record.
License agreement requires a nonempty number **and state**. Address-only
positive evidence is capped at .20; an ADDR-only candidate without name
similarity >= .9 or corroborating phone/license is also capped at .20. All
candidate scores and feature columns are retained. Exact NPI rows have score
1 and `npi_exact=1`, with fuzzy feature columns null because scoring is skipped.

Union-find uses the configurable .85 threshold and 50-record alarm. Both exact
NPI and transitive fuzzy unions enforce person/organization separation.
Attempting a 51-member component dumps its records and stops. There is no
reviewed-cluster exemption, automatic threshold increase, or type override.

Entity IDs are the first 16 hex characters of SHA-256 of the UTF-8 compact JSON
array of sorted complete record IDs, e.g. `["a","b"]`. Hash collisions halt.
Golden names prefer a nonempty NPPES name; a suppressed blank NPPES identity
never replaces another source's meaningful name. Golden addresses prefer the
latest practice address, falling back to mailing if no practice address exists.
All valid address keys and NPIs remain in separate tables. Address first/last
seen values are source vintages, not assertions of continuous occupancy.

The four accepted tables are published only when all gates pass:
`entities.parquet`, `entity_addresses.parquet`, `entity_npis.parquet`,
`record_to_entity.parquet`. `gate.json` is marked building before work and
complete only after successful publication. A failed run preserves previously
accepted tables, if any; consumers must inspect gate status before use.

## Evaluation and human review

The generated positives use NPIs present in NPPES, CMS and LEIE, preferring
raw-name variants. Hard negatives have the same surname and an actual shared
city but different NPIs. The shared city is retained explicitly, since one
record can have several addresses. Different NPIs are a proposed negative
label, not proof; the human review can correct labels with a reason.

This requested sample tests exact-NPI clustering well. It does **not** establish
fuzzy recall for records lacking NPIs. Generated labels are not independent
identity truth. National precision/recall and the 20 accepted/rejected examples
remain **unmeasured** until clustering can run. Fixture metrics are not national
results.

The pipeline requires a human attestation bound to the sample Parquet SHA-256
and all 30 pair IDs listed in `review_request.json`:

```json
{
  "sample_sha256": "<copy from review_request.json>",
  "pair_ids": ["<all 30 exact pair IDs>"],
  "reviewer": "<actual human reviewer>",
  "confirmed": true,
  "corrections": [
    {"pair_id": "<reviewed pair ID>", "label": 0, "reason": "<human explanation>"}
  ]
}
```

`corrections` is optional. Corrections apply to evaluation without rewriting the
original generated sample. The pipeline never synthesizes a real human review.
No attestation has been recorded for the national data.

## Validation and outstanding Gate C work

`python -m pytest`: **118 passed**. Resolution coverage includes:

- Required Robert Chen three-record merge and separate fourth identity.
- Byte-identical Parquet from independent runs with reversed input ordering.
- Both exact-NPI and fuzzy 51-record alarms, including review dumps.
- Person/organization conflicts and unknown-type transitive bridges.
- Split pair limits, candidate deduplication and independent exact NPI unions.
- Golden-name preference, suppressed identities and address-history retention.
- SQL/reference scoring agreement, blank/null features and address safeguards.
- Clustering-based precision/recall, false merges/misses and explicit human review.

National preflight evidence is reproducible; see the committed manifest.
National candidate generation, score audit, accepted entity determinism,
precision >= .98, recall >= .85, and the human review are still pending.

The proposed next work is to repair PECOS type inference with provenance,
review the 10 remaining conflicts, then review the 16 large exact-NPI groups.
Any authorized exception should identify the exact reviewed members and forbid
additional fuzzy merges into that component. The default alarm must remain for
unreviewed groups. Gate C must be rerun before edge construction.
