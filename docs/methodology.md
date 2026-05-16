# Vigil Medicare Fraud Detection System — Technical Methodology

**Version:** 2.1.0
**Last Updated:** 2026-05-08
**For:** Expert witness declaration, Daubert qualification, program-integrity documentation
**Classification:** For Official Use Only (FOUO)

---

## 1. Purpose and Scope

The Vigil system analyzes CMS Medicare Part B billing data to identify providers
with statistically anomalous billing patterns consistent with known Medicare
fraud schemes. It is designed as a **lead-generation and investigative-triage
tool**, not as a final determination of fraud. Every lead produced by the system
requires independent verification against underlying claim records before any
enforcement action.

This document supersedes v1.1.0, which used a circular validation methodology
that has since been corrected (see § 11 — Methodology Change Log).

---

## 2. Data Sources

| Source | Description | Vintage |
|---|---|---|
| CMS Medicare Physician & Other Practitioners — by Provider | Public-use file aggregating all Part B claims per NPI per year. Contains total services, beneficiaries, and Medicare payments per HCPCS code per provider. **Five years (2018-2022)** are used for training; **2022** is the production scoring year. | 2018, 2019, 2020, 2021, 2022 |
| CMS Medicare Physician & Other Practitioners — by Provider and HCPCS | Service-line aggregates per provider per HCPCS code, used to compute billing-entropy and E&M-upcoding ratio for the 2022 scoring year. | 2022 |
| OIG List of Excluded Individuals/Entities (LEIE) | Providers excluded from federal healthcare program participation under 42 U.S.C. § 1320a-7. Any billing by an excluded provider on or after the exclusion date is a per-claim violation of the False Claims Act (31 U.S.C. § 3729). | Refreshed at pipeline run (typically weekly) |

All sources are government-published and admissible under FRE 902(5) as
self-authenticating public records. SHA-256 hashes of the downloaded files are
stored at ingest time for chain-of-custody.

---

## 3. Temporal Holdout Validation Methodology

### 3.1 The problem with prior validation (v1.x)

Earlier versions of Vigil used **all current LEIE exclusions** as both training
labels and validation labels. The system also included `is_excluded` as a model
input feature. The reported ROC-AUC of 1.0 was therefore a measurement of the
model's ability to reproduce its own training labels — not a measurement of
predictive power. A defense expert would correctly characterise this as
**circular reasoning**: the model had memorised the answer key.

This was identified during the v2.0 redesign and corrected before any external
deployment.

### 3.2 The temporal holdout split

The CMS billing data covers calendar year 2022. The LEIE is split at
**January 1, 2023**:

- **Training labels (hard positives):** Providers excluded *before* 2023, who
  also appear in CMS billing data from 2018-2022. Their billing reflects known
  fraud activity captured at or before the exclusion date.
- **Holdout labels (validation only):** Providers excluded *on or after*
  January 1, 2023, who appeared in 2022 CMS data. These providers were billing
  during the scoring year and were caught by OIG **afterward**. The model has
  never seen their exclusion status during training.

Asking *"does the model flag these holdout providers using their 2022 billing
data alone?"* is a genuine out-of-sample test of predictive power. It is the
only validation methodology that survives Daubert challenge for a fraud-
detection system trained on government exclusion data.

### 3.3 Removal of the `is_excluded` feature

`is_excluded` was removed from the model input features in v2.0.0. The model
now learns billing-anomaly patterns that **correlate with eventual exclusion**,
not "is this provider already on a government list."

---

## 4. Feature Engineering

Features are computed per provider, per data year, with peer comparisons
performed within that year's peer groups (specialty × state, minimum 10
providers; falls back to specialty-only when group is too small).

| Feature | Description | Fraud relevance |
|---|---|---|
| `payment_vs_peer` | Total payment ÷ peer-median total payment | Volume anomaly |
| `services_vs_peer` | Total services ÷ peer-median services | Service-volume anomaly |
| `benes_vs_peer` | Beneficiaries ÷ peer-median beneficiaries | Patient-volume anomaly |
| `ppb_vs_peer` | Payment-per-beneficiary ÷ peer-median payment-per-bene | **Per-patient cost anomaly (size-invariant; strongest fraud signal)** |
| `payment_per_service_vs_peer` | Payment-per-claim ÷ peer-median payment-per-claim | Per-claim upcoding signal |
| `payment_zscore` | Z-score of total payment within peer group | Statistical-outlier detection |
| `services_per_bene` | Services ÷ beneficiaries | Intensity ratio |
| `payment_per_bene_norm` | Log(1 + payment/bene) | Log-scaled per-patient cost |
| `total_payment_log` | Log(1 + total payment) | Log-scaled total volume |
| `total_services_log` | Log(1 + total services) | Log-scaled service count |
| `num_procedure_types_norm` | Log(1 + count of distinct HCPCS) | Billing complexity |
| `billing_entropy` | Shannon entropy of HCPCS distribution | Low entropy = concentration on few high-value codes (upcoding) |
| `em_upcoding_ratio` | Fraction of E&M claims at highest complexity level | E&M upcoding |
| `hotspot_state` | 1 if state ∈ {FL, TX, CA, NY, LA, MI, NJ, IL, GA, MD} | Geographic risk per OIG enforcement data |
| `yoy_payment_change` | Provider's 2021→2022 payment change minus peer-median YoY trend | Sudden billing surge |
| `is_opt_out` | Binary: opted-out of Medicare | Enrollment status |
| `months_enrolled` | Months in current enrollment period | New-provider velocity |

**Excluded from model inputs:**
`is_excluded` is computed and stored for display and post-scoring filtering,
but is **never** passed to any model. Including it would reintroduce the v1.x
circular reasoning problem.

---

## 5. Model Architecture

The composite risk score is produced by an **ensemble of three independent
models**:

### 5.1 XGBoost gradient-boosted classifier (50% weight)

Semi-supervised training with two positive signal sources:

- **Hard positives (weight 5.0):** providers in the LEIE training set
  (excluded before 2023) — confirmed fraud or fraud-adjacent misconduct.
  Across five years of CMS data this yields ~860 provider-year examples.
- **Soft positives (weight 0.2, fraud-specific):** providers showing patterns
  specifically associated with billing fraud:
  - per-patient cost ≥ 5× peer median (`ppb_vs_peer`), OR
  - E&M-upcoding ratio ≥ 0.7, OR
  - billing-entropy ≤ 0.4 AND `payment_vs_peer` ≥ 5×

Holdout providers (excluded 2023+) are dropped from the training set entirely
to prevent contamination.

Output is a calibrated fraud probability in [0, 1]. Raw probabilities are
preserved — percentile ranking is not applied.

### 5.2 Isolation Forest (30% weight)

Unsupervised. n_estimators=200, contamination=0.02, max_features=0.8. Output
converted to a percentile rank.

### 5.3 Autoencoder — MLP reconstruction error (20% weight)

Multi-layer perceptron trained to reconstruct **normal providers only** (bottom
90th percentile of `payment_zscore`). Architecture:
64 → 32 → 16 → 32 → 64 (symmetric encoder-decoder), ReLU, Adam, early stopping.
Output normalised by maximum reconstruction error.

### 5.4 Composite

```
composite = 0.50 × xgboost_prob + 0.30 × iso_percentile + 0.20 × ae_normalised_error
risk_score = composite × 100        (range 0-100)
```

### 5.5 Volume-specialty adjustment

For volume-intensive specialties (clinical lab, DME, ambulance, pharmacy, home
health, SNF), total payment volume is structurally inflated by patient volume.
For these specialties, `ppb_vs_peer` (per-patient cost) is the meaningful
fraud signal. Providers with normal per-patient costs receive a discount:

| ppb_vs_peer | Discount |
|---|---|
| < 2× peer median | 85 % |
| 2–5× peer median | 55 % |
| 5–15× peer median | 25 % |
| ≥ 15× peer median | None |

LEIE-excluded providers are never discounted.

---

## 6. Validation Results (v2.1.0)

### 6.1 Headline numbers

| Metric | Value | Notes |
|---|---|---|
| Dataset size (production scoring year) | 1,230,275 providers | CMS 2022 |
| Training rows (5-year stack) | 5,765,875 provider-years | 2018-2022, holdout providers removed |
| Hard positives (training) | 861 | LEIE excluded before 2023 |
| Holdout providers | 325 | LEIE excluded 2023+, billed in 2022 |
| ROC-AUC | **0.58** | Above-random discrimination |
| Average precision | 0.0005 | Reflects extreme class imbalance (325 / 1.23M) |
| Holdout mean score | 21.3 | vs population mean 16.4 — 1.3× separation |

### 6.2 Recall by threshold

| Threshold | Flagged | Holdout caught | Overall recall | Precision |
|---|---|---|---|---|
| 70 | 42,503 | 37 | **11.4 %** | 0.09 % |
| 75 | 34,735 | 35 | 10.8 % | 0.10 % |
| 80 | 41 | 0 | 0 % | 0 % |

### 6.3 Billing-fraud-specific recall — the metric that matters

Of the 325 holdout providers, only **138** were excluded for billing-related
conduct (LEIE codes 1128a1, 1128a3, 1128b7, 1128b8, 1128b9, 1156). The
remaining 187 were excluded for **non-billing conduct** — drug offences,
patient abuse, license revocations, criminal convictions unrelated to billing.
Those 187 providers' billing data does not contain the predictive signal.

| Metric | Value |
|---|---|
| Billing-fraud holdout providers | 138 / 325 (42 %) |
| Non-billing exclusions | 187 / 325 (58 %) |
| **Billing-fraud recall @ 70** | **21 / 138 = 15.2 %** |

This is the figure to use in expert testimony. Overall holdout recall
understates model performance because it includes 187 providers whose
exclusions are inherently undetectable from billing data.

### 6.4 Investigation triage tiers

| Tier | Score range | Provider count |
|---|---|---|
| High risk | ≥ 70 | 42,464 |
| Moderate risk | 50–69 | 2,340 |
| Total actionable (≥ 50) | | 44,804 |

Investigators triage high-risk providers first, then moderate-risk during
capacity overflow. Each provider's record surfaces SHAP feature attributions
identifying which billing patterns drove the score.

### 6.5 Honest interpretation

A **ROC-AUC of 0.58 with billing-fraud recall of 15 %** is a defensible result
for annual aggregate billing data. It is materially better than chance,
materially worse than what claim-level data could achieve. The system finds
roughly 1-in-7 billing fraudsters using only public data — useful as an
investigative-triage tool, **not** as a basis for direct enforcement action.

A ROC-AUC of 1.0 (as v1.x reported) would be an indication of data leakage,
not predictive power.

---

## 7. Feature Attribution (SHAP)

For the top 10,000 providers by risk score, SHAP values are computed using
the XGBoost TreeExplainer and stored in the providers table for UI display.
For each provider, the three features with the largest absolute SHAP
contributions are surfaced in the investigative brief.

Global feature importance among top scorers (mean |SHAP|, illustrative):
`payment_vs_peer`, `payment_zscore`, `ppb_vs_peer`, `num_procedure_types_norm`,
`benes_vs_peer`. Exact values depend on the latest training run and are
stored in `data/processed/validation_report.json`.

---

## 8. Limitations and Required Verification

1. **Annual aggregate data.** CMS Part B PUF is one row per NPI per year.
   The system cannot identify individual claim numbers, service dates, or
   beneficiaries. Many fraud schemes are visible only at the claim level
   (impossible billing days, services on dates of beneficiary death,
   geographic impossibilities). These cannot be detected from PUF data alone.

2. **15 % billing-fraud recall ceiling.** The realistic ceiling for a model
   trained on annual aggregate data is approximately 15-25 % recall on
   billing-fraud cases. Higher recall requires claim-level data, available
   via the CMS Research Data Assistance Center (ResDAC) Data Use Agreement.

3. **LEIE is an imperfect label.** LEIE captures providers OIG has already
   sanctioned. The majority of Medicare fraud is undetected at any moment.
   Validation against LEIE understates true positive rates.

4. **2022 billing data.** The scoring dataset reflects billing in calendar
   year 2022. Providers' billing patterns may have changed since then.

5. **Statistical anomaly ≠ fraud.** A high risk score indicates a statistically
   unusual billing pattern. Outliers may reflect legitimate specialised
   practice, rural sole-practitioner status, or high-acuity patient
   populations. Independent clinical and investigative review is required
   before any enforcement action.

6. **No individual patient data.** The system cannot identify specific
   beneficiaries. Determination of medical necessity requires record-by-record
   review of underlying claims.

---

## 9. Recommended path forward

| Tier | Data source | Estimated billing-fraud recall ceiling |
|---|---|---|
| **0 — current** | CMS PUF + LEIE (public, free) | ~15-25 % |
| **1 — add NPPES + multi-year + feedback loop** | All public; build investigator-feedback dataset over time | ~25-35 % |
| **2 — CMS Research DUA** | Standard Analytical Files (claim-level) via ResDAC | ~50-70 % |
| **3 — payer partnership** | Real-time claims feed (HIPAA BAA) | 80 %+ |

Tier 2 is the largest single step. A ResDAC DUA application typically takes
6 months and requires either a researcher PI or institutional sponsor.
Sample data fees: $25k-$100k/year. A FISMA-Moderate compliance environment
is required.

---

## 10. Data chain of custody

- All source files downloaded from `data.cms.gov` and `oig.hhs.gov`. SHA-256
  hashes recorded at ingest.
- Pipeline runs are deterministic given fixed inputs and `random_state=42`.
- All processing performed on isolated infrastructure. No external API calls
  during scoring or training.
- Scored results stored in PostgreSQL with row-level timestamps. Score history
  is append-only.

---

## 11. Methodology change log

### v2.1.0 (2026-05-08)

- Five-year training stack (2018-2022) replaces single-year training. Training
  positive count increased from 10 to 861.
- Holdout providers (LEIE excluded 2023+) explicitly dropped from training data
  to prevent contamination from labelling them as negatives in historical years.
- Soft-positive criteria reworked: removed pure volume thresholds, replaced with
  fraud-specific signals (`ppb_vs_peer ≥ 5`, `em_upcoding_ratio ≥ 0.7`, low
  entropy + high volume). Hard-positive weight raised from 2.0 to 5.0; soft-
  positive weight reduced from 0.3 to 0.2.
- Added features: `hotspot_state`, `yoy_payment_change`,
  `payment_per_service_vs_peer`.
- Validation report adds **billing-fraud-specific recall** distinct from
  overall LEIE recall. Lower threshold range (30-95) added to support the
  moderate-risk tier (50-69).
- Billing-fraud recall @70 improved from 3.6 % to 15.2 %.

### v2.0.0 (2026-04-?)

- **Removed `is_excluded` from model input features.** This eliminated the
  data leakage that produced v1.x's spurious ROC-AUC of 1.0.
- **Removed the LEIE score floor.** Artificially flooring confirmed-excluded
  providers crowded out new investigation leads from the scored output.
- **Introduced temporal holdout validation.** LEIE split at 2023-01-01: pre-
  2023 exclusions are training labels, post-2023 exclusions are validation
  holdout.

### v1.1.0 and earlier — DEPRECATED

- Used `is_excluded` as both a training feature and a validation label.
- Reported ROC-AUC of 1.0, which reflected memorisation of the LEIE list,
  not predictive power.
- Applied a 85.0 score floor to all LEIE providers.
- These versions should not be cited in expert testimony.

---

## 12. Citation

> "Vigil Medicare Fraud Detection System, Version 2.1.0, trained on five
> years of CMS Medicare Part B Public Use File data (2018-2022; n=1,230,275
> providers in the production scoring year) and the OIG List of Excluded
> Individuals/Entities. Composite risk scores are produced by an ensemble
> of three anomaly-detection models (XGBoost, Isolation Forest, Autoencoder).
> Validation uses a temporal holdout: the model is trained on LEIE exclusions
> dated before January 1, 2023, and evaluated against providers excluded on
> or after that date who appeared in 2022 CMS billing data. The model
> achieves a ROC-AUC of 0.58 and 15.2 % recall on billing-fraud-specific
> holdout providers at the 70-point investigation threshold. All findings
> require verification against underlying CMS claim records before any
> enforcement action."
