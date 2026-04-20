"""
analysis.py — Deterministic investigative brief generator.

NOTE on facility-type providers (labs, imaging, DME):
The physician billing anomaly model produces false positives for these specialties
because high services-per-patient ratios and high shared-patient counts are
structurally normal for them. This module detects facility types and adjusts
both the scheme classification and the narrative accordingly.

Synthesizes a provider's fraud flags, billing records, and referral network
into a structured brief formatted for Special Agents and program integrity analysts.

No LLM required — all language is generated from real data using rule-based logic.
Called by GET /api/providers/{npi}/analysis.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


# ── Facility-type specialty detection ─────────────────────────────────────────
# These provider types have naturally high services-per-patient ratios and
# dense shared-patient networks. Flagging them with physician fraud signals
# produces false positives — they need specialty-specific fraud logic instead.

_FACILITY_SPECIALTIES: frozenset[str] = frozenset({
    "Clinical Laboratory", "Independent Clinical Laboratory",
    "Clinical Laboratory - Independent", "Pathology",
    "Anatomical & Clinical Pathology", "Clinical Pathology",
    "Anatomic Pathology",
    "Diagnostic Radiology", "Diagnostic Imaging",
    "Independent Diagnostic Testing Facility",
    "Portable X-ray Supplier", "Nuclear Medicine",
    "Mammography",
    "Durable Medical Equipment & Medical Supplies",
    "Pharmacy", "Specialty Pharmacy",
    "Mass Immunizer Roster Biller",
    "Ambulance Service Provider",
    "Ambulatory Surgical Center",
    "Home Health Agency", "Hospice",
    "Skilled Nursing Facility",
    "Slide Preparation Facility",
})

_LAB_SPECIALTIES: frozenset[str] = frozenset({
    "Clinical Laboratory", "Independent Clinical Laboratory",
    "Clinical Laboratory - Independent", "Pathology",
    "Anatomical & Clinical Pathology", "Clinical Pathology",
    "Anatomic Pathology", "Slide Preparation Facility",
})

_IMAGING_SPECIALTIES: frozenset[str] = frozenset({
    "Diagnostic Radiology", "Diagnostic Imaging",
    "Independent Diagnostic Testing Facility",
    "Portable X-ray Supplier", "Nuclear Medicine", "Mammography",
})


def _is_facility(specialty: str | None) -> bool:
    return (specialty or "") in _FACILITY_SPECIALTIES

def _is_lab(specialty: str | None) -> bool:
    return (specialty or "") in _LAB_SPECIALTIES

def _is_imaging(specialty: str | None) -> bool:
    return (specialty or "") in _IMAGING_SPECIALTIES


def _facility_scheme_and_narrative(specialty: str) -> tuple[str, str]:
    """Return a specialty-appropriate scheme name + narrative for facility providers."""
    if _is_lab(specialty):
        return (
            "Laboratory Billing Review — High-Volume Facility",
            f"This provider is a clinical laboratory. High services-per-patient ratios "
            f"and large shared-patient networks are structurally expected for labs — a "
            f"single patient visit routinely generates 10–20 individual HCPCS billing "
            f"codes (CBC, metabolic panel, lipids, urinalysis, etc.), and labs share "
            f"patients with virtually every referring physician in their region. "
            f"The standard physician billing anomaly model does not apply here. "
            f"Lab-specific fraud patterns to investigate include: billing for tests not "
            f"ordered by a physician, duplicate claims across multiple NPIs, test kit "
            f"fraud (billing for tests performed on samples that were never collected "
            f"from an actual patient), and specimen identity fraud. "
            f"Review should focus on ordering physician relationships and whether "
            f"volume is consistent with the lab's documented patient population — "
            f"not on services-per-patient ratios, which will always be high."
        )
    elif _is_imaging(specialty):
        return (
            "Imaging/Diagnostic Facility Review — High-Volume Facility",
            f"This provider is a diagnostic imaging or testing facility. Multiple "
            f"imaging studies per patient per encounter, combined with referrals from "
            f"a large panel of ordering physicians, naturally produce high shared-patient "
            f"counts and elevated total payments. "
            f"Imaging-specific fraud patterns include: billing for reads on images not "
            f"taken, unbundling global procedure codes, upcoding imaging complexity, "
            f"self-referral arrangements in violation of the Stark Law (42 U.S.C. § 1395nn), "
            f"and payment arrangements with ordering physicians. "
            f"Review should focus on ordering physician concentration and whether "
            f"any single referral source accounts for a disproportionate share of volume."
        )
    else:
        return (
            "Facility-Type Provider Review",
            f"This provider is a facility-type entity (DME, pharmacy, ambulance, or similar). "
            f"Standard physician billing anomaly metrics (services-per-patient ratios, "
            f"peer payment comparisons) are not clinically meaningful for this provider type. "
            f"Relevant fraud patterns depend on the specific provider category."
        )


# ── Scheme classification ──────────────────────────────────────────────────────

_SCHEME_RULES: list[tuple[set[str], str, str]] = [
    # (required_flag_types, scheme_name, base_narrative) — checked in order, first match wins
    (
        {"leie_match", "billing_volume"},
        "Post-Exclusion Billing — 42 U.S.C. § 1320a-7",
        "The provider is on the OIG List of Excluded Individuals/Entities (LEIE) and is "
        "prohibited from receiving any Medicare reimbursement. Every claim submitted after "
        "the exclusion date is an individual False Claims Act violation (31 U.S.C. § 3729), "
        "subject to civil penalties of up to $27,894 per claim plus treble damages. This "
        "category of case is typically referred directly to the U.S. Attorney's Office given "
        "the clear statutory violation.",
    ),
    (
        {"deceased_patient"},
        "Phantom Billing — Deceased Beneficiary Identity Fraud",
        "Claims were submitted for services allegedly rendered to Medicare beneficiaries "
        "after their recorded date of death. This is a definitive indicator of either phantom "
        "billing (fabricating services for real patients) or beneficiary identity theft — "
        "two of the most prosecuted Medicare fraud categories under 18 U.S.C. § 1347.",
    ),
    (
        {"impossible_hours"},
        "Phantom Services — Impossible Billing Hours",
        "The provider's total claimed service time mathematically exceeds 24 hours in a single "
        "day across the billing year. This is physically impossible and establishes, as a matter "
        "of arithmetic, that a portion of billed services were never rendered. This evidence "
        "pattern is directly actionable under the False Claims Act without further clinical review.",
    ),
    (
        {"hub_spoke", "referral_cluster", "billing_volume"},
        "Coordinated Drug Billing Fraud + Kickback Ring",
        "The provider sits at the center of a dense, suspicious referral network while "
        "simultaneously posting billing volumes that are extreme statistical outliers. "
        "This combination — anomalous billing concentrated at a network hub — is the "
        "signature pattern of organized Medicare fraud rings in which referring providers "
        "funnel patients to the hub in exchange for kickbacks or revenue sharing under "
        "42 U.S.C. § 1320a-7b(b) (Anti-Kickback Statute).",
    ),
    (
        {"hub_spoke", "referral_cluster"},
        "Hub-and-Spoke Kickback Network",
        "The provider functions as a high-traffic hub in a suspicious referral topology. "
        "Multiple providers are routing disproportionate patient volume through this practice "
        "in a pattern inconsistent with normal clinical referral behavior. Hub-and-spoke "
        "structures are the hallmark of Anti-Kickback Statute violations, where referring "
        "providers receive direct or indirect remuneration for patient referrals.",
    ),
    (
        {"upcoding", "billing_volume"},
        "Systematic E&M Upcoding + Volume Inflation",
        "Two independent signals converge: the provider is billing the highest-complexity "
        "E&M codes (99205/99215) at rates far exceeding specialty peers, and total billing "
        "volume is a significant statistical outlier. Routine upcoding of lower-complexity "
        "visits inflates per-claim reimbursement, while volume inflation multiplies the "
        "aggregate overpayment. Together they suggest a systematic rather than incidental "
        "billing pattern.",
    ),
    (
        {"upcoding"},
        "E&M Level-5 Upcoding",
        "The provider's rate of billing the highest-complexity evaluation and management "
        "codes (Level-5: 99205 for new patients, 99215 for established) significantly "
        "exceeds specialty norms. These codes require comprehensive history, examination, "
        "and high medical decision-making complexity under AMA guidelines. Sustained "
        "overuse is the defining characteristic of E&M upcoding schemes.",
    ),
    (
        {"billing_volume"},
        "Extreme Billing Volume Outlier",
        "Total Medicare payments place this provider in the extreme tail of the specialty "
        "and geographic peer distribution. High billing volume alone does not establish "
        "fraud — legitimate high-volume specialists exist — but the magnitude warrants "
        "claims-level review to confirm that documented medical necessity supports the "
        "volume and that services were actually rendered.",
    ),
]


def _classify_scheme(
    flag_types: set[str],
    billing_records: list,
    provider: Any,
) -> tuple[str, str]:
    """Return (scheme_name, base_narrative) based on flag patterns and billing data."""

    # LEIE exclusion — check provider attribute directly (most authoritative) and
    # both flag type variants ("leie_exclusion" from our updated pipeline,
    # "leie_match" from older pipeline runs).
    if getattr(provider, "is_excluded", False) or bool(
        flag_types & {"leie_exclusion", "leie_match"}
    ):
        return (
            "Post-Exclusion Billing — 42 U.S.C. § 1320a-7",
            "The provider is on the OIG List of Excluded Individuals/Entities (LEIE) and is "
            "prohibited from receiving any Medicare reimbursement. Every claim submitted after "
            "the exclusion date is an individual False Claims Act violation (31 U.S.C. § 3729), "
            "subject to civil penalties of up to $27,894 per claim plus treble damages. This "
            "category of case is typically referred directly to the U.S. Attorney's Office given "
            "the clear statutory violation.",
        )

    # Drug injection pattern check: 3+ J-codes + high payment + network flags
    j_code_count = sum(
        1 for r in billing_records if (r.hcpcs_code or "").startswith("J")
    )
    total_payment = float(provider.total_payment or 0)
    has_network_flags = bool(
        flag_types & {"hub_spoke", "referral_cluster"}
    )

    if j_code_count >= 3 and total_payment > 300_000:
        if has_network_flags:
            return (
                "Oncology/Infusion Drug Billing Fraud + Referral Kickback Ring",
                f"The provider has billed Medicare for {j_code_count} distinct injectable "
                f"drug procedure codes — a hallmark of the buy-and-bill infusion model in "
                f"which the provider purchases drugs, administers them in-office, and bills "
                f"Medicare for both the drug and administration fees. The combination of "
                f"extreme drug billing volume and a suspicious referral network strongly "
                f"suggests that patient volume is being manufactured or inflated to support "
                f"phantom or unit-inflated drug claims, with referring providers potentially "
                f"compensated in violation of the Anti-Kickback Statute (42 U.S.C. § 1320a-7b).",
            )
        else:
            return (
                "Buy-and-Bill Drug Inflation Scheme",
                f"The provider has billed Medicare for {j_code_count} distinct injectable "
                f"drug procedure codes (J-codes), with billing volumes inconsistent with "
                f"the number of beneficiaries served. In the buy-and-bill model, providers "
                f"purchase drugs at contracted or market prices and bill Medicare at the "
                f"allowed reimbursement rate. Fraud in this model typically takes the form "
                f"of unit inflation (billing more mg than administered), billing for drugs "
                f"not purchased through approved channels, or outright phantom billing for "
                f"infusions never given.",
            )

    # Rule-based matching
    for required, name, narrative in _SCHEME_RULES:
        if required & flag_types:  # any overlap
            return name, narrative

    # Fallback
    return (
        "Multi-Signal Anomalous Billing Pattern",
        "The provider's billing profile deviates significantly from specialty and geographic "
        "peers across multiple independent dimensions. No single dominant fraud scheme "
        "pattern was matched, but the combination of signals warrants investigative review "
        "to determine whether the anomalies reflect billing errors, capacity misrepresentation, "
        "or intentional fraud.",
    )


# ── Billing anomaly annotation ─────────────────────────────────────────────────

def _annotate_billing_record(record: Any, specialty: str) -> str | None:
    """Return an investigator-facing anomaly sentence for a billing record, or None."""
    if not record.total_services or not record.total_beneficiaries:
        return None

    benes = int(record.total_beneficiaries)
    services = int(record.total_services)
    ratio = services / max(benes, 1)
    code = record.hcpcs_code or ""
    desc = (record.hcpcs_description or code)[:55]
    payment = float(record.total_medicare_payment or 0)
    avg = float(record.avg_medicare_payment or 0)

    if code.startswith("J"):
        if ratio > 200:
            return (
                f"{code} ({desc}): {services:,} service units billed for only {benes:,} "
                f"patients — {ratio:,.0f} units per patient. For this drug class, such a ratio "
                f"is clinically implausible and is a primary indicator of unit inflation or "
                f"phantom billing. Total paid: ${payment:,.0f} at ${avg:.2f}/unit."
            )
        elif ratio > 50:
            return (
                f"{code} ({desc}): {ratio:.0f} service lines per patient across {benes:,} "
                f"beneficiaries. Volume is inconsistent with typical administration frequency "
                f"for this drug. Total Medicare paid: ${payment:,.0f}."
            )
        else:
            return (
                f"{code} ({desc}): ${payment:,.0f} total paid across {benes:,} patients "
                f"({services:,} service lines at ${avg:.2f} avg)."
            )

    elif code in ("99205", "99215", "99345", "99350"):
        return (
            f"{code} (Level-5 E&M — highest complexity): billed for {benes:,} patients "
            f"at ${avg:.2f}/visit, ${payment:,.0f} total. High-complexity codes require "
            f"comprehensive documentation under AMA E&M guidelines."
        )

    elif ratio > 100:
        return (
            f"{code} ({desc}): {ratio:.0f} services per patient — significantly above "
            f"expected frequency for this procedure. ${payment:,.0f} total paid."
        )

    # Default: just report top-line numbers for the top codes
    return (
        f"{code} ({desc}): ${payment:,.0f} total paid, "
        f"{services:,} services across {benes:,} patients."
    )


# ── Network suspect annotation ─────────────────────────────────────────────────

def _classify_relationship(
    center_npi: str,
    edge: Any,
    suspect: Any,
    all_edges: list,
) -> tuple[str, str, str]:
    """
    Returns (direction_label, risk_reason, recommended_action) for a network suspect.
    """
    suspect_npi = suspect.npi
    shared = int(edge.shared_patients or 0)
    suspect_name = f"{suspect.name_first or ''} {suspect.name_last or ''}".strip() or suspect_npi
    suspect_score = float(suspect.risk_score or 0)

    # Check bidirectionality (mutual referral = stronger kickback signal)
    reverse = any(
        (e.source_npi == suspect_npi and e.target_npi == center_npi) or
        (e.source_npi == center_npi and e.target_npi == suspect_npi)
        for e in all_edges
        if e.id != edge.id
    )

    if edge.source_npi == center_npi:
        direction = f"Subject → {suspect_name}"
    elif edge.target_npi == center_npi:
        direction = f"{suspect_name} → Subject"
    else:
        direction = f"Subject ↔ {suspect_name}"
    if reverse:
        direction = f"Subject ↔ {suspect_name} (bidirectional — mutual referral)"

    # Risk reason
    reason_parts = []
    if shared > 150:
        reason_parts.append(f"{shared:,} shared Medicare patients — extremely high volume")
    elif shared > 50:
        reason_parts.append(f"{shared:,} shared Medicare patients")
    elif shared > 0:
        reason_parts.append(f"{shared} shared patients")
    if suspect.is_excluded:
        reason_parts.append("provider is LEIE-excluded")
    if suspect_score >= 85:
        reason_parts.append(f"independent risk score {suspect_score:.0f}/100")
    elif suspect_score >= 70:
        reason_parts.append(f"risk score {suspect_score:.0f}")
    reason = " · ".join(reason_parts) if reason_parts else "Flagged by network detection model"

    # Action
    if suspect.is_excluded:
        action = (
            f"Subpoena referral and payment records. Any Medicare claims submitted "
            f"through or facilitated by an excluded provider expose all parties to "
            f"False Claims Act liability. Refer to OIG immediately."
        )
    elif reverse and shared > 50:
        action = (
            f"Pull shared patient roster for both practices. Cross-reference appointment "
            f"records, referral logs, and any financial arrangements. Bidirectional "
            f"high-volume patient sharing is a primary Anti-Kickback Statute indicator."
        )
    elif suspect_score >= 80:
        action = (
            f"Review as potential co-conspirator. High independent risk score "
            f"({suspect_score:.0f}) supports coordinated scheme hypothesis. "
            f"Cross-reference claims timelines and shared patient records."
        )
    else:
        action = (
            f"Request referral documentation for {shared:,} shared patient encounters. "
            f"Verify medical necessity and confirm no compensation arrangement exists."
        )

    return direction, reason, action


# ── Recommended action builder ────────────────────────────────────────────────

def _build_actions(
    provider: Any,
    flags: list,
    billing_anomalies: list,
    network_suspects: list,
    priority: int,
) -> list[dict]:
    actions = []
    flag_types = {f.flag_type for f in flags}
    step = 0

    def add(category: str, action: str, detail: str):
        nonlocal step
        step += 1
        actions.append({"step": step, "category": category, "action": action, "detail": detail})

    # Immediate containment
    if priority <= 2:
        add(
            "Immediate",
            "Initiate prepayment review with MAC",
            "Contact the Medicare Administrative Contractor to flag this NPI for "
            "prepayment medical review. Suspends automatic claim processing and "
            "forces manual documentation review before any further payments are released.",
        )

    # LEIE
    if provider.is_excluded or "leie_match" in flag_types:
        add(
            "Legal — FCA Referral",
            "Refer to OIG for False Claims Act civil penalty assessment",
            f"Provider is on the LEIE. Identify all Medicare claims submitted after the "
            f"exclusion date ({provider.leie_date or 'on file'}). Each claim is a "
            f"standalone FCA violation: civil penalty up to $27,894/claim + treble damages. "
            f"Compile a complete claims manifest for the U.S. Attorney's Office.",
        )

    # Deceased billing
    if "deceased_patient" in flag_types:
        add(
            "Claims Audit",
            "Cross-reference billed beneficiaries against SSA Death Master File",
            "Pull all Part B claims for this NPI and match beneficiary IDs against "
            "SSA DMF. For each post-death claim, obtain the beneficiary's death certificate, "
            "the claim submission date, and the service date. This evidence packet supports "
            "both civil FCA claims and potential criminal referral under 18 U.S.C. § 1347.",
        )

    # Top billing anomaly
    if billing_anomalies:
        top = billing_anomalies[0]
        add(
            "Claims Audit",
            f"Pull 100% of claims for HCPCS {top['hcpcs']} and request medical records",
            f"This code accounts for the highest-value billing anomaly. Request HCPCS-level "
            f"claim detail from the MAC, then draw a statistically valid sample (minimum 50 "
            f"records). For each sampled claim: (1) verify the service date, (2) confirm "
            f"the drug/service was actually provided, (3) verify units administered match "
            f"units billed, and (4) confirm medical necessity documentation supports the claim.",
        )

    # Network investigation
    if network_suspects:
        top = network_suspects[0]
        add(
            "Network Investigation",
            f"Subpoena referral records — {top['name']} ({top['specialty'] or 'Unknown'})",
            f"{top['shared_patients']:,} shared Medicare patients between the subject and "
            f"{top['name']} in {top['state'] or 'unknown state'}. Request: all appointment "
            f"records for shared patients, referral authorization logs, any written or verbal "
            f"compensation agreements, and financial records showing payments between practices. "
            f"{'Bidirectional pattern is a primary kickback indicator.' if '↔' in top['direction'] else ''}",
        )

    if len(network_suspects) > 1:
        others = ", ".join(s["name"] for s in network_suspects[1:4])
        add(
            "Network Investigation",
            "Expand investigation to full referral cluster",
            f"Run 2-hop network analysis from subject NPI. Additional high-priority targets: "
            f"{others}. Look for geographic clustering (shared office address), common billing "
            f"agents, or shared ownership structures that would indicate an organized operation.",
        )

    # Hub-spoke
    if "hub_spoke" in flag_types:
        add(
            "Network Investigation",
            "Map hub-and-spoke topology and identify ring leadership",
            "Request CMS Shared Patient Patterns data for all providers in the cluster. "
            "Identify the provider(s) with the highest in-degree centrality — these are "
            "the likely organizers of the kickback arrangement. Cross-reference with "
            "CMS enrollment records for common ownership or management.",
        )

    # Impossible hours
    if "impossible_hours" in flag_types:
        add(
            "Claims Audit",
            "Reconstruct daily claim timeline to document impossible hours",
            "Pull all claims sorted by service date. For each date where claimed time "
            "exceeds 24 hours, calculate the mathematical shortfall. This data point is "
            "direct, arithmetic evidence of phantom billing — no clinical judgment required "
            "and highly effective for summary judgment motions.",
        )

    # Documentation close
    add(
        "Case Management",
        "Open formal investigation case and set enforcement deadlines",
        "Create a Vigil case record to centralize all findings, attach this investigative "
        "brief, and assign to a Special Agent. Recommended timeline: 30-day deadline for "
        "initial claims audit completion; 90-day deadline for field investigation decision "
        "(pursue civil referral, criminal referral, or close with administrative action).",
    )

    return actions


# ── Narrative builder ─────────────────────────────────────────────────────────

def _build_narrative(
    name: str,
    provider: Any,
    scheme_name: str,
    base_narrative: str,
    billing_anomalies: list,
    network_suspects: list,
    flags: list,
    total_exposure: float,
) -> str:
    score = float(provider.risk_score or 0)
    specialty = provider.specialty or "Unknown Specialty"
    state = provider.state or "Unknown State"
    total_payment = float(provider.total_payment or 0)
    flag_count = len(flags)
    pct_label = "top <1%" if score >= 95 else "top 1%" if score >= 90 else "top 5%"

    paras = []

    # ── Opening — identity + score context
    paras.append(
        f"{name} is a {specialty} provider practicing in {state} who received "
        f"${total_payment:,.0f} in Medicare reimbursements in the 2022 Part B dataset. "
        f"The Vigil risk scoring model assigns this provider a score of {score:.0f}/100, "
        f"placing them in the {pct_label} of all 1.2 million scored Medicare Part B providers. "
        f"A total of {flag_count} active fraud detection signal{'s were' if flag_count != 1 else ' was'} "
        f"identified across five independent detection layers. "
        f"The primary scheme classification is: **{scheme_name}**."
    )

    # ── Scheme narrative
    paras.append(base_narrative)

    # ── Billing anomalies
    if billing_anomalies:
        lines = ["**Billing record analysis identified the following specific anomalies:**"]
        for i, a in enumerate(billing_anomalies[:5], 1):
            lines.append(f"{i}. {a['anomaly_text']}")
        paras.append("\n".join(lines))

    # ── Network
    if network_suspects:
        net = [
            f"**Network analysis** identified {len(network_suspects)} high-priority "
            f"co-investigation targets from the referral graph:"
        ]
        for s in network_suspects[:4]:
            net.append(
                f"• **{s['name']}** ({s['specialty'] or 'Unknown specialty'}, {s['state'] or 'Unknown'}) "
                f"— {s['reason']}. Recommended action: {s['action']}"
            )
        if total_exposure > 0:
            net.append(
                f"\nThe combined estimated excess above peer benchmarks across all flagged "
                f"signals is **${total_exposure:,.0f}**. This figure represents the floor "
                f"of potential overpayment and does not account for network co-conspirators."
            )
        paras.append("\n".join(net))

    # ── Legal note
    paras.append(
        "**Evidentiary basis:** All findings derive from CMS Medicare Part B 2022 Public Use "
        "File, OIG LEIE records, and CMS Physician Shared Patient Patterns data — all "
        "government records admissible without authentication under FRE 902(5). This brief "
        "was generated by the Vigil automated fraud detection system and should be verified "
        "against underlying claims before any enforcement action. This document is designated "
        "for law enforcement and program integrity use only (FOUO)."
    )

    return "\n\n".join(paras)


# ── Main entry point ──────────────────────────────────────────────────────────

def generate_analysis(
    provider: Any,
    flags: list,
    billing_records: list,
    network_edges: list,
    neighbor_providers: list,
) -> dict[str, Any]:
    """
    Generate a complete investigative brief from ORM objects.
    Returns a JSON-serializable dict suitable for the /analysis API endpoint.
    """
    now = datetime.now(timezone.utc)
    score = float(provider.risk_score or 0)
    flag_types = {f.flag_type for f in flags}
    total_payment = float(provider.total_payment or 0)
    name = f"{provider.name_first or ''} {provider.name_last or ''}".strip() or provider.npi

    # ── Priority classification
    is_facility = _is_facility(provider.specialty)

    if provider.is_excluded or score >= 90:
        priority, priority_label = 1, "IMMEDIATE"
    elif score >= 75:
        priority, priority_label = 2, "HIGH"
    elif score >= 55:
        priority, priority_label = 3, "MEDIUM"
    else:
        priority, priority_label = 4, "ROUTINE"

    # Downgrade facility-type providers: their high scores are model artifacts,
    # not evidence of the physician fraud patterns the score was built to detect.
    if is_facility and not provider.is_excluded:
        priority = min(priority + 1, 4)
        priority_label = {1: "IMMEDIATE", 2: "HIGH", 3: "MEDIUM", 4: "ROUTINE"}[priority]

    # ── Scheme
    if is_facility:
        scheme_name, base_narrative = _facility_scheme_and_narrative(provider.specialty or "")
    else:
        scheme_name, base_narrative = _classify_scheme(flag_types, billing_records, provider)

    # ── Key findings
    key_findings: list[dict] = []

    if score > 0:
        pct = "top <1%" if score >= 95 else "top 1%" if score >= 90 else "top 5%"
        key_findings.append({
            "label": "Risk Score",
            "value": f"{score:.0f} / 100",
            "detail": f"Places provider in {pct} of all scored Medicare Part B providers nationally.",
        })

    zscore = float(provider.payment_zscore or 0)
    if zscore > 2:
        key_findings.append({
            "label": "Payment Z-Score",
            "value": f"{zscore:.1f}σ above peer mean",
            "detail": (
                f"Medicare payments are {zscore:.1f} standard deviations above the "
                f"specialty/state peer mean. Z > 3.0 is statistically exceptional (p < 0.001) "
                f"and a primary CMS data analysis review trigger."
            ),
        })

    vs_peer = float(provider.payment_vs_peer or 0)
    if vs_peer > 1.5:
        key_findings.append({
            "label": "Payment vs. Peer Median",
            "value": f"{vs_peer:.1f}× peer median",
            "detail": (
                f"Billing {vs_peer:.1f}× the median {provider.specialty or 'specialty'} "
                f"provider in {provider.state or 'their state'}. CMS data analysis reviews "
                f"typically target providers billing > 2× the peer median."
            ),
        })

    if total_payment > 0:
        key_findings.append({
            "label": "Total Medicare Paid (2022)",
            "value": f"${total_payment:,.0f}",
            "detail": (
                f"Across {int(provider.total_services or 0):,} services billed for "
                f"{int(provider.total_beneficiaries or 0):,} unique beneficiaries."
            ),
        })

    entropy = float(provider.billing_entropy or 0)
    if 0 < entropy < 1.5:
        key_findings.append({
            "label": "Billing Entropy",
            "value": f"{entropy:.3f} (concentrated)",
            "detail": (
                "Low Shannon entropy means billing is hyper-concentrated on a very small "
                "number of procedure codes — characteristic of single-scheme fraud where "
                "the same high-value code is billed repeatedly."
            ),
        })
    elif entropy > 3.5:
        key_findings.append({
            "label": "Billing Entropy",
            "value": f"{entropy:.3f} (dispersed)",
            "detail": (
                "Unusually high procedure diversity may indicate unbundling across "
                "multiple service categories or use of multiple provider identities."
            ),
        })

    em_ratio = float(provider.em_upcoding_ratio or 0)
    if em_ratio > 0.4:
        key_findings.append({
            "label": "E&M Level-5 Ratio",
            "value": f"{em_ratio * 100:.1f}% of all E&M visits",
            "detail": (
                f"Level-5 E&M codes represent {em_ratio * 100:.1f}% of all office visit billing. "
                f"National average is 10–20% for most specialties. Ratios above 40% are a "
                f"primary upcoding selection criterion in CMS data analysis."
            ),
        })

    if provider.is_excluded:
        key_findings.append({
            "label": "LEIE Status",
            "value": "FEDERALLY EXCLUDED",
            "detail": (
                f"On the OIG exclusion list"
                f"{(' since ' + provider.leie_date) if provider.leie_date else ''}. "
                f"Reason: {provider.leie_reason or 'on file with OIG'}. "
                f"All billing after exclusion date is a per-claim FCA violation."
            ),
        })

    services_per_bene = float(provider.services_per_bene or 0)
    if services_per_bene > 30 and not is_facility:
        key_findings.append({
            "label": "Services per Beneficiary",
            "value": f"{services_per_bene:.1f}",
            "detail": (
                f"{services_per_bene:.1f} service lines per patient — far above "
                f"typical clinical patterns. High ratios indicate either unbundling, "
                f"repeated billing for the same service, or outright phantom billing."
            ),
        })
    elif services_per_bene > 30 and is_facility:
        key_findings.append({
            "label": "Services per Beneficiary",
            "value": f"{services_per_bene:.1f} (expected for facility type)",
            "detail": (
                f"High services-per-patient ratios are structurally normal for "
                f"{provider.specialty or 'this provider type'}. Each patient encounter "
                f"typically generates multiple discrete HCPCS billing codes. "
                f"This metric is not a fraud indicator for this specialty."
            ),
        })

    # ── Billing anomalies
    billing_anomalies: list[dict] = []
    for rec in billing_records[:12]:
        text = _annotate_billing_record(rec, provider.specialty or "")
        if text:
            billing_anomalies.append({
                "hcpcs": rec.hcpcs_code,
                "description": rec.hcpcs_description,
                "total_paid": float(rec.total_medicare_payment or 0),
                "services": int(rec.total_services or 0),
                "beneficiaries": int(rec.total_beneficiaries or 0),
                "services_per_bene": round(
                    (int(rec.total_services or 0)) / max(int(rec.total_beneficiaries or 1), 1), 1
                ),
                "anomaly_text": text,
            })

    # ── Network suspects
    neighbor_map = {p.npi: p for p in neighbor_providers}

    suspicious_edges = sorted(
        [e for e in network_edges if e.is_suspicious],
        key=lambda e: e.shared_patients or 0,
        reverse=True,
    )[:8]

    network_suspects: list[dict] = []
    seen: set[str] = set()

    for edge in suspicious_edges:
        s_npi = (
            edge.target_npi if edge.source_npi == provider.npi else edge.source_npi
        )
        if s_npi in seen or s_npi not in neighbor_map:
            continue
        seen.add(s_npi)

        suspect = neighbor_map[s_npi]
        direction, reason, action = _classify_relationship(
            provider.npi, edge, suspect, network_edges
        )
        suspect_name = (
            f"{suspect.name_first or ''} {suspect.name_last or ''}".strip()
            or s_npi
        )

        network_suspects.append({
            "npi": s_npi,
            "name": suspect_name,
            "specialty": suspect.specialty,
            "state": suspect.state,
            "risk_score": float(suspect.risk_score or 0),
            "is_excluded": bool(suspect.is_excluded),
            "shared_patients": int(edge.shared_patients or 0),
            "direction": direction,
            "reason": reason,
            "action": action,
        })

    # ── Estimated exposure
    exposure_from_flags = sum(
        float(f.estimated_overpayment or 0) for f in flags if f.estimated_overpayment
    )
    if exposure_from_flags == 0:
        peer_median = float(provider.peer_median_payment or 0)
        exposure_from_flags = max(0.0, total_payment - peer_median) if peer_median > 0 else 0.0

    # ── Recommended actions
    actions = _build_actions(
        provider, flags, billing_anomalies, network_suspects, priority
    )

    # ── Full narrative
    narrative = _build_narrative(
        name, provider, scheme_name, base_narrative,
        billing_anomalies, network_suspects, flags, exposure_from_flags,
    )

    # Model confidence: derived from XGBoost score when available, else from
    # composite risk score. LEIE providers get 0.97 floor (statutory certainty).
    xgb = float(getattr(provider, "xgboost_score", None) or 0)
    if provider.is_excluded:
        model_confidence = max(xgb, 0.97)
    elif xgb > 0:
        model_confidence = round(xgb, 3)
    else:
        model_confidence = round(min(score / 100, 0.99), 3)

    return {
        "npi": provider.npi,
        "provider_name": name,
        "specialty": provider.specialty,
        "state": provider.state,
        "risk_score": score,
        "priority": priority,
        "priority_label": priority_label,
        # Both keys for frontend compatibility (scheme_type = canonical, scheme_label = display)
        "scheme_type": scheme_name,
        "scheme_label": scheme_name,
        "model_confidence": model_confidence,
        "narrative": narrative,
        # Split narrative into paragraphs for frontend rendering
        "narrative_paragraphs": [p for p in narrative.split("\n\n") if p.strip()],
        "key_findings": key_findings,
        "billing_anomalies": billing_anomalies,
        "network_suspects": network_suspects,
        "recommended_actions": actions,
        "estimated_exposure": exposure_from_flags if exposure_from_flags > 0 else None,
        "active_signals": len(flags),
        "suspicious_edges": len(suspicious_edges),
        "generated_at": now.isoformat(),
        "data_source": (
            "CMS Medicare Part B 2022 Public Use File · "
            "OIG List of Excluded Individuals/Entities (LEIE) · "
            "CMS Physician Shared Patient Patterns 2015"
        ),
    }
