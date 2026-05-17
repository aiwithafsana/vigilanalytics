"""
PDF evidence package generator for a provider.

Requires: reportlab
"""
from __future__ import annotations
from datetime import datetime
from io import BytesIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.models import Provider, User


def generate_provider_pdf(
    provider: "Provider",
    requested_by: "User",
    *,
    methodology_version: str = "2.1.0",
    prior_access_count: int | None = None,
    prior_distinct_users: int | None = None,
) -> bytes:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
        )
    except ImportError:
        raise RuntimeError("reportlab is required for PDF generation. pip install reportlab")

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, leftMargin=inch, rightMargin=inch,
                             topMargin=inch, bottomMargin=inch)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("title", parent=styles["Heading1"], fontSize=18, textColor=colors.HexColor("#1e293b"))
    section_style = ParagraphStyle("section", parent=styles["Heading2"], fontSize=12,
                                    textColor=colors.HexColor("#475569"), spaceBefore=16)
    body_style = styles["BodyText"]

    def label_val(label: str, value) -> list:
        return [Paragraph(f"<b>{label}</b>", body_style), Paragraph(str(value or "—"), body_style)]

    def fmt_money(v) -> str:
        if v is None:
            return "—"
        v = float(v)
        if v >= 1_000_000:
            return f"${v/1_000_000:.2f}M"
        if v >= 1_000:
            return f"${v/1_000:.1f}K"
        return f"${v:,.2f}"

    story = []

    # Header
    story.append(Paragraph("VIGIL — Provider Intelligence Report", title_style))
    story.append(Paragraph(
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} · By: {requested_by.name} ({requested_by.role})",
        body_style,
    ))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#e2e8f0")))
    story.append(Spacer(1, 12))

    # Provider identity
    story.append(Paragraph("Provider Identity", section_style))
    id_data = [
        label_val("NPI", provider.npi),
        label_val("Name", f"{provider.name_first or ''} {provider.name_last or ''}".strip()),
        label_val("Specialty", provider.specialty),
        label_val("Location", f"{provider.city}, {provider.state}"),
        label_val("Data Year", provider.data_year),
    ]
    id_table = Table([[row[0], row[1]] for row in id_data], colWidths=[2 * inch, 4 * inch])
    id_table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f8fafc")),
        ("PADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(id_table)
    story.append(Spacer(1, 12))

    # Risk scores
    story.append(Paragraph("Risk Assessment", section_style))
    risk_data = [
        label_val("Composite Risk Score", f"{provider.risk_score or '—'} / 100"),
        label_val("XGBoost Score", provider.xgboost_score),
        label_val("Isolation Forest Score", provider.isolation_score),
        label_val("Autoencoder Score", provider.autoencoder_score),
        label_val("Payment Z-Score", provider.payment_zscore),
    ]
    risk_table = Table([[r[0], r[1]] for r in risk_data], colWidths=[2 * inch, 4 * inch])
    risk_table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f8fafc")),
        ("PADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(risk_table)
    story.append(Spacer(1, 12))

    # Billing metrics
    story.append(Paragraph("Billing Metrics vs. Peer Median", section_style))
    billing_data = [
        ["Metric", "Provider", "Peer Median", "Ratio"],
        ["Total Payment", fmt_money(provider.total_payment), fmt_money(provider.peer_median_payment),
         f"{provider.payment_vs_peer or '—'}x"],
        ["Total Services", str(provider.total_services or "—"), str(provider.peer_median_services or "—"),
         f"{provider.services_vs_peer or '—'}x"],
        ["Beneficiaries", str(provider.total_beneficiaries or "—"), str(provider.peer_median_benes or "—"),
         f"{provider.benes_vs_peer or '—'}x"],
        ["Payment / Bene", fmt_money(provider.payment_per_bene), "—", "—"],
        ["Services / Bene", str(provider.services_per_bene or "—"), "—", "—"],
    ]
    billing_table = Table(billing_data, colWidths=[2 * inch, 1.5 * inch, 1.5 * inch, 1 * inch])
    billing_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1e293b")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("PADDING", (0, 0), (-1, -1), 6),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
    ]))
    story.append(billing_table)
    story.append(Spacer(1, 12))

    # ── Estimated financial impact ──────────────────────────────────────────
    # Compute a coarse "billing in excess of peer norm" estimate for case
    # sizing purposes.  Two methods:
    #
    #   Per-patient excess  = total_benes  × (provider_ppb - peer_median_ppb)
    #   Per-claim excess    = total_services × (provider_pps - peer_median_pps)
    #
    # These are independent estimates; reporting both gives the attorney a
    # range rather than a single number that overstates precision.
    #
    # Strong disclaimer in the PDF — these are NOT damages calculations.
    # Actual overpayment requires claim-level review and is determined by
    # auditors using contract-rate analysis, not aggregate billing.
    try:
        ppb_excess = None
        if provider.payment_per_bene and provider.total_beneficiaries:
            # peer_median_ppb may be in provider model or computed from
            # peer_median_payment / peer_median_benes
            peer_ppb = None
            if hasattr(provider, "peer_median_ppb") and provider.peer_median_ppb:
                peer_ppb = float(provider.peer_median_ppb)
            elif provider.peer_median_payment and provider.peer_median_benes:
                peer_ppb = float(provider.peer_median_payment) / float(provider.peer_median_benes)
            if peer_ppb is not None:
                ppb_excess = max(
                    0.0,
                    (float(provider.payment_per_bene) - peer_ppb) * int(provider.total_beneficiaries),
                )

        total_excess = None
        if provider.total_payment and provider.peer_median_payment and provider.benes_vs_peer:
            # If the provider's billing were exactly at peer rates per patient,
            # it would total: peer_ppb × actual_beneficiaries.  Excess is the
            # gap between actual total_payment and that hypothetical.
            if ppb_excess is not None:
                total_excess = ppb_excess

        story.append(Paragraph("Estimated Financial Impact", section_style))
        story.append(Paragraph(
            'These are <b>coarse estimates of billing in excess of specialty peers</b>, '
            'NOT damages calculations.  Actual overpayment is determined by claim-by-claim '
            'review against contracted Medicare rates and is typically lower than the '
            'aggregate gap shown here.  Provided to support contingency-economics '
            'decisions; <b>do not cite these figures in pleadings</b> without independent verification.',
            ParagraphStyle("warn", parent=body_style, fontSize=9, textColor=colors.HexColor("#92400e")),
        ))

        impact_rows = [["Method", "Estimate"]]
        if ppb_excess is not None:
            impact_rows.append([
                "Per-patient excess (annual)",
                f"{fmt_money(ppb_excess)}",
            ])
        if total_excess is not None and total_excess != ppb_excess:
            impact_rows.append([
                "Total billing above peer norm",
                fmt_money(total_excess),
            ])
        if provider.total_payment:
            impact_rows.append([
                "Provider total Medicare billing (year)",
                fmt_money(provider.total_payment),
            ])

        if len(impact_rows) > 1:
            impact_table = Table(impact_rows, colWidths=[3 * inch, 3 * inch])
            impact_table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1e293b")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("PADDING", (0, 0), (-1, -1), 6),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
            ]))
            story.append(impact_table)
        else:
            story.append(Paragraph("Insufficient data to estimate impact.", body_style))
    except Exception:
        # Never let an impact-calculation error block PDF generation
        story.append(Paragraph(
            "Estimated financial impact could not be computed for this provider.",
            body_style,
        ))
    story.append(Spacer(1, 12))

    # Anomaly flags
    flags = provider.flags or []
    if flags:
        story.append(Paragraph("Anomaly Flags", section_style))
        for flag in flags:
            severity = flag.get("severity", "").upper()
            color = "#ef4444" if severity == "CRITICAL" else "#f59e0b" if severity == "HIGH" else "#64748b"
            story.append(Paragraph(
                f'<font color="{color}"><b>[{severity}]</b></font> {flag.get("text", "")}',
                body_style,
            ))
            story.append(Spacer(1, 4))
        story.append(Spacer(1, 8))

    # LEIE status
    story.append(Paragraph("LEIE Exclusion Status", section_style))
    if provider.is_excluded:
        story.append(Paragraph(
            f'<font color="#ef4444"><b>CONFIRMED EXCLUDED</b></font> — '
            f'Date: {provider.leie_date or "unknown"} · Reason: {provider.leie_reason or "unknown"}',
            body_style,
        ))
    else:
        story.append(Paragraph("No LEIE exclusion record found — this is a net-new investigative lead.", body_style))

    # Chain-of-custody / first-view metadata — methodology §10.
    # Embedding this in the PDF makes the artefact itself self-documenting
    # for first-to-file qui tam evidence; the count cannot be retroactively
    # changed once the PDF leaves the system.
    story.append(Spacer(1, 12))
    story.append(Paragraph("Chain of Custody (Vigil)", section_style))
    if prior_access_count is None:
        cot_text = (
            f"Methodology: Vigil v{methodology_version}. "
            f"Score reflects CMS Part B scoring as of {provider.scored_at or 'unknown'}. "
            f"Audit metadata not captured for this export."
        )
    elif prior_access_count == 0:
        cot_text = (
            f"<b>First Vigil access.</b> Methodology v{methodology_version}. "
            f"As of generation time, Vigil records show no prior user has accessed "
            f"or exported this provider record. "
            f"Score reflects CMS Part B scoring as of {provider.scored_at or 'unknown'}. "
            f"This metadata is preserved in audit_log and may be relevant to "
            f"first-to-file qui tam determinations under 31 U.S.C. § 3730(b)(5)."
        )
    else:
        distinct_part = (
            f"by {prior_distinct_users} distinct user(s) "
            if prior_distinct_users is not None else ""
        )
        cot_text = (
            f"Methodology v{methodology_version}. "
            f"As of generation time, Vigil records show {prior_access_count} prior "
            f"access event(s) {distinct_part}on this provider record. "
            f"Score reflects CMS Part B scoring as of {provider.scored_at or 'unknown'}."
        )
    story.append(Paragraph(cot_text, body_style))

    story.append(Spacer(1, 24))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#e2e8f0")))
    story.append(Paragraph(
        "This report is generated by Vigil and is intended for authorized use only. "
        "All data sourced from CMS Part B public use files and HHS-OIG LEIE.",
        ParagraphStyle("footer", parent=body_style, fontSize=8, textColor=colors.HexColor("#94a3b8")),
    ))

    doc.build(story)
    return buf.getvalue()
