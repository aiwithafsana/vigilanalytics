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


def generate_provider_pdf(provider: "Provider", requested_by: "User") -> bytes:
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

    story.append(Spacer(1, 24))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#e2e8f0")))
    story.append(Paragraph(
        "This report is generated by Vigil and is intended for authorized use only. "
        "All data sourced from CMS Part B public use files and HHS-OIG LEIE.",
        ParagraphStyle("footer", parent=body_style, fontSize=8, textColor=colors.HexColor("#94a3b8")),
    ))

    doc.build(story)
    return buf.getvalue()
