"""
lead_pack_pdf.py — Render a LeadPack as a polished PDF deliverable.

This is the artifact Vigil sends to a state AG / MFCU director / FCA-firm
partner ahead of a 30-minute meeting.  Format goals:

  - Cover page: jurisdiction headline + summary numbers
  - One concise provider page per lead, ranked
  - Methodology citation page at the end so the recipient can verify
    Vigil's claims against the source data
  - Plain, professional formatting — no animations, no marketing fluff

The output is bytes; the calling endpoint streams it as
application/pdf with a sensible filename.
"""
from __future__ import annotations

import io
from datetime import datetime, timezone

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    HRFlowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from app.services.lead_pack import LeadPack, LeadProvider


# OIG exclusion codes most directly tied to billing fraud (mirrors
# ExclusionTimingPanel.tsx).  Used to annotate the LEIE-reason cell.
_BILLING_FRAUD_CODES = {"1128a1", "1128a3", "1128b7", "1128b8", "1128b9", "1156"}


def _fmt_money(v) -> str:
    """Human-readable USD: $X.XM, $XXk, $XXX."""
    if v is None:
        return "—"
    try:
        amt = float(v)
    except (TypeError, ValueError):
        return "—"
    if amt >= 1_000_000:
        return f"${amt/1_000_000:.1f}M"
    if amt >= 1_000:
        return f"${amt/1_000:.0f}k"
    if amt >= 1:
        return f"${amt:.0f}"
    return "$0"


def _fmt_leie_date(s: str | None) -> str:
    """Convert YYYYMMDD to YYYY-MM-DD; pass through anything else."""
    if not s:
        return "—"
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _exclusion_label(leie_date: str | None, billing_year: int = 2022) -> str:
    """Classify LEIE exclusion vs. billing year as PREDATES/DURING/POSTDATES."""
    if not leie_date or len(leie_date) != 8 or not leie_date.isdigit():
        return "—"
    try:
        y = int(leie_date[:4])
    except ValueError:
        return "—"
    if y < billing_year:
        return "PREDATES — per-claim FCA exposure"
    if y == billing_year:
        return "DURING — confirmed training positive"
    return f"POSTDATES — predictive signal ({y - billing_year}y after billing)"


def render_lead_pack_pdf(pack: LeadPack) -> bytes:
    """Render the lead pack as PDF.  Returns the raw bytes."""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
        title=_filename_stem(pack),
        author="Vigil Analytics",
    )
    styles = getSampleStyleSheet()
    body  = ParagraphStyle("body",  parent=styles["BodyText"], fontSize=10, leading=13)
    small = ParagraphStyle("small", parent=styles["BodyText"], fontSize=8,  leading=10,
                           textColor=colors.HexColor("#475569"))
    h1    = ParagraphStyle("h1",    parent=styles["Heading1"], fontSize=20, leading=24,
                           textColor=colors.HexColor("#0f172a"), spaceAfter=12)
    h2    = ParagraphStyle("h2",    parent=styles["Heading2"], fontSize=14, leading=18,
                           textColor=colors.HexColor("#1e293b"), spaceAfter=6)
    eyebrow = ParagraphStyle("eyebrow", parent=styles["BodyText"], fontSize=8, leading=10,
                             textColor=colors.HexColor("#0369a1"),
                             spaceAfter=4)

    story: list = []

    # ── COVER PAGE ────────────────────────────────────────────────────────────
    story.extend(_cover_page(pack, h1, h2, body, small, eyebrow))
    story.append(PageBreak())

    # ── ONE PAGE PER LEAD ─────────────────────────────────────────────────────
    for i, lead in enumerate(pack.leads, 1):
        story.extend(_lead_page(lead, i, len(pack.leads), h2, body, small, eyebrow))
        if i < len(pack.leads):
            story.append(PageBreak())

    # ── METHODOLOGY PAGE ──────────────────────────────────────────────────────
    story.append(PageBreak())
    story.extend(_methodology_page(h2, body, small))

    doc.build(story, onFirstPage=_draw_header_footer, onLaterPages=_draw_header_footer)
    return buf.getvalue()


def filename_for_pack(pack: LeadPack) -> str:
    """Suggested download filename for the API to set Content-Disposition."""
    return f"vigil-lead-pack-{_filename_stem(pack)}.pdf"


def _filename_stem(pack: LeadPack) -> str:
    parts = [
        (pack.state or "national").lower(),
        (pack.specialty or "all").lower().replace(" ", "-"),
        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    ]
    return "-".join(parts)


# ── Page builders ────────────────────────────────────────────────────────────

def _cover_page(pack: LeadPack, h1, h2, body, small, eyebrow) -> list:
    """Cover page: headline + jurisdiction summary + how to read."""
    state_label     = pack.state or "All states"
    specialty_label = pack.specialty or "All specialties"
    out = []

    out.append(Paragraph("VIGIL ANALYTICS — INVESTIGATIVE LEAD PACK", eyebrow))
    out.append(Paragraph(
        f"{len(pack.leads)} high-risk providers — {specialty_label}, {state_label}",
        h1,
    ))
    out.append(Paragraph(
        f"Generated {pack.generated_at[:10]} from CMS Medicare Part B 2022 data.  "
        f"Each provider in this pack has been independently scored by Vigil's "
        f"validated detection methodology (see appendix).",
        body,
    ))
    out.append(Spacer(1, 18))

    out.append(Paragraph("Jurisdiction summary", h2))
    summary_rows = [
        ["Total providers in jurisdiction",  f"{pack.total_in_jurisdiction:,}"],
        ["High-risk providers (score ≥ 70)", f"{pack.total_high_risk:,}"],
        ["LEIE-excluded providers",          f"{pack.leie_count:,}"],
        ["Leads in this pack",               f"{len(pack.leads)}"],
        ["Address-cluster members",          f"{pack.address_cluster_count}"],
        ["Combined excess billing (leads)",  _fmt_money(pack.excess_billing_sum)],
    ]
    t = Table(summary_rows, colWidths=[3.5 * inch, 2.5 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, 0), colors.HexColor("#0f172a")),
        ("TEXTCOLOR",    (0, 0), (0, -1), colors.HexColor("#475569")),
        ("FONTNAME",     (1, 0), (1, -1), "Helvetica-Bold"),
        ("ALIGN",        (1, 0), (1, -1), "RIGHT"),
        ("PADDING",      (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
        ("LINEBELOW",    (0, 0), (-1, -1), 0.25, colors.HexColor("#e2e8f0")),
    ]))
    out.append(t)
    out.append(Spacer(1, 24))

    out.append(Paragraph("How to use this document", h2))
    out.append(Paragraph(
        "Each subsequent page is a single provider, ranked by Vigil's composite "
        "signal (risk score + excess billing + breadth of fraud flags + co-location). "
        "Per-page sections:",
        body,
    ))
    out.append(Spacer(1, 6))
    out.append(Paragraph(
        "• <b>Signals</b> — the fraud-flag categories that fired for this provider<br/>"
        "• <b>Financial impact</b> — estimated billing in excess of specialty peer median<br/>"
        "• <b>LEIE status</b> — federal exclusion classification, with FCA-exposure implications<br/>"
        "• <b>Co-location</b> — other providers registered at the same practice address",
        body,
    ))
    out.append(Spacer(1, 12))
    out.append(Paragraph(
        "These figures are coarse estimates intended to support investigative triage. "
        "They are not damages calculations and should not be cited in pleadings without "
        "independent verification against claim-level CMS records.",
        small,
    ))
    return out


def _lead_page(lead: LeadProvider, i: int, total: int, h2, body, small, eyebrow) -> list:
    """One per-provider page."""
    out = []
    out.append(Paragraph(f"LEAD {i} OF {total} · RANK SCORE {lead.rank_score:.1f}", eyebrow))
    out.append(Paragraph(lead.name, h2))
    out.append(Paragraph(
        f"NPI {lead.npi} · {lead.specialty or 'unspecified'} · "
        f"{(lead.city or '—')}, {lead.state or '—'}",
        small,
    ))
    out.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#e2e8f0"),
                          spaceBefore=4, spaceAfter=10))

    # ── Headline scoring row ──────────────────────────────────────────────────
    head_rows = [
        ["Risk score",        f"{(lead.risk_score or 0):.0f} / 100"],
        ["Total Medicare billing (2022)", _fmt_money(lead.total_payment)],
        ["Estimated excess vs. peers",    _fmt_money(lead.excess_billing)],
        ["Fraud-flag categories", f"{lead.distinct_flag_count} ({', '.join(lead.flag_types) or '—'})"],
        ["LEIE status",       (_exclusion_label(lead.leie_date) if lead.is_excluded else "Not currently on LEIE")],
        ["Address cluster",   (f"{lead.address_cluster_size} providers at this address"
                               if lead.address_cluster_size >= 3 else "Not in a cluster")],
    ]
    t = Table(head_rows, colWidths=[2.5 * inch, 4.0 * inch])
    t.setStyle(TableStyle([
        ("TEXTCOLOR",      (0, 0), (0, -1), colors.HexColor("#475569")),
        ("FONTNAME",       (1, 0), (1, -1), "Helvetica-Bold"),
        ("PADDING",        (0, 0), (-1, -1), 6),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
        ("LINEBELOW",      (0, 0), (-1, -1), 0.25, colors.HexColor("#e2e8f0")),
        ("VALIGN",         (0, 0), (-1, -1), "TOP"),
    ]))
    out.append(t)
    out.append(Spacer(1, 14))

    # ── Severity callout for LEIE PREDATES ────────────────────────────────────
    if lead.is_excluded and lead.leie_date and len(lead.leie_date) == 8:
        try:
            y = int(lead.leie_date[:4])
        except ValueError:
            y = 9999
        if y < 2022:
            out.append(Paragraph(
                "<b>Active False Claims Act exposure.</b>  This provider was on the "
                f"OIG LEIE before the 2022 billing year (excluded {_fmt_leie_date(lead.leie_date)}).  "
                f"Every Medicare claim submitted during 2022 is a per-claim violation of "
                f"31 U.S.C. § 3729.  Statutory damages currently $13,946–$27,894 per claim plus "
                f"3× actual damages.",
                ParagraphStyle("alert", parent=body, textColor=colors.HexColor("#991b1b"),
                               leftIndent=8, rightIndent=8,
                               backColor=colors.HexColor("#fef2f2")),
            ))
            out.append(Spacer(1, 10))

    # ── Recommended next investigative step ───────────────────────────────────
    next_step = _recommend_next_step(lead)
    if next_step:
        out.append(Paragraph("Recommended next step", eyebrow))
        out.append(Paragraph(next_step, body))

    return out


def _recommend_next_step(lead: LeadProvider) -> str:
    """Plain-English recommendation based on which signals fired."""
    bullets: list[str] = []
    if lead.is_excluded and lead.leie_date and len(lead.leie_date) == 8 and lead.leie_date < "20220101":
        bullets.append(
            "<b>Subpoena Medicare claim records for 2022.</b> Each post-exclusion "
            "claim is a discrete FCA violation."
        )
    if lead.address_cluster_size >= 3:
        bullets.append(
            f"<b>Investigate co-located entities.</b> {lead.address_cluster_size} providers "
            f"share this practice address — verify whether common ownership or principals."
        )
    if (lead.excess_billing or 0) >= 250_000:
        bullets.append(
            f"<b>Pull claim-level billing.</b> Estimated excess of {_fmt_money(lead.excess_billing)} "
            f"vs. peer norm warrants line-item review against beneficiary records."
        )
    if lead.distinct_flag_count >= 3:
        bullets.append(
            f"<b>Cross-reference with state board records.</b> {lead.distinct_flag_count} "
            f"distinct fraud-flag categories indicate multiple independent red flags."
        )
    if not bullets:
        return (
            "Statistical outlier within specialty peer group.  "
            "Manual review of billing pattern recommended before resource commitment."
        )
    return "<br/><br/>".join(bullets)


def _methodology_page(h2, body, small) -> list:
    """Appendix: how Vigil scored these providers; Daubert-defensibility notes."""
    out = []
    out.append(Paragraph("Methodology", h2))
    out.append(Paragraph(
        "Vigil scores every Medicare Part B–billing provider using an ensemble of "
        "three machine-learning models:",
        body,
    ))
    out.append(Spacer(1, 4))
    out.append(Paragraph(
        "• <b>XGBoost</b> (50%) — supervised gradient-boosted classifier trained on "
        "providers excluded by OIG before January 1, 2023.  Outputs a calibrated "
        "fraud probability."
        "<br/>"
        "• <b>Isolation Forest</b> (30%) — unsupervised anomaly detector identifying "
        "billing patterns far from the bulk specialty distribution."
        "<br/>"
        "• <b>Autoencoder</b> (20%) — neural network trained to reconstruct normal "
        "provider profiles; reconstruction error scales with anomaly severity.",
        body,
    ))
    out.append(Spacer(1, 8))

    out.append(Paragraph("Validation", h2))
    out.append(Paragraph(
        "The methodology is validated against a <b>temporal holdout</b>: providers "
        "excluded by OIG on or after January 1, 2023 were held out of training and "
        "used to test whether Vigil could predict exclusion from 2022 billing "
        "patterns alone.  Vigil achieves <b>15.2% billing-fraud recall at score "
        "threshold 70</b> on the holdout — meaning the model independently flagged "
        "15% of providers OIG later excluded based on their billing patterns, "
        "before OIG acted.  All findings are explainable via per-feature SHAP "
        "contributions stored in Vigil's database.",
        body,
    ))
    out.append(Spacer(1, 8))

    out.append(Paragraph("Limitations", h2))
    out.append(Paragraph(
        "Vigil's data is annual aggregate Medicare Part B billing for 2022.  Claim-"
        "level detail (specific dates of service, beneficiary identifiers, individual "
        "claim amounts) is not accessible to Vigil without a CMS Research Data "
        "Assistance Center (ResDAC) agreement.  Findings in this pack are "
        "investigative-triage signals; they are not damages calculations and must be "
        "verified against claim-level data before being cited in pleadings.",
        small,
    ))
    return out


# ── Page chrome ───────────────────────────────────────────────────────────────

def _draw_header_footer(canvas, doc):
    """Vigil branding + page number on every page."""
    canvas.saveState()
    canvas.setFont("Helvetica-Bold", 8)
    canvas.setFillColor(colors.HexColor("#0f172a"))
    canvas.drawString(0.75 * inch, letter[1] - 0.45 * inch, "VIGIL ANALYTICS")
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#64748b"))
    canvas.drawRightString(
        letter[0] - 0.75 * inch,
        letter[1] - 0.45 * inch,
        f"Investigative Lead Pack · Page {doc.page}",
    )
    canvas.line(0.75 * inch, letter[1] - 0.55 * inch,
                letter[0] - 0.75 * inch, letter[1] - 0.55 * inch)
    # Footer
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(colors.HexColor("#94a3b8"))
    canvas.drawString(
        0.75 * inch, 0.45 * inch,
        "Vigil Analytics methodology: temporal-holdout-validated, Daubert-defensible.  "
        "All findings require independent verification before enforcement use.",
    )
    canvas.restoreState()
