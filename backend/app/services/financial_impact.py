"""
financial_impact.py — coarse "billing in excess of peer norm" estimates.

These are NOT damages calculations.  Actual overpayment is determined by
claim-by-claim review against contracted Medicare rates and typically lands
lower than the aggregate gap surfaced here.  We use these figures to support
contingency-economics decisions — investigators and attorneys deciding which
providers are worth a deeper look first.

Method
------
The defensible-in-court approach is the "but-for peer-median rate" estimate:

    expected_payment  =  peer_median_payment_per_beneficiary  ×  actual_beneficiaries
    excess_payment    =  max(0, actual_payment - expected_payment)

The narrative: *"If this provider charged the median per-patient rate for their
specialty and state, they would have billed N.  They actually billed M.
M - N = excess."*

This is anchored to a per-patient rate (size-invariant), then scaled by the
provider's actual patient count.  Robust to practice-size differences,
unlike a raw `total_payment − peer_median_total_payment` subtraction.

Returned values are floats in USD.  Callers should format for display.
"""
from __future__ import annotations

from dataclasses import dataclass


# Provider attributes the calculation needs.  We accept any object exposing
# these (the SQLAlchemy Provider model, a Pydantic schema, a plain dict).
# Each may be None when the provider doesn't have the data computed yet.
_PROVIDER_ATTRS = (
    "payment_per_bene",
    "total_beneficiaries",
    "total_payment",
    "peer_median_ppb",
    "peer_median_payment",
    "peer_median_benes",
)


@dataclass(frozen=True)
class FinancialImpact:
    """Coarse excess-billing estimate vs specialty/state peers.

    All amounts in USD.  None for any field means the inputs were
    insufficient to compute that figure.
    """
    expected_payment: float | None      # what they "would have" billed at peer rate
    actual_payment:   float | None      # what they actually billed
    excess_billing:   float | None      # headline number for UI / PDF
    excess_per_bene:  float | None      # per-patient over-charge, for context
    peer_ppb_used:    float | None      # the median rate we anchored to
    method:           str               # "per_patient" | "unavailable"

    def to_dict(self) -> dict:
        return {
            "expected_payment": self.expected_payment,
            "actual_payment":   self.actual_payment,
            "excess_billing":   self.excess_billing,
            "excess_per_bene":  self.excess_per_bene,
            "peer_ppb_used":    self.peer_ppb_used,
            "method":           self.method,
            "formatted_excess": format_money(self.excess_billing),
            "disclaimer": (
                "Coarse estimate of billing in excess of specialty/state peer median.  "
                "Not a damages calculation.  Actual overpayment is determined by "
                "claim-by-claim audit and typically lower than this aggregate gap."
            ),
        }


def _as_float(v) -> float | None:
    """Coerce Decimal / numeric / None to float."""
    if v is None:
        return None
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    return out if out == out else None    # filter NaN


def compute_financial_impact(provider) -> FinancialImpact:
    """
    Compute the per-patient-anchored excess-billing estimate for one provider.

    Accepts any object with the attributes listed in _PROVIDER_ATTRS (e.g. the
    SQLAlchemy Provider model, a Pydantic schema, a plain dict via SimpleNamespace).
    """
    ppb         = _as_float(getattr(provider, "payment_per_bene", None))
    n_benes     = getattr(provider, "total_beneficiaries", None)
    actual_pay  = _as_float(getattr(provider, "total_payment", None))
    peer_ppb    = _as_float(getattr(provider, "peer_median_ppb", None))
    peer_pay    = _as_float(getattr(provider, "peer_median_payment", None))
    peer_benes  = _as_float(getattr(provider, "peer_median_benes", None))

    # Fall back to deriving peer_ppb from peer_median_payment / peer_median_benes
    # when the column isn't populated directly.
    if peer_ppb is None and peer_pay is not None and peer_benes:
        peer_ppb = peer_pay / peer_benes

    if not (peer_ppb and ppb and n_benes and actual_pay):
        return FinancialImpact(
            expected_payment=None,
            actual_payment=actual_pay,
            excess_billing=None,
            excess_per_bene=None,
            peer_ppb_used=peer_ppb,
            method="unavailable",
        )

    expected = peer_ppb * int(n_benes)
    excess   = max(0.0, actual_pay - expected)
    per_pat  = max(0.0, ppb - peer_ppb)

    return FinancialImpact(
        expected_payment=expected,
        actual_payment=actual_pay,
        excess_billing=excess,
        excess_per_bene=per_pat,
        peer_ppb_used=peer_ppb,
        method="per_patient",
    )


def format_money(amount: float | None) -> str | None:
    """Human-readable USD formatting: $X.XM, $XXk, $XXX."""
    if amount is None:
        return None
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.1f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.0f}k"
    if amount >= 1:
        return f"${amount:.0f}"
    return "$0"
