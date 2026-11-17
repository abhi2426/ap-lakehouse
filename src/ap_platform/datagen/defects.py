"""
defects — the catalogue of deliberate data defects, and the ledger that records
every injection.

Why a ledger. A synthetic dataset that is merely messy is useless for testing a
DQ framework: you cannot tell a rule that missed a defect from a defect that was
never injected. Every injection here writes a row saying which record was broken,
how, and which DQ rule is expected to catch it. The Step 4 exercises then reduce
to one query: injected defects LEFT JOIN caught defects, and anything on the left
with nothing on the right is a gap in the rules.

Rates are per-mille of eligible records unless stated. They are tuned to be
visible in a 50k-invoice dataset without swamping it — roughly what a real
mid-maturity AP function looks like, except duplicates, which are deliberately
exaggerated so the scoring model has something to work with.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DefectSpec:
    code: str
    description: str
    entity: str
    rate: float                    # probability per eligible record
    expected_rule: str | None      # DQ rule that should catch it, None = detection is a Step 4 exercise
    dq_dimension: str
    severity_expected: str


# ---------------------------------------------------------------------------
# The catalogue. Every item maps to a pain point from the Step 1 document.
# ---------------------------------------------------------------------------
CATALOGUE: list[DefectSpec] = [
    DefectSpec("NEAR_DUP_INVOICE", "Same invoice re-submitted with punctuation or leading-zero variation on the supplier invoice number",
               "invoice_header", 0.012, "DQ_INV_HDR_0301", "UNIQUENESS", "WARN"),
    DefectSpec("EXACT_DUP_PK", "Identical primary key emitted twice in one extract",
               "invoice_header", 0.002, "DQ_INV_HDR_0003", "UNIQUENESS", "ABORT"),
    DefectSpec("DUP_DIFFERENT_CHANNEL", "Same invoice arriving via two capture channels under different document ids",
               "invoice_header", 0.004, "DQ_INV_HDR_0301", "UNIQUENESS", "WARN"),

    DefectSpec("BANK_CHANGE_UNVERIFIED", "Vendor bank account changed with no verification event, payment follows within 14 days",
               "vendor_bank", 0.020, "DQ_VEN_BNK_0703", "ACCURACY", "WARN"),
    DefectSpec("BANK_ACCOUNT_MALFORMED", "Bank account containing spaces or punctuation",
               "vendor_bank", 0.015, "DQ_VEN_BNK_0205", "VALIDITY", "WARN"),

    DefectSpec("ZERO_AMOUNT", "Invoice with a zero gross amount",
               "invoice_header", 0.003, "DQ_INV_HDR_0203", "VALIDITY", "QUARANTINE"),
    DefectSpec("NEGATIVE_NON_CREDIT", "Negative amount on a document typed as an invoice rather than a credit memo",
               "invoice_header", 0.002, "DQ_INV_HDR_0402", "CONSISTENCY", "QUARANTINE"),
    DefectSpec("POSITIVE_CREDIT_MEMO", "Credit memo carrying a positive amount, i.e. the debit/credit indicator was lost",
               "invoice_header", 0.004, "DQ_INV_HDR_0402", "CONSISTENCY", "QUARANTINE"),

    DefectSpec("MISSING_TAX_CODE", "Invoice line with no tax code",
               "invoice_line", 0.035, "DQ_INV_LIN_0103", "COMPLETENESS", "WARN"),
    DefectSpec("TAX_AMOUNT_INCONSISTENT", "Tax amount not matching the statutory rate for the code used",
               "invoice_line", 0.018, "DQ_INV_LIN_0403", "ACCURACY", "WARN"),
    DefectSpec("INACTIVE_GL_ACCOUNT", "Posting to a GL account marked inactive",
               "invoice_line", 0.008, "DQ_INV_LIN_0503", "REFERENTIAL_INTEGRITY", "WARN"),

    DefectSpec("ORPHAN_INVOICE_LINE", "Invoice line whose header is absent from the extract",
               "invoice_line", 0.004, "DQ_INV_LIN_0502", "REFERENTIAL_INTEGRITY", "QUARANTINE"),
    DefectSpec("ORPHAN_INVOICE_VENDOR", "Invoice referencing a vendor not present in the vendor master",
               "invoice_header", 0.003, "DQ_INV_HDR_0501", "REFERENTIAL_INTEGRITY", "QUARANTINE"),

    DefectSpec("DATE_FORMAT_CHAOS", "Date emitted in an alternative format within the same column",
               "invoice_header", 0.025, "DQ_INV_HDR_0201", "VALIDITY", "QUARANTINE"),
    DefectSpec("DATE_UNPARSEABLE", "Date that no configured format can parse",
               "invoice_header", 0.002, "DQ_INV_HDR_0201", "VALIDITY", "QUARANTINE"),
    DefectSpec("FUTURE_POSTING_DATE", "Posting date beyond the extract date",
               "invoice_header", 0.002, "DQ_INV_HDR_0202", "VALIDITY", "QUARANTINE"),

    DefectSpec("CURRENCY_FORMAT_CHAOS", "Amount carrying thousands separators, a currency symbol or a European decimal comma",
               "invoice_header", 0.030, None, "VALIDITY", "WARN"),
    DefectSpec("CURRENCY_CODE_INVALID", "Currency field holding a symbol or a blank instead of an ISO code",
               "invoice_header", 0.003, "DQ_INV_HDR_0204", "VALIDITY", "QUARANTINE"),

    DefectSpec("KEY_WHITESPACE", "Trailing or leading whitespace on a join key",
               "invoice_header", 0.040, None, "CONSISTENCY", "WARN"),
    DefectSpec("KEY_CASE_INCONSISTENT", "Join key emitted in lower case where the master is upper case",
               "invoice_header", 0.020, None, "CONSISTENCY", "WARN"),

    DefectSpec("NULL_MANDATORY_VENDOR", "Invoice with no vendor id",
               "invoice_header", 0.002, "DQ_INV_HDR_0102", "COMPLETENESS", "QUARANTINE"),
    DefectSpec("NULL_MANDATORY_SUPPLIER_INV", "Invoice with no supplier invoice number",
               "invoice_header", 0.006, "DQ_INV_HDR_0101", "COMPLETENESS", "QUARANTINE"),
    DefectSpec("NULL_CAPTURE_TS", "Invoice with no capture timestamp, so cycle time cannot be computed",
               "invoice_header", 0.050, "DQ_INV_HDR_0104", "COMPLETENESS", "WARN"),

    DefectSpec("OVER_RECEIPT", "Receipt quantity exceeding the PO quantity beyond tolerance",
               "po_history", 0.020, "DQ_PO_HIS_0701", "ACCURACY", "WARN"),
    DefectSpec("GRIR_UNMATCHED_RECEIPT", "Goods received with no corresponding invoice",
               "po_history", 0.045, None, "CONSISTENCY", "WARN"),
    DefectSpec("GRIR_UNMATCHED_INVOICE", "Invoice against a PO line with no receipt",
               "invoice_line", 0.030, None, "CONSISTENCY", "WARN"),

    DefectSpec("HEADER_LINE_MISMATCH", "Header gross not equal to the sum of lines plus tax",
               "invoice_header", 0.008, "DQ_INV_0401", "CONSISTENCY", "QUARANTINE"),
    DefectSpec("UNMAPPED_DOMAIN_VALUE", "Payment terms code absent from value_domain_mapping",
               "invoice_header", 0.006, "DQ_ALL_0504", "REFERENTIAL_INTEGRITY", "QUARANTINE"),
    DefectSpec("LATE_CORRECTION", "Correction to a closed period arriving past the lateness window",
               "invoice_header", 0.010, "DQ_INV_HDR_0602", "TIMELINESS", "QUARANTINE"),
    DefectSpec("SOD_BREACH", "Same actor created the vendor and approved its invoice",
               "approval_event", 0.006, None, "ACCURACY", "WARN"),
]

BY_CODE = {d.code: d for d in CATALOGUE}


# ---------------------------------------------------------------------------
# Drift events. Three, one per tenant, each a different class of problem.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DriftEvent:
    event_id: str
    tenant_id: str
    entity: str
    effective_from_day: int        # day index into the delta series when it starts
    drift_type: str
    classification: str
    column_name: str
    detail: str
    expected_action: str


DRIFT_EVENTS: list[DriftEvent] = [
    DriftEvent(
        "DRIFT_001", "TEN001", "invoice_header", 8,
        "COLUMN_ADDED", "ADDITIVE", "ZUONR",
        "SAP support pack adds an assignment field at the end of the positional layout. "
        "Harmless for a header-based reader; fatal for a positional one, because every "
        "ordinal after it is unaffected only if the column is genuinely appended last.",
        "AUTO_ACCEPTED — log it, rescue the value, carry on.",
    ),
    DriftEvent(
        "DRIFT_002", "TEN002", "invoice_header", 12,
        "TYPE_CHANGED", "BREAKING", "INVOICE_AMOUNT",
        "Oracle patch emits the amount as a JSON string with an embedded currency symbol "
        "instead of a number. try_cast returns null, so totals silently collapse rather "
        "than erroring. This is the most dangerous drift class in the catalogue.",
        "HALT this source only. Do not auto-accept: a silent null is worse than a failed batch.",
    ),
    DriftEvent(
        "DRIFT_003", "TEN003", "invoice_header", 15,
        "COLUMN_RENAMED", "BREAKING", "po-number -> purchase-order-number",
        "Coupa release renames a column. A header-based reader sees the old column vanish "
        "and a new one appear. Auto-accepting produces a table where PO coverage drops to "
        "zero overnight and nobody notices for a week.",
        "HALT this source. Resolution is a schema_mapping change, not a code change.",
    ),
]


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------
@dataclass
class DefectLedger:
    """Records every injection so detection can be measured against it."""

    entries: list[dict[str, Any]] = field(default_factory=list)

    def record(
        self,
        *,
        tenant_id: str,
        entity: str,
        code: str,
        business_key: str,
        detail: str = "",
        original_value: Any = None,
        injected_value: Any = None,
    ) -> None:
        spec = BY_CODE.get(code)
        self.entries.append(
            {
                "tenant_id": tenant_id,
                "entity": entity,
                "defect_code": code,
                "business_key": business_key,
                "detail": detail,
                "original_value": None if original_value is None else str(original_value)[:200],
                "injected_value": None if injected_value is None else str(injected_value)[:200],
                "expected_rule": spec.expected_rule if spec else None,
                "dq_dimension": spec.dq_dimension if spec else None,
                "expected_severity": spec.severity_expected if spec else None,
            }
        )

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for e in self.entries:
            out[e["defect_code"]] = out.get(e["defect_code"], 0) + 1
        return dict(sorted(out.items()))

    def __len__(self) -> int:
        return len(self.entries)


def should_inject(rng: random.Random, spec: DefectSpec, multiplier: float = 1.0) -> bool:
    """Single place the injection decision is made, so rates stay auditable."""
    return rng.random() < spec.rate * multiplier
