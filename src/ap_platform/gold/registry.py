"""
registry — every gold job id mapped to the builder that produces it.

There is no fallback. A job in transformation_config without an entry here
fails the plan, before anything writes: config alone must not be able to
conjure a gold table, because every gold table is a decision about grain.
"""

from __future__ import annotations

from . import aggregates as A
from . import dimensions as D
from . import facts as F

REGISTRY = {
    # dimensions
    "GD_DIM_DATE": D.dim_date,
    "GD_DIM_FISCAL_CALENDAR": D.dim_fiscal_calendar,
    "GD_DIM_TENANT": D.dim_tenant,
    "GD_DIM_COMPANY": D.dim_company,
    "GD_DIM_VENDOR": D.dim_vendor,
    "GD_DIM_VENDOR_BANK": D.dim_vendor_bank,
    "GD_DIM_CURRENCY": D.dim_currency,
    "GD_DIM_PAYMENT_TERMS": D.dim_payment_terms,
    "GD_DIM_GL_ACCOUNT": D.dim_gl_account,
    "GD_DIM_COST_CENTER": D.dim_cost_center,
    "GD_DIM_DOC_TYPE": D.dim_doc_type,
    "GD_DIM_CAPTURE_CHANNEL": D.dim_capture_channel,
    "GD_DIM_DQ_RULE": D.dim_dq_rule,
    # facts
    "GD_FACT_INVOICE_HEADER": F.fact_invoice_header,
    "GD_FACT_INVOICE_LINE": F.fact_invoice_line,
    "GD_FACT_PAYMENT": F.fact_payment,
    "GD_FACT_PAYMENT_APPLICATION": F.fact_payment_application,
    "GD_FACT_OPEN_ITEM_SNAPSHOT": F.fact_open_item_snapshot,
    "GD_FACT_GRIR_SNAPSHOT": F.fact_grir_snapshot,
    "GD_FACT_INVOICE_LIFECYCLE": F.fact_invoice_lifecycle,
    "GD_FACT_APPROVAL_EVENT": F.fact_approval_event,
    "GD_FACT_DUPLICATE_CANDIDATE": F.fact_duplicate_candidate,
    "GD_FACT_VENDOR_CHANGE_EVENT": F.fact_vendor_change_event,
    "GD_FACT_MATCH_EXCEPTION": F.fact_match_exception,
    "GD_FACT_DQ_RESULT": F.fact_dq_result,
    "GD_FACT_RECONCILIATION": F.fact_reconciliation,
    # aggregates
    "GD_AGG_AP_AGING_DAILY": A.agg_ap_aging_daily,
    "GD_AGG_DPO_MONTHLY": A.agg_dpo_monthly,
    "GD_AGG_VENDOR_SPEND_MONTHLY": A.agg_vendor_spend_monthly,
    "GD_AGG_DQ_SCORECARD_DAILY": A.agg_dq_scorecard_daily,
}
