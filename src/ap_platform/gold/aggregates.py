"""
aggregates — four pre-computed rollups for the dashboards.

==========================  ===================================================
agg_ap_aging_daily          tenant x snapshot date x company x currency x bucket
agg_dpo_monthly             tenant x month: spend, payments, days to pay, on time
agg_vendor_spend_monthly    tenant x vendor x month: invoice count and spend
agg_dq_scorecard_daily      tenant x layer x entity x day: rules and pass rates
==========================  ===================================================

Each is a pure function of gold facts, so it can be dropped and rebuilt at
any time. Amounts are in group currency unless the column says otherwise.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from ..silver.pipeline import Build, JobContext
from .facts import _done, _g


def agg_ap_aging_daily(ctx: JobContext) -> Build:
    s = _g(ctx, "fact_ap_open_item_snapshot")
    latest = s.agg(F.max("snapshot_date").alias("d")).collect()[0].d
    s = s.filter(F.col("snapshot_date") == F.lit(latest))
    out = (s.groupBy("tenant_id", "snapshot_date", "company_code", "currency", "aging_bucket")
             .agg(F.count("*").alias("open_items"), F.sum("open_amount").alias("open_amount"),
                  F.sum("open_amount_group").alias("open_amount_group"),
                  F.sum(F.when(F.col("is_blocked"), F.col("open_amount_group"))).alias("blocked_amount_group"),
                  F.sum(F.col("open_amount_group_fx_missing").cast("int")).alias("items_missing_fx")))
    b = _done(ctx, out)
    b.replace_predicate = f"snapshot_date = DATE'{latest}'" if latest else "1 = 0"
    return b


agg_ap_aging_daily.columns = ["tenant_id", "snapshot_date", "company_code", "currency", "aging_bucket"]


def agg_dpo_monthly(ctx: JobContext) -> Build:
    inv = _g(ctx, "fact_ap_invoice_header")
    app = _g(ctx, "fact_ap_payment_application").filter(~F.col("is_void"))
    spend = (inv.groupBy("tenant_id", F.date_format("posting_date", "yyyy-MM").alias("year_month"))
                .agg(F.count("*").alias("invoices_posted"), F.sum("gross_amount_group").alias("spend_group")))
    pay = (app.groupBy("tenant_id", F.date_format("payment_date", "yyyy-MM").alias("year_month"))
              .agg(F.count("*").alias("applications"),
                   F.sum("applied_amount").alias("applied_amount_txn"),
                   (F.sum(F.col("days_to_pay") * F.abs("applied_amount")) / F.sum(F.abs("applied_amount")))
                   .alias("weighted_days_to_pay"),
                   F.avg(F.col("paid_on_time").cast("double")).alias("on_time_rate"),
                   F.sum(F.abs(F.coalesce("discount_taken", F.lit(0)))).alias("discount_taken_txn")))
    out = spend.join(pay, ["tenant_id", "year_month"], "full")
    return _done(ctx, out)


agg_dpo_monthly.columns = ["tenant_id", "year_month"]


def agg_vendor_spend_monthly(ctx: JobContext) -> Build:
    inv = _g(ctx, "fact_ap_invoice_header")
    out = (inv.groupBy("tenant_id", "vendor_id", F.date_format("invoice_date", "yyyy-MM").alias("year_month"))
              .agg(F.count("*").alias("invoices"), F.sum("gross_amount_group").alias("spend_group"),
                   F.sum(F.col("has_po").cast("int")).alias("invoices_with_po"),
                   F.max("vendor_key").alias("vendor_key")))
    return _done(ctx, out)


agg_vendor_spend_monthly.columns = ["tenant_id", "vendor_id", "year_month"]


def agg_dq_scorecard_daily(ctx: JobContext) -> Build:
    d = _g(ctx, "fact_dq_result")
    out = (d.groupBy("tenant_id", "layer", "entity", F.to_date("evaluated_at").alias("eval_date"))
            .agg(F.countDistinct(F.when(F.col("outcome") != "NOT_EVALUATED", F.col("rule_id"))).alias("rules_evaluated"),
                 F.countDistinct(F.when(F.col("outcome") == "NOT_EVALUATED", F.col("rule_id"))).alias("rules_not_evaluated"),
                 F.sum("records_failed").alias("records_failed"), F.sum("records_evaluated").alias("records_evaluated"),
                 F.countDistinct(F.when(F.col("outcome").isin("FAIL", "THRESHOLD_BREACH"), F.col("rule_id")))
                 .alias("rules_alerting")))
    out = out.withColumn("record_pass_pct", F.round(100 * (1 - F.col("records_failed") / F.col("records_evaluated")), 3))
    return _done(ctx, out)


agg_dq_scorecard_daily.columns = ["tenant_id", "layer", "entity", "eval_date"]
