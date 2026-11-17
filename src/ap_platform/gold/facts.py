"""
facts — the 13 gold facts.

==============================  ==========================================  =============
table                           grain                                       type
==============================  ==========================================  =============
fact_ap_invoice_header          invoice (tenant, company, doc, FY)          transaction
fact_ap_invoice_line            invoice line                                transaction
fact_ap_payment                 payment document                            transaction
fact_ap_payment_application     payment x invoice application (bridge)      transaction
fact_ap_open_item_snapshot      open item x snapshot date                   periodic snap
fact_grir_snapshot              PO line x snapshot date                     periodic snap
fact_invoice_lifecycle          invoice, one row, milestone timestamps      accumulating
fact_approval_event             approval workflow event                     transaction
fact_duplicate_candidate        scored invoice pair                         factless-ish
fact_vendor_change_event        vendor attribute / bank change              transaction
fact_match_exception            invoice line that did not match cleanly     transaction
fact_dq_result                  rule x tenant x entity x batch              transaction
fact_reconciliation             silver-to-gold check x tenant x table       transaction
==============================  ==========================================  =============

Every builder reads silver columns by their canonical names, the ones
``schema_mapping`` declares. If a builder fails on a missing column, compare
``spark.table(<silver table>).columns`` with what the builder selects: that
is the most likely real bug in this layer, and the fix is here, not in config.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import tables
from ..silver import dq as DQ
from ..silver.pipeline import Build, JobContext
from .keys import G, as_of_date, date_key, need, resolve_scd2, sk, with_group_amount

S = "ap_silver.ap."


def _s(ctx: JobContext, name: str) -> DataFrame:
    need(ctx.spark, S + name)
    df = ctx.spark.table(S + name)
    if "is_current" in df.columns:
        df = df.filter("is_current")
    if "is_deleted" in df.columns:
        df = df.filter(~F.col("is_deleted"))
    if ctx.tenants:
        df = df.filter(F.col("tenant_id").isin(ctx.tenants))
    return df


def _g(ctx: JobContext, name: str) -> DataFrame:
    need(ctx.spark, G + name)
    return ctx.spark.table(G + name)


def _done(ctx: JobContext, df: DataFrame, n_in: int | None = None, **kw) -> Build:
    df = df.withColumn("gold_batch_id", F.lit(ctx.batch_id)).withColumn("gold_updated_at", F.current_timestamp())
    n = df.count()
    return Build(df=df, records_in=n if n_in is None else n_in, records_out=n, full_set=True, **kw)


# ---------------------------------------------------------------------------

def fact_invoice_header(ctx: JobContext) -> Build:
    spark = ctx.spark
    h = _s(ctx, "sl_invoice_header")
    ap = _s(ctx, "sl_ap_item").select("tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "due_date",
                                      "paid_amount", "open_amount", "is_open")
    df = h.join(ap, ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year"], "left")
    df = resolve_scd2(df, _g(ctx, "dim_vendor"), ["tenant_id", "vendor_id"], "invoice_date", "vendor_key")
    df = with_group_amount(spark, df, "gross_amount", "currency", "invoice_date", "gross_amount_group")
    out = df.select(
        "tenant_id", "company_code", "invoice_doc_id", "fiscal_year",
        sk("tenant_id", "company_code", "invoice_doc_id", "fiscal_year").alias("invoice_key"),
        sk("tenant_id", "company_code").alias("company_key"), "vendor_key", "vendor_id",
        "supplier_invoice_number", "po_number", F.col("po_number").isNotNull().alias("has_po"),
        date_key("invoice_date").alias("invoice_date_key"), date_key("posting_date").alias("posting_date_key"),
        date_key("due_date").alias("due_date_key"),
        "invoice_date", "posting_date", "due_date", "capture_ts",
        F.col("doc_type_canonical").alias("doc_type_code"),
        F.col("capture_channel_canonical").alias("capture_channel_code"),
        F.col("payment_terms_canonical").alias("payment_terms_code"),
        F.col("payment_block_reason"), F.col("payment_block_reason").isNotNull().alias("is_blocked"),
        "currency", "gross_amount", "tax_amount", (F.col("gross_amount") - F.coalesce("tax_amount", F.lit(0))).alias("net_amount"),
        "gross_amount_group", "gross_amount_group_fx_missing",
        "paid_amount", "open_amount", "is_open",
        F.datediff("posting_date", "invoice_date").alias("days_invoice_to_post"),
        F.col("dq_warnings"), F.col("_version"))
    return _done(ctx, out)


fact_invoice_header.columns = ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "vendor_key"]


def fact_invoice_line(ctx: JobContext) -> Build:
    spark = ctx.spark
    l = _s(ctx, "sl_invoice_line")
    hdr = _g(ctx, "fact_ap_invoice_header").select(
        "tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "invoice_key", "vendor_key",
        "invoice_date", "posting_date_key")
    m = _s(ctx, "sl_three_way_match").select("tenant_id", "company_code", "invoice_doc_id", "fiscal_year",
                                             "line_number", "match_status", "price_variance_pct")
    k = ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year"]
    df = l.join(hdr, k, "left").join(m, k + ["line_number"], "left")
    df = with_group_amount(spark, df, "net_amount", "currency", "invoice_date", "net_amount_group") \
        if "currency" in df.columns else df
    if "currency" not in l.columns:
        cur = _s(ctx, "sl_invoice_header").select(*k, "currency")
        df = with_group_amount(spark, df.join(cur, k, "left"), "net_amount", "currency", "invoice_date",
                               "net_amount_group")
    out = df.select(
        *k, "line_number", "invoice_key", "vendor_key", "posting_date_key",
        sk("tenant_id", "gl_account").alias("gl_account_key"), sk("tenant_id", "cost_center").alias("cost_center_key"),
        "gl_account", "cost_center", "po_number", "po_line_number", "quantity", "uom", "tax_code_src",
        "currency", "net_amount", "net_amount_group", "net_amount_group_fx_missing",
        F.coalesce("match_status", F.when(F.col("po_number").isNull(), "NO_PO")).alias("match_status"),
        "price_variance_pct", F.col("invoice_key").isNull().alias("is_orphan_line"))
    return _done(ctx, out)


fact_invoice_line.columns = ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "line_number"]


def fact_payment(ctx: JobContext) -> Build:
    spark = ctx.spark
    p = _s(ctx, "sl_payment")
    df = resolve_scd2(p, _g(ctx, "dim_vendor"), ["tenant_id", "vendor_id"], "payment_date", "vendor_key")
    df = with_group_amount(spark, df, "payment_amount", "currency", "payment_date", "payment_amount_group")
    out = df.select(
        "tenant_id", "company_code", "payment_doc_id", "fiscal_year",
        sk("tenant_id", "company_code", "payment_doc_id", "fiscal_year").alias("payment_key"),
        "vendor_key", "vendor_id", date_key("payment_date").alias("payment_date_key"), "payment_date",
        "run_id", "run_date", "payment_method_src", "is_proposal", "is_void", "currency", "payment_amount",
        "payment_amount_group", "payment_amount_group_fx_missing")
    return _done(ctx, out)


fact_payment.columns = ["tenant_id", "payment_doc_id", "fiscal_year", "payment_key"]


def fact_payment_application(ctx: JobContext) -> Build:
    pa = _s(ctx, "sl_payment_application")
    pay = _g(ctx, "fact_ap_payment").select("tenant_id", "company_code", "payment_doc_id", "payment_key",
                                            "payment_date", "is_void", "vendor_key")
    inv = _g(ctx, "fact_ap_invoice_header").select(
        F.col("tenant_id").alias("_it"), F.col("company_code").alias("_icc"), F.col("invoice_doc_id").alias("_idoc"),
        F.col("fiscal_year").alias("_ify"), "invoice_key", "invoice_date", "due_date", "gross_amount")
    df = pa.join(pay, ["tenant_id", "company_code", "payment_doc_id"], "left")
    cond = ((F.col("tenant_id") == F.col("_it")) & (F.col("invoice_doc_id") == F.col("_idoc"))
            & (F.col("invoice_fiscal_year").isNull() | (F.col("invoice_fiscal_year") == F.col("_ify")))
            & (F.col("invoice_company_code").isNull() | (F.col("invoice_company_code") == F.col("_icc"))))
    df = df.join(inv, cond, "left").drop("_it")
    # A doc id that matches several invoices (different fiscal years, source gave
    # none) would fan out; keep the invoice nearest before the payment.
    w = Window.partitionBy("tenant_id", "company_code", "run_date", "run_id", "payment_doc_id", "invoice_doc_id",
                           "invoice_fiscal_year", "invoice_line_number", "application_id") \
        .orderBy(F.when(F.col("invoice_date") <= F.col("payment_date"), 0).otherwise(1), F.col("invoice_date").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    out = df.select(
        "tenant_id", "company_code", "run_date", "run_id", "payment_doc_id", "invoice_doc_id",
        "invoice_fiscal_year", "invoice_line_number", "application_id",
        sk("tenant_id", "company_code", "run_date", "run_id", "payment_doc_id", "invoice_doc_id",
           "invoice_fiscal_year", "invoice_line_number", "application_id").alias("application_key"),
        "payment_key", "invoice_key", "vendor_key", "payment_date", "invoice_date", "due_date", "currency",
        "applied_amount", "discount_taken",
        F.datediff("payment_date", "invoice_date").alias("days_to_pay"),
        F.datediff("payment_date", "due_date").alias("days_after_due"),
        (F.datediff("payment_date", "due_date") <= 0).alias("paid_on_time"),
        (F.abs("applied_amount") + F.abs(F.coalesce("discount_taken", F.lit(0))) < F.abs("gross_amount") - 0.01)
        .alias("is_partial"),
        F.coalesce("is_void", F.lit(False)).alias("is_void"))
    return _done(ctx, out)


fact_payment_application.columns = ["tenant_id", "company_code", "run_date", "run_id", "payment_doc_id",
                                    "invoice_doc_id", "invoice_fiscal_year", "invoice_line_number", "application_id"]


def fact_open_item_snapshot(ctx: JobContext) -> Build:
    """Open items AS OF the snapshot date, recomputed from silver, not copied
    from the current-state sl_ap_item: aging as at a past date cannot be read
    off today's state once partial payments and voids exist."""
    spark = ctx.spark
    asof = as_of_date(spark, getattr(ctx, "as_of", None))
    h = _s(ctx, "sl_invoice_header").filter(F.col("posting_date") <= asof)
    ap = _s(ctx, "sl_ap_item").select("tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "due_date",
                                      "accounting_doc_id", "line_item")
    paid = (_g(ctx, "fact_ap_payment_application").filter((F.col("payment_date") <= asof) & ~F.col("is_void"))
            .groupBy("tenant_id", "invoice_key")
            .agg(F.sum(F.abs("applied_amount") + F.abs(F.coalesce("discount_taken", F.lit(0)))).alias("settled")))
    k = ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year"]
    df = (h.join(ap, k, "left")
           .withColumn("invoice_key", sk("tenant_id", "company_code", "invoice_doc_id", "fiscal_year"))
           .join(paid, ["tenant_id", "invoice_key"], "left"))
    sign = F.when(F.col("gross_amount") < 0, -1).otherwise(1)
    open_abs = F.greatest(F.abs("gross_amount") - F.coalesce("settled", F.lit(0)), F.lit(0))
    df = (df.withColumn("open_amount", open_abs * sign).filter(open_abs > 0.01)
            .withColumn("snapshot_date", asof)
            .withColumn("days_past_due", F.datediff(F.col("snapshot_date"), F.col("due_date"))))
    df = resolve_scd2(df, _g(ctx, "dim_vendor"), ["tenant_id", "vendor_id"], "snapshot_date", "vendor_key")
    df = with_group_amount(spark, df, "open_amount", "currency", "snapshot_date", "open_amount_group")
    bucket = (F.when(F.col("days_past_due") <= 0, "CURRENT").when(F.col("days_past_due") <= 30, "1-30")
               .when(F.col("days_past_due") <= 60, "31-60").when(F.col("days_past_due") <= 90, "61-90")
               .otherwise("90+"))
    out = df.select(
        "tenant_id", "snapshot_date", date_key("snapshot_date").alias("snapshot_date_key"), "company_code",
        F.coalesce("accounting_doc_id", "invoice_doc_id").alias("accounting_doc_id"), "fiscal_year",
        F.coalesce("line_item", F.lit(1)).alias("line_item"), "invoice_key", "vendor_key", "vendor_id",
        "invoice_date", "due_date", "days_past_due", bucket.alias("aging_bucket"), "currency", "gross_amount",
        "open_amount", "open_amount_group", "open_amount_group_fx_missing",
        F.col("payment_block_reason").isNotNull().alias("is_blocked"))
    b = _done(ctx, out)
    d = out.select("snapshot_date").limit(1).collect()
    b.replace_predicate = f"snapshot_date = DATE'{d[0].snapshot_date}'" if d else "1 = 0"
    return b


fact_open_item_snapshot.columns = ["tenant_id", "snapshot_date", "company_code", "accounting_doc_id",
                                   "fiscal_year", "line_item"]


def fact_grir_snapshot(ctx: JobContext) -> Build:
    asof = as_of_date(ctx.spark, getattr(ctx, "as_of", None))
    g = _s(ctx, "sl_grir_position").withColumn("snapshot_date", asof)
    out = g.select("tenant_id", "snapshot_date", date_key("snapshot_date").alias("snapshot_date_key"),
                   "company_code", "po_number", "po_line_number", "vendor_id", "currency", "ordered_qty",
                   "received_qty", "invoiced_qty", "received_value", "invoiced_value", "grir_qty", "grir_value",
                   "grir_status", "last_receipt_date",
                   F.datediff(F.col("snapshot_date"), F.col("last_receipt_date")).alias("days_since_receipt"))
    b = _done(ctx, out)
    d = out.select("snapshot_date").limit(1).collect()
    b.replace_predicate = f"snapshot_date = DATE'{d[0].snapshot_date}'" if d else "1 = 0"
    return b


fact_grir_snapshot.columns = ["tenant_id", "snapshot_date", "po_number", "po_line_number"]


def fact_invoice_lifecycle(ctx: JobContext) -> Build:
    """Accumulating snapshot: one row per invoice, milestone timestamps filled
    in as they happen. touch_count is the number of routing steps before the
    final approval (touchless = approved in one step).
    approval_evidence says whether the tenant even has an approval stream:
    a zero touch count without one is absence of evidence, not touchless."""
    spark = ctx.spark
    h = _s(ctx, "sl_invoice_header")
    k = ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year"]
    if tables.exists(spark, S + "sl_approval_event"):
        ev = _s(ctx, "sl_approval_event")
        appr = ev.groupBy(*k).agg(
            F.min("event_ts").alias("first_approval_ts"),
            F.max(F.when(F.col("event_type").isin("APPROVED", "APPROVE", "FINAL_APPROVAL"), F.col("event_ts")))
             .alias("final_approval_ts"),
            # A "touch" is a routing step before the final approval (ROUTED, NO
            # RESPONSE, ...). The sources send no auto-approval event, so the
            # strict definition (no human approval at all) is not measurable;
            # touchless here means approved in one step, with no routing.
            F.sum(F.when(~F.col("event_type").isin("APPROVED", "APPROVE", "FINAL_APPROVAL", "AUTO_APPROVED"), 1)
                  .otherwise(0)).alias("touch_count"),
            F.count("*").alias("approval_events"))
    else:
        appr = h.select(*k).limit(0).withColumn("first_approval_ts", F.lit(None).cast("timestamp")) \
                .withColumn("final_approval_ts", F.lit(None).cast("timestamp")) \
                .withColumn("touch_count", F.lit(0)).withColumn("approval_events", F.lit(0))
    pay = (_s(ctx, "sl_ap_item").select(*k, "last_payment_date", "is_open", "payment_count"))
    tenants = spark.table(G + "dim_tenant").select("tenant_id", "has_approval_stream") \
        if tables.exists(spark, G + "dim_tenant") else None
    df = h.join(appr, k, "left").join(pay, k, "left")
    if tenants is not None:
        df = df.join(tenants, "tenant_id", "left")
    else:
        df = df.withColumn("has_approval_stream", F.lit(None).cast("boolean"))
    capture = F.coalesce(F.col("capture_ts"), F.col("posting_date").cast("timestamp"))
    out = df.select(
        *k, sk(*k).alias("invoice_key"), "vendor_id",
        F.to_date(capture).alias("capture_date"), "capture_ts", "invoice_date", "posting_date",
        "first_approval_ts", "final_approval_ts",
        F.when(~F.coalesce("is_open", F.lit(True)), F.col("last_payment_date")).alias("fully_paid_date"),
        F.coalesce("touch_count", F.lit(0)).alias("touch_count"),
        F.coalesce("approval_events", F.lit(0)).alias("approval_events"),
        F.when(F.col("has_approval_stream"), "EVENT_STREAM").otherwise("NONE").alias("approval_evidence"),
        (F.coalesce("touch_count", F.lit(0)) == 0).alias("is_touchless"),
        F.datediff(F.coalesce(F.to_date("final_approval_ts"), F.lit(None)), F.to_date(capture)).alias("days_capture_to_approval"),
        F.datediff(F.col("last_payment_date"), F.to_date(capture)).alias("days_capture_to_pay"),
        F.col("capture_channel_canonical").alias("capture_channel_code"))
    return _done(ctx, out)


fact_invoice_lifecycle.columns = ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "capture_date"]


def fact_approval_event(ctx: JobContext) -> Build:
    e = _s(ctx, "sl_approval_event")
    out = e.select("tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "event_seq",
                   sk("tenant_id", "company_code", "invoice_doc_id", "fiscal_year").alias("invoice_key"),
                   "event_type", "actor", "event_ts", date_key(F.to_date("event_ts")).alias("event_date_key"),
                   "event_comment")
    return _done(ctx, out)


fact_approval_event.columns = ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "event_seq"]


def fact_duplicate_candidate(ctx: JobContext) -> Build:
    d = _s(ctx, "sl_duplicate_candidate")
    out = d.select("tenant_id", "candidate_pair_id", "vendor_id", "company_code_a", "invoice_doc_id_a",
                   "fiscal_year_a", "company_code_b", "invoice_doc_id_b", "fiscal_year_b", "gross_amount",
                   "days_apart", "same_normalised_number", "same_exact_number", "same_amount", "score",
                   "score_band", "disposition", "scored_at")
    if tables.exists(ctx.spark, G + "fact_duplicate_candidate"):
        # dispositions are recorded in gold by reviewers: never overwrite them
        prev = ctx.spark.table(G + "fact_duplicate_candidate").select(
            "tenant_id", "candidate_pair_id", F.col("disposition").alias("_prev_disp"))
        out = (out.join(prev, ["tenant_id", "candidate_pair_id"], "left")
                  .withColumn("disposition", F.coalesce("_prev_disp", "disposition")).drop("_prev_disp"))
    return _done(ctx, out)


fact_duplicate_candidate.columns = ["tenant_id", "candidate_pair_id"]


def fact_vendor_change_event(ctx: JobContext) -> Build:
    spark = ctx.spark
    ev = _s(ctx, "sl_vendor_change_event")
    pay = _s(ctx, "sl_payment").filter(~F.coalesce(F.col("is_void"), F.lit(False))).select(
        "tenant_id", "vendor_id", "payment_date", "payment_amount")
    nxt = (ev.alias("e").join(pay.alias("p"), (F.col("e.tenant_id") == F.col("p.tenant_id"))
                              & (F.col("e.vendor_id") == F.col("p.vendor_id"))
                              & (F.col("p.payment_date") >= F.col("e.change_date")), "left")
             .groupBy("e.tenant_id", "e.vendor_id", "e.change_type", "e.valid_from")
             .agg(F.min("p.payment_date").alias("next_payment_date")))
    df = ev.join(nxt, ["tenant_id", "vendor_id", "change_type", "valid_from"], "left")
    df = resolve_scd2(df, _g(ctx, "dim_vendor"), ["tenant_id", "vendor_id"], "valid_from", "vendor_key")
    out = df.select(
        "tenant_id", "vendor_id", "change_type", "valid_from", "vendor_key", "change_date",
        date_key("change_date").alias("change_date_key"), "changed_by", "is_verified", "changed_fields",
        "bank_country", "bank_account_last4", "next_payment_date",
        F.datediff("next_payment_date", "change_date").alias("days_to_next_payment"),
        ((F.col("change_type") == "BANK_ACCOUNT") & ~F.coalesce("is_verified", F.lit(False))
         & (F.datediff("next_payment_date", "change_date") <= 14)).alias("is_fraud_signal"))
    # GOLD-layer DQ rules (DQ_VEN_BNK_0703 / 0704) are evaluated here, WARN only.
    rules = DQ.load_rules(spark, "GOLD", "vendor_change_event")
    tenants = [r.tenant_id for r in out.select("tenant_id").distinct().collect()]
    gate = DQ.evaluate_rows(out.withColumn("is_deleted", F.lit(False)), rules, DQ.Context(spark, "vendor_change_event",
                                                                                           ["tenant_id", "vendor_id"]),
                            tenants)
    aggs, ne, _ = DQ.evaluate_aggregates(out.withColumn("is_deleted", F.lit(False)), rules, tenants)
    rows = [o.row(ctx.batch_id, "GOLD", "vendor_change_event") for o in gate.outcomes + aggs]
    rows += DQ.not_evaluated_rows(gate.not_evaluated + ne, tenants, ctx.batch_id, "GOLD", "vendor_change_event")
    return _done(ctx, out, dq_rows=rows, rules_evaluated=len(gate.outcomes) + len(aggs))


fact_vendor_change_event.columns = ["tenant_id", "vendor_id", "change_type", "valid_from"]


def fact_match_exception(ctx: JobContext) -> Build:
    m = _s(ctx, "sl_three_way_match").filter(~F.col("match_status").isin("MATCHED", "MATCHED_TWO_WAY"))
    lines = _g(ctx, "fact_ap_invoice_line").select("tenant_id", "company_code", "invoice_doc_id", "fiscal_year",
                                                   "line_number", "vendor_key", "invoice_key", "net_amount_group")
    k = ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "line_number"]
    out = m.join(lines, k, "left").select(
        *k, "invoice_key", "vendor_key", "match_status", "po_reference", "po_number_resolved", "po_line_number",
        "invoiced_qty", "received_qty", "po_qty", "invoice_unit_price", "po_unit_price", "price_variance_pct",
        "line_amount", F.col("net_amount_group").alias("line_amount_group"), "evaluated_at")
    return _done(ctx, out)


fact_match_exception.columns = ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "line_number"]


def fact_dq_result(ctx: JobContext) -> Build:
    d = ctx.spark.table("ap_control.ops.dq_result_log")
    out = d.select("batch_id", "rule_id", "tenant_id", "layer", "entity", "dq_dimension", "severity",
                   "evaluated_at", date_key(F.to_date("evaluated_at")).alias("evaluated_date_key"),
                   "records_evaluated", "records_passed", "records_failed", "pass_rate_pct", "threshold_pct",
                   "outcome", "action_taken")
    return _done(ctx, out)


fact_dq_result.columns = ["batch_id", "rule_id", "tenant_id", "layer", "entity"]


def fact_reconciliation(ctx: JobContext) -> Build:
    """Silver-to-gold reconciliation: counts and amounts per tenant per table.
    Every variance is explained or it is a bug; ``explanation`` says which."""
    spark = ctx.spark
    checks = [
        ("sl_invoice_header", "fact_ap_invoice_header", "gross_amount", "gross_amount"),
        ("sl_invoice_line", "fact_ap_invoice_line", "net_amount", "net_amount"),
        ("sl_payment", "fact_ap_payment", "payment_amount", "payment_amount"),
        ("sl_payment_application", "fact_ap_payment_application", "applied_amount", "applied_amount"),
    ]
    rows = []
    for sl, gd, sa, ga in checks:
        if not (tables.exists(spark, S + sl) and tables.exists(spark, G + gd)):
            continue
        s = _s(ctx, sl).groupBy("tenant_id").agg(F.count("*").alias("sc"), F.sum(sa).alias("sa"))
        g = spark.table(G + gd).groupBy("tenant_id").agg(F.count("*").alias("gc"), F.sum(ga).alias("ga"))
        for r in s.join(g, "tenant_id", "full").collect():
            sc, gc = int(r.sc or 0), int(r.gc or 0)
            sav, gav = float(r.sa or 0), float(r.ga or 0)
            ok = sc == gc and abs(sav - gav) <= 0.01
            rows.append((r.tenant_id, sl, gd, sc, gc, sc - gc, round(sav, 2), round(gav, 2), round(sav - gav, 2),
                         "MATCHED" if ok else "VARIANCE",
                         None if ok else "gold row count or amount differs from silver: a failed or skipped job, "
                                         "or a fan-out/collapse in the builder"))
    out = spark.createDataFrame(rows, "tenant_id STRING, silver_table STRING, gold_table STRING, silver_count BIGINT, "
                                      "gold_count BIGINT, count_variance BIGINT, silver_amount DOUBLE, gold_amount DOUBLE, "
                                      "amount_variance DOUBLE, status STRING, explanation STRING") \
        .withColumn("checked_at", F.current_timestamp())
    recon = [{"recon_id": f"{ctx.batch_id}|{r[2]}|{r[0]}", "batch_id": ctx.batch_id, "tenant_id": r[0],
              "recon_type": "SILVER_TO_GOLD", "entity": r[2], "source_count": r[3], "target_count": r[4],
              "count_variance": r[5], "status": r[9], "explanation": r[10]} for r in rows]
    return _done(ctx, out, recon_rows=recon)


fact_reconciliation.columns = ["tenant_id", "gold_table"]
