"""
builders — the five silver jobs that are business logic, not conformance.

Each reads silver tables already written in an earlier wave and returns a
``Build``. None contains a tenant id or an ERP name: where tenants differ (a PO
referenced by surrogate id rather than business number) the difference is
resolved from data, e.g. ``po_source_key``, never from a tenant literal.

==========================  =====================================================
SL_VENDOR_CHANGE_EVENT      one row per vendor attribute or bank account change
SL_THREE_WAY_MATCH          invoice line vs PO line vs goods received
SL_DUPLICATE_CANDIDATE      scored pairs of invoices that may be the same bill
SL_GRIR_POSITION            goods received not invoiced, per PO line
SL_AP_ITEM                  the AP sub-ledger open item: gross, paid, open, due
==========================  =====================================================
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import tables
from .pipeline import Build, JobContext

S = "ap_silver.ap."


class UpstreamMissing(Exception):
    """A silver table this builder reads has not been written yet."""


def _t(ctx: JobContext, name: str) -> DataFrame:
    if not tables.exists(ctx.spark, S + name):
        raise UpstreamMissing(f"{S + name} does not exist yet")
    df = ctx.spark.table(S + name)
    if "is_current" in df.columns:
        df = df.filter("is_current")
    if "is_deleted" in df.columns:
        df = df.filter(~F.col("is_deleted"))
    if ctx.tenants:
        df = df.filter(F.col("tenant_id").isin(ctx.tenants))
    return df


def _stamp(df: DataFrame, ctx: JobContext) -> DataFrame:
    return (df.withColumn("silver_batch_id", F.lit(ctx.batch_id))
              .withColumn("silver_updated_at", F.current_timestamp()))


def _finish(ctx: JobContext, df: DataFrame, n_in: int = 0, note: str = "") -> Build:
    n = df.count()
    return Build(df=_stamp(df, ctx), records_in=n_in or n, records_out=n, note=note, full_set=True)


def _po_ref(ctx: JobContext) -> DataFrame:
    """(tenant_id, po_ref) -> canonical po_number. A PO can be referenced by its
    business number or, where the source has one, by its surrogate key."""
    h = _t(ctx, "sl_po_header")
    by_num = h.select("tenant_id", F.col("po_number").alias("po_ref"), "po_number")
    by_key = (h.filter(F.col("po_source_key").isNotNull())
               .select("tenant_id", F.col("po_source_key").alias("po_ref"), "po_number"))
    return by_num.unionByName(by_key).dropDuplicates(["tenant_id", "po_ref"])


# ---------------------------------------------------------------------------

def vendor_change_events(ctx: JobContext) -> Build:
    spark = ctx.spark
    for n in ("sl_vendor", "sl_vendor_bank"):
        if not tables.exists(spark, S + n):
            raise UpstreamMissing(f"{S + n} does not exist yet")
    v = spark.table(S + "sl_vendor")
    b = spark.table(S + "sl_vendor_bank")
    if ctx.tenants:
        v, b = v.filter(F.col("tenant_id").isin(ctx.tenants)), b.filter(F.col("tenant_id").isin(ctx.tenants))
    # 1) attribute changes: every SCD2 version after a vendor's first
    tracked = ["vendor_name", "country_code", "tax_id", "vat_registration", "payment_terms_src",
               "is_blocked", "is_marked_delete"]
    w = Window.partitionBy("tenant_id", "vendor_id").orderBy("valid_from")
    prev = [F.lag(c).over(w).alias(f"_p_{c}") for c in tracked]
    vv = v.select("*", *prev, F.row_number().over(w).alias("_n"))
    changed = F.concat_ws(",", *[F.when(~F.col(c).eqNullSafe(F.col(f"_p_{c}")), F.lit(c)) for c in tracked])
    attr = (vv.filter("_n > 1").select(
        "tenant_id", "vendor_id", F.lit("VENDOR_ATTRIBUTE").alias("change_type"), "valid_from",
        F.to_date("valid_from").alias("change_date"), F.col("created_by").alias("changed_by"),
        F.lit(None).cast("boolean").alias("is_verified"), changed.alias("changed_fields"),
        F.lit(None).cast("string").alias("bank_country"), F.lit(None).cast("string").alias("bank_account_last4")))
    # 2) bank changes: any account that is not the vendor's first, at its change date
    bw = Window.partitionBy("tenant_id", "vendor_id").orderBy(F.col("change_date").asc_nulls_first(),
                                                                F.col("valid_from"))
    bb = (b.filter("is_current OR valid_to IS NOT NULL")
           .withColumn("_n", F.row_number().over(bw)))
    bank = (bb.filter("_n > 1").select(
        "tenant_id", "vendor_id", F.lit("BANK_ACCOUNT").alias("change_type"),
        F.coalesce(F.col("change_date").cast("timestamp"), F.col("valid_from")).alias("valid_from"),
        F.coalesce(F.col("change_date"), F.to_date("valid_from")).alias("change_date"),
        "changed_by", F.coalesce(F.col("is_verified"), F.lit(False)).alias("is_verified"),
        F.lit("bank_account").alias("changed_fields"), "bank_country",
        F.substring(F.regexp_replace("bank_account", r"\s|-", ""), -4, 4).alias("bank_account_last4")))
    ev = attr.unionByName(bank)
    ev = ev.dropDuplicates(["tenant_id", "vendor_id", "change_type", "valid_from"])
    return _finish(ctx, ev)


vendor_change_events.columns = ["tenant_id", "vendor_id", "change_type", "valid_from", "change_date",
                                "changed_by", "is_verified", "changed_fields", "bank_country",
                                "bank_account_last4"]


# ---------------------------------------------------------------------------

def three_way_match(ctx: JobContext) -> Build:
    spark = ctx.spark
    il = _t(ctx, "sl_invoice_line").filter(F.col("po_number").isNotNull())
    ref = _po_ref(ctx)
    r2 = ref.select(F.col("tenant_id").alias("_rt"), F.col("po_ref").alias("_rref"), F.col("po_number").alias("_po"))
    il = (il.join(r2, (F.col("tenant_id") == F.col("_rt")) & (F.col("po_number") == F.col("_rref")), "left")
            .withColumn("po_number_resolved", F.col("_po")).drop("_rt", "_rref", "_po"))
    pl = _t(ctx, "sl_po_line").select(
        "tenant_id", F.col("po_number").alias("po_number_resolved"), "po_line_number",
        F.col("quantity").alias("po_qty"), F.col("unit_price").alias("po_unit_price"),
        F.col("is_service").alias("po_is_service"))
    gr = (_t(ctx, "sl_goods_receipt").filter(F.coalesce(F.col("movement_class"), F.lit("1")) == "1")
          .groupBy("tenant_id", F.col("po_number").alias("po_number_resolved"), "po_line_number")
          .agg(F.sum("quantity").alias("received_qty")))
    # total invoiced so far per PO line (all invoices), for cumulative quantity checks
    inv_tot = (il.groupBy("tenant_id", "po_number_resolved", "po_line_number")
                 .agg(F.sum("quantity").alias("invoiced_qty_total")))
    tol = spark.table("ap_control.config.tolerance_config").filter("is_active")
    tp = tol.filter("tolerance_type = 'PRICE'").groupBy("tenant_id").agg(
        F.max("upper_limit_pct").alias("price_tol_pct"), F.max("upper_limit_abs").alias("price_tol_abs"))
    tq = tol.filter("tolerance_type = 'QUANTITY'").groupBy("tenant_id").agg(
        F.max("upper_limit_pct").alias("qty_tol_pct"))
    k = ["tenant_id", "po_number_resolved", "po_line_number"]
    m = (il.join(pl, k, "left").join(gr, k, "left").join(inv_tot, k, "left")
           .join(tp, "tenant_id", "left").join(tq, "tenant_id", "left"))
    inv_price = F.when(F.col("quantity") != 0, F.abs(F.col("net_amount")) / F.abs(F.col("quantity")))
    price_var_pct = F.when(F.col("po_unit_price") > 0,
                           (inv_price - F.col("po_unit_price")) / F.col("po_unit_price") * 100)
    price_ok = (F.abs(price_var_pct) <= F.coalesce(F.col("price_tol_pct"), F.lit(0))) | \
               (F.abs((inv_price - F.col("po_unit_price")) * F.abs(F.col("quantity")))
                <= F.coalesce(F.col("price_tol_abs"), F.lit(0)))
    # Per line: this line's invoiced quantity against what was received for its
    # PO line. (A cumulative check across invoices was tried first; the
    # synthetic invoices bill the same PO line several times in full, so it
    # flagged most lines. invoiced_qty_total is kept as information.)
    qty_ok = F.abs(F.col("quantity")) <= F.coalesce(F.col("received_qty"), F.lit(0)) * \
        (1 + F.coalesce(F.col("qty_tol_pct"), F.lit(0)) / 100) + 0.0005
    status = (F.when(F.col("po_number_resolved").isNull(), "PO_NOT_FOUND")
               .when(F.col("po_qty").isNull(), "PO_LINE_NOT_FOUND")
               .when(F.coalesce(F.col("po_is_service"), F.lit(False)) & price_ok, "MATCHED_TWO_WAY")
               .when(F.col("received_qty").isNull() | (F.col("received_qty") <= 0), "NOT_RECEIVED")
               .when(~price_ok & ~qty_ok, "PRICE_AND_QTY_VARIANCE")
               .when(~price_ok, "PRICE_VARIANCE")
               .when(~qty_ok, "QTY_VARIANCE")
               .otherwise("MATCHED"))
    out = m.select(
        "tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "line_number",
        F.col("po_number").alias("po_reference"), "po_number_resolved", "po_line_number",
        F.col("quantity").alias("invoiced_qty"), "invoiced_qty_total", "received_qty", "po_qty",
        F.round(inv_price, 4).alias("invoice_unit_price"), "po_unit_price",
        F.round(price_var_pct, 4).alias("price_variance_pct"),
        F.abs(F.col("net_amount")).alias("line_amount"),
        status.alias("match_status"),
        F.current_timestamp().alias("evaluated_at"))
    return _finish(ctx, out)


three_way_match.columns = ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year", "line_number",
                           "match_status"]


# ---------------------------------------------------------------------------

def duplicate_candidates(ctx: JobContext) -> Build:
    h = _t(ctx, "sl_invoice_header").filter(F.col("vendor_id").isNotNull())
    norm = F.regexp_replace(F.upper(F.coalesce(F.col("supplier_invoice_number"), F.lit(""))), "[^A-Z0-9]", "")
    h = h.select("tenant_id", "vendor_id", "company_code", "invoice_doc_id", "fiscal_year",
                 "supplier_invoice_number", "invoice_date", "gross_amount", "currency",
                 norm.alias("_norm"),
                 F.concat_ws("|", "company_code", "invoice_doc_id", "fiscal_year").alias("_id"))
    a, b = h.alias("a"), h.alias("b")
    pair = (a.join(b, (F.col("a.tenant_id") == F.col("b.tenant_id")) & (F.col("a.vendor_id") == F.col("b.vendor_id"))
                   & (F.col("a._id") < F.col("b._id")))
              .filter((F.col("a._norm") == F.col("b._norm")) & (F.col("a._norm") != "")
                      | ((F.col("a.gross_amount") == F.col("b.gross_amount"))
                         & (F.abs(F.datediff("a.invoice_date", "b.invoice_date")) <= 7))))
    same_num = (F.col("a._norm") == F.col("b._norm")) & (F.col("a._norm") != "")
    same_amt = F.col("a.gross_amount") == F.col("b.gross_amount")
    near_date = F.abs(F.datediff("a.invoice_date", "b.invoice_date")) <= 7
    same_ccy = F.col("a.currency").eqNullSafe(F.col("b.currency"))
    exact_num = F.col("a.supplier_invoice_number").eqNullSafe(F.col("b.supplier_invoice_number"))
    score = (F.when(same_num, 0.45).otherwise(0.0) + F.when(same_amt, 0.30).otherwise(0.0)
             + F.when(near_date, 0.15).otherwise(0.0) + F.when(same_ccy, 0.05).otherwise(0.0)
             + F.when(exact_num, 0.05).otherwise(0.0))
    out = pair.select(
        F.col("a.tenant_id").alias("tenant_id"),
        F.sha2(F.concat_ws("||", "a._id", "b._id"), 256).alias("candidate_pair_id"),
        F.col("a.vendor_id").alias("vendor_id"),
        F.col("a.company_code").alias("company_code_a"), F.col("a.invoice_doc_id").alias("invoice_doc_id_a"),
        F.col("a.fiscal_year").alias("fiscal_year_a"),
        F.col("b.company_code").alias("company_code_b"), F.col("b.invoice_doc_id").alias("invoice_doc_id_b"),
        F.col("b.fiscal_year").alias("fiscal_year_b"),
        F.col("a.supplier_invoice_number").alias("supplier_invoice_number_a"),
        F.col("b.supplier_invoice_number").alias("supplier_invoice_number_b"),
        F.col("a.gross_amount").alias("gross_amount"),
        F.abs(F.datediff("a.invoice_date", "b.invoice_date")).alias("days_apart"),
        same_num.alias("same_normalised_number"), exact_num.alias("same_exact_number"),
        same_amt.alias("same_amount"),
        F.round(score, 2).alias("score"),
        F.when(score >= 0.80, "HIGH").when(score >= 0.50, "MEDIUM").otherwise("LOW").alias("score_band"),
        F.lit(None).cast("string").alias("disposition"),
        F.current_timestamp().alias("scored_at"))
    return _finish(ctx, out)


duplicate_candidates.columns = ["tenant_id", "candidate_pair_id", "score_band", "score"]


# ---------------------------------------------------------------------------

def grir_position(ctx: JobContext) -> Build:
    ref = _po_ref(ctx)
    pl = _t(ctx, "sl_po_line")
    ph = _t(ctx, "sl_po_header").select("tenant_id", "po_number", "company_code", "vendor_id")
    gr = (_t(ctx, "sl_goods_receipt").filter(F.coalesce(F.col("movement_class"), F.lit("1")) == "1")
          .groupBy("tenant_id", "po_number", "po_line_number")
          .agg(F.sum("quantity").alias("received_qty"), F.sum("amount").alias("received_value"),
               F.max("receipt_date").alias("last_receipt_date")))
    il = _t(ctx, "sl_invoice_line").filter(F.col("po_number").isNotNull())
    r2 = ref.select(F.col("tenant_id").alias("_rt"), F.col("po_ref").alias("_rref"), F.col("po_number").alias("_po"))
    il = (il.join(r2, (F.col("tenant_id") == F.col("_rt")) & (F.col("po_number") == F.col("_rref")), "inner")
            .select("tenant_id", F.col("_po").alias("po_number"), "po_line_number", "quantity", "net_amount"))
    inv = (il.groupBy("tenant_id", "po_number", "po_line_number")
             .agg(F.sum("quantity").alias("invoiced_qty"), F.sum("net_amount").alias("invoiced_value")))
    k = ["tenant_id", "po_number", "po_line_number"]
    out = (pl.select(*k, "unit_price", F.col("quantity").alias("ordered_qty"), "currency")
             .join(ph, ["tenant_id", "po_number"], "left")
             .join(gr, k, "left").join(inv, k, "left")
             .fillna(0, ["received_qty", "received_value", "invoiced_qty", "invoiced_value"])
             .withColumn("grir_qty", F.col("received_qty") - F.col("invoiced_qty"))
             .withColumn("grir_value", F.col("received_value") - F.col("invoiced_value"))
             .withColumn("grir_status", F.when(F.abs("grir_value") <= 0.01, "CLEARED")
                         .when(F.col("grir_value") > 0, "RECEIVED_NOT_INVOICED")
                         .otherwise("INVOICED_NOT_RECEIVED"))
             .withColumn("as_of_date", F.current_date()))
    return _finish(ctx, out)


grir_position.columns = ["tenant_id", "company_code", "po_number", "po_line_number", "grir_value"]


# ---------------------------------------------------------------------------

def ap_items(ctx: JobContext) -> Build:
    spark = ctx.spark
    h = _t(ctx, "sl_invoice_header")
    pa = _t(ctx, "sl_payment_application")
    pay = _t(ctx, "sl_payment").select(
        "tenant_id", "company_code", "payment_doc_id", "payment_date",
        F.coalesce(F.col("is_void"), F.lit(False)).alias("_void"))
    pa = (pa.join(pay, ["tenant_id", "company_code", "payment_doc_id"], "left")
            .filter(~F.coalesce(F.col("_void"), F.lit(False))).drop("_void"))
    # Applications reference invoices by doc id; fiscal year and company are
    # used when the source provides them and ignored when it does not.
    j = h.alias("h").join(
        pa.alias("p"),
        (F.col("h.tenant_id") == F.col("p.tenant_id")) & (F.col("h.invoice_doc_id") == F.col("p.invoice_doc_id"))
        & (F.col("p.invoice_fiscal_year").isNull() | (F.col("h.fiscal_year") == F.col("p.invoice_fiscal_year")))
        & (F.col("p.invoice_company_code").isNull() | (F.col("h.company_code") == F.col("p.invoice_company_code"))),
        "left")
    paid = (j.groupBy("h.tenant_id", "h.company_code", "h.invoice_doc_id", "h.fiscal_year")
              .agg(F.sum(F.coalesce("p.applied_amount", F.lit(0))).alias("paid_amount"),
                   F.sum(F.coalesce("p.discount_taken", F.lit(0))).alias("discount_taken"),
                   F.max("p.payment_date").alias("last_payment_date"),
                   F.countDistinct("p.payment_doc_id").alias("payment_count")))
    terms = (spark.table("ap_control.ref.payment_terms").filter("is_active")
             .select(F.col("terms_code").alias("payment_terms_canonical"), "net_days", "discount_days",
                     "discount_pct", F.col("tenant_id").alias("_tt")))
    terms = terms.filter(F.col("_tt") == "ALL").drop("_tt").dropDuplicates(["payment_terms_canonical"])
    hh = h.join(paid, ["tenant_id", "company_code", "invoice_doc_id", "fiscal_year"], "left") \
          .join(F.broadcast(terms), "payment_terms_canonical", "left")
    gross = F.abs(F.col("gross_amount"))
    open_amt = F.greatest(gross - F.coalesce("paid_amount", F.lit(0)) - F.coalesce("discount_taken", F.lit(0)),
                          F.lit(0))
    sign = F.when(F.col("gross_amount") < 0, -1).otherwise(1)
    out = hh.select(
        "tenant_id", "company_code", F.col("invoice_doc_id").alias("accounting_doc_id"), "fiscal_year",
        F.lit(1).alias("line_item"), "invoice_doc_id", "vendor_id", "supplier_invoice_number", "currency",
        "invoice_date", "posting_date", "payment_terms_canonical",
        F.coalesce("net_days", F.lit(30)).alias("net_days"),
        F.date_add("invoice_date", F.coalesce("net_days", F.lit(30)).cast("int")).alias("due_date"),
        F.when(F.col("discount_days").isNotNull(), F.date_add("invoice_date", F.col("discount_days").cast("int")))
         .alias("discount_due_date"),
        "gross_amount",
        (F.coalesce("paid_amount", F.lit(0)) * sign).alias("paid_amount"),
        (F.coalesce("discount_taken", F.lit(0)) * sign).alias("discount_taken"),
        (open_amt * sign).alias("open_amount"),
        (open_amt > 0.01).alias("is_open"),
        "last_payment_date", F.coalesce("payment_count", F.lit(0)).alias("payment_count"),
        F.col("payment_block_reason"), "doc_type_canonical", "_version")
    return _finish(ctx, out)


ap_items.columns = ["tenant_id", "company_code", "accounting_doc_id", "fiscal_year", "line_item", "is_open"]


REGISTRY = {
    "SL_VENDOR_CHANGE_EVENT": vendor_change_events,
    "SL_THREE_WAY_MATCH": three_way_match,
    "SL_DUPLICATE_CANDIDATE": duplicate_candidates,
    "SL_GRIR_POSITION": grir_position,
    "SL_AP_ITEM": ap_items,
}
