"""
dimensions — the 13 gold dimensions.

=====================  ===========================  ======================  =========
table                  grain                        key                     history
=====================  ===========================  ======================  =========
dim_date               calendar day                 date_key (yyyymmdd)     static
dim_fiscal_calendar    tenant x day                 tenant_id, date_key     static
dim_tenant             tenant                       tenant_id               SCD1
dim_company            tenant x company code        company_key             SCD1
dim_vendor             vendor version               vendor_key              SCD2
dim_vendor_bank        bank account version         vendor_bank_key         SCD2
dim_currency           currency                     currency_code           SCD1
dim_payment_terms      canonical terms code         payment_terms_code      SCD1
dim_gl_account         tenant x account             gl_account_key          SCD1
dim_cost_center        tenant x cost centre         cost_center_key         SCD1
dim_doc_type           canonical document type      doc_type_code           SCD1
dim_capture_channel    canonical channel            capture_channel_code    SCD1
dim_dq_rule            DQ rule                      rule_id                 SCD1
=====================  ===========================  ======================  =========

Sensitive columns stop at silver: dim_vendor carries no tax id or VAT number,
dim_vendor_bank carries the last four characters of the account only.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..silver.pipeline import Build, JobContext
from .keys import G, need, sk

S = "ap_silver.ap."


def _done(ctx: JobContext, df: DataFrame) -> Build:
    df = df.withColumn("gold_batch_id", F.lit(ctx.batch_id)).withColumn("gold_updated_at", F.current_timestamp())
    n = df.count()
    return Build(df=df, records_in=n, records_out=n, full_set=True)


def dim_date(ctx: JobContext) -> Build:
    spark = ctx.spark
    d = spark.sql("SELECT explode(sequence(DATE'2020-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS d")
    df = d.select(
        F.date_format("d", "yyyyMMdd").cast("int").alias("date_key"), F.col("d").alias("calendar_date"),
        F.year("d").alias("year"), F.quarter("d").alias("quarter"), F.month("d").alias("month"),
        F.date_format("d", "MMMM").alias("month_name"), F.date_format("d", "yyyy-MM").alias("year_month"),
        F.weekofyear("d").alias("iso_week"), F.dayofweek("d").alias("day_of_week"),
        F.date_format("d", "EEEE").alias("day_name"), F.dayofweek("d").isin(1, 7).alias("is_weekend"),
        (F.last_day("d") == F.col("d")).alias("is_month_end"))
    return _done(ctx, df)


dim_date.columns = ["date_key"]


def dim_fiscal_calendar(ctx: JobContext) -> Build:
    """Per-tenant fiscal year and period, from tenant_registry.fiscal_calendar.

    CALENDAR: FY = calendar year. JUL_JUN: FY ends June, named by end year.
    CUSTOM (4-4-5 retail): 13-week quarters of 4, 4 and 5 weeks from 1 January;
    days after week 52 fall in period 12. Simplified, and documented as such:
    a real 4-4-5 calendar comes from the tenant, not from a formula.
    """
    spark = ctx.spark
    t = spark.table("ap_control.config.tenant_registry").filter("is_active").select(
        "tenant_id", "fiscal_calendar", F.coalesce("fiscal_year_offset", F.lit(0)).alias("off"))
    d = spark.sql("SELECT explode(sequence(DATE'2020-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS d")
    x = t.crossJoin(d)
    m, y = F.month("d"), F.year("d")
    week = F.least(F.floor((F.dayofyear("d") - 1) / 7) + 1, F.lit(52))
    p445 = F.when(week <= 4, 1).when(week <= 8, 2).when(week <= 13, 3).when(week <= 17, 4).when(week <= 21, 5) \
            .when(week <= 26, 6).when(week <= 30, 7).when(week <= 34, 8).when(week <= 39, 9) \
            .when(week <= 43, 10).when(week <= 47, 11).otherwise(12)
    fy = F.when(F.col("fiscal_calendar") == "JUL_JUN", F.when(m >= 7, y + 1).otherwise(y)).otherwise(y)
    fp = F.when(F.col("fiscal_calendar") == "JUL_JUN", ((m + 5) % 12) + 1) \
          .when(F.col("fiscal_calendar") == "CUSTOM", p445).otherwise(m)
    df = x.select("tenant_id", F.date_format("d", "yyyyMMdd").cast("int").alias("date_key"),
                  F.col("d").alias("calendar_date"), fy.alias("fiscal_year"), fp.cast("int").alias("fiscal_period"),
                  F.ceil(fp / 3).cast("int").alias("fiscal_quarter"), "fiscal_calendar")
    return _done(ctx, df)


dim_fiscal_calendar.columns = ["tenant_id", "date_key"]


def dim_tenant(ctx: JobContext) -> Build:
    df = ctx.spark.table("ap_control.config.tenant_registry").select(
        "tenant_id", "tenant_name", "erp_type", "functional_currency", "group_currency", "fiscal_calendar",
        "sla_tier", "isolation_tier", "onboarding_status", "is_active",
        # TEN003 has no approval stream: carried as data so metrics can say so
        F.col("tenant_id").alias("_t"))
    appr = (ctx.spark.table("ap_control.config.ingestion_config").filter("is_active AND entity = 'approval_event'")
            .select(F.col("tenant_id").alias("_t")).distinct().withColumn("has_approval_stream", F.lit(True)))
    df = df.join(appr, "_t", "left").withColumn("has_approval_stream", F.coalesce("has_approval_stream", F.lit(False))) \
           .drop("_t")
    return _done(ctx, df)


dim_tenant.columns = ["tenant_id"]


def dim_company(ctx: JobContext) -> Build:
    df = ctx.spark.table("ap_control.ref.company_code").filter("is_active").select(
        sk("tenant_id", "company_code").alias("company_key"), "tenant_id", "company_code", "company_name",
        "country_code", "functional_currency", "fiscal_calendar")
    return _done(ctx, df)


dim_company.columns = ["company_key", "tenant_id", "company_code"]


def dim_vendor(ctx: JobContext) -> Build:
    need(ctx.spark, S + "sl_vendor")
    v = ctx.spark.table(S + "sl_vendor")
    df = v.select(
        sk("tenant_id", "vendor_id", "valid_from").alias("vendor_key"),
        "tenant_id", "vendor_id", "vendor_number", "vendor_name", "country_code", "city", "vendor_currency",
        "payment_terms_src", "payment_terms_canonical", "is_blocked", "is_marked_delete", "is_deleted",
        "created_date", "valid_from", "valid_to", "is_current")
    return _done(ctx, df)


dim_vendor.columns = ["vendor_key", "tenant_id", "vendor_id", "valid_from", "payment_terms_canonical"]


def dim_vendor_bank(ctx: JobContext) -> Build:
    need(ctx.spark, S + "sl_vendor_bank")
    b = ctx.spark.table(S + "sl_vendor_bank")
    clean = F.regexp_replace("bank_account", r"[\s\-]", "")
    df = b.select(
        sk("tenant_id", "vendor_id", "bank_country", "bank_key", "bank_account", "valid_from").alias("vendor_bank_key"),
        "tenant_id", "vendor_id", "bank_country",
        F.concat(F.lit("****"), F.substring(clean, -4, 4)).alias("bank_account_masked"),
        "is_verified", "change_date", "changed_by", "is_deleted", "valid_from", "valid_to", "is_current")
    return _done(ctx, df)


dim_vendor_bank.columns = ["vendor_bank_key", "valid_from"]


def dim_currency(ctx: JobContext) -> Build:
    spark = ctx.spark
    c = spark.table("ap_control.ref.currency_rate").select(F.col("from_currency").alias("currency_code")).distinct()
    if spark.catalog.tableExists(S + "sl_invoice_header"):
        c = c.unionByName(spark.table(S + "sl_invoice_header").select(F.col("currency").alias("currency_code"))).distinct()
    c = c.filter(F.col("currency_code").rlike("^[A-Z]{3}$"))
    has_rate = spark.table("ap_control.ref.currency_rate").select(F.col("from_currency").alias("currency_code")) \
                    .distinct().withColumn("has_fx_rate", F.lit(True))
    df = c.join(has_rate, "currency_code", "left").fillna(False, ["has_fx_rate"])
    return _done(ctx, df)


dim_currency.columns = ["currency_code"]


def dim_payment_terms(ctx: JobContext) -> Build:
    df = (ctx.spark.table("ap_control.ref.payment_terms").filter("is_active")
          .select(F.col("terms_code").alias("payment_terms_code"), "terms_label", "net_days", "discount_pct",
                  "discount_days", "baseline_date_rule", F.col("tenant_id").alias("defined_for"))
          .dropDuplicates(["payment_terms_code"]))
    return _done(ctx, df)


dim_payment_terms.columns = ["payment_terms_code"]


def dim_gl_account(ctx: JobContext) -> Build:
    df = ctx.spark.table("ap_control.ref.gl_account").select(
        sk("tenant_id", "gl_account").alias("gl_account_key"), "tenant_id", "company_code", "gl_account",
        "gl_account_name", "account_type", "account_group", "is_recon_account", "is_grir_account", "is_active"
    ).dropDuplicates(["gl_account_key"])
    return _done(ctx, df)


dim_gl_account.columns = ["gl_account_key"]


def dim_cost_center(ctx: JobContext) -> Build:
    spark = ctx.spark
    parts = [spark.table(S + t).select("tenant_id", "cost_center")
             for t in ("sl_invoice_line", "sl_po_line", "sl_invoice_distribution")
             if spark.catalog.tableExists(S + t)]
    if not parts:
        need(spark, S + "sl_invoice_line")
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)
    df = (df.filter(F.col("cost_center").isNotNull()).distinct()
            .withColumn("cost_center_key", sk("tenant_id", "cost_center")))
    return _done(ctx, df)


dim_cost_center.columns = ["cost_center_key"]


def _domain_dim(ctx: JobContext, domain: str, code_col: str) -> DataFrame:
    return (ctx.spark.table("ap_control.config.value_domain_mapping")
            .filter((F.col("domain") == domain) & F.col("is_active"))
            .groupBy(F.col("canonical_value").alias(code_col))
            .agg(F.first("canonical_label", ignorenulls=True).alias("label"),
                 F.collect_set(F.concat_ws(":", "tenant_id", "source_value")).alias("source_values")))


def dim_doc_type(ctx: JobContext) -> Build:
    return _done(ctx, _domain_dim(ctx, "DOC_TYPE", "doc_type_code"))


dim_doc_type.columns = ["doc_type_code"]


def dim_capture_channel(ctx: JobContext) -> Build:
    df = _domain_dim(ctx, "CAPTURE_CHANNEL", "capture_channel_code").withColumn(
        "is_touchless_capable", F.col("capture_channel_code").isin("EDI", "PORTAL"))
    return _done(ctx, df)


dim_capture_channel.columns = ["capture_channel_code"]


def dim_dq_rule(ctx: JobContext) -> Build:
    df = ctx.spark.table("ap_control.config.dq_rule_registry").select(
        "rule_id", "rule_name", "tenant_id", "layer", "entity", "scope", "dq_dimension", "rule_type", "severity",
        "fail_threshold_pct", "owner", "business_impact", "is_active")
    return _done(ctx, df)


dim_dq_rule.columns = ["rule_id"]
