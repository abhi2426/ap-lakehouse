"""
keys — surrogate keys, point-in-time dimension resolution, FX and dates.

Surrogate keys are deterministic hashes (``xxhash64``) of the natural key and,
for SCD2 dimensions, ``valid_from``. Deterministic means a rebuild produces the
same keys, so facts never need re-keying when a dimension is rebuilt, and a
MERGE on the business key is always safe to re-run.

``resolve_scd2`` joins a fact to the dimension version valid at an as-of date
using a HALF-OPEN interval: ``valid_from <= d < coalesce(valid_to, +inf)``.
A closed interval matches two versions on the day of a change and silently
duplicates the fact; integrity check 4 in notebook 31 exists to catch that.

A fact row whose dimension member is not found keeps a NULL key rather than
being dropped: the row is real spend even if the vendor master is late.
Integrity check 2 counts those orphans.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import tables

G = "ap_gold.ap."
GROUP_CCY_RATE_TYPE = "DAILY_MID"


class UpstreamMissing(Exception):
    """A table this builder reads has not been written yet."""


def need(spark: SparkSession, *names: str) -> None:
    for n in names:
        if not tables.exists(spark, n):
            raise UpstreamMissing(f"{n} does not exist yet")


def sk(*cols) -> Column:
    """Deterministic BIGINT surrogate key from one or more columns."""
    return F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("~")) if isinstance(c, str)
                        else F.coalesce(c.cast("string"), F.lit("~")) for c in cols])


def date_key(col) -> Column:
    c = F.col(col) if isinstance(col, str) else col
    return F.date_format(c, "yyyyMMdd").cast("int")


def resolve_scd2(fact: DataFrame, dim: DataFrame, keys: list[str], as_of: str, key_col: str,
                 out_col: str | None = None) -> DataFrame:
    """Attach ``dim.key_col`` valid at ``fact.as_of`` (half-open interval)."""
    out_col = out_col or key_col
    d = dim.select(*[F.col(k).alias(f"_d_{k}") for k in keys], F.col("valid_from").alias("_d_from"),
                   F.col("valid_to").alias("_d_to"), F.col(key_col).alias(f"_d_{out_col}"))
    ts = F.col(as_of).cast("timestamp")
    cond = [fact[k] == d[f"_d_{k}"] for k in keys]
    cond.append(d["_d_from"] <= F.coalesce(ts, F.current_timestamp()))
    cond.append(d["_d_to"].isNull() | (F.coalesce(ts, F.current_timestamp()) < d["_d_to"]))
    j = fact.join(d, cond, "left")
    return j.drop(*[f"_d_{k}" for k in keys], "_d_from", "_d_to").withColumnRenamed(f"_d_{out_col}", out_col)


def with_group_amount(spark: SparkSession, df: DataFrame, amount: str, ccy: str, on_date: str,
                      out: str) -> DataFrame:
    """Convert ``amount`` to each tenant's group currency at the daily rate.

    Rates are stored from currency to USD. Group currency is USD for all three
    tenants; a tenant with another group currency would cross via USD, which
    this function does with two lookups. Missing rate -> NULL plus
    ``<out>_fx_missing = true``, never a silent 1.0.
    """
    tr = spark.table("ap_control.config.tenant_registry").select("tenant_id", "group_currency")
    r = (spark.table("ap_control.ref.currency_rate")
         .filter((F.col("rate_type") == GROUP_CCY_RATE_TYPE) & F.col("is_active") & (F.col("to_currency") == "USD"))
         .select("from_currency", "rate_date", F.col("exchange_rate").cast("decimal(18,8)").alias("_r")))
    r_to = r.select(F.col("from_currency").alias("_gc"), F.col("rate_date").alias("_gd"), F.col("_r").alias("_rg"))
    df = df.join(tr.withColumnRenamed("group_currency", "_grp"), "tenant_id", "left")
    df = df.join(r, (df[ccy] == r.from_currency) & (df[on_date] == r.rate_date), "left").drop("from_currency", "rate_date")
    df = df.join(r_to, (df["_grp"] == r_to["_gc"]) & (df[on_date] == r_to["_gd"]), "left").drop("_gc", "_gd")
    usd = F.when(F.col(ccy) == "USD", F.col(amount)).otherwise(F.col(amount) * F.col("_r"))
    grp = F.when(F.col("_grp") == F.col(ccy), F.col(amount)).when(F.col("_grp") == "USD", usd) \
           .otherwise(usd / F.col("_rg"))
    return (df.withColumn(out, F.round(grp, 2).cast("decimal(20,2)"))
              .withColumn(f"{out}_fx_missing", F.col(out).isNull() & F.col(amount).isNotNull())
              .drop("_r", "_rg", "_grp"))


def as_of_date(spark: SparkSession, override: str | None = None):
    """The business 'today'. Synthetic data ends in July 2026, so using the
    real current date would age every item by months. Default: the latest
    extract date ingested, which is what 'as of the last load' means."""
    if override:
        return F.to_date(F.lit(override))
    if tables.exists(spark, "ap_control.ops.file_ledger"):
        d = spark.sql("SELECT max(file_date) d FROM ap_control.ops.file_ledger WHERE status = 'INGESTED'").collect()[0].d
        if d:
            return F.lit(d).cast("date")
    return F.current_date()
