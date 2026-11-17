"""
scd2 — type-2 history with half-open validity intervals.

Contract of every SCD2 table written here
-----------------------------------------
* ``valid_from`` (inclusive) / ``valid_to`` (exclusive, NULL = open) / ``is_current``.
* Exactly one ``is_current`` row per key; intervals per key do not overlap and
  have no gaps. Notebook 24 section 4 asserts both.
* A key's FIRST version starts at 1900-01-01. A vendor first seen in the
  2026-07-01 extract still existed when its 2025 invoices were posted;
  starting history at the extract date would make every point-in-time join
  for older invoices miss.
* Idempotent: versions not newer than the current row are ignored, so a re-run
  with the same input changes nothing.

Why half-open
-------------
With closed intervals a change on day D belongs to both the old version
(valid_to = D) and the new one (valid_from = D). Any join ``BETWEEN valid_from
AND valid_to`` then matches two rows on that day and silently doubles the fact.
Gold's ``resolve_scd2`` uses ``>= valid_from AND < valid_to`` for this reason.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import tables

EPOCH = "1900-01-01 00:00:00"
SCD_COLS = ["valid_from", "valid_to", "is_current", "scd_hash"]


def prepare(spark: SparkSession, df: DataFrame, target: str, keys: list[str], tracked: list[str]) -> DataFrame:
    """New versions to insert, with valid_from/valid_to/is_current assigned."""
    tracked = [c for c in tracked if c in df.columns] + ["is_deleted"]
    h = F.sha2(F.to_json(F.struct(*[F.col(c) for c in tracked])), 256)
    w = Window.partitionBy(*keys).orderBy("_version")
    df = (df.withColumn("scd_hash", h)
            .withColumn("_eff", F.col("source_file_date").cast("timestamp"))
            .withColumn("_prev_h", F.lag("scd_hash").over(w))
            .filter(F.col("_prev_h").isNull() | (F.col("scd_hash") != F.col("_prev_h")))
            .drop("_prev_h"))
    if tables.exists(spark, target):
        cur = (spark.table(target).filter("is_current")
               .select(*[F.col(k).alias(f"_ck_{k}") for k in keys], F.col("scd_hash").alias("_cur_h"),
                       F.col("valid_from").alias("_cur_from"), F.col("_version").alias("_cur_ver")))
        cond = None
        for k in keys:
            c = F.col(k).eqNullSafe(F.col(f"_ck_{k}"))
            cond = c if cond is None else cond & c
        df = df.join(cur, cond, "left").drop(*[f"_ck_{k}" for k in keys])
        df = df.filter(F.col("_cur_ver").isNull() | (F.col("_version") > F.col("_cur_ver")))
        w1 = Window.partitionBy(*keys).orderBy("_version")
        df = (df.withColumn("_rk", F.row_number().over(w1))
                .filter(~((F.col("_rk") == 1) & F.col("_cur_h").isNotNull() & (F.col("scd_hash") == F.col("_cur_h"))))
                .drop("_rk"))
    else:
        df = (df.withColumn("_cur_h", F.lit(None).cast("string"))
                .withColumn("_cur_from", F.lit(None).cast("timestamp"))
                .withColumn("_cur_ver", F.lit(None).cast("string")))
    w2 = Window.partitionBy(*keys).orderBy("_version")
    first_ever = (F.row_number().over(w2) == 1) & F.col("_cur_from").isNull()
    vf = F.when(first_ever, F.to_timestamp(F.lit(EPOCH))).otherwise(
        F.greatest(F.col("_eff"), F.coalesce(F.col("_cur_from") + F.expr("INTERVAL 1 SECOND"), F.col("_eff"))))
    df = df.withColumn("valid_from", vf)
    # one version per (key, valid_from): the latest wins
    w3 = Window.partitionBy(*keys, "valid_from").orderBy(F.col("_version").desc())
    df = df.withColumn("_d", F.row_number().over(w3)).filter("_d = 1").drop("_d")
    w4 = Window.partitionBy(*keys).orderBy("valid_from")
    df = (df.withColumn("valid_to", F.lead("valid_from").over(w4))
            .withColumn("is_current", F.col("valid_to").isNull()))
    return df


def apply(spark: SparkSession, df: DataFrame, target: str, keys: list[str], tracked: list[str],
          cluster_by: list[str], dry_run: bool = False) -> dict:
    new = prepare(spark, df, target, keys, tracked)
    closes = None
    if tables.exists(spark, target):
        first_new = new.groupBy(*[F.col(k).alias(f"_nk_{k}") for k in keys]) \
                       .agg(F.min("valid_from").alias("_close_at"))
        cur = spark.table(target).filter("is_current")
        cond = None
        for k in keys:
            c = F.col(k).eqNullSafe(F.col(f"_nk_{k}"))
            cond = c if cond is None else cond & c
        closes = (cur.join(first_new, cond, "inner")
                  .withColumn("valid_to", F.col("_close_at")).withColumn("is_current", F.lit(False))
                  .drop("_close_at", *[f"_nk_{k}" for k in keys]))
    new = new.drop("_eff", "_cur_h", "_cur_from", "_cur_ver")
    if not dry_run:
        # Both frames read the target. Pin them before the first write, or
        # `new` is recomputed after the closes land and every new version
        # looks like a brand-new key (valid_from 1900-01-01).
        stg = "ap_silver.staging.stg_scd2_" + target.split(".")[-1]
        new = tables.stage(spark, new, stg + "_new")
        if closes is not None:
            closes = tables.stage(spark, closes, stg + "_close")
    stats = {"inserted": new.count(), "closed": closes.count() if closes is not None else 0}
    if dry_run:
        return stats
    if closes is not None and stats["closed"]:
        tables.merge(spark, closes, target, keys + ["valid_from"], cluster_by=cluster_by, insert=False)
    if stats["inserted"]:
        tables.merge(spark, new, target, keys + ["valid_from"], cluster_by=cluster_by, update=False)
    return stats
