"""
maintenance — OPTIMIZE and VACUUM from config, and the SCD2 integrity check.

At PoC volumes OPTIMIZE and VACUUM do nothing useful; they are here so the
mechanism is real. The integrity check is the part that matters at any size.

SCD2 invariants checked on every SCD2 table
-------------------------------------------
1. exactly one ``is_current`` row per key;
2. per key, each version's ``valid_to`` equals the next version's
   ``valid_from`` (no gaps, no overlaps), and only the last is open.

A key with two current versions doubles every amount joined to it. A key with
a gap makes two correct-looking reports disagree. Both are invisible until
someone asks a point-in-time question.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import tables
from ..spec import load_specs

RETENTION_FLOOR_HOURS = 168


def optimize_plan(spark: SparkSession, frequency: str | None = None) -> list[tuple]:
    out = []
    for s in load_specs(spark):
        freq = (s.raw.get("optimize_frequency") or "NEVER").upper()
        if freq == "NEVER" or (frequency and freq != frequency.upper()):
            continue
        out.append((s.job_id, s.target_table, freq, tables.exists(spark, s.target_table)))
    return out


def optimize(spark: SparkSession, frequency: str | None = None, apply: bool = False) -> list[str]:
    done = []
    for job_id, table, freq, exists in optimize_plan(spark, frequency):
        if not exists:
            continue
        if apply:
            spark.sql(f"OPTIMIZE {table}")
        done.append(f"{'OPTIMIZED' if apply else 'would OPTIMIZE'} {table} ({freq})")
    return done


def vacuum(spark: SparkSession, retain_hours: int = RETENTION_FLOOR_HOURS, dry_run: bool = True) -> list[str]:
    if retain_hours < RETENTION_FLOOR_HOURS:
        raise ValueError(f"retain_hours {retain_hours} is below the {RETENTION_FLOOR_HOURS}h floor: "
                         "time travel and concurrent readers depend on it")
    out = []
    for s in load_specs(spark):
        if s.layer == "SILVER" and tables.exists(spark, s.target_table):
            spark.sql(f"VACUUM {s.target_table} RETAIN {retain_hours} HOURS" + (" DRY RUN" if dry_run else ""))
            out.append(f"{'VACUUM DRY RUN' if dry_run else 'VACUUMED'} {s.target_table}")
    return out


def scd2_integrity(spark: SparkSession) -> list[tuple]:
    """(table, check, offending_keys, example) for every SCD2 table. Empty = clean."""
    findings = []
    for s in load_specs(spark):
        if s.write_mode != "SCD2_MERGE" or not tables.exists(spark, s.target_table):
            continue
        keys = [k for k in s.merge_keys if k != "valid_from"]
        df = spark.table(s.target_table)
        multi = df.filter("is_current").groupBy(*keys).count().filter("count <> 1")
        none_cur = df.groupBy(*keys).agg(F.sum(F.col("is_current").cast("int")).alias("c")).filter("c = 0")
        w = Window.partitionBy(*keys).orderBy("valid_from")
        seq = (df.withColumn("_next_from", F.lead("valid_from").over(w))
                 .filter((F.col("_next_from").isNotNull() & ~F.col("valid_to").eqNullSafe(F.col("_next_from")))
                         | (F.col("_next_from").isNull() & F.col("valid_to").isNotNull())
                         | (F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from")))))
        for name, bad in (("more than one current version", multi), ("no current version", none_cur),
                          ("gap or overlap in validity", seq)):
            n = bad.select(*keys).distinct().count()
            if n:
                ex = bad.select(*keys).limit(1).collect()[0].asDict()
                findings.append((s.target_table, name, n, str(ex)))
    return findings
