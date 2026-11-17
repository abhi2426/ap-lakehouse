"""
pipeline — the generic silver job: conform, gate, choose, hand back to write.

Every bronze-fed silver job without a registered builder runs through here.
That is 11 of the 16 silver jobs, and none of them has entity-specific code.

The choice rule: **silver holds the latest version of each key that passed
the gate.** Versions newer than that which failed are quarantined. So a late
correction that fails DQ does not overwrite the good version already in
silver, and a first full load that contains both still lands the good one.
For SCD2 jobs every passing version is kept, in order, as history.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import tables
from ..spec import JobSpec
from . import conform as CF
from . import dq as DQ

WATERMARKS = "ap_control.ops.silver_watermark"
QUARANTINE_REGISTRY = "ap_control.ops.quarantine_registry"
RECON = "ap_control.ops.reconciliation_control"


@dataclass
class JobContext:
    spark: SparkSession
    spec: JobSpec
    batch_id: str
    layer: str
    dry_run: bool = False
    full_reload: bool = False
    tenants: list[str] | None = None
    key_filter: DataFrame | None = None       # redrive: only these business keys


@dataclass
class Build:
    df: DataFrame | None = None               # rows to write
    quarantine: DataFrame | None = None       # rows to divert
    dq_rows: list[dict] = field(default_factory=list)
    recon_rows: list[dict] = field(default_factory=list)
    watermarks: dict = field(default_factory=dict)
    records_in: int = 0
    records_out: int = 0
    records_quarantined: int = 0
    aborted: dict = field(default_factory=dict)
    note: str = ""
    status: str = "SUCCEEDED"
    rules_evaluated: int = 0
    rules_not_evaluated: int = 0
    replace_predicate: str | None = None       # SNAPSHOT write mode
    full_set: bool = False                     # df is the COMPLETE set for the slice (full recompute)


def read_watermarks(spark: SparkSession, job_id: str) -> dict:
    if not tables.exists(spark, WATERMARKS):
        return {}
    return {r.tenant_id: r.watermark_ts for r in spark.table(WATERMARKS)
            .filter(F.col("job_id") == job_id).collect()}


def business_key_json(keys: list[str]):
    return F.to_json(F.struct(*[F.col(k) for k in keys]))


def drop_helpers(df: DataFrame, keep: list[str]) -> DataFrame:
    return df.select(*[F.col(f"`{c}`") for c in keep if c in df.columns])


def build_conformed(ctx: JobContext) -> Build:
    spark, spec = ctx.spark, ctx.spec
    entity = spec.entity
    ec = CF.load_entity_config(spark, entity, spec.source_tables[0])
    keys = spec.merge_keys
    tenants = [t for t in ec.tenants if (not ctx.tenants or t in ctx.tenants)
               and spec.tenant_id in ("ALL", t)]
    if not tenants:
        return Build(status="NO_NEW_DATA", note=f"no tenant maps entity {entity}")
    since = None if (ctx.full_reload or ctx.key_filter is not None) else read_watermarks(spark, spec.job_id)
    versions = CF.conform(spark, ec, keys, since=since, tenants=tenants)
    if versions is None:
        return Build(status="NO_NEW_DATA", note="watermarks current; nothing new in bronze")
    if ctx.key_filter is not None:
        kf = ctx.key_filter
        versions = versions.join(kf, [versions[k].eqNullSafe(kf[k]) for k in keys], "left_semi")

    n_versions = versions.count()
    if n_versions == 0:
        return Build(status="NO_NEW_DATA", note="no rows for these keys" if ctx.key_filter is not None
                     else "watermarks current; nothing new in bronze")

    # Exact duplicates WITHIN one delivered file (same key, same content, same
    # file) are a source defect: count them before collapsing, so the
    # uniqueness rule can report what it caught rather than a silent pass.
    exact_dups = {r.tenant_id: int(r.n) for r in versions.groupBy("tenant_id", *keys, "record_hash", "source_file")
                  .count().filter("count > 1").groupBy("tenant_id")
                  .agg(F.sum(F.col("count") - 1).alias("n")).collect()}
    # Identical restatements (same key, same record hash) carry no information.
    w = Window.partitionBy(*keys, "record_hash").orderBy(F.col("_version").desc())
    versions = versions.withColumn("_dup", F.row_number().over(w)).filter("_dup = 1").drop("_dup")

    versions = add_changed_fields(spark, versions, ec, keys, spec)
    rules = DQ.load_rules(spark, ctx.layer, entity)

    def conform_other(other_entity: str, key_df: DataFrame):
        oec = CF.load_entity_config(spark, other_entity)
        okeys = [k for k in key_df.columns]
        other = CF.conform(spark, oec, okeys, tenants=tenants)
        if other is None:
            return None
        other = other.join(key_df, okeys, "left_semi")
        jk = okeys + ["line_number"] if "line_number" in other.columns else okeys
        lw = Window.partitionBy(*jk).orderBy(F.col("_version").desc())
        return other.withColumn("_rn", F.row_number().over(lw)).filter("_rn = 1").drop("_rn")

    dctx = DQ.Context(spark, entity, keys, conform_other)
    gate = DQ.evaluate_rows(versions, rules, dctx, tenants)
    DQ.add_samples(gate, keys)
    df = gate.df

    passes = (F.size("_dq_fail_q") == 0) & (F.size("_dq_abort") == 0)
    kw = Window.partitionBy(*keys)
    df = df.withColumn("_pass", passes).withColumn(
        "_latest_pass", F.max(F.when(F.col("_pass"), F.col("_version"))).over(kw))
    # Pin the gate's verdicts before anything is written. The frame reads the
    # target table (changed_fields, FK resolvers); evaluated again after the
    # MERGE it would see the NEW state and could quarantine different rows
    # from the ones it chose. Dry runs write nothing, so they skip this.
    if not ctx.dry_run:
        df = tables.stage(spark, df, f"ap_silver.staging.stg_{spec.job_id.lower()}")
    scd2 = spec.write_mode == "SCD2_MERGE"
    if scd2:
        chosen = df.filter("_pass")
    else:
        ow = Window.partitionBy(*keys).orderBy(F.col("_version").desc(), F.col("file_row_number").desc())
        chosen = (df.filter(F.col("_pass") & (F.col("_version") == F.col("_latest_pass")))
                    .withColumn("_rn", F.row_number().over(ow)).filter("_rn = 1"))
    quarantine = df.filter(~F.col("_pass") & (F.col("_latest_pass").isNull()
                                               | (F.col("_version") > F.col("_latest_pass"))))

    agg_outs, agg_ne, agg_abort = DQ.evaluate_aggregates(chosen, rules, tenants)
    for o in agg_outs:
        n_dup = exact_dups.get(o.tenant, 0)
        if o.rule.rule_type == "UNIQUE" and o.rule.severity == "ABORT" and o.outcome == "PASS" and n_dup:
            o.action = "EXACT_DUPLICATES_COLLAPSED"
            o.sample = json.dumps({"exact_duplicate_rows_collapsed": n_dup})
    aborted = {**gate.aborted_tenants, **agg_abort}
    if aborted:
        bad = list(aborted)
        chosen = chosen.filter(~F.col("tenant_id").isin(bad))
        quarantine = quarantine.filter(~F.col("tenant_id").isin(bad))

    out_cols = ec.output_columns() + ["dq_warnings", "silver_batch_id", "silver_updated_at"]
    chosen = (chosen.withColumn("dq_warnings", F.when(F.size("_dq_fail_w") > 0, F.array_join("_dq_fail_w", ",")))
                    .withColumn("silver_batch_id", F.lit(ctx.batch_id))
                    .withColumn("silver_updated_at", F.current_timestamp()))
    chosen = drop_helpers(chosen, out_cols)

    q_cols = ec.output_columns()
    quarantine = (quarantine
                  .withColumn("rule_ids", F.array_join(F.array_distinct(F.concat("_dq_fail_q", "_dq_abort")), ","))
                  .withColumn("business_key", business_key_json(keys))
                  .withColumn("quarantine_id", F.sha2(F.concat_ws("|", F.lit(entity), F.col("tenant_id"),
                                                                   F.col("business_key"), F.col("_version")), 256))
                  .withColumn("quarantine_batch_id", F.lit(ctx.batch_id))
                  .withColumn("quarantined_at", F.current_timestamp()))
    quarantine = drop_helpers(quarantine, q_cols + ["rule_ids", "business_key", "quarantine_id",
                                                    "quarantine_batch_id", "quarantined_at"])

    counts = {r.tenant_id: (None, r.ts) for r in versions.groupBy("tenant_id").agg(
        F.max("ingest_ts").alias("ts")).collect()}
    n_out = chosen.count()
    n_q = quarantine.count()

    dq_rows = [o.row(ctx.batch_id, ctx.layer, entity) for o in gate.outcomes + agg_outs]
    dq_rows += DQ.not_evaluated_rows(gate.not_evaluated + agg_ne, tenants, ctx.batch_id, ctx.layer, entity)

    # Reconciliation (DQ_REC_0702): every key in scope is either written or quarantined-only.
    key_cnt = {r.tenant_id: r.n for r in versions.select("tenant_id", *keys).distinct()
               .groupBy("tenant_id").count().withColumnRenamed("count", "n").collect()}
    out_cnt = {r.tenant_id: r.n for r in chosen.select("tenant_id", *keys).distinct()
               .groupBy("tenant_id").count().withColumnRenamed("count", "n").collect()}
    q_only = {r.tenant_id: r.n for r in quarantine.select("tenant_id", *keys).distinct()
              .join(chosen.select("tenant_id", *keys).distinct(), ["tenant_id"] + keys, "left_anti")
              .groupBy("tenant_id").count().withColumnRenamed("count", "n").collect()}
    recon = []
    for t in tenants:
        if t not in key_cnt:
            continue
        src, tgt, qo = key_cnt.get(t, 0), out_cnt.get(t, 0), q_only.get(t, 0)
        status = "ABORTED" if t in aborted else ("MATCHED" if src == tgt + qo else "VARIANCE")
        recon.append({"recon_id": f"{ctx.batch_id}|{spec.job_id}|{t}", "batch_id": ctx.batch_id, "tenant_id": t,
                      "recon_type": "BRONZE_TO_SILVER_KEYS", "entity": entity, "source_count": src,
                      "target_count": tgt, "count_variance": src - tgt - qo,
                      "status": status,
                      "explanation": (f"{qo} key(s) quarantined with no passing version"
                                      if qo else None) if t not in aborted else aborted[t]})
        dq_rows.append({**DQ.Outcome(DQ.Rule("DQ_REC_0702", "ALL", ctx.layer, "*", "AGGREGATE", "CONSISTENCY",
                                             "ROW_COUNT", "silver_count + quarantined_count = bronze_count",
                                             {}, "ABORT", None), t, src,
                                     0 if status != "VARIANCE" else 1,
                                     "PASS" if status != "VARIANCE" else "FAIL").row(ctx.batch_id, ctx.layer, entity)})

    # A re-drive touches a handful of keys: it must never move the watermark.
    wm = {} if ctx.key_filter is not None else {t: v[1] for t, v in counts.items() if t not in aborted}
    note = []
    if aborted:
        note.append("aborted: " + "; ".join(f"{t}: {why}" for t, why in aborted.items()))
    if gate.not_evaluated or agg_ne:
        note.append(f"{len(gate.not_evaluated) + len(agg_ne)} rule(s) not evaluated")
    return Build(df=chosen, quarantine=quarantine, dq_rows=dq_rows, recon_rows=recon, watermarks=wm,
                 records_in=n_versions, records_out=n_out, records_quarantined=n_q, aborted=aborted,
                 note="; ".join(note), status="PARTIAL" if aborted else "SUCCEEDED",
                 rules_evaluated=len({o.rule.rule_id for o in gate.outcomes + agg_outs}),
                 rules_not_evaluated=len({r.rule_id for r, _ in gate.not_evaluated + agg_ne}))


def add_changed_fields(spark: SparkSession, df: DataFrame, ec, keys: list[str], spec: JobSpec) -> DataFrame:
    """``changed_fields``: canonical fields that differ from the previous version.

    The previous version is the prior one in this batch, or for the first
    version in the batch the row already in silver. Empty for a first
    appearance. Lets a rule say *what kind* of restatement it cares about:
    the late-correction rule fires on a changed amount, not on a payment
    block coming off.
    """
    canon = [c for c, _ in ec.canonical_fields() if c not in keys and c in df.columns]
    if not canon:
        return df.withColumn("changed_fields", F.array().cast("array<string>"))
    w = Window.partitionBy(*keys).orderBy("_version")
    df = df.withColumn("_prev", F.lag(F.struct(*canon)).over(w))
    if tables.exists(spark, spec.target_table):
        t = spark.table(spec.target_table)
        if "is_current" in t.columns:
            t = t.filter("is_current")
        have = [c for c in canon if c in t.columns]
        t = t.select(*[F.col(k).alias(f"_tk_{k}") for k in keys],
                     F.struct(*[F.col(c) if c in have else F.lit(None).alias(c) for c in canon]).alias("_tprev"))
        cond = [df[k].eqNullSafe(t[f"_tk_{k}"]) for k in keys]
        df = (df.join(t, cond, "left").drop(*[f"_tk_{k}" for k in keys])
                .withColumn("_prev", F.coalesce(F.col("_prev"), F.col("_tprev"))).drop("_tprev"))
    items = [F.when(F.col("_prev").isNotNull() & ~F.col(c).eqNullSafe(F.col(f"_prev.{c}")), F.lit(c)) for c in canon]
    return df.withColumn("changed_fields", F.filter(F.array(*items), lambda x: x.isNotNull())).drop("_prev")


# ---------------------------------------------------------------------------
# Side writes shared by every silver job
# ---------------------------------------------------------------------------

REG_SCHEMA = ("quarantine_id STRING, batch_id STRING, tenant_id STRING, layer STRING, entity STRING, "
              "business_key STRING, rule_ids STRING, quarantined_at TIMESTAMP, status STRING, "
              "assigned_to STRING, resolution_notes STRING, resolved_at TIMESTAMP, redrive_batch_id STRING")


def write_quarantine(spark: SparkSession, q: DataFrame, entity: str, layer: str) -> None:
    target = f"ap_silver.quarantine.q_{entity}"
    tables.merge(spark, q, target, ["quarantine_id"], cluster_by=["tenant_id"], update=False)
    reg = q.select("quarantine_id", F.col("quarantine_batch_id").alias("batch_id"), "tenant_id",
                   F.lit(layer).alias("layer"), F.lit(entity).alias("entity"), "business_key", "rule_ids",
                   "quarantined_at", F.lit("OPEN").alias("status"), F.lit(None).cast("string").alias("assigned_to"),
                   F.lit(None).cast("string").alias("resolution_notes"),
                   F.lit(None).cast("timestamp").alias("resolved_at"),
                   F.lit(None).cast("string").alias("redrive_batch_id"))
    tables.merge(spark, reg, QUARANTINE_REGISTRY, ["quarantine_id"], update=False)


def write_watermarks(spark: SparkSession, job_id: str, source: str, wm: dict, batch_id: str) -> None:
    from ..runlog import now
    rows = [{"job_id": job_id, "tenant_id": t, "source_table": source, "watermark_ts": ts,
             "batch_id": batch_id, "updated_at": now()} for t, ts in wm.items() if ts is not None]
    if rows:
        schema = ("job_id STRING, tenant_id STRING, source_table STRING, watermark_ts TIMESTAMP, "
                  "batch_id STRING, updated_at TIMESTAMP")
        tables.merge(spark, tables.rows_to_df(spark, rows, schema), WATERMARKS, ["job_id", "tenant_id"])


RECON_SCHEMA = ("recon_id STRING, batch_id STRING, tenant_id STRING, recon_type STRING, entity STRING, "
                "company_code STRING, currency STRING, business_date DATE, source_count BIGINT, "
                "target_count BIGINT, count_variance BIGINT, source_amount DECIMAL(20,2), "
                "target_amount DECIMAL(20,2), amount_variance DECIMAL(20,2), variance_pct DECIMAL(9,6), "
                "status STRING, explanation STRING, checked_at TIMESTAMP")


def write_recon(spark: SparkSession, rows: list[dict]) -> None:
    from ..runlog import now
    if rows:
        rows = [{**r, "checked_at": now()} for r in rows]
        tables.append(spark, tables.rows_to_df(spark, rows, RECON_SCHEMA), RECON)
