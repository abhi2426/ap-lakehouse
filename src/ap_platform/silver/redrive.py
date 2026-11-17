"""
redrive — put quarantined records back through the SAME gate.

A re-drive that writes "corrected" records straight to silver is an override
with extra steps. This one re-reads the original bronze records for the
quarantined business keys, re-conforms them with the CURRENT config (so a
new value_domain_mapping row or a fixed transform_expr takes effect), and
runs them through the same DQ gate and the same writer as notebook 21.

Outcome per quarantined row:
* the key now has a passing version at least as new → RESOLVED_CORRECTED,
  stamped with the re-drive batch id;
* it still fails → stays OPEN (the gate said no again, for a reason the
  scorecard will show).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import tables
from ..spec import load_specs
from .pipeline import QUARANTINE_REGISTRY


def backlog(spark: SparkSession) -> DataFrame:
    return (spark.table(QUARANTINE_REGISTRY).filter("status = 'OPEN'")
            .groupBy("tenant_id", "entity", "rule_ids").count()
            .orderBy(F.col("count").desc()))


def unmapped_report(spark: SparkSession) -> DataFrame | None:
    """Missing value_domain_mapping rows, with the YAML line that would add each."""
    parts = []
    for r in spark.sql("SHOW TABLES IN ap_silver.quarantine").collect():
        q = spark.table(f"ap_silver.quarantine.{r.tableName}")
        if "unmapped_domain_detail" not in q.columns:
            continue
        reg = spark.table(QUARANTINE_REGISTRY).filter("status = 'OPEN'").select("quarantine_id")
        parts.append(q.join(reg, "quarantine_id", "left_semi")
                      .filter(F.col("unmapped_domain_detail").isNotNull())
                      .select("tenant_id", F.lit(r.tableName[2:]).alias("entity"),
                              F.explode(F.split("unmapped_domain_detail", ",")).alias("dv")))
    if not parts:
        return None
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)
    df = (df.withColumn("domain", F.split("dv", ":").getItem(0))
            .withColumn("source_value", F.expr("substring(dv, instr(dv, ':') + 1)"))
            .groupBy("tenant_id", "domain", "source_value").agg(
                F.count("*").alias("rows"), F.collect_set("entity").alias("entities")))
    return df.withColumn("yaml_to_add", F.concat(
        F.lit("- {tenant_id: "), F.col("tenant_id"), F.lit(", domain: "), F.col("domain"),
        F.lit(", source_value: "), F.col("source_value"), F.lit(", canonical_value: <CANONICAL>, is_active: true}")))


def redrive(spark: SparkSession, entity: str, *, tenants: list[str] | None = None, dry_run: bool = True,
            actor: str = "unknown") -> dict:
    from .. import orchestrator as O

    spec = next((s for s in load_specs(spark) if s.layer == "SILVER" and s.entity == entity), None)
    if spec is None:
        raise ValueError(f"no active silver job conforms entity {entity}")
    reg = spark.table(QUARANTINE_REGISTRY).filter((F.col("status") == "OPEN") & (F.col("entity") == entity))
    if tenants:
        reg = reg.filter(F.col("tenant_id").isin(tenants))
    # Pin the backlog as it was BEFORE the re-drive: the run below writes new
    # registry rows, and a lazy frame read afterwards would include them.
    reg = tables.stage(spark, reg, f"ap_silver.staging.stg_redrive_{entity}")
    n_open = reg.count()
    if n_open == 0:
        return {"entity": entity, "open_before": 0, "status": "NOTHING_TO_REDRIVE"}
    tgt = spark.table(spec.target_table) if tables.exists(spark, spec.target_table) else None
    key_schema = ", ".join(f"`{k}` {tgt.schema[k].dataType.simpleString()}" if tgt is not None and k in tgt.columns
                           else f"`{k}` STRING" for k in spec.merge_keys)
    keys_df = (reg.select(F.from_json("business_key", key_schema).alias("k")).select("k.*").distinct())

    run = O.run(spark, "SILVER", jobs=[spec.job_id], key_filters={spec.job_id: keys_df}, dry_run=dry_run,
                actor=actor, pipeline_name="silver_redrive", verbose=True)
    b = run.last_builds.get(spec.job_id)
    out = {"entity": entity, "job_id": spec.job_id, "open_before": n_open, "redrive_batch_id": run.batch_id,
           "dry_run": dry_run, "rows_now_passing": b.records_out if b else 0,
           "rows_still_failing": b.records_quarantined if b else 0}
    if b is None or b.df is None:
        return out

    chosen = b.df.select(F.to_json(F.struct(*[F.col(k) for k in spec.merge_keys])).alias("business_key"),
                         F.col("_version").alias("_cv"))
    q = spark.table(f"ap_silver.quarantine.q_{entity}").select("quarantine_id", "_version")
    fixed = (reg.join(q, "quarantine_id").join(chosen, "business_key")
               .filter(F.col("_cv") >= F.col("_version")).select("quarantine_id").distinct())
    out["resolved"] = fixed.count()
    out["still_open"] = n_open - out["resolved"]
    if not dry_run and out["resolved"]:
        ids = [r.quarantine_id for r in fixed.collect()]
        for i in range(0, len(ids), 300):
            chunk = ", ".join(f"'{x}'" for x in ids[i:i + 300])
            tables.update(spark, QUARANTINE_REGISTRY,
                          f"status = 'RESOLVED_CORRECTED', resolved_at = current_timestamp(), "
                          f"redrive_batch_id = '{run.batch_id}', "
                          f"resolution_notes = 're-driven through the gate by {actor}'",
                          f"quarantine_id IN ({chunk}) AND status = 'OPEN'")
    return out
