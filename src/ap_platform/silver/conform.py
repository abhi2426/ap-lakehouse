"""
conform — bronze record maps to canonical rows, driven entirely by config.

For one entity, for each tenant that has ``schema_mapping`` rows for it:

1. pull each mapped source field out of ``record`` under its source name;
2. evaluate every ``transform_expr`` and ``try_cast`` it ONCE to the canonical
   type (the double-cast bug from the original build is not reproducible here:
   there is exactly one cast per field);
3. translate bound code fields through ``value_domain_mapping`` (via
   ``domain_binding``), counting values that have no mapping row;
4. attach lineage and a sortable ``_version`` so "latest wins" is well defined
   for all three load types.

The output is **every version** in scope, not the latest per key: choosing a
version happens after the DQ gate (see ``pipeline.py``), because the right
answer is "the latest version that passed", not "the latest version".

Load-type semantics
-------------------
* CDC: ``cdc_operation = 'D'`` is a delete version. Order: ``cdc_sequence``.
* INCREMENTAL: order by watermark, then extract date. Cannot express deletes.
* FULL_SNAPSHOT: a key present in an earlier snapshot but absent from the
  latest one gets a synthetic delete version at the latest snapshot date.
  Absence IS the deletion signal for this load type, so silver must diff.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import tables

LINEAGE = ["source_system_id", "record_key", "record_hash", "cdc_operation", "cdc_sequence",
           "source_watermark", "source_file", "source_file_date", "file_row_number", "load_type",
           "ingest_batch_id", "ingest_ts"]

# Hooks that rewrite a transform_expr before evaluation. Empty in production;
# the local test harness can register one. Kept here so it is visible.
EXPR_REWRITERS: list = []


@dataclass
class EntityConfig:
    entity: str
    mappings: dict[str, list[dict]]      # tenant -> mapping rows
    bindings: list[dict]
    load_types: dict[str, str]           # tenant -> load_type
    bronze_table: str
    contract_columns: dict | None = None # tenant -> every column any contract version accepted

    @property
    def tenants(self) -> list[str]:
        return sorted(self.mappings)

    def canonical_fields(self) -> list[tuple[str, str]]:
        """(name, datatype) across all tenants, first declaration wins."""
        seen: dict[str, str] = {}
        for rows in self.mappings.values():
            for r in rows:
                seen.setdefault(r["canonical_field"], r["canonical_datatype"])
        return list(seen.items())

    def output_columns(self) -> list[str]:
        cols = ["tenant_id"] + [c for c, _ in self.canonical_fields()]
        cols += [b["target_field"] for b in self.bindings]
        cols += ["unmapped_domain_values", "unmapped_domain_detail", "unmapped_warn_values",
                 "is_deleted", "is_restatement", "changed_fields", "_version"] + LINEAGE
        return cols


def load_entity_config(spark: SparkSession, entity: str, bronze_table: str | None = None) -> EntityConfig:
    maps: dict[str, list[dict]] = {}
    for r in spark.sql(f"""
            SELECT * FROM ap_control.config.schema_mapping
            WHERE is_active AND entity = '{entity}'
            ORDER BY tenant_id, coalesce(ordinal, 9999), canonical_field""").collect():
        maps.setdefault(r.tenant_id, []).append(r.asDict())
    bindings = []
    if tables.exists(spark, "ap_control.config.domain_binding"):
        bindings = [r.asDict() for r in spark.sql(f"""
            SELECT * FROM ap_control.config.domain_binding
            WHERE is_active AND entity = '{entity}' ORDER BY binding_id""").collect()]
    lts = {r.tenant_id: r.load_type for r in spark.sql(f"""
            SELECT tenant_id, load_type FROM ap_control.config.ingestion_config
            WHERE is_active AND entity = '{entity}'""").collect()}
    cc: dict[str, set] = {}
    if tables.exists(spark, "ap_control.ops.schema_contract"):
        import json as _json
        for r in spark.sql(f"""SELECT tenant_id, columns_json FROM ap_control.ops.schema_contract
                               WHERE entity = '{entity}'""").collect():
            cc.setdefault(r.tenant_id, set()).update(c["name"] for c in _json.loads(r.columns_json))
    return EntityConfig(entity, maps, bindings, lts, bronze_table or f"ap_bronze.ap.br_{entity}", cc)


def _q(c: str) -> str:
    return "`" + c.replace("`", "``") + "`"


def _expr(text: str) -> str:
    for fn in EXPR_REWRITERS:
        text = fn(text)
    return text


def conform_tenant(spark: SparkSession, ec: EntityConfig, tenant: str, bronze: DataFrame) -> DataFrame:
    rows = ec.mappings[tenant]
    # Mapped fields, plus every column any contract version accepted: a
    # transform_expr may legitimately read a column that is not the declared
    # source_field (a coalesce across a rename, for instance).
    sources = sorted({r["source_field"] for r in rows if r.get("source_field")}
                     | set((ec.contract_columns or {}).get(tenant, set())))
    df = bronze.select(
        "tenant_id", *LINEAGE,
        *[F.col("record").getItem(s).alias(s) for s in sources])
    exprs = []
    for r in rows:
        e = r.get("transform_expr")
        if not e:
            e = _q(r["source_field"]) if r.get("source_field") else "NULL"
        if r.get("default_value") is not None and r.get("source_field"):
            e = f"coalesce(({e}), '{r['default_value']}')"
        exprs.append(f"try_cast(({_expr(e)}) AS {r['canonical_datatype']}) AS {_q(r['canonical_field'])}")
    return df.selectExpr("tenant_id", *[_q(c) for c in LINEAGE], *exprs)


def _snapshot_deletes(df: DataFrame, keys: list[str]) -> DataFrame:
    """Synthetic delete versions for keys missing from the latest snapshot."""
    latest = df.agg(F.max("source_file_date").alias("d")).collect()[0].d
    if latest is None:
        return df.limit(0)
    w = Window.partitionBy(*keys).orderBy(F.col("source_file_date").desc(), F.col("file_row_number").desc())
    last = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    gone = last.filter(F.col("source_file_date") < F.lit(latest))
    return (gone.withColumn("cdc_operation", F.lit("D"))
                .withColumn("source_file_date", F.lit(latest).cast("date"))
                .withColumn("record_hash", F.sha2(F.concat(F.col("record_hash"), F.lit("|deleted")), 256))
                .withColumn("source_file", F.concat(F.lit("snapshot-absence:"), F.col("source_file"))))


def conform(spark: SparkSession, ec: EntityConfig, keys: list[str], *,
            since: dict[str, object] | None = None, tenants: list[str] | None = None) -> DataFrame:
    """All versions in scope for the entity, conformed. ``since`` maps tenant
    to an ingest_ts watermark; None or a missing tenant means full history."""
    bronze_all = spark.table(ec.bronze_table)
    parts = []
    for t in ec.tenants:
        if tenants and t not in tenants:
            continue
        b = bronze_all.filter(F.col("tenant_id") == t)
        wm = (since or {}).get(t)
        lt = ec.load_types.get(t, "INCREMENTAL")
        if wm is not None:
            new = b.filter(F.col("ingest_ts") > F.lit(wm))
            if lt == "FULL_SNAPSHOT":
                # A snapshot is only meaningful whole: if anything new arrived,
                # re-read every snapshot so absence can be computed.
                if not new.limit(1).collect():
                    continue
            else:
                b = new
        c = conform_tenant(spark, ec, t, b)
        if lt == "FULL_SNAPSHOT":
            c = c.unionByName(_snapshot_deletes(c, [k for k in keys if k in c.columns]))
        parts.append(c)
    if not parts:
        return None
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p, allowMissingColumns=True)
    # every canonical field exists even if no tenant in scope declares it
    for name, dtype in ec.canonical_fields():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(dtype))

    df = apply_domains(spark, df, ec)
    df = (df.withColumn("is_deleted", F.coalesce(F.upper("cdc_operation") == "D", F.lit(False)))
            .withColumn("_version", F.concat_ws(
                "|", F.lpad(F.coalesce(F.col("cdc_sequence"), F.lit(0)).cast("string"), 19, "0"),
                F.coalesce(F.col("source_watermark"), F.lit("")),
                F.coalesce(F.col("source_file_date").cast("string"), F.lit("")),
                F.lpad(F.coalesce(F.col("file_row_number"), F.lit(0)).cast("string"), 10, "0"))))
    # Restatement: a version of a source record that was first delivered in an
    # earlier file. First appearances are not corrections, whatever their date.
    first = (spark.table(ec.bronze_table).groupBy("tenant_id", "record_key")
             .agg(F.min("source_file_date").alias("_first_seen")))
    df = (df.join(first, ["tenant_id", "record_key"], "left")
            .withColumn("is_restatement", F.coalesce(F.col("source_file_date") > F.col("_first_seen"), F.lit(False)))
            .drop("_first_seen"))
    return df


def apply_domains(spark: SparkSession, df: DataFrame, ec: EntityConfig) -> DataFrame:
    unmapped_q, unmapped_w, detail = [], [], []
    if not ec.bindings:
        return (df.withColumn("unmapped_domain_values", F.lit(0))
                  .withColumn("unmapped_domain_detail", F.lit(None).cast("string"))
                  .withColumn("unmapped_warn_values", F.lit(0)))
    vdm = spark.table("ap_control.config.value_domain_mapping").filter("is_active")
    for b in ec.bindings:
        src, tgt, dom = b["source_field"], b["target_field"], b["domain"]
        if src not in df.columns:
            df = df.withColumn(tgt, F.lit(None).cast("string"))
            continue
        lk = (vdm.filter(F.col("domain") == dom)
                 .select(F.col("tenant_id").alias("_vt"), F.upper(F.trim("source_value")).alias("_vs"),
                         F.col("canonical_value").alias(f"_cv_{tgt}"))
                 .dropDuplicates(["_vt", "_vs"]))
        df = (df.join(F.broadcast(lk), (F.col("tenant_id") == F.col("_vt"))
                      & (F.upper(F.trim(F.col(_q(src)).cast("string"))) == F.col("_vs")), "left")
                .drop("_vt", "_vs")
                .withColumnRenamed(f"_cv_{tgt}", tgt))
        miss = (F.col(_q(src)).isNotNull() & (F.trim(F.col(_q(src)).cast("string")) != "")
                & F.col(tgt).isNull())
        flag = F.when(miss, 1).otherwise(0)
        (unmapped_q if b["on_unmapped"] == "QUARANTINE" else unmapped_w).append(flag)
        detail.append(F.when(miss, F.concat(F.lit(f"{dom}:"), F.col(_q(src)).cast("string"))))
    zero = F.lit(0)
    df = df.withColumn("unmapped_domain_values", sum(unmapped_q, zero) if unmapped_q else zero)
    df = df.withColumn("unmapped_warn_values", sum(unmapped_w, zero) if unmapped_w else zero)
    df = df.withColumn("unmapped_domain_detail", F.concat_ws(",", *detail) if detail else F.lit(None))
    return df.withColumn("unmapped_domain_detail",
                         F.when(F.col("unmapped_domain_detail") == "", None).otherwise(F.col("unmapped_domain_detail")))
