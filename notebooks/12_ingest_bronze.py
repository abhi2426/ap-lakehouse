# Databricks notebook source
# MAGIC %md
# MAGIC # 12 — Ingest bronze
# MAGIC
# MAGIC Loops `ap_control.config.ingestion_config` and ingests every tenant and
# MAGIC entity through **one generic engine** (`src/ap_platform/ingestion/engine.py`).
# MAGIC There is no per-tenant code anywhere.
# MAGIC
# MAGIC | Widget | First run | Why |
# MAGIC |---|---|---|
# MAGIC | `tenants` | `TEN001` | prove one tenant works before all three |
# MAGIC | `dry_run` | `true`, then `false` | exercises profiling, drift, reading and bronze DQ; writes nothing |
# MAGIC | `use_autoloader` | `false` | the file ledger already gives exactly-once; `true` adds Auto Loader discovery and falls back automatically |
# MAGIC
# MAGIC ### Three things that look like failures and are the pipeline working
# MAGIC 1. **Two sources halt.** TEN002 `invoice_header` (day 12: amount becomes text)
# MAGIC    and TEN003 `invoice_header` (day 15: `po-number` renamed). BREAKING drift
# MAGIC    halts that one source; everything else keeps loading (decision D9).
# MAGIC 2. **Rescued data appears.** TEN001's appended `ZUONR` column (ADDITIVE,
# MAGIC    auto-accepted) and stray text in numeric columns land in `_rescued_data`.
# MAGIC 3. **A poison file, if you plant one, is set aside**, not fatal.
# MAGIC
# MAGIC **Idempotency check:** run this notebook twice. The second run must report
# MAGIC `--` (no new data) everywhere and bronze row counts must not move.

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Repos/ap-lakehouse", "Repo root")
dbutils.widgets.text("tenants", "", "Tenants (comma list, blank = all)")
dbutils.widgets.text("entities", "", "Entities (comma list, blank = all)")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run")
dbutils.widgets.dropdown("use_autoloader", "false", ["true", "false"], "Use Auto Loader discovery")

REPO_ROOT = dbutils.widgets.get("repo_root")
TENANTS = [t.strip() for t in dbutils.widgets.get("tenants").split(",") if t.strip()] or None
ENTITIES = [t.strip() for t in dbutils.widgets.get("entities").split(",") if t.strip()] or None
DRY_RUN = dbutils.widgets.get("dry_run") == "true"
USE_AL = dbutils.widgets.get("use_autoloader") == "true"

import sys
SRC = f"{REPO_ROOT}/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import importlib
import ap_platform.tables as tables
import ap_platform.runlog as runlog
import ap_platform.ingestion.contracts as C
import ap_platform.ingestion.engine as eng
for _m in (tables, runlog, C, eng):
    importlib.reload(_m)

try:
    ACTOR = spark.sql("SELECT current_user()").collect()[0][0]
except Exception:
    ACTOR = "unknown"
print(f"tenants={TENANTS or 'ALL'} entities={ENTITIES or 'ALL'} dry_run={DRY_RUN} autoloader={USE_AL}")

# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

run = eng.run_ingestion(spark, tenants=TENANTS, entities=ENTITIES, dry_run=DRY_RUN,
                        use_autoloader=USE_AL, actor=ACTOR)
print(f"\nbatch {run.batch_id}: {run.status}" + ("  (DRY RUN — nothing written)" if DRY_RUN else ""))
display(spark.createDataFrame(run.summary_rows(), runlog.SUMMARY_SCHEMA))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC Rows per tenant and entity, with rescued counts. The drift log should show
# MAGIC three events: one ADDITIVE (accepted), two BREAKING (open, halting).

# COMMAND ----------

if not DRY_RUN:
    entities = [r.entity for r in spark.sql(
        "SELECT DISTINCT entity FROM ap_control.config.ingestion_config WHERE is_active").collect()]
    parts = [f"""SELECT tenant_id, '{e}' AS entity, count(*) AS rows,
                        sum(CASE WHEN is_rescued THEN 1 ELSE 0 END) AS rescued,
                        count(DISTINCT source_file) AS files
                 FROM ap_bronze.ap.br_{e} GROUP BY tenant_id"""
             for e in sorted(entities) if spark.catalog.tableExists(f"ap_bronze.ap.br_{e}")]
    display(spark.sql(" UNION ALL ".join(parts) + " ORDER BY tenant_id, entity"))
    display(spark.sql("""
        SELECT tenant_id, entity, drift_type, classification, column_name, action_taken, status,
               substring_index(sample_file, '/', -1) AS sample_file
        FROM ap_control.ops.schema_drift_log ORDER BY detected_at"""))
    display(spark.sql("""
        SELECT tenant_id, sum(rows_ingested) AS rows, count(*) AS files,
               sum(CASE WHEN status = 'POISON' THEN 1 ELSE 0 END) AS poison
        FROM ap_control.ops.file_ledger GROUP BY tenant_id ORDER BY tenant_id"""))
