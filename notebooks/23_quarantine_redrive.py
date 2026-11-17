# Databricks notebook source
# MAGIC %md
# MAGIC # 23 — Quarantine re-drive (exercise E1)
# MAGIC
# MAGIC Re-drive puts quarantined records back through **the same gate** with the
# MAGIC **current** config. It never writes a "corrected" row straight to silver:
# MAGIC that would be an override with extra steps.
# MAGIC
# MAGIC ## Exercise E1 — the TEN003 `PCE` unit of measure
# MAGIC 1. Run with `entity` blank. It lists the backlog and the unmapped-domain
# MAGIC    report, and stops. The report names the exact missing mapping rows.
# MAGIC 2. Add the row to `config/value_domains_silver.yaml`
# MAGIC    (`TEN003 / UOM / PCE → EACH`; the file header has it ready to uncomment)
# MAGIC    and re-run notebook 02.
# MAGIC 3. Re-run this notebook with `entity=invoice_line`, `dry_run=false`.
# MAGIC    Repeat for `po_line` and `po_history`.
# MAGIC 4. Re-run notebook 21 (`mode=run`) so the derived tables (three-way
# MAGIC    match, GR/IR) pick up the lines that are now in silver.
# MAGIC
# MAGIC Verify at the bottom: no NULL `uom` for TEN003, and the registry showing
# MAGIC `RESOLVED_CORRECTED` with a `redrive_batch_id`.

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Repos/ap-lakehouse", "Repo root")
dbutils.widgets.text("entity", "", "Entity to re-drive (blank = report only)")
dbutils.widgets.text("tenants", "", "Tenants (comma list, blank = all)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run")

REPO_ROOT = dbutils.widgets.get("repo_root")
ENTITY = dbutils.widgets.get("entity").strip()
TENANTS = [t.strip() for t in dbutils.widgets.get("tenants").split(",") if t.strip()] or None
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

import sys
SRC = f"{REPO_ROOT}/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import importlib
import json
import ap_platform.tables as tables
import ap_platform.runlog as runlog
import ap_platform.spec as spec
import ap_platform.silver.conform as conform
import ap_platform.silver.dq as dq
import ap_platform.silver.scd2 as scd2
import ap_platform.silver.pipeline as pipeline
import ap_platform.silver.builders as builders
import ap_platform.orchestrator as orch
import ap_platform.silver.redrive as redrive
for _m in (tables, runlog, spec, conform, dq, scd2, pipeline, builders, orch, redrive):
    importlib.reload(_m)

try:
    ACTOR = spark.sql("SELECT current_user()").collect()[0][0]
except Exception:
    ACTOR = "unknown"

# COMMAND ----------

# MAGIC %md ## 1. Backlog

# COMMAND ----------

display(redrive.backlog(spark))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Unmapped-domain report
# MAGIC Every value that reached silver with no `value_domain_mapping` row, with
# MAGIC the YAML line that would fix it. Replace `<CANONICAL>` with the right value.
# MAGIC `PAYMENT_TERMS` codes like `ZZ99`, `TBC` are planted defects
# MAGIC (UNMAPPED_DOMAIN_VALUE) — those should stay quarantined until the
# MAGIC source fixes them, not be mapped to something plausible.

# COMMAND ----------

rep = redrive.unmapped_report(spark)
if rep is not None:
    display(rep.orderBy("tenant_id", "domain", "source_value"))

# COMMAND ----------

# MAGIC %md ## 3. Re-drive one entity

# COMMAND ----------

if not ENTITY:
    print("entity is blank: report only. Fix the config, re-run 02, then set entity and dry_run=false.")
else:
    result = redrive.redrive(spark, ENTITY, tenants=TENANTS, dry_run=DRY_RUN, actor=ACTOR)
    print(json.dumps(result, indent=2, default=str))
    if DRY_RUN:
        print("\nDRY RUN — nothing written. Set dry_run=false to apply.")

# COMMAND ----------

# MAGIC %md ## 4. Verify

# COMMAND ----------

display(spark.sql("""
    SELECT tenant_id, entity, status, count(*) AS rows, max(redrive_batch_id) AS redrive_batch_id
    FROM ap_control.ops.quarantine_registry
    WHERE tenant_id = 'TEN003' GROUP BY tenant_id, entity, status ORDER BY entity, status"""))
if spark.catalog.tableExists("ap_silver.ap.sl_invoice_line"):
    display(spark.sql("""
        SELECT uom, count(*) AS lines FROM ap_silver.ap.sl_invoice_line
        WHERE tenant_id = 'TEN003' GROUP BY uom ORDER BY uom"""))
