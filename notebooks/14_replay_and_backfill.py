# Databricks notebook source
# MAGIC %md
# MAGIC # 14 — Replay, backfill, unhalt, reset (on demand)
# MAGIC
# MAGIC Four operations, picked with the `operation` widget. **Every one is a dry
# MAGIC run unless `apply=true`**, and prints exactly what it would change first.
# MAGIC
# MAGIC | operation | What it does | Widgets used |
# MAGIC |---|---|---|
# MAGIC | `unhalt` | accept a drifted shape: contract v+1 from the drifted file, drift → RESOLVED | tenant, entity, notes |
# MAGIC | `replay_batch` | delete what one batch loaded; next run of 12 reloads it | batch_id |
# MAGIC | `backfill` | same, for every file in an extract-date range | tenant, entity, date_from, date_to |
# MAGIC | `reset_checkpoint` | forget Auto Loader discovery state for a source | tenant, entity |
# MAGIC
# MAGIC ## Exercise E2 — unhalt a drifted source (do this in Step 3)
# MAGIC 1. Read the drift: `SELECT * FROM ap_control.observability.vw_bronze_open_drift`.
# MAGIC 2. **TEN003 rename** (`po-number` → `purchase-order-number`). Decide it is
# MAGIC    acceptable. Run this notebook with `operation=unhalt, tenant=TEN003,
# MAGIC    entity=invoice_header`, check the printed diff, then `apply=true`.
# MAGIC 3. **Fix the meaning, not just the shape.** The contract now accepts the new
# MAGIC    column, but `schema_mapping` still reads `po-number`. In
# MAGIC    `config/schema_mapping.yaml`, change TEN003 invoice_header `po_number`'s
# MAGIC    `transform_expr` to
# MAGIC    `` coalesce(nullif(upper(trim(`purchase-order-number`)),''), nullif(upper(trim(`po-number`)),'')) ``
# MAGIC    (keep `source_field: po-number`), re-run notebook 02. Without this step
# MAGIC    PO coverage for TEN003 silently drops to zero from day 15.
# MAGIC 4. Re-run notebook 12. The 8 held-back TEN003 files load.
# MAGIC 5. **TEN002 amount-as-text** (`"$1,009,737.72"`): unhalt it the same way.
# MAGIC    Accepting the shape retypes the contract column to STRING. Here no
# MAGIC    mapping change is needed, and it is worth checking why: since Step 4
# MAGIC    every amount mapping parses currency symbols, thousands separators and
# MAGIC    European decimals. Look at TEN002 `gross_amount` in
# MAGIC    `config/schema_mapping.yaml` before you accept, and convince yourself
# MAGIC    the values will survive. Accepting a shape whose meaning you have not
# MAGIC    checked is how a silent null gets into silver.
# MAGIC 6. Re-run notebook 12. The 9 held-back TEN002 files load.

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Repos/ap-lakehouse", "Repo root")
dbutils.widgets.dropdown("operation", "unhalt", ["unhalt", "replay_batch", "backfill", "reset_checkpoint"], "Operation")
dbutils.widgets.text("tenant", "TEN003", "Tenant")
dbutils.widgets.text("entity", "invoice_header", "Entity (blank = all, backfill only)")
dbutils.widgets.text("batch_id", "", "Batch id (replay_batch)")
dbutils.widgets.text("date_from", "2026-07-01", "From extract date (backfill)")
dbutils.widgets.text("date_to", "2026-07-03", "To extract date (backfill)")
dbutils.widgets.text("notes", "Coupa R43 renamed po-number; accepted after review", "Steward notes (unhalt)")
dbutils.widgets.dropdown("apply", "false", ["true", "false"], "Apply")

REPO_ROOT = dbutils.widgets.get("repo_root")
OP = dbutils.widgets.get("operation")
TENANT = dbutils.widgets.get("tenant").strip()
ENTITY = dbutils.widgets.get("entity").strip() or None
APPLY = dbutils.widgets.get("apply") == "true"

import sys
SRC = f"{REPO_ROOT}/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import importlib
import json
from datetime import date
import ap_platform.tables as tables
import ap_platform.ingestion.contracts as C
import ap_platform.ingestion.engine as E
import ap_platform.ingestion.operations as ops
for _m in (tables, C, E, ops):
    importlib.reload(_m)

try:
    ACTOR = spark.sql("SELECT current_user()").collect()[0][0]
except Exception:
    ACTOR = "unknown"

# COMMAND ----------

if OP == "unhalt":
    result = ops.unhalt(spark, TENANT, ENTITY, ACTOR, dbutils.widgets.get("notes"), apply=APPLY)
elif OP == "replay_batch":
    result = ops.replay_batch(spark, dbutils.widgets.get("batch_id").strip(), apply=APPLY)
elif OP == "backfill":
    result = ops.backfill(spark, TENANT, ENTITY, date.fromisoformat(dbutils.widgets.get("date_from")),
                          date.fromisoformat(dbutils.widgets.get("date_to")), apply=APPLY)
else:
    result = ops.reset_checkpoint("/Volumes/ap_bronze/landing/checkpoints", TENANT, ENTITY or "", apply=APPLY)

print(json.dumps(result, indent=2, default=str))
if not APPLY:
    print("\nDRY RUN. Nothing changed. Set apply=true to do it.")

# COMMAND ----------

display(spark.sql("""
    SELECT tenant_id, entity, drift_type, classification, column_name, status, action_taken, resolved_by, notes
    FROM ap_control.ops.schema_drift_log ORDER BY detected_at"""))
