# Databricks notebook source
# MAGIC %md
# MAGIC # 21 — Run silver
# MAGIC
# MAGIC 16 jobs in waves derived from `depends_on`. **Run in this order:**
# MAGIC
# MAGIC 1. `mode = plan` — validates every spec and resolves the graph. No side effects.
# MAGIC 2. `mode = dry_run` — runs the whole pipeline including the DQ gate; writes nothing.
# MAGIC 3. `mode = run` — for real.
# MAGIC
# MAGIC | Wave | Jobs |
# MAGIC |---|---|
# MAGIC | 1 | SL_VENDOR |
# MAGIC | 2 | invoice header, payment, PO header, vendor bank |
# MAGIC | 3 | approval event, duplicate candidate, invoice line, payment application, PO line, vendor change event |
# MAGIC | 4 | AP item, goods receipt, invoice distribution |
# MAGIC | 5 | GR/IR position, three-way match |
# MAGIC
# MAGIC ## The planted finding you must not mistake for a bug
# MAGIC TEN003 sends `PCE` for "each" and `value_domain_mapping` has no row for it,
# MAGIC so `uom` maps to NULL and **DQ_ALL_0504 quarantines those invoice lines,
# MAGIC PO lines and receipts**. Knock-on effects that are also correct: the
# MAGIC TEN003 header-to-line totals no longer agree for affected invoices, and
# MAGIC three-way match under-reports TEN003, because a line that never reached
# MAGIC silver cannot be matched. Fixing it is exercise E1 (notebook 23).

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Repos/ap-lakehouse", "Repo root")
dbutils.widgets.dropdown("mode", "plan", ["plan", "dry_run", "run"], "Mode")
dbutils.widgets.text("jobs", "", "Jobs (comma list, blank = all)")
dbutils.widgets.text("tenants", "", "Tenants (comma list, blank = all)")
dbutils.widgets.dropdown("full_reload", "false", ["true", "false"], "Ignore watermarks")

REPO_ROOT = dbutils.widgets.get("repo_root")
MODE = dbutils.widgets.get("mode")
JOBS = [j.strip() for j in dbutils.widgets.get("jobs").split(",") if j.strip()] or None
TENANTS = [t.strip() for t in dbutils.widgets.get("tenants").split(",") if t.strip()] or None
FULL = dbutils.widgets.get("full_reload") == "true"

import sys
SRC = f"{REPO_ROOT}/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import importlib
import ap_platform.tables as tables
import ap_platform.runlog as runlog
import ap_platform.spec as spec
import ap_platform.silver.conform as conform
import ap_platform.silver.dq as dq
import ap_platform.silver.scd2 as scd2
import ap_platform.silver.pipeline as pipeline
import ap_platform.silver.builders as builders
import ap_platform.orchestrator as orch
import ap_platform.lint as lint
for _m in (tables, runlog, spec, conform, dq, scd2, pipeline, builders, orch, lint):
    importlib.reload(_m)

try:
    ACTOR = spark.sql("SELECT current_user()").collect()[0][0]
except Exception:
    ACTOR = "unknown"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tenant-agnostic code check
# MAGIC Asserts that no tenant id or ERP name appears in the executable code of
# MAGIC `src/ap_platform/silver/` (docstrings and comments are allowed to mention
# MAGIC them). If this fails, someone has added tenant-specific logic to the
# MAGIC platform, and onboarding TEN004 would need a code change.

# COMMAND ----------

findings = lint.scan_for_tenant_literals(f"{SRC}/ap_platform/silver")
for f in findings:
    print("  ", f)
assert not findings, f"{len(findings)} tenant-specific literal(s) in silver code"
print("silver code is tenant-agnostic")

# COMMAND ----------

# MAGIC %md ## 2. Plan

# COMMAND ----------

p = orch.plan(spark, "SILVER", JOBS)
display(spark.createDataFrame(p.rows(), orch.PLAN_SCHEMA))
if not p.ok:
    raise SystemExit("PLAN FAILED — see the errors column. Nothing was written.")
print(f"plan OK: {len(p.jobs)} jobs in {max(j.wave for j in p.jobs)} waves")

# COMMAND ----------

# MAGIC %md ## 3. Run (dry run or real)

# COMMAND ----------

if MODE in ("dry_run", "run"):
    run = orch.run(spark, "SILVER", jobs=JOBS, tenants=TENANTS, dry_run=(MODE == "dry_run"),
                   full_reload=FULL, actor=ACTOR)
    print(f"\nbatch {run.batch_id}: {run.status}" + ("  (DRY RUN — nothing written)" if MODE == "dry_run" else ""))
    display(spark.createDataFrame(run.summary_rows(), runlog.SUMMARY_SCHEMA))
else:
    print("mode=plan: nothing run. Set mode=dry_run, then mode=run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Checkpoint
# MAGIC Rows per silver table per tenant, and what was quarantined, by rule.

# COMMAND ----------

if MODE == "run":
    names = [r.tableName for r in spark.sql("SHOW TABLES IN ap_silver.ap").collect()]
    parts = [f"SELECT '{n}' AS table_name, tenant_id, count(*) AS rows FROM ap_silver.ap.{n} "
             + ("WHERE is_current " if n in ("sl_vendor", "sl_vendor_bank") else "")
             + "GROUP BY tenant_id" for n in sorted(names)]
    display(spark.sql(" UNION ALL ".join(parts) + " ORDER BY table_name, tenant_id"))
    display(spark.sql("""
        SELECT tenant_id, entity, rule_ids, count(*) AS rows
        FROM ap_control.ops.quarantine_registry WHERE status = 'OPEN'
        GROUP BY tenant_id, entity, rule_ids ORDER BY tenant_id, entity, rows DESC"""))
