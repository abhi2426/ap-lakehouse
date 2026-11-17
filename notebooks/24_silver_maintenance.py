# Databricks notebook source
# MAGIC %md
# MAGIC # 24 — Silver maintenance (on demand)
# MAGIC
# MAGIC 1. **OPTIMIZE** driven by `transformation_config.optimize_frequency`.
# MAGIC 2. **VACUUM**, dry run by default, with a 168-hour retention floor that the
# MAGIC    code refuses to go below. Leave both defaults alone.
# MAGIC 3. *(reserved)*
# MAGIC 4. **SCD2 integrity** — run this section every time, even when 1 and 2 are
# MAGIC    pointless at PoC volume. Exactly one current version per key, and no
# MAGIC    gaps or overlaps in validity. Zero rows = clean.

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Repos/ap-lakehouse", "Repo root")
dbutils.widgets.dropdown("optimize", "false", ["true", "false"], "Run OPTIMIZE")
dbutils.widgets.dropdown("vacuum_dry_run", "true", ["true", "false"], "VACUUM dry run")
dbutils.widgets.dropdown("run_vacuum", "false", ["true", "false"], "Run VACUUM at all")

REPO_ROOT = dbutils.widgets.get("repo_root")
OPTIMIZE = dbutils.widgets.get("optimize") == "true"
VAC_DRY = dbutils.widgets.get("vacuum_dry_run") == "true"
RUN_VAC = dbutils.widgets.get("run_vacuum") == "true"

import sys
SRC = f"{REPO_ROOT}/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import importlib
import ap_platform.tables as tables
import ap_platform.spec as spec
import ap_platform.silver.maintenance as mnt
for _m in (tables, spec, mnt):
    importlib.reload(_m)

# COMMAND ----------

# MAGIC %md ## 1. OPTIMIZE

# COMMAND ----------

for line in mnt.optimize(spark, apply=OPTIMIZE):
    print(line)

# COMMAND ----------

# MAGIC %md ## 2. VACUUM (dry run unless you say otherwise)

# COMMAND ----------

if RUN_VAC:
    for line in mnt.vacuum(spark, dry_run=VAC_DRY):
        print(line)
else:
    print("VACUUM skipped (run_vacuum=false).")

# COMMAND ----------

# MAGIC %md ## 4. SCD2 integrity — expect zero rows

# COMMAND ----------

findings = mnt.scd2_integrity(spark)
if findings:
    display(spark.createDataFrame(findings, "table_name STRING, check_name STRING, offending_keys BIGINT, example STRING"))
    raise AssertionError(f"{len(findings)} SCD2 integrity problem(s)")
print("SCD2 integrity: clean (one current version per key, no gaps, no overlaps)")
display(spark.sql("""
    SELECT 'sl_vendor' AS table_name, count(*) AS versions, count(DISTINCT tenant_id, vendor_id) AS keys,
           sum(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows
    FROM ap_silver.ap.sl_vendor
    UNION ALL
    SELECT 'sl_vendor_bank', count(*), count(DISTINCT tenant_id, vendor_id, bank_country, bank_key, bank_account),
           sum(CASE WHEN is_current THEN 1 ELSE 0 END)
    FROM ap_silver.ap.sl_vendor_bank"""))
