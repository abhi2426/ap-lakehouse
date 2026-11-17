# Databricks notebook source
# MAGIC %md
# MAGIC # 11 — Register schema contracts (the drift baseline)
# MAGIC
# MAGIC For every active `ingestion_config` row, profiles the **earliest** landed
# MAGIC file and registers its columns and type classes as contract version 1.
# MAGIC Every later file is compared to this contract in notebook 12.
# MAGIC
# MAGIC **This notebook establishes the baseline, so run it before ingesting
# MAGIC drifted data.** It always profiles the earliest file, and the planted
# MAGIC drift events start on days 8, 12 and 15, so a fresh generation is safe.
# MAGIC A source that already has a current contract is skipped unless
# MAGIC `force=true`. Re-baselining after drift has been ingested would bake the
# MAGIC drift into the contract and the detection would never fire again.
# MAGIC
# MAGIC Headerless sources (TEN001) get their column names from
# MAGIC `schema_mapping.ordinal`; positions beyond the mapping are named `_c<n>`.

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Repos/ap-lakehouse", "Repo root")
dbutils.widgets.text("tenants", "", "Tenants (comma list, blank = all)")
dbutils.widgets.dropdown("force", "false", ["true", "false"], "Re-baseline existing contracts")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run")

REPO_ROOT = dbutils.widgets.get("repo_root")
TENANTS = [t.strip() for t in dbutils.widgets.get("tenants").split(",") if t.strip()] or None
FORCE = dbutils.widgets.get("force") == "true"
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

import sys
SRC = f"{REPO_ROOT}/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import importlib
import ap_platform.tables as tables
import ap_platform.ingestion.contracts as C
import ap_platform.ingestion.engine as eng
for _m in (tables, C, eng):
    importlib.reload(_m)

try:
    ACTOR = spark.sql("SELECT current_user()").collect()[0][0]
except Exception:
    ACTOR = "unknown"

# COMMAND ----------

ARTIFACTS = "/Volumes/ap_control/ops/artifacts"
results = []
for cfg in eng.load_sources(spark, TENANTS):
    existing = eng.current_contract(spark, cfg.tenant_id, cfg.entity)
    if existing and not FORCE:
        results.append((cfg.tenant_id, cfg.entity, existing.version, len(existing.columns), "KEPT", None))
        continue
    files = eng.discover(cfg)
    if not files:
        results.append((cfg.tenant_id, cfg.entity, None, None, "NO FILES", "nothing landed yet"))
        continue
    first = files[0]
    try:
        prof = eng.profile(cfg, first)
    except C.PoisonFile as exc:
        results.append((cfg.tenant_id, cfg.entity, None, None, "POISON", str(exc)))
        continue
    version = (existing.version + 1) if existing else 1
    contract = C.Contract(cfg.tenant_id, cfg.entity, version, prof.columns)
    if not DRY_RUN:
        ref = cfg.row.get("schema_contract_ref")
        eng.register_contract(spark, contract, first.path, ACTOR,
                              "BASELINE" if not existing else "FORCED_REBASELINE",
                              artifact_path=f"{ARTIFACTS}/{ref}" if ref else None)
    numbers = sum(1 for c in contract.columns if c.type_class == "NUMBER")
    results.append((cfg.tenant_id, cfg.entity, version, len(contract.columns),
                    "REGISTERED" if not DRY_RUN else "WOULD REGISTER",
                    f"{first.name} ({numbers} NUMBER columns)"))

display(spark.createDataFrame(results, "tenant_id STRING, entity STRING, version INT, columns INT, "
                                       "status STRING, detail STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check
# MAGIC 30 current contracts expected (10 + 11 + 9). TEN001 invoice_header should
# MAGIC have 18 columns: the 19th (ZUONR) only appears from day 8 and is the
# MAGIC ADDITIVE drift the next notebook should detect.

# COMMAND ----------

if not DRY_RUN:
    display(spark.sql("""
        SELECT tenant_id, count(*) AS contracts, sum(column_count) AS columns
        FROM ap_control.ops.schema_contract WHERE is_current
        GROUP BY tenant_id ORDER BY tenant_id"""))
    n = spark.sql("SELECT count(*) c FROM ap_control.ops.schema_contract WHERE is_current").collect()[0].c
    print(f"current contracts: {n} (expect 30)")
