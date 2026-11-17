# Databricks notebook source
# MAGIC %md
# MAGIC # 31 — Run gold
# MAGIC
# MAGIC 30 jobs in five waves, through **the same orchestrator as silver**
# MAGIC (`orchestrator.run(spark, "GOLD")`). There is no second runner.
# MAGIC
# MAGIC One asymmetry with silver: an unregistered silver job falls back to the
# MAGIC generic conformance builder; an unregistered **gold** job fails the plan.
# MAGIC Every gold table is a modelling decision about grain and keys.
# MAGIC
# MAGIC `as_of` (blank = the latest extract date loaded, 2026-07-21 for the
# MAGIC synthetic data) is the business date used for snapshots and aging. Using
# MAGIC today's real date would age every synthetic item by months.
# MAGIC
# MAGIC Section 4 runs four integrity checks in increasing order of how quietly they fail.

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Repos/ap-lakehouse", "Repo root")
dbutils.widgets.dropdown("mode", "plan", ["plan", "dry_run", "run"], "Mode")
dbutils.widgets.text("jobs", "", "Jobs (comma list, blank = all)")
dbutils.widgets.text("as_of", "", "As-of date yyyy-mm-dd (blank = last extract date)")

REPO_ROOT = dbutils.widgets.get("repo_root")
MODE = dbutils.widgets.get("mode")
JOBS = [j.strip() for j in dbutils.widgets.get("jobs").split(",") if j.strip()] or None
AS_OF = dbutils.widgets.get("as_of").strip() or None

import sys
SRC = f"{REPO_ROOT}/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import importlib
import ap_platform.tables as tables
import ap_platform.runlog as runlog
import ap_platform.spec as spec
import ap_platform.silver.dq as dq
import ap_platform.silver.pipeline as pipeline
import ap_platform.gold.keys as keys
import ap_platform.gold.dimensions as dims
import ap_platform.gold.facts as facts
import ap_platform.gold.aggregates as aggs
import ap_platform.gold.registry as registry
import ap_platform.orchestrator as orch
import ap_platform.lint as lint
for _m in (tables, runlog, spec, dq, pipeline, keys, dims, facts, aggs, registry, orch, lint):
    importlib.reload(_m)

if AS_OF:
    # make the override visible to snapshot builders
    pipeline.JobContext.as_of = AS_OF

try:
    ACTOR = spark.sql("SELECT current_user()").collect()[0][0]
except Exception:
    ACTOR = "unknown"

# COMMAND ----------

# MAGIC %md ## 1. Tenant-agnostic check and plan

# COMMAND ----------

findings = lint.scan_for_tenant_literals(f"{SRC}/ap_platform/gold")
assert not findings, findings
p = orch.plan(spark, "GOLD", JOBS)
display(spark.createDataFrame(p.rows(), orch.PLAN_SCHEMA))
if not p.ok:
    raise SystemExit("PLAN FAILED — see the errors column. Nothing was written.")
print(f"plan OK: {len(p.jobs)} jobs in {max(j.wave for j in p.jobs)} waves; gold code is tenant-agnostic")

# COMMAND ----------

# MAGIC %md ## 2–3. Run

# COMMAND ----------

if MODE in ("dry_run", "run"):
    run = orch.run(spark, "GOLD", jobs=JOBS, dry_run=(MODE == "dry_run"), actor=ACTOR)
    print(f"\nbatch {run.batch_id}: {run.status}" + ("  (DRY RUN — nothing written)" if MODE == "dry_run" else ""))
    display(spark.createDataFrame(run.summary_rows(), runlog.SUMMARY_SCHEMA))
else:
    print("mode=plan: nothing run. A gold dry run on a fresh lake skips most jobs (their inputs do not exist yet); "
          "use mode=run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Integrity checks
# MAGIC 1. **Row counts** — the obvious one.
# MAGIC 2. **Orphan dimension keys** — a NULL `vendor_key` means the vendor master had
# MAGIC    not arrived (or the vendor was quarantined). Kept deliberately; must be watched.
# MAGIC 3. **Fan-out** — `fact_ap_payment_application` must have at least as many rows as
# MAGIC    payments and MORE than invoices settled (an invoice settled by two payments
# MAGIC    is two applications). Applications equal to invoices settled, with partial
# MAGIC    payments present, means the bridge collapsed and partial payments are lost.
# MAGIC    In the synthetic data each payment settles exactly one invoice, so
# MAGIC    applications = payments is expected; applications > invoices is the test.
# MAGIC 4. **Point-in-time resolution** — more than one row per invoice key means
# MAGIC    `resolve_scd2` matched two dimension versions. The one to care about.

# COMMAND ----------

if MODE == "run":
    names = sorted(r.tableName for r in spark.sql("SHOW TABLES IN ap_gold.ap").collect())
    display(spark.sql(" UNION ALL ".join(f"SELECT '{n}' AS table_name, count(*) AS rows FROM ap_gold.ap.{n}"
                                         for n in names) + " ORDER BY table_name"))

    display(spark.sql("""
        SELECT 'fact_ap_invoice_header' AS fact, tenant_id, count(*) AS rows,
               sum(CASE WHEN vendor_key IS NULL THEN 1 ELSE 0 END) AS orphan_vendor_key
        FROM ap_gold.ap.fact_ap_invoice_header GROUP BY tenant_id
        UNION ALL
        SELECT 'fact_ap_payment', tenant_id, count(*), sum(CASE WHEN vendor_key IS NULL THEN 1 ELSE 0 END)
        FROM ap_gold.ap.fact_ap_payment GROUP BY tenant_id
        ORDER BY fact, tenant_id"""))

    fan = spark.sql("""
        SELECT a.tenant_id, count(*) AS applications, count(DISTINCT a.payment_key) AS payments,
               count(DISTINCT a.invoice_key) AS invoices_settled,
               sum(CASE WHEN is_partial THEN 1 ELSE 0 END) AS partial_applications
        FROM ap_gold.ap.fact_ap_payment_application a GROUP BY a.tenant_id ORDER BY a.tenant_id""")
    display(fan)

    dup = spark.sql("""
        SELECT tenant_id, company_code, invoice_doc_id, fiscal_year, count(*) AS n
        FROM ap_gold.ap.fact_ap_invoice_header
        GROUP BY tenant_id, company_code, invoice_doc_id, fiscal_year HAVING count(*) > 1""")
    n_dup = dup.count()
    print(f"point-in-time check: {n_dup} invoice key(s) with more than one row (expect 0)")
    if n_dup:
        display(dup.limit(20))
    assert n_dup == 0, "resolve_scd2 duplicated facts: check the half-open interval join"

    display(spark.sql("SELECT * FROM ap_gold.ap.fact_reconciliation ORDER BY tenant_id, gold_table"))
