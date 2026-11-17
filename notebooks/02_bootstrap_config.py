# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bootstrap the Control Plane from Config-as-Code
# MAGIC
# MAGIC Reads every YAML under `config/` and projects it into `ap_control`.
# MAGIC Idempotent, converging and audited: see the module docstring in
# MAGIC `src/ap_platform/config_loader.py` for why each of those matters.
# MAGIC
# MAGIC **Run this whenever config changes.** It is the only sanctioned way to
# MAGIC change control-plane content. Hand-editing a control table works, and is
# MAGIC exactly how a platform ends up with behaviour nobody can explain.
# MAGIC
# MAGIC **Convergence.** All YAML files are parsed first and merged per target
# MAGIC table, so a table declared by two files (schema_mapping) is loaded as one
# MAGIC set. Rows no longer declared are deactivated with a null-safe MERGE.

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Repos/ap-lakehouse", "Repo root")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run")
dbutils.widgets.dropdown("generate_fx", "true", ["true", "false"], "Generate FX rates")

REPO_ROOT = dbutils.widgets.get("repo_root")
DRY_RUN = dbutils.widgets.get("dry_run") == "true"
GENERATE_FX = dbutils.widgets.get("generate_fx") == "true"

# COMMAND ----------

import sys

SRC = f"{REPO_ROOT}/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

# Reload so an edited module is picked up without detaching the notebook.
import importlib
import ap_platform.config_loader as cl

importlib.reload(cl)

try:
    ACTOR = spark.sql("SELECT current_user()").collect()[0][0]
except Exception:
    ACTOR = "unknown"
print(f"repo_root={REPO_ROOT}  actor={ACTOR}  dry_run={DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cmd 3 — Validate everything before writing anything
# MAGIC A dry run first. Partially-applied configuration is worse than none: half
# MAGIC the rules updated and half not is a state no one can reason about.

# COMMAND ----------

from pathlib import Path

CONFIG_DIR = Path(REPO_ROOT) / "config"

all_blocks = []
parse_errors = []
for path in sorted(list(CONFIG_DIR.rglob("*.yaml")) + list(CONFIG_DIR.rglob("*.yml"))):
    try:
        all_blocks.extend(cl.parse_yaml_file(path))
    except cl.ConfigValidationError as exc:
        parse_errors.append(str(exc))

if parse_errors:
    for e in parse_errors:
        print(f"PARSE ERROR: {e}")
    raise SystemExit("Fix the YAML before continuing. Nothing was written.")

# Merge per target table across files, exactly as load_directory does, so the
# dry run validates what will actually be loaded (and catches a key declared
# in two different files).
all_blocks = cl._coalesce_by_target(all_blocks)

validation_errors = []
for block in all_blocks:
    errs = cl.validate_block(spark, block)
    status = "OK" if not errs else f"{len(errs)} ERROR(S)"
    print(f"{status:>12}  {block.target_table:<45} {len(block.rows):>5} rows  v{block.config_version}")
    validation_errors.extend(errs)

if validation_errors:
    print("\n--- validation errors ---")
    for e in validation_errors:
        print(f"  {e}")
    raise SystemExit("Validation failed. Nothing was written.")

print(f"\nValidated {len(all_blocks)} blocks across {len(set(b.source_file for b in all_blocks))} files.")

# COMMAND ----------

# MAGIC %md ## Cmd 4 — Load

# COMMAND ----------

results = cl.load_directory(spark, CONFIG_DIR, actor=ACTOR, dry_run=DRY_RUN, fail_fast=True)

summary = [
    (r.target_table, r.config_version, r.inserted, r.updated, r.deactivated, r.status)
    for r in results
]
display(
    spark.createDataFrame(
        summary,
        "target_table STRING, config_version STRING, inserted INT, updated INT, deactivated INT, status STRING",
    )
)

if DRY_RUN:
    print("DRY RUN — nothing was written. Counts above are what would have changed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cmd 5 — Generate FX rates
# MAGIC Two years of daily rates, deterministic from a fixed seed so re-running
# MAGIC produces identical values and the load stays idempotent. A random walk,
# MAGIC not a constant: a flat rate would hide every FX bug in the platform, and
# MAGIC FX errors are one of the pain points this project exists to catch.
# MAGIC
# MAGIC Production alternative: land the tenant's own `TCURR` extract, or a feed
# MAGIC from ECB/Refinitiv. Never invent rates in production — a rate that does
# MAGIC not match the tenant's books makes every consolidated figure indefensible.

# COMMAND ----------

if GENERATE_FX and not DRY_RUN:
    import math
    import random
    from datetime import date, timedelta
    from decimal import Decimal

    def _d(x):  # DECIMAL columns refuse Python floats in createDataFrame
        return Decimal(f"{x:.8f}")

    START = date(2024, 1, 1)
    END = date(2026, 12, 31)   # covers the whole simulated delivery window
    BASE = {  # to USD
        "EUR": 1.0850, "GBP": 1.2700, "AUD": 0.6600,
        "CAD": 0.7350, "USD": 1.0000,
    }
    VOL = {"EUR": 0.0025, "GBP": 0.0030, "AUD": 0.0040, "CAD": 0.0025, "USD": 0.0}

    rows = []
    for ccy, base in BASE.items():
        rng = random.Random(f"fx-{ccy}-v1")          # deterministic per currency
        rate, d = base, START
        while d <= END:
            if ccy != "USD":
                rate *= math.exp(rng.gauss(0, VOL[ccy]))
                rate = max(base * 0.80, min(base * 1.20, rate))   # keep it plausible
            for rate_type in ("DAILY_MID", "SPOT"):
                rows.append(("ALL", ccy, "USD", rate_type, d, _d(rate), "SYNTHETIC_POC", True))
            # Month-end and revaluation rates: same value, different type. Using
            # DAILY_MID where MONTH_END is required is a classic silent error,
            # so both types exist from day one to make the mistake possible to catch.
            if (d + timedelta(days=1)).day == 1:
                for rate_type in ("MONTH_END", "REVALUATION"):
                    rows.append(("ALL", ccy, "USD", rate_type, d, _d(rate), "SYNTHETIC_POC", True))
            d += timedelta(days=1)

    schema = (
        "tenant_id STRING, from_currency STRING, to_currency STRING, rate_type STRING, "
        "rate_date DATE, exchange_rate DECIMAL(18,8), rate_source STRING, is_active BOOLEAN"
    )
    from pyspark.sql import functions as F

    fx = spark.createDataFrame(rows, schema).withColumn("updated_at", F.current_timestamp())
    from ap_platform import tables

    # MERGE, not overwrite: an overwrite replaces the schema and drops the
    # column comments notebook 04 requires. Deterministic rates, so idempotent.
    tables.merge(spark, fx, "ap_control.ref.currency_rate",
                 ["tenant_id", "from_currency", "to_currency", "rate_type", "rate_date"])
    print(f"Wrote {fx.count():,} FX rate rows for {len(BASE)} currencies, {START} to {END}.")
else:
    print("FX generation skipped.")

# COMMAND ----------

# MAGIC %md ## Cmd 6 — Verify the control plane is coherent

# COMMAND ----------

checks = spark.sql("""
WITH c AS (
  SELECT 'tenants active'            AS check_name, count(*) AS n, 3   AS expected_min FROM ap_control.config.tenant_registry      WHERE is_active
  UNION ALL SELECT 'source systems',       count(*), 3   FROM ap_control.config.source_system_registry WHERE is_active
  UNION ALL SELECT 'ingestion configs',    count(*), 25  FROM ap_control.config.ingestion_config       WHERE is_active
  UNION ALL SELECT 'schema mappings',      count(*), 100 FROM ap_control.config.schema_mapping         WHERE is_active
  UNION ALL SELECT 'value domain mappings',count(*), 30  FROM ap_control.config.value_domain_mapping   WHERE is_active
  UNION ALL SELECT 'dq rules',             count(*), 20  FROM ap_control.config.dq_rule_registry       WHERE is_active
  UNION ALL SELECT 'transformation jobs',  count(*), 20  FROM ap_control.config.transformation_config  WHERE is_active
  UNION ALL SELECT 'payment terms',        count(*), 9   FROM ap_control.ref.payment_terms             WHERE is_active
  UNION ALL SELECT 'tax codes',            count(*), 10  FROM ap_control.ref.tax_code                  WHERE is_active
  UNION ALL SELECT 'gl accounts',          count(*), 15  FROM ap_control.ref.gl_account
  UNION ALL SELECT 'company codes',        count(*), 7   FROM ap_control.ref.company_code              WHERE is_active
  UNION ALL SELECT 'fx rates',             count(*), 5000 FROM ap_control.ref.currency_rate            WHERE is_active
)
SELECT check_name, n AS actual, expected_min,
       CASE WHEN n >= expected_min THEN 'OK' ELSE 'LOW' END AS status
FROM c ORDER BY check_name
""")
display(checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referential integrity of the config itself
# MAGIC The control plane has its own referential integrity, and nothing enforces
# MAGIC it but this query. An ingestion_config pointing at a tenant that does not
# MAGIC exist produces a task that can never succeed.

# COMMAND ----------

orphans = spark.sql("""
SELECT 'ingestion_config -> tenant_registry' AS relationship, i.config_id AS offending_key
FROM ap_control.config.ingestion_config i
LEFT JOIN ap_control.config.tenant_registry t
  ON i.tenant_id = t.tenant_id AND t.is_active
WHERE i.is_active AND t.tenant_id IS NULL

UNION ALL
SELECT 'ingestion_config -> source_system_registry', i.config_id
FROM ap_control.config.ingestion_config i
LEFT JOIN ap_control.config.source_system_registry s
  ON i.tenant_id = s.tenant_id AND i.source_system_id = s.source_system_id AND s.is_active
WHERE i.is_active AND s.source_system_id IS NULL

UNION ALL
-- Every active ingestion entity needs at least one mapping, or bronze lands
-- and silver silently produces an empty table.
SELECT 'ingestion_config -> schema_mapping (no mappings)', i.config_id
FROM ap_control.config.ingestion_config i
LEFT JOIN (
  SELECT DISTINCT tenant_id, entity FROM ap_control.config.schema_mapping WHERE is_active
) m ON i.tenant_id = m.tenant_id AND i.entity = m.entity
WHERE i.is_active AND m.entity IS NULL

UNION ALL
-- Every mapped entity needs exactly one primary key definition per tenant.
SELECT 'schema_mapping (no primary key)', concat_ws('/', tenant_id, entity)
FROM ap_control.config.schema_mapping
WHERE is_active
GROUP BY tenant_id, entity
HAVING sum(CASE WHEN is_primary_key THEN 1 ELSE 0 END) = 0

UNION ALL
SELECT 'tenant_registry -> sla_config', t.tenant_id
FROM ap_control.config.tenant_registry t
LEFT JOIN (SELECT DISTINCT sla_tier FROM ap_control.config.sla_config WHERE is_active) s
  ON t.sla_tier = s.sla_tier
WHERE t.is_active AND s.sla_tier IS NULL
""")

n_orphans = orphans.count()
display(orphans)
assert n_orphans == 0, f"{n_orphans} config integrity violations — see table above"
print("Control plane loaded and internally consistent. Next: 03_generate_synthetic_data")
