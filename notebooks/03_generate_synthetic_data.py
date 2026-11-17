# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Generate Synthetic Source Data
# MAGIC
# MAGIC Produces three tenants' worth of deliberately messy AP data, each in its
# MAGIC own ERP's native shape and format, and lands it in
# MAGIC `/Volumes/ap_bronze/landing/inbox/{tenant}/{entity}/{yyyy}/{mm}/{dd}/`.
# MAGIC
# MAGIC Nothing downstream of this notebook knows the data is synthetic. Step 3
# MAGIC reads these files through `ap_control.config.ingestion_config` exactly as
# MAGIC it would read a real SFTP drop.
# MAGIC
# MAGIC **The defect ledger is the reason this notebook exists.** Messy data on
# MAGIC its own cannot test a DQ framework: a rule that finds nothing is
# MAGIC indistinguishable from a defect that was never injected. Every injection
# MAGIC writes a row naming the record, the defect, and the rule expected to catch
# MAGIC it. Step 4's grading exercise is then one query:
# MAGIC `injected LEFT JOIN caught WHERE caught IS NULL`.
# MAGIC
# MAGIC **Idempotency.** Every value derives from the seed, and each tenant's
# MAGIC inbox subtree is purged before writing, so a re-run replaces the landing
# MAGIC zone with byte-identical files. Re-running at a different `scale` cannot
# MAGIC leave a previous run's files behind for the next batch to pick up.
# MAGIC
# MAGIC **Free Edition note.** `scale` defaults to 0.04 for the first pass (375
# MAGIC files, 45,532 rows). At 0.2 it produces 415 files and 226,140 rows, roughly
# MAGIC 10k invoices across the three tenants instead of 50k. The shape, the
# MAGIC defect mix and the drift events are identical; only the row counts shrink.
# MAGIC Set `scale` to 1.0 for the full brief. In production the equivalent knob
# MAGIC does not exist: you get whatever the source sends.

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Repos/ap-lakehouse", "Repo root")
dbutils.widgets.text("inbox_root", "/Volumes/ap_bronze/landing/inbox", "Inbox volume root")
dbutils.widgets.text("scale", "0.04", "Scale factor (0.04 first pass, 0.2 full pass)")
dbutils.widgets.text("initial_load_date", "2026-07-01", "Initial load date")
dbutils.widgets.text("n_delta_days", "20", "Number of daily deltas")
dbutils.widgets.dropdown("snapshot_mode", "auto", ["auto", "all"], "TEN001 snapshots")
dbutils.widgets.dropdown("purge", "true", ["true", "false"], "Purge inbox first")
dbutils.widgets.dropdown("persist_tables", "true", ["true", "false"], "Persist ledger/manifest")

REPO_ROOT = dbutils.widgets.get("repo_root")
INBOX_ROOT = dbutils.widgets.get("inbox_root").rstrip("/")
SCALE = float(dbutils.widgets.get("scale"))
N_DELTA_DAYS = int(dbutils.widgets.get("n_delta_days"))
SNAPSHOT_MODE = dbutils.widgets.get("snapshot_mode")
PURGE = dbutils.widgets.get("purge") == "true"
PERSIST = dbutils.widgets.get("persist_tables") == "true"

# COMMAND ----------

import importlib
import json
import os
import sys
import uuid
from datetime import date, datetime

SRC = f"{REPO_ROOT}/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import ap_platform.datagen.defects as dfx
import ap_platform.datagen.generators as gen
import ap_platform.datagen.writers as wr

for _m in (dfx, gen, wr):
    importlib.reload(_m)

RUN_ID = f"DG-{datetime.utcnow():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
print(f"run_id       : {RUN_ID}")
print(f"inbox root   : {INBOX_ROOT}")
print(f"scale        : {SCALE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Which tenants, and where the volume lives
# MAGIC
# MAGIC The tenant list comes from `tenant_registry`, not from a literal in this
# MAGIC notebook. When TEN004 is onboarded in Step 6, the only change needed here
# MAGIC is a row in that table plus a profile entry in `generators.TENANT_PROFILE`
# MAGIC — and the second of those is a property of the *fake* source, not of the
# MAGIC platform.

# COMMAND ----------

registered = [
    r.tenant_id for r in spark.sql("""
        SELECT tenant_id
        FROM ap_control.config.tenant_registry
        WHERE is_active AND onboarding_status IN ('ACTIVE', 'ONBOARDING')
        ORDER BY tenant_id
    """).collect()
]
tenants = [t for t in registered if t in gen.TENANT_PROFILE]
skipped = [t for t in registered if t not in gen.TENANT_PROFILE]

print(f"registered and generating : {tenants}")
if skipped:
    print(f"registered, NO generator profile (expected for a real source): {skipped}")
assert tenants, "no active tenant has a generator profile; check tenant_registry"

# Writing through the local file API works for managed volumes on serverless.
# The alternative, dbutils.fs.put, is one call per file and would be far slower
# here. Either way the volume is the durable landing zone: no DBFS root, no
# external location, which is decision D7 and also a Free Edition constraint.
os.makedirs(INBOX_ROOT, exist_ok=True)
print(f"inbox writable: {os.access(INBOX_ROOT, os.W_OK)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate the business dataset
# MAGIC
# MAGIC Semantic defects only at this stage: duplicates, orphans, over-receipts,
# MAGIC sign errors, SoD breaches. These are facts about the business data and
# MAGIC survive any file format.

# COMMAND ----------

ledger = dfx.DefectLedger()
gcfg = gen.GenConfig(scale=SCALE)

datasets = []
for t in tenants:
    d = gen.generate_tenant(gcfg, t, ledger)
    datasets.append(d)
    counts = ", ".join(f"{k}={v:,}" for k, v in d.summary().items() if v)
    print(f"{t}: {counts}")

print(f"\nsemantic defects injected: {len(ledger):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Render to native formats and land the files
# MAGIC
# MAGIC Representational defects are injected here: date formats, unparseable
# MAGIC dates, thousands separators, decimal commas, currency symbols, blank
# MAGIC currency codes, whitespace and case damage on join keys. Then delivery,
# MAGIC in each tenant's declared load type:
# MAGIC
# MAGIC | Tenant | Load type | Deletes representable? |
# MAGIC |---|---|---|
# MAGIC | TEN001 | FULL_SNAPSHOT | by absence from the next snapshot |
# MAGIC | TEN002 | CDC | yes, `CDC_OP = 'D'` |
# MAGIC | TEN003 | INCREMENTAL | **no** — a cancelled invoice just stops arriving |
# MAGIC
# MAGIC That last cell is a real control gap, left in deliberately. Step 6 records
# MAGIC it in the control register; it is not something a reconciliation query can
# MAGIC fix, because the information never arrives.

# COMMAND ----------

wcfg = wr.WriteConfig(
    root=INBOX_ROOT,
    initial_load_date=date.fromisoformat(dbutils.widgets.get("initial_load_date")),
    n_delta_days=N_DELTA_DAYS,
    snapshot_days=None if SNAPSHOT_MODE == "all" else (),
    purge=PURGE,
)

manifest = wr.write_all(datasets, wcfg, ledger)

print(f"files written       : {len(manifest):,}")
print(f"rows written        : {sum(m['record_count'] for m in manifest):,}")
print(f"bytes written       : {sum(m['byte_count'] for m in manifest):,}")
print(f"total defects logged: {len(ledger):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema drift events
# MAGIC
# MAGIC Three, one per tenant, each a different class of problem. Step 3 must
# MAGIC auto-accept the first and halt on the other two. Note that the two
# MAGIC breaking drifts are dangerous in opposite ways: DRIFT_002 produces
# MAGIC *silent nulls* where a cast fails, and DRIFT_003 produces a column that
# MAGIC is simply gone. Neither raises an error on read.

# COMMAND ----------

drift_rows = [m for m in manifest if m.get("drift_event_id")]
for d in dfx.DRIFT_EVENTS:
    files = [m for m in drift_rows if m["drift_event_id"] == d.event_id]
    first = min((m["extract_date"] for m in files), default=None)
    print(f"{d.event_id}  {d.tenant_id}  {d.entity:16} {d.drift_type:16} "
          f"{d.classification:9} from day {d.effective_from_day:>2} "
          f"({first or 'NOT LANDED'})  files={len(files)}")
    print(f"           column   : {d.column_name}")
    print(f"           expected : {d.expected_action}")

assert all(any(m["drift_event_id"] == d.event_id for m in drift_rows)
           for d in dfx.DRIFT_EVENTS
           if d.tenant_id in tenants and d.effective_from_day <= N_DELTA_DAYS), \
    "a drift event did not land: check n_delta_days and snapshot_mode"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persist the ledger and the manifest
# MAGIC
# MAGIC These two tables are the **test harness**, not the control plane. They
# MAGIC describe the current contents of the landing zone, which this notebook has
# MAGIC just replaced wholesale, so they are overwritten rather than merged. They
# MAGIC live in `ap_control.ops` for convenience of joining; in a real deployment
# MAGIC they would sit in a separate `ap_test` catalog so that nothing about
# MAGIC production behaviour could ever depend on knowing where the defects are.

# COMMAND ----------

from pyspark.sql.types import (IntegerType, LongType, StringType, StructField,
                               StructType)

LEDGER_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("tenant_id", StringType(), False),
    StructField("entity", StringType(), False),
    StructField("defect_code", StringType(), False),
    StructField("business_key", StringType(), True),
    StructField("detail", StringType(), True),
    StructField("original_value", StringType(), True),
    StructField("injected_value", StringType(), True),
    StructField("expected_rule", StringType(), True),
    StructField("dq_dimension", StringType(), True),
    StructField("expected_severity", StringType(), True),
])

MANIFEST_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("tenant_id", StringType(), False),
    StructField("entity", StringType(), False),
    StructField("load_type", StringType(), True),
    StructField("load_day", IntegerType(), True),
    StructField("extract_date", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("record_count", LongType(), True),
    StructField("byte_count", LongType(), True),
    StructField("column_count", IntegerType(), True),
    StructField("op_counts", StringType(), True),
    StructField("drift_event_id", StringType(), True),
])


def _rows(items, schema):
    names = [f.name for f in schema.fields]
    return [tuple(r.get(n) for n in names) for r in items]


if PERSIST:
    led = [{**e, "run_id": RUN_ID} for e in ledger.entries]
    man = [{**m, "run_id": RUN_ID} for m in manifest]

    from ap_platform import tables

    tables.overwrite(spark, spark.createDataFrame(_rows(led, LEDGER_SCHEMA), LEDGER_SCHEMA),
                     "ap_control.ops.synthetic_defect_ledger")
    tables.overwrite(spark, spark.createDataFrame(_rows(man, MANIFEST_SCHEMA), MANIFEST_SCHEMA),
                     "ap_control.ops.synthetic_file_manifest")

    spark.sql("""
        COMMENT ON TABLE ap_control.ops.synthetic_defect_ledger IS
        'TEST HARNESS. One row per deliberately injected data defect, naming the
         record and the DQ rule expected to catch it. Overwritten by notebook 03.
         Production DQ logic must never read this table.'
    """)
    spark.sql("""
        COMMENT ON TABLE ap_control.ops.synthetic_file_manifest IS
        'TEST HARNESS. One row per file landed in the bronze inbox by notebook 03,
         with its record count, load type and any schema-drift event in effect.'
    """)
    print("persisted ap_control.ops.synthetic_defect_ledger and synthetic_file_manifest")

# A copy on the artifacts volume too, so the ledger outlives a table drop and can
# be diffed between runs outside Spark.
art_dir = f"/Volumes/ap_control/ops/artifacts/datagen/{RUN_ID}"
try:
    os.makedirs(art_dir, exist_ok=True)
    with open(f"{art_dir}/defect_ledger.json", "w") as fh:
        json.dump(ledger.entries, fh, indent=1, default=str)
    with open(f"{art_dir}/file_manifest.json", "w") as fh:
        json.dump(manifest, fh, indent=1, default=str)
    with open(f"{art_dir}/run_config.json", "w") as fh:
        json.dump({"run_id": RUN_ID, "scale": SCALE, "seed": gcfg.seed,
                   "tenants": tenants, "n_delta_days": N_DELTA_DAYS,
                   "initial_load_date": wcfg.initial_load_date.isoformat(),
                   "snapshot_mode": SNAPSHOT_MODE}, fh, indent=1)
    print(f"artifacts    : {art_dir}")
except Exception as exc:                                   # noqa: BLE001
    print(f"artifact volume write skipped ({type(exc).__name__}: {exc}). "
          "Create /Volumes/ap_control/ops/artifacts in notebook 00 if missing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. What landed, and what was broken
# MAGIC
# MAGIC Read the second table below as the answer key for Step 4. Every code has a
# MAGIC target rule except the handful whose `expected_rule` is null — those are
# MAGIC deliberate gaps, and finding them is Exercise 2.6.

# COMMAND ----------

if PERSIST:
    display(spark.sql("""
        SELECT tenant_id, load_type, entity,
               count(*)                       AS files,
               sum(record_count)              AS rows_landed,
               count(DISTINCT extract_date)   AS days,
               max(drift_event_id)            AS drift
        FROM ap_control.ops.synthetic_file_manifest
        GROUP BY tenant_id, load_type, entity
        ORDER BY tenant_id, entity
    """))

# COMMAND ----------

if PERSIST:
    display(spark.sql("""
        SELECT defect_code, dq_dimension, expected_severity,
               coalesce(expected_rule, '** NO RULE — exercise 2.6 **') AS expected_rule,
               count(*)                                   AS injected,
               count(DISTINCT tenant_id)                  AS tenants,
               concat_ws(',', collect_set(tenant_id))     AS tenant_list
        FROM ap_control.ops.synthetic_defect_ledger
        GROUP BY ALL
        ORDER BY expected_severity, injected DESC
    """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Landing-zone sanity check
# MAGIC
# MAGIC Every active `ingestion_config` row must have at least one file for the
# MAGIC initial load date, matching its declared `file_glob`. A miss here means
# MAGIC config and source disagree, which is the single most common cause of a
# MAGIC silently empty bronze table.

# COMMAND ----------

import fnmatch

cfg_rows = spark.sql("""
    SELECT tenant_id, entity, file_glob, source_path_pattern, load_type
    FROM ap_control.config.ingestion_config
    WHERE is_active
    ORDER BY tenant_id, entity
""").collect()

d0 = wcfg.initial_load_date
misses, hits = [], 0
for r in cfg_rows:
    if r.tenant_id not in tenants:
        continue
    folder = (r.source_path_pattern
              .replace("{tenant_id}", r.tenant_id)
              .replace("{entity}", r.entity)
              .replace("{yyyy}", f"{d0.year:04d}")
              .replace("{mm}", f"{d0.month:02d}")
              .replace("{dd}", f"{d0.day:02d}"))
    try:
        names = os.listdir(folder)
    except FileNotFoundError:
        names = []
    matched = [n for n in names if fnmatch.fnmatch(n, r.file_glob)]
    if matched:
        hits += 1
    else:
        misses.append(f"{r.tenant_id}/{r.entity}: no file matching "
                      f"{r.file_glob} in {folder}")

print(f"ingestion_config rows with a matching initial-load file: {hits}/"
      f"{len([r for r in cfg_rows if r.tenant_id in tenants])}")
for m in misses:
    print("  MISS:", m)
assert not misses, "config declares sources that did not land; see misses above"

print(f"\nOK. Run notebook 04 next to validate the whole foundation.\nrun_id: {RUN_ID}")
