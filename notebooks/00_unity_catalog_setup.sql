-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 — Unity Catalog Setup
-- MAGIC
-- MAGIC Creates the four catalogs, their schemas, managed volumes for landing files,
-- MAGIC and the grant model. Idempotent: every statement is `IF NOT EXISTS` or a
-- MAGIC `GRANT` (grants are additive and safe to re-run).
-- MAGIC
-- MAGIC ## Free Edition reality check — read before running
-- MAGIC
-- MAGIC | Statement | Free Edition | Fallback |
-- MAGIC |---|---|---|
-- MAGIC | `CREATE CATALOG` | Usually allowed, but some Free/trial workspaces pin you to `workspace` or `main` | Run `set_prefix` in Cmd 3 to `main.ap_` and use schemas instead of catalogs: `main.ap_control_config`, etc. Everything downstream reads the names from widgets, so nothing else changes |
-- MAGIC | `CREATE SCHEMA` / `CREATE VOLUME` (managed) | Allowed | none needed |
-- MAGIC | `CREATE EXTERNAL LOCATION` | **Not available** — no cloud credential to attach | We use managed volumes only, by design. This is why the project never references `s3://` or `abfss://` |
-- MAGIC | `CREATE GROUP` | **Not a SQL statement in Databricks at all.** Groups live in the account console / SCIM / Terraform | Create the five groups in Account Console → User management → Groups. If your Free Edition workspace is single-user with no group management, run Cmd 8 instead: it records the intended entitlement model in `ap_control.config.access_model` so the design is testable and reviewable even when it cannot be enforced |
-- MAGIC | `GRANT ... TO <group>` | Fails if the principal does not exist | Cmd 7 is wrapped so a missing principal logs a warning instead of aborting the notebook |
-- MAGIC | `ALTER TABLE ... SET ROW FILTER` | May be unavailable | Covered in Step 6 (governance). Fallback is a secure view per tenant. Not used here |
-- MAGIC
-- MAGIC **Production alternative to all of this:** none of it should be a notebook.
-- MAGIC Catalogs, schemas, volumes, groups and grants belong in Terraform
-- MAGIC (`databricks_catalog`, `databricks_schema`, `databricks_volume`,
-- MAGIC `databricks_group`, `databricks_grants`) so that access is reviewable in a
-- MAGIC pull request and reproducible across dev/test/prod. This notebook exists
-- MAGIC because Free Edition has no Terraform-capable service principal.

-- COMMAND ----------

-- MAGIC %md ## Cmd 1 — Parameters

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("env", "dev", "Environment (dev/test/prod)")
-- MAGIC dbutils.widgets.dropdown("layout", "catalog", ["catalog", "schema_fallback"], "Layout")
-- MAGIC
-- MAGIC env = dbutils.widgets.get("env")
-- MAGIC layout = dbutils.widgets.get("layout")
-- MAGIC
-- MAGIC # In `catalog` layout we get four real catalogs. In `schema_fallback` we fold
-- MAGIC # everything into one catalog and encode the layer in the schema name, for
-- MAGIC # workspaces that will not let us create catalogs.
-- MAGIC if layout == "catalog":
-- MAGIC     NAMES = {
-- MAGIC         "control": "ap_control",
-- MAGIC         "bronze": "ap_bronze",
-- MAGIC         "silver": "ap_silver",
-- MAGIC         "gold": "ap_gold",
-- MAGIC     }
-- MAGIC else:
-- MAGIC     NAMES = {k: f"main.ap_{k}" for k in ("control", "bronze", "silver", "gold")}
-- MAGIC
-- MAGIC spark.conf.set("ap.layout", layout)
-- MAGIC for k, v in NAMES.items():
-- MAGIC     spark.conf.set(f"ap.catalog.{k}", v)
-- MAGIC print(NAMES)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cmd 2 — Catalogs
-- MAGIC Four catalogs, one per layer. The control plane is deliberately its own
-- MAGIC catalog: it has a different access profile (engineers write config, analysts
-- MAGIC never see it) and a different retention profile (operational logs age out
-- MAGIC faster than financial records).

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ap_control
  COMMENT 'Control plane: configuration-as-data, DQ rules, run logs, audit. Source of truth for pipeline behaviour.';

CREATE CATALOG IF NOT EXISTS ap_bronze
  COMMENT 'Raw landed data, append-only, source fidelity preserved. No business logic.';

CREATE CATALOG IF NOT EXISTS ap_silver
  COMMENT 'Conformed, deduplicated, DQ-gated canonical AP model. Business logic lives here.';

CREATE CATALOG IF NOT EXISTS ap_gold
  COMMENT 'Dimensional model and semantic layer for consumption.';

-- COMMAND ----------

-- MAGIC %md ## Cmd 3 — Schemas

-- COMMAND ----------

-- ap_control: config (declarative, from YAML), ref (reference data), ops (runtime logs)
CREATE SCHEMA IF NOT EXISTS ap_control.config
  COMMENT 'Declarative configuration loaded from the repo YAML. Never hand-edited in production.';
CREATE SCHEMA IF NOT EXISTS ap_control.ref
  COMMENT 'Reference and lookup data: currency rates, payment terms, GL accounts, tax codes.';
CREATE SCHEMA IF NOT EXISTS ap_control.ops
  COMMENT 'Runtime operational tables: run logs, DQ results, quarantine, reconciliation, drift.';

-- ap_bronze: landing (volumes) + ap (tables)
CREATE SCHEMA IF NOT EXISTS ap_bronze.landing
  COMMENT 'Managed volumes holding raw tenant files before ingestion.';
CREATE SCHEMA IF NOT EXISTS ap_bronze.ap
  COMMENT 'Bronze Delta tables, one per source entity, shared across tenants.';

CREATE SCHEMA IF NOT EXISTS ap_silver.ap
  COMMENT 'Silver conformed AP entities.';
CREATE SCHEMA IF NOT EXISTS ap_silver.quarantine
  COMMENT 'Records diverted by QUARANTINE-severity DQ rules, with rule lineage, awaiting re-drive.';

CREATE SCHEMA IF NOT EXISTS ap_gold.ap
  COMMENT 'Gold star schema: dimensions, facts, aggregates.';
CREATE SCHEMA IF NOT EXISTS ap_gold.semantic
  COMMENT 'Metric views implementing the KPI dictionary. The only layer analysts should query.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cmd 4 — Volumes
-- MAGIC Managed volumes only. `inbox` is where tenants drop files; `archive` is where
-- MAGIC processed files move; `quarantine` holds poison files that could not be parsed
-- MAGIC at all. `artifacts` holds the defect ledger and config snapshots.
-- MAGIC
-- MAGIC **Production alternative:** in a real deployment `inbox` would be an external
-- MAGIC volume over a cloud bucket that the tenant's SFTP or integration platform
-- MAGIC writes to directly, so we never move bytes through a workspace. The folder
-- MAGIC contract below is deliberately identical either way, so switching is a config
-- MAGIC change, not a rewrite.

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS ap_bronze.landing.inbox
  COMMENT 'Tenant file drop. Contract: /Volumes/ap_bronze/landing/inbox/<tenant_id>/<entity>/<yyyy>/<mm>/<dd>/<file>';

CREATE VOLUME IF NOT EXISTS ap_bronze.landing.archive
  COMMENT 'Successfully ingested files, moved here by the ingestion framework. Same folder contract.';

CREATE VOLUME IF NOT EXISTS ap_bronze.landing.poison
  COMMENT 'Files that could not be read at all (corrupt, wrong format, unreadable encoding). Never silently dropped.';

CREATE VOLUME IF NOT EXISTS ap_control.ops.artifacts
  COMMENT 'Generated artifacts: synthetic-data defect ledger, config snapshots, schema contracts.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cmd 5 — The intended group model
-- MAGIC
-- MAGIC Five roles. The split that matters is `ap_engineer` vs `ap_analyst_*`:
-- MAGIC engineers can read bronze (raw, unmasked, including vendor bank details),
-- MAGIC analysts cannot — they only ever see gold, filtered to their own tenant.
-- MAGIC
-- MAGIC | Group | ap_control | ap_bronze | ap_silver | ap_gold |
-- MAGIC |---|---|---|---|---|
-- MAGIC | `ap_platform_admin` | ALL PRIVILEGES | ALL PRIVILEGES | ALL PRIVILEGES | ALL PRIVILEGES |
-- MAGIC | `ap_engineer` | SELECT + MODIFY on ops, SELECT on config | SELECT, MODIFY | SELECT, MODIFY | SELECT, MODIFY |
-- MAGIC | `ap_analyst_TEN001` | none | none | none | SELECT on `ap_gold.semantic`, row-filtered to TEN001 |
-- MAGIC | `ap_analyst_TEN002` | none | none | none | SELECT on `ap_gold.semantic`, row-filtered to TEN002 |
-- MAGIC | `ap_analyst_TEN003` | none | none | none | SELECT on `ap_gold.semantic`, row-filtered to TEN003 |
-- MAGIC | `ap_auditor` | SELECT (all — this is the point of an auditor) | SELECT | SELECT | SELECT, all tenants |
-- MAGIC
-- MAGIC Two deliberate choices worth defending. Analysts get **no** access to
-- MAGIC `ap_silver` even for their own tenant, because silver holds unmasked bank
-- MAGIC details and pre-DQ-gate data that will produce numbers contradicting gold.
-- MAGIC The auditor gets read access to `ap_control.ops` specifically so they can
-- MAGIC verify the DQ and reconciliation controls ran, without asking us for evidence.

-- COMMAND ----------

-- MAGIC %md ## Cmd 6 — Create groups (account console, not SQL)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Databricks has no `CREATE GROUP` SQL. Groups are account-level objects.
-- MAGIC # This cell attempts the SCIM API and tells you exactly what to do if it fails.
-- MAGIC GROUPS = [
-- MAGIC     "ap_platform_admin",
-- MAGIC     "ap_engineer",
-- MAGIC     "ap_analyst_TEN001",
-- MAGIC     "ap_analyst_TEN002",
-- MAGIC     "ap_analyst_TEN003",
-- MAGIC     "ap_auditor",
-- MAGIC ]
-- MAGIC
-- MAGIC try:
-- MAGIC     from databricks.sdk import WorkspaceClient
-- MAGIC
-- MAGIC     w = WorkspaceClient()
-- MAGIC     existing = {g.display_name for g in w.groups.list()}
-- MAGIC     for g in GROUPS:
-- MAGIC         if g in existing:
-- MAGIC             print(f"  exists   {g}")
-- MAGIC         else:
-- MAGIC             w.groups.create(display_name=g)
-- MAGIC             print(f"  created  {g}")
-- MAGIC     GROUPS_AVAILABLE = True
-- MAGIC except Exception as e:
-- MAGIC     GROUPS_AVAILABLE = False
-- MAGIC     print(f"Group management unavailable: {type(e).__name__}: {e}\n")
-- MAGIC     print("This is expected on Free Edition. Two options:")
-- MAGIC     print("  1. Create these groups manually in Account Console > User management > Groups:")
-- MAGIC     for g in GROUPS:
-- MAGIC         print(f"       {g}")
-- MAGIC     print("  2. Skip enforcement and run Cmd 8, which records the intended model")
-- MAGIC     print("     in ap_control.config.access_model so it stays reviewable and testable.")
-- MAGIC
-- MAGIC spark.conf.set("ap.groups_available", str(GROUPS_AVAILABLE).lower())

-- COMMAND ----------

-- MAGIC %md ## Cmd 7 — Grants (skipped gracefully if principals do not exist)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC GRANTS = [
-- MAGIC     # (privileges, securable_type, securable, principal)
-- MAGIC     ("ALL PRIVILEGES", "CATALOG", "ap_control", "ap_platform_admin"),
-- MAGIC     ("ALL PRIVILEGES", "CATALOG", "ap_bronze", "ap_platform_admin"),
-- MAGIC     ("ALL PRIVILEGES", "CATALOG", "ap_silver", "ap_platform_admin"),
-- MAGIC     ("ALL PRIVILEGES", "CATALOG", "ap_gold", "ap_platform_admin"),
-- MAGIC
-- MAGIC     ("USE CATALOG", "CATALOG", "ap_control", "ap_engineer"),
-- MAGIC     ("USE SCHEMA, SELECT", "SCHEMA", "ap_control.config", "ap_engineer"),
-- MAGIC     ("USE SCHEMA, SELECT", "SCHEMA", "ap_control.ref", "ap_engineer"),
-- MAGIC     ("USE SCHEMA, SELECT, MODIFY", "SCHEMA", "ap_control.ops", "ap_engineer"),
-- MAGIC     ("USE CATALOG, USE SCHEMA, SELECT, MODIFY, CREATE TABLE", "CATALOG", "ap_bronze", "ap_engineer"),
-- MAGIC     ("USE CATALOG, USE SCHEMA, SELECT, MODIFY, CREATE TABLE", "CATALOG", "ap_silver", "ap_engineer"),
-- MAGIC     ("USE CATALOG, USE SCHEMA, SELECT, MODIFY, CREATE TABLE", "CATALOG", "ap_gold", "ap_engineer"),
-- MAGIC     ("READ VOLUME, WRITE VOLUME", "VOLUME", "ap_bronze.landing.inbox", "ap_engineer"),
-- MAGIC     ("READ VOLUME, WRITE VOLUME", "VOLUME", "ap_bronze.landing.archive", "ap_engineer"),
-- MAGIC     ("READ VOLUME, WRITE VOLUME", "VOLUME", "ap_control.ops.artifacts", "ap_engineer"),
-- MAGIC
-- MAGIC     # Auditor: read everything, write nothing. Including the ops logs, so they can
-- MAGIC     # self-serve control evidence.
-- MAGIC     ("USE CATALOG, USE SCHEMA, SELECT", "CATALOG", "ap_control", "ap_auditor"),
-- MAGIC     ("USE CATALOG, USE SCHEMA, SELECT", "CATALOG", "ap_bronze", "ap_auditor"),
-- MAGIC     ("USE CATALOG, USE SCHEMA, SELECT", "CATALOG", "ap_silver", "ap_auditor"),
-- MAGIC     ("USE CATALOG, USE SCHEMA, SELECT", "CATALOG", "ap_gold", "ap_auditor"),
-- MAGIC ]
-- MAGIC
-- MAGIC # Analysts: gold semantic layer only. Row filtering to their own tenant is
-- MAGIC # applied in Step 6; without it these grants would expose all tenants, so the
-- MAGIC # semantic schema is NOT granted until the filter exists.
-- MAGIC for t in ("TEN001", "TEN002", "TEN003"):
-- MAGIC     GRANTS.append(("USE CATALOG", "CATALOG", "ap_gold", f"ap_analyst_{t}"))
-- MAGIC
-- MAGIC ok, skipped = 0, []
-- MAGIC for privs, kind, securable, principal in GRANTS:
-- MAGIC     stmt = f"GRANT {privs} ON {kind} {securable} TO `{principal}`"
-- MAGIC     try:
-- MAGIC         spark.sql(stmt)
-- MAGIC         ok += 1
-- MAGIC     except Exception as e:
-- MAGIC         skipped.append((stmt, f"{type(e).__name__}: {str(e)[:120]}"))
-- MAGIC
-- MAGIC print(f"Grants applied: {ok}, skipped: {len(skipped)}")
-- MAGIC for stmt, err in skipped:
-- MAGIC     print(f"  SKIPPED  {stmt}\n           {err}")
-- MAGIC if skipped:
-- MAGIC     print("\nSkipped grants are expected when the groups do not exist yet.")
-- MAGIC     print("Create the groups, then re-run this cell. Grants are idempotent.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cmd 8 — Record the intended access model as data
-- MAGIC Whether or not the grants could be enforced, the intent is stored. This makes
-- MAGIC the access design reviewable in a PR, testable by a negative-access test in
-- MAGIC Step 6, and presentable to an auditor as a control description.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.config.access_model (
  principal      STRING  COMMENT 'Group name as it exists (or should exist) in the account console.',
  securable_type STRING  COMMENT 'CATALOG | SCHEMA | TABLE | VOLUME.',
  securable      STRING  COMMENT 'Fully qualified name of the object.',
  privileges     STRING  COMMENT 'Comma-separated privilege list.',
  tenant_scope   STRING  COMMENT 'ALL for cross-tenant roles, or the single tenant_id an analyst is limited to.',
  row_filtered   BOOLEAN COMMENT 'TRUE if a row filter must also be applied for this grant to be safe.',
  rationale      STRING  COMMENT 'Why this access exists. Required: an unexplained grant is an audit finding.',
  enforced       BOOLEAN COMMENT 'FALSE when the platform could not apply the grant (e.g. Free Edition).',
  updated_at     TIMESTAMP COMMENT 'Last time this row was written by the setup notebook.'
)
USING DELTA
COMMENT 'Intended entitlement model, declared as data so it is reviewable and testable even where it cannot be enforced.';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime, timezone
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC enforced = spark.conf.get("ap.groups_available", "false") == "true"
-- MAGIC now = datetime.now(timezone.utc)
-- MAGIC
-- MAGIC rows = [
-- MAGIC     ("ap_platform_admin", "CATALOG", c, "ALL PRIVILEGES", "ALL", False,
-- MAGIC      "Platform operation and break-glass. Membership must be small and access-reviewed quarterly.", enforced, now)
-- MAGIC     for c in ("ap_control", "ap_bronze", "ap_silver", "ap_gold")
-- MAGIC ] + [
-- MAGIC     ("ap_engineer", "CATALOG", "ap_bronze", "USE CATALOG, USE SCHEMA, SELECT, MODIFY, CREATE TABLE", "ALL", False,
-- MAGIC      "Must read raw unmasked data to debug ingestion. This is the highest-risk standing grant in the platform.", enforced, now),
-- MAGIC     ("ap_engineer", "SCHEMA", "ap_control.config", "USE SCHEMA, SELECT", "ALL", False,
-- MAGIC      "Read-only: config changes go through the repo and the bootstrap notebook, never by hand.", enforced, now),
-- MAGIC     ("ap_engineer", "SCHEMA", "ap_control.ops", "USE SCHEMA, SELECT, MODIFY", "ALL", False,
-- MAGIC      "Pipelines write run logs and DQ results here under the engineer identity.", enforced, now),
-- MAGIC     ("ap_auditor", "CATALOG", "ap_control", "USE CATALOG, USE SCHEMA, SELECT", "ALL", False,
-- MAGIC      "Self-service control evidence: proves DQ and reconciliation ran without asking the team.", enforced, now),
-- MAGIC     ("ap_auditor", "CATALOG", "ap_gold", "USE CATALOG, USE SCHEMA, SELECT", "ALL", False,
-- MAGIC      "Cross-tenant read is intentional for audit. Access is logged.", enforced, now),
-- MAGIC ] + [
-- MAGIC     (f"ap_analyst_{t}", "SCHEMA", "ap_gold.semantic", "USE SCHEMA, SELECT", t, True,
-- MAGIC      "Tenant self-service. UNSAFE without a row filter on tenant_id - see D1 in the Step 1 decision log.", False, now)
-- MAGIC     for t in ("TEN001", "TEN002", "TEN003")
-- MAGIC ]
-- MAGIC
-- MAGIC cols = ["principal", "securable_type", "securable", "privileges", "tenant_scope",
-- MAGIC         "row_filtered", "rationale", "enforced", "updated_at"]
-- MAGIC df = spark.createDataFrame(rows, cols)
-- MAGIC
-- MAGIC # Idempotent: full replace of the declared model, which is the correct semantic
-- MAGIC # for a declarative access design - a grant removed from the repo must disappear.
-- MAGIC df.write.mode("overwrite").option("overwriteSchema", "true") \
-- MAGIC   .saveAsTable("ap_control.config.access_model")
-- MAGIC
-- MAGIC display(spark.table("ap_control.config.access_model").orderBy("principal", "securable"))

-- COMMAND ----------

-- MAGIC %md ## Cmd 9 — Verify

-- COMMAND ----------

-- MAGIC %python
-- MAGIC checks = []
-- MAGIC for c in ("ap_control", "ap_bronze", "ap_silver", "ap_gold"):
-- MAGIC     n = spark.sql(f"SHOW SCHEMAS IN {c}").count()
-- MAGIC     checks.append((f"catalog {c}", "OK", f"{n} schemas"))
-- MAGIC for v in ("ap_bronze.landing.inbox", "ap_bronze.landing.archive",
-- MAGIC           "ap_bronze.landing.poison", "ap_control.ops.artifacts"):
-- MAGIC     cat, sch, vol = v.split(".")
-- MAGIC     found = spark.sql(f"SHOW VOLUMES IN {cat}.{sch}") \
-- MAGIC                  .filter(f"volume_name = '{vol}'").count()
-- MAGIC     checks.append((f"volume {v}", "OK" if found else "MISSING", f"/Volumes/{cat}/{sch}/{vol}"))
-- MAGIC
-- MAGIC display(spark.createDataFrame(checks, ["object", "status", "detail"]))
-- MAGIC assert all(s == "OK" for _, s, _ in checks), "Setup incomplete - see table above"
-- MAGIC print("\nUnity Catalog foundation ready. Next: 01_control_plane_ddl.sql")
