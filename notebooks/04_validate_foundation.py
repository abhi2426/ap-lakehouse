# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Validate the Foundation
# MAGIC
# MAGIC The gate at the end of Step 2. Nothing in Step 3 should be written until
# MAGIC every check here passes, because each failure mode below produces a
# MAGIC pipeline that runs green and delivers nothing:
# MAGIC
# MAGIC * a `file_glob` that matches no file → an empty bronze table, no error
# MAGIC * a `primary_key_columns` naming a column the file lacks → a MERGE on null
# MAGIC * a `source_field` that does not exist → a canonical column of all nulls
# MAGIC * a `depends_on` cycle → a DAG that deadlocks on first run
# MAGIC * a DQ rule referenced but never registered → a defect class nobody checks
# MAGIC
# MAGIC All five are silent. That is why this is a notebook with an `assert` at the
# MAGIC end and not a page of guidance.
# MAGIC
# MAGIC **Run order:** 00 → 01 → 02 → 03 → **04**. Re-run 04 after any config change.
# MAGIC
# MAGIC Checks are `BLOCKING` or `ADVISORY`. Blocking failures raise; advisory ones
# MAGIC print and are expected to be non-empty — the control gaps documented in
# MAGIC Step 1 are real and this notebook reports them rather than hiding them.

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Repos/ap-lakehouse", "Repo root")
dbutils.widgets.text("initial_load_date", "2026-07-01", "Initial load date")
dbutils.widgets.dropdown("check_files", "true", ["true", "false"], "Check landed files")

REPO_ROOT = dbutils.widgets.get("repo_root")
CHECK_FILES = dbutils.widgets.get("check_files") == "true"

# COMMAND ----------

import fnmatch
import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime

D0 = date.fromisoformat(dbutils.widgets.get("initial_load_date"))


@dataclass
class Result:
    name: str
    severity: str          # BLOCKING | ADVISORY
    passed: bool
    detail: str = ""
    findings: list = field(default_factory=list)


RESULTS: list[Result] = []


def check(name: str, severity: str = "BLOCKING"):
    """Run a check function, capture findings, never let one failure stop the rest.

    A validation notebook that aborts on the first problem makes you fix issues
    one run at a time. Collecting everything first is the difference between one
    debugging session and eleven.
    """
    def deco(fn):
        try:
            findings = fn() or []
        except Exception as exc:                            # noqa: BLE001
            RESULTS.append(Result(name, severity, False,
                                  f"{type(exc).__name__}: {exc}"))
            print(f"[ERROR ] {name}: {type(exc).__name__}: {exc}")
            return fn
        ok = not findings
        RESULTS.append(Result(name, severity, ok,
                              f"{len(findings)} finding(s)" if findings else "clean",
                              list(findings)[:50]))
        tag = "PASS  " if ok else ("FAIL  " if severity == "BLOCKING" else "NOTE  ")
        print(f"[{tag}] {name}" + (f" — {len(findings)} finding(s)" if findings else ""))
        for f in list(findings)[:8]:
            print(f"           {f}")
        if len(findings) > 8:
            print(f"           ... and {len(findings) - 8} more")
        return fn
    return deco


def rows(sql: str):
    return spark.sql(sql).collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Structure: catalogs, schemas, volumes, control tables

# COMMAND ----------

EXPECTED_SCHEMAS = [
    "ap_control.config", "ap_control.ref", "ap_control.ops",
    "ap_bronze.landing", "ap_bronze.ap",
    "ap_silver.ap", "ap_silver.quarantine",
    "ap_gold.ap", "ap_gold.semantic",
]

EXPECTED_TABLES = [
    "ap_control.config.access_model",
    "ap_control.config.tenant_registry",
    "ap_control.config.source_system_registry",
    "ap_control.config.ingestion_config",
    "ap_control.config.schema_mapping",
    "ap_control.config.value_domain_mapping",
    "ap_control.config.dq_rule_registry",
    "ap_control.config.transformation_config",
    "ap_control.config.tolerance_config",
    "ap_control.config.sla_config",
    "ap_control.config.domain_binding",
    "ap_control.ref.currency_rate",
    "ap_control.ref.payment_terms",
    "ap_control.ref.gl_account",
    "ap_control.ref.tax_code",
    "ap_control.ref.company_code",
    "ap_control.ops.batch_run_log",
    "ap_control.ops.task_audit_log",
    "ap_control.ops.dq_result_log",
    "ap_control.ops.quarantine_registry",
    "ap_control.ops.reconciliation_control",
    "ap_control.ops.schema_drift_log",
    "ap_control.ops.config_load_audit",
    "ap_control.ops.onboarding_checklist",
]

EXPECTED_VOLUMES = [
    "ap_bronze.landing.inbox", "ap_bronze.landing.archive",
    "ap_bronze.landing.poison", "ap_control.ops.artifacts",
]


@check("schemas exist")
def _():
    have = set()
    for cat in ("ap_control", "ap_bronze", "ap_silver", "ap_gold"):
        try:
            have |= {f"{cat}.{r.databaseName}" for r in rows(f"SHOW SCHEMAS IN {cat}")}
        except Exception as exc:                            # noqa: BLE001
            return [f"catalog {cat} not reachable: {exc}"]
    return [f"missing schema {s}" for s in EXPECTED_SCHEMAS if s not in have]


@check("control tables exist")
def _():
    out = []
    for t in EXPECTED_TABLES:
        if not spark.catalog.tableExists(t):
            out.append(f"missing table {t}")
    return out


@check("landing volumes exist")
def _():
    have = set()
    for sch in ("ap_bronze.landing", "ap_control.ops"):
        try:
            have |= {f"{sch}.{r.volume_name}" for r in rows(f"SHOW VOLUMES IN {sch}")}
        except Exception as exc:                            # noqa: BLE001
            return [f"cannot list volumes in {sch}: {exc}"]
    return [f"missing volume {v}" for v in EXPECTED_VOLUMES if v not in have]


@check("every control table carries column comments")
def _():
    # An uncommented control table is a control table somebody will misuse.
    # Enforced here rather than trusted, because the DDL is long enough to forget.
    out = []
    for t in EXPECTED_TABLES:
        if not spark.catalog.tableExists(t):
            continue
        cols = rows(f"DESCRIBE TABLE {t}")
        missing = [c.col_name for c in cols
                   if c.col_name and not c.col_name.startswith("#")
                   and not (c.comment or "").strip()]
        if missing:
            out.append(f"{t}: {len(missing)} column(s) with no comment "
                       f"({', '.join(missing[:4])}…)")
    return out


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Config content and referential integrity
# MAGIC
# MAGIC The control plane is a database. It gets the same integrity treatment as
# MAGIC any other, except that Unity Catalog will not enforce it for us here, so
# MAGIC the constraints live as queries that run on every release.

# COMMAND ----------

@check("tenant_registry seeded and coherent")
def _():
    out = []
    n = rows("SELECT count(*) c FROM ap_control.config.tenant_registry")[0].c
    if n < 3:
        out.append(f"expected at least 3 tenants, found {n}")
    out += [f"{r.tenant_id}: {r.problem}" for r in rows("""
        SELECT * FROM (
        SELECT tenant_id,
               CASE
                 WHEN functional_currency IS NULL THEN 'no functional currency'
                 WHEN group_currency IS NULL THEN 'no group currency'
                 WHEN fiscal_calendar IS NULL THEN 'no fiscal calendar'
                 WHEN sla_tier IS NULL THEN 'no SLA tier'
                 WHEN retention_years IS NULL THEN 'no retention period'
               END AS problem
        FROM ap_control.config.tenant_registry
        WHERE is_active
        ) WHERE problem IS NOT NULL   -- HAVING without GROUP BY is an aggregate: Spark rejects it
    """)]
    return out


@check("ingestion_config references resolve")
def _():
    return [f"{r.config_id}: {r.problem}" for r in rows("""
        SELECT * FROM (
        WITH ic AS (SELECT * FROM ap_control.config.ingestion_config WHERE is_active)
        SELECT ic.config_id,
               CASE
                 WHEN t.tenant_id IS NULL THEN 'unknown tenant_id'
                 WHEN s.source_system_id IS NULL THEN 'unknown source_system_id'
                 WHEN m.n IS NULL THEN 'no schema_mapping rows for this entity'
                 WHEN m.pk = 0 THEN 'schema_mapping declares no primary key'
                 WHEN ic.load_type = 'CDC'
                      AND (ic.cdc_sequence_column IS NULL
                           OR ic.cdc_operation_column IS NULL)
                      THEN 'CDC load with no sequence or operation column'
                 WHEN ic.load_type = 'INCREMENTAL' AND ic.watermark_column IS NULL
                      THEN 'INCREMENTAL load with no watermark column'
                 WHEN ic.source_format IN ('CSV', 'DELIMITED') AND ic.delimiter IS NULL
                      THEN 'delimited format with no delimiter'
               END AS problem
        FROM ic
        LEFT JOIN ap_control.config.tenant_registry t USING (tenant_id)
        LEFT JOIN ap_control.config.source_system_registry s USING (source_system_id)
        LEFT JOIN (
            SELECT tenant_id, entity, count(*) n,
                   sum(CASE WHEN is_primary_key THEN 1 ELSE 0 END) pk
            FROM ap_control.config.schema_mapping
            WHERE is_active GROUP BY tenant_id, entity
        ) m ON m.tenant_id = ic.tenant_id AND m.entity = ic.entity
        ) WHERE problem IS NOT NULL
    """)]


@check("schema_mapping has no orphan or duplicated canonical fields")
def _():
    out = [f"{r.tenant_id}/{r.entity}: canonical field {r.canonical_field} "
           f"mapped {r.n} times" for r in rows("""
        SELECT tenant_id, entity, canonical_field, count(*) n
        FROM ap_control.config.schema_mapping
        WHERE is_active
        GROUP BY ALL HAVING count(*) > 1
    """)]
    out += [f"{r.tenant_id}/{r.entity}: mapping with no ingestion_config row"
            for r in rows("""
        SELECT DISTINCT m.tenant_id, m.entity
        FROM ap_control.config.schema_mapping m
        LEFT JOIN ap_control.config.ingestion_config i
               ON i.tenant_id = m.tenant_id AND i.entity = m.entity AND i.is_active
        WHERE m.is_active AND i.config_id IS NULL
    """)]
    # A derived field (no source_field) must say how it is derived, or it is
    # just a column nobody filled in.
    out += [f"{r.mapping_id}: no source_field and no transform_expr"
            for r in rows("""
        SELECT mapping_id FROM ap_control.config.schema_mapping
        WHERE is_active AND source_field IS NULL
          AND (transform_expr IS NULL OR trim(transform_expr) = '')
    """)]
    return out


@check("canonical model is consistent across tenants", "ADVISORY")
def _():
    # Not every tenant can supply every canonical field — TEN003 has no approval
    # stream and no VAT number. This lists the asymmetries so they stay a
    # conscious decision rather than a surprise in a gold metric.
    return [f"{r.entity}.{r.canonical_field}: mapped for {r.tenants} of "
            f"{r.total_tenants} tenants ({r.have})" for r in rows("""
        WITH t AS (
            SELECT count(DISTINCT tenant_id) total
            FROM ap_control.config.ingestion_config WHERE is_active
        ), m AS (
            SELECT entity, canonical_field,
                   count(DISTINCT tenant_id) tenants,
                   concat_ws(',', collect_set(tenant_id)) have
            FROM ap_control.config.schema_mapping WHERE is_active
            GROUP BY entity, canonical_field
        ), e AS (
            SELECT entity, count(DISTINCT tenant_id) n
            FROM ap_control.config.ingestion_config WHERE is_active GROUP BY entity
        )
        SELECT m.entity, m.canonical_field, m.tenants, e.n AS total_tenants, m.have
        FROM m JOIN e USING (entity), t
        WHERE m.tenants < e.n
        ORDER BY m.entity, m.canonical_field
    """)]


@check("dq_rule_registry is well formed")
def _():
    out = [f"{r.rule_id}: {r.problem}" for r in rows("""
        SELECT * FROM (
        SELECT rule_id,
               CASE
                 WHEN severity NOT IN ('ABORT', 'QUARANTINE', 'WARN')
                      THEN concat('invalid severity ', severity)
                 WHEN rule_expression IS NULL OR trim(rule_expression) = ''
                      THEN 'no rule expression'
                 WHEN dq_dimension IS NULL THEN 'no dimension'
                 WHEN fail_threshold_pct IS NOT NULL
                      AND (fail_threshold_pct < 0 OR fail_threshold_pct > 100)
                      THEN concat('threshold out of range: ', fail_threshold_pct)
                 WHEN effective_to IS NOT NULL AND effective_to < effective_from
                      THEN 'effective_to before effective_from'
               END AS problem
        FROM ap_control.config.dq_rule_registry
        WHERE is_active
        ) WHERE problem IS NOT NULL
    """)]
    # Decision D6: every tenant must carry the two tenant_id guards, because a
    # null or wrong tenant_id under a shared-table model is a data leak, not a
    # data quality issue. Confirmed as the whole of the isolation control (D1).
    out += [f"{r.tenant_id}: no ABORT-severity tenant_id rule in scope"
            for r in rows("""
        SELECT t.tenant_id
        FROM ap_control.config.tenant_registry t
        LEFT JOIN ap_control.config.dq_rule_registry d
               ON d.is_active AND d.severity = 'ABORT'
              AND lower(d.rule_expression) LIKE '%tenant_id%'
              AND (d.tenant_id IS NULL OR d.tenant_id IN ('ALL', t.tenant_id))
        WHERE t.is_active
        GROUP BY t.tenant_id HAVING count(d.rule_id) = 0
    """)]
    return out


@check("transformation_config DAG resolves and is acyclic")
def _():
    jobs = rows("""
        SELECT job_id, depends_on FROM ap_control.config.transformation_config
        WHERE is_active
    """)
    known = {j.job_id for j in jobs}
    deps: dict[str, list[str]] = {}
    out = []
    for j in jobs:
        d = [x.strip() for x in (j.depends_on or "").split(",") if x.strip()]
        deps[j.job_id] = d
        out += [f"{j.job_id}: depends on unknown job {x}" for x in d if x not in known]

    # Kahn's algorithm: anything left unvisited sits in a cycle.
    indeg = {k: 0 for k in deps}
    for k, ds in deps.items():
        for d in ds:
            if d in indeg:
                indeg[k] += 1
    queue = [k for k, v in indeg.items() if v == 0]
    seen = 0
    while queue:
        cur = queue.pop()
        seen += 1
        for k, ds in deps.items():
            if cur in ds:
                indeg[k] -= 1
                if indeg[k] == 0:
                    queue.append(k)
    if seen < len(deps):
        stuck = sorted(k for k, v in indeg.items() if v > 0)
        out.append(f"cycle involving {len(stuck)} job(s): {', '.join(stuck[:6])}")
    return out


@check("SCD2 declared only where decision D5 allows")
def _():
    # D5: SCD2 on vendor, vendor_bank, gl_account, cost_center, payment_terms.
    # SCD2 everywhere is a storage bill and a join hazard; SCD2 nowhere loses the
    # fraud trail on bank details. The allow-list is the decision, so it is tested.
    allowed = ("vendor", "vendor_bank", "gl_account", "cost_center", "payment_terms")
    return [f"{r.job_id}: SCD2 on {r.target_table}, outside the D5 allow-list"
            for r in rows("""
        SELECT job_id, target_table, scd_type
        FROM ap_control.config.transformation_config
        WHERE is_active AND upper(coalesce(scd_type, '')) IN ('SCD2', '2')
    """) if not any(a in (r.target_table or "").lower() for a in allowed)]


@check("reference data covers the tenants")
def _():
    out = []
    out += [f"{r.tenant_id}: no FX path {r.functional_currency}->{r.group_currency}"
            for r in rows("""
        SELECT t.tenant_id, t.functional_currency, t.group_currency
        FROM ap_control.config.tenant_registry t
        LEFT JOIN ap_control.ref.currency_rate c
               ON c.from_currency = t.functional_currency
              AND c.to_currency = t.group_currency AND c.is_active
        WHERE t.is_active AND t.functional_currency <> t.group_currency
        GROUP BY ALL HAVING count(c.exchange_rate) = 0
    """)]
    out += [f"{r.tenant_id}: only {r.n} company_code row(s)" for r in rows("""
        SELECT t.tenant_id, count(c.company_code) n
        FROM ap_control.config.tenant_registry t
        LEFT JOIN ap_control.ref.company_code c USING (tenant_id)
        WHERE t.is_active GROUP BY t.tenant_id HAVING count(c.company_code) = 0
    """)]
    out += [f"value domain {r.domain}: mapped for only {r.tenants} tenant(s)"
            for r in rows("""
        SELECT domain, count(DISTINCT tenant_id) tenants
        FROM ap_control.config.value_domain_mapping WHERE is_active
        GROUP BY domain HAVING count(DISTINCT tenant_id) < 3
    """)]
    return out


@check("sensitive fields are flagged before any masking is applied")
def _():
    # Decision D7 masks bank account, tax id and contact data in silver and gold.
    # Masking cannot be applied to a field nobody marked sensitive, so the flag
    # is the actual control and it is checked here, not in Step 6.
    must = {"vendor_bank": ("bank_account", "bank_key"),
            "vendor_master": ("tax_id",)}
    out = []
    for entity, fields in must.items():
        for f in fields:
            miss = rows(f"""
                SELECT t.tenant_id
                FROM ap_control.config.tenant_registry t
                LEFT JOIN ap_control.config.schema_mapping m
                       ON m.tenant_id = t.tenant_id AND m.entity = '{entity}'
                      AND m.canonical_field = '{f}' AND m.is_sensitive
                WHERE t.is_active
                GROUP BY t.tenant_id HAVING count(m.mapping_id) = 0
            """)
            out += [f"{r.tenant_id}: {entity}.{f} not flagged is_sensitive"
                    for r in miss]
    return out


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Source contract: does config match the files that actually landed?
# MAGIC
# MAGIC This is the check that earns the notebook. Every declared primary key and
# MAGIC every mapped `source_field` is looked up in the real landed file, read
# MAGIC through the very reader options the config declares. TEN001 is positional
# MAGIC and headerless, so there the test is on field *count* against the highest
# MAGIC declared ordinal instead of on names.
# MAGIC
# MAGIC This check found a genuine bug during Step 2: TEN003's `invoice_line` PK
# MAGIC named `supplier-number`, a column that file does not carry.

# COMMAND ----------

def _reader_for(r):
    opts = json.loads(r.reader_options or "{}")
    if r.source_format == "JSON":
        rdr = spark.read.format("json")
    else:
        rdr = (spark.read.format("csv")
               .option("delimiter", r.delimiter or ",")
               .option("header", "true" if r.has_header else "false"))
        if r.quote_char:
            rdr = rdr.option("quote", r.quote_char)
    for k, v in opts.items():
        rdr = rdr.option(k, v)
    return rdr


def _folder(r, d: date) -> str:
    return (r.source_path_pattern
            .replace("{tenant_id}", r.tenant_id).replace("{entity}", r.entity)
            .replace("{yyyy}", f"{d.year:04d}").replace("{mm}", f"{d.month:02d}")
            .replace("{dd}", f"{d.day:02d}"))


CFG = rows("""
    SELECT tenant_id, entity, source_format, delimiter, has_header, quote_char,
           reader_options, source_path_pattern, file_glob, primary_key_columns,
           load_type
    FROM ap_control.config.ingestion_config
    WHERE is_active ORDER BY tenant_id, entity
""") if CHECK_FILES else []

FILE_COLUMNS: dict[tuple[str, str], list[str]] = {}


@check("initial-load files landed for every active source")
def _():
    if not CHECK_FILES:
        return []
    out = []
    for r in CFG:
        folder = _folder(r, D0)
        try:
            names = os.listdir(folder)
        except FileNotFoundError:
            names = []
        if not [n for n in names if fnmatch.fnmatch(n, r.file_glob)]:
            out.append(f"{r.tenant_id}/{r.entity}: nothing matching "
                       f"{r.file_glob} in {folder}")
    return out


@check("landed files are readable with the declared reader options")
def _():
    if not CHECK_FILES:
        return []
    out = []
    for r in CFG:
        folder = _folder(r, D0)
        path = f"{folder}/{r.file_glob}"
        try:
            df = _reader_for(r).load(path)
            FILE_COLUMNS[(r.tenant_id, r.entity)] = list(df.columns)
            if not df.columns:
                out.append(f"{r.tenant_id}/{r.entity}: file read but no columns")
        except Exception as exc:                            # noqa: BLE001
            out.append(f"{r.tenant_id}/{r.entity}: unreadable — "
                       f"{type(exc).__name__}: {str(exc)[:120]}")
    return out


@check("declared primary key columns exist in the source file")
def _():
    out = []
    for r in CFG:
        cols = FILE_COLUMNS.get((r.tenant_id, r.entity))
        if cols is None:
            continue
        pks = [p.strip() for p in (r.primary_key_columns or "").split(",") if p.strip()]
        if not pks:
            out.append(f"{r.tenant_id}/{r.entity}: no primary key declared")
            continue
        if r.has_header or r.source_format == "JSON":
            out += [f"{r.tenant_id}/{r.entity}: PK column '{p}' not in file "
                    f"({len(cols)} columns)" for p in pks if p not in cols]
        else:
            # Headerless: the PK is validated against the mapping's ordinals below.
            pass
    return out


@check("mapped source fields exist in the source file")
def _():
    out = []
    mapping = rows("""
        SELECT tenant_id, entity, source_field, ordinal, canonical_field
        FROM ap_control.config.schema_mapping
        WHERE is_active AND source_field IS NOT NULL
    """)
    by_entity: dict[tuple[str, str], list] = {}
    for m in mapping:
        by_entity.setdefault((m.tenant_id, m.entity), []).append(m)

    for r in CFG:
        key = (r.tenant_id, r.entity)
        cols = FILE_COLUMNS.get(key)
        if cols is None:
            continue
        ms = by_entity.get(key, [])
        if not ms:
            out.append(f"{r.tenant_id}/{r.entity}: no mapped source fields")
            continue
        if r.has_header or r.source_format == "JSON":
            out += [f"{r.tenant_id}/{r.entity}: source_field '{m.source_field}' "
                    f"(-> {m.canonical_field}) not in file"
                    for m in ms if m.source_field not in cols]
        else:
            ords = [m.ordinal for m in ms if m.ordinal is not None]
            if not ords:
                out.append(f"{r.tenant_id}/{r.entity}: headerless source with no "
                           f"ordinals in schema_mapping — the layout is undefined")
            elif max(ords) >= len(cols):
                out.append(f"{r.tenant_id}/{r.entity}: mapping reaches ordinal "
                           f"{max(ords)} but the file has {len(cols)} field(s)")
    return out


@check("headerless sources have a dense, gapless ordinal layout")
def _():
    # A gap in the ordinals of a positional layout means a column is being read
    # from the wrong place — every field after the gap is shifted, and the data
    # still looks plausible. Worth failing a release over.
    out = []
    for r in CFG:
        if r.has_header or r.source_format == "JSON":
            continue
        ords = sorted({m.ordinal for m in rows(f"""
            SELECT DISTINCT ordinal FROM ap_control.config.schema_mapping
            WHERE is_active AND tenant_id = '{r.tenant_id}'
              AND entity = '{r.entity}' AND ordinal IS NOT NULL
        """)})
        if ords and ords != list(range(len(ords))):
            gaps = [i for i in range(max(ords) + 1) if i not in ords]
            out.append(f"{r.tenant_id}/{r.entity}: ordinal gaps at {gaps[:6]}")
    return out


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test-harness coherence
# MAGIC
# MAGIC The defect ledger names the rule expected to catch each injection. If a
# MAGIC named rule is not registered and active, Step 4 will measure a gap that is
# MAGIC really a typo. The reverse — a defect with no rule at all — is deliberate,
# MAGIC and is reported as advisory because finding those is Exercise 2.6.

# COMMAND ----------

HAS_LEDGER = spark.catalog.tableExists("ap_control.ops.synthetic_defect_ledger")


@check("every rule named by the defect ledger is registered and active")
def _():
    if not HAS_LEDGER:
        return []
    return [f"defect {r.defect_code} expects rule {r.expected_rule}, which is "
            f"{'missing' if r.registered == 0 else 'inactive'}" for r in rows("""
        SELECT l.defect_code, l.expected_rule, count(d.rule_id) AS registered
        FROM (SELECT DISTINCT defect_code, expected_rule
              FROM ap_control.ops.synthetic_defect_ledger
              WHERE expected_rule IS NOT NULL) l
        LEFT JOIN ap_control.config.dq_rule_registry d
               ON d.rule_id = l.expected_rule AND d.is_active
        GROUP BY ALL HAVING count(d.rule_id) = 0
    """)]


@check("defect classes with no rule to catch them", "ADVISORY")
def _():
    if not HAS_LEDGER:
        return []
    return [f"{r.defect_code} ({r.dq_dimension}, {r.n} injected) has no expected rule"
            for r in rows("""
        SELECT defect_code, dq_dimension, count(*) n
        FROM ap_control.ops.synthetic_defect_ledger
        WHERE expected_rule IS NULL
        GROUP BY ALL ORDER BY n DESC
    """)]


@check("each tenant received defects in every dimension", "ADVISORY")
def _():
    if not HAS_LEDGER:
        return []
    return [f"{r.tenant_id}: no {r.dq_dimension} defects injected" for r in rows("""
        SELECT t.tenant_id, d.dq_dimension
        FROM (SELECT DISTINCT tenant_id FROM ap_control.ops.synthetic_defect_ledger) t
        CROSS JOIN (SELECT DISTINCT dq_dimension
                    FROM ap_control.ops.synthetic_defect_ledger) d
        LEFT JOIN ap_control.ops.synthetic_defect_ledger l
               ON l.tenant_id = t.tenant_id AND l.dq_dimension = d.dq_dimension
        GROUP BY ALL HAVING count(l.defect_code) = 0
    """)]


@check("delivery series is complete and reconciles")
def _():
    if not spark.catalog.tableExists("ap_control.ops.synthetic_file_manifest"):
        return []
    out = []
    # Each tenant's load type must actually be reflected in what landed.
    out += [f"{r.tenant_id}: manifest says {r.manifest_type}, config says "
            f"{r.config_type}" for r in rows("""
        SELECT m.tenant_id,
               max(m.load_type) AS manifest_type,
               max(i.load_type) AS config_type
        FROM ap_control.ops.synthetic_file_manifest m
        JOIN ap_control.config.ingestion_config i
          ON i.tenant_id = m.tenant_id AND i.entity = m.entity AND i.is_active
        GROUP BY m.tenant_id
        HAVING max(m.load_type) <> max(i.load_type)
    """)]
    # An entity that landed exactly one file has no delta series at all, which
    # for a CDC or incremental source means nothing will ever be tested.
    # Vendor master data is exempt: the generator delivers no vendor changes for
    # the CDC and incremental tenants, so one initial file is the correct series.
    out += [f"{r.tenant_id}/{r.entity}: only one file, no delta series"
            for r in rows("""
        SELECT tenant_id, entity, count(*) n
        FROM ap_control.ops.synthetic_file_manifest
        WHERE load_type <> 'FULL_SNAPSHOT'
          AND entity NOT IN ('vendor_master', 'vendor_bank')
        GROUP BY ALL HAVING count(*) < 2
    """)]
    out += [f"{r.tenant_id}/{r.entity}: {r.n} empty file(s) landed" for r in rows("""
        SELECT tenant_id, entity, count(*) n
        FROM ap_control.ops.synthetic_file_manifest
        WHERE record_count = 0 GROUP BY ALL
    """)]
    return out


@check("all three schema-drift events landed")
def _():
    if not spark.catalog.tableExists("ap_control.ops.synthetic_file_manifest"):
        return []
    landed = {r.drift_event_id for r in rows("""
        SELECT DISTINCT drift_event_id FROM ap_control.ops.synthetic_file_manifest
        WHERE drift_event_id IS NOT NULL
    """)}
    return [f"drift event {e} did not land in any file"
            for e in ("DRIFT_001", "DRIFT_002", "DRIFT_003") if e not in landed]


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verdict

# COMMAND ----------

summary = spark.createDataFrame(
    [(r.name, r.severity, "PASS" if r.passed else "FAIL", r.detail,
      " | ".join(str(x) for x in r.findings[:3])) for r in RESULTS],
    "check STRING, severity STRING, status STRING, detail STRING, sample STRING",
)
display(summary)

# COMMAND ----------

blocking = [r for r in RESULTS if r.severity == "BLOCKING" and not r.passed]
advisory = [r for r in RESULTS if r.severity == "ADVISORY" and not r.passed]

print(f"checks run      : {len(RESULTS)}")
print(f"blocking failed : {len(blocking)}")
print(f"advisory notes  : {len(advisory)}")
print()

if advisory:
    print("Advisory findings — expected, and each one is a documented decision:")
    for r in advisory:
        print(f"  {r.name}: {r.detail}")
    print()

if blocking:
    print("BLOCKING failures:")
    for r in blocking:
        print(f"  {r.name}: {r.detail}")
        for f in r.findings[:5]:
            print(f"      {f}")

assert not blocking, (
    f"{len(blocking)} blocking check(s) failed: "
    f"{', '.join(r.name for r in blocking)}"
)

print("Foundation validated. Step 2 is complete.")
print("Carry into Step 3: ingestion_config drives the loop; schema_mapping drives "
      "the canonical projection; ap_control.ops.batch_run_log and task_audit_log "
      "are already waiting for their first rows.")
