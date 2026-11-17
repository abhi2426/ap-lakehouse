"""
engine — one generic bronze ingestion engine driven by ingestion_config.

There is no per-tenant code in this file. Everything that differs between the
three ERPs (format, delimiter, header, encoding, keys, load type, CDC columns,
watermark) comes from ``ap_control.config.ingestion_config``; positional column
names for headerless files come from ``schema_mapping.ordinal``.

Bronze shape (one table per entity, shared by all tenants)
---------------------------------------------------------
The record itself is kept as ``record MAP<STRING,STRING>``: every source field,
under its source name, as the text the file contained. Three ERPs with three
column vocabularies can then share one table per entity, a new source column
never needs DDL, and nothing is lost to an early cast. Typing is silver's job.

Exactly-once without a stream checkpoint
----------------------------------------
``ap_control.ops.file_ledger`` records every file by path. A file is marked
IN_PROGRESS before its rows are appended and INGESTED after. A re-run skips
INGESTED and POISON files; a file left IN_PROGRESS by a crash has its partial
rows deleted from bronze and is ingested again. Re-running the notebook with
nothing new therefore changes nothing, which is the idempotency check in the
runbook.

Fail isolation (decision D9)
----------------------------
Each source (tenant x entity) is its own task. A BREAKING drift halts that
source only: its drifted file and every later file stay pending, every other
source keeps loading. A poison file is copied to the poison volume and
skipped; the rest of the batch continues.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import tables
from ..runlog import RunLog, now
from . import contracts as C

LEDGER = "ap_control.ops.file_ledger"
CONTRACTS = "ap_control.ops.schema_contract"
DRIFT_LOG = "ap_control.ops.schema_drift_log"
DQ_LOG = "ap_control.ops.dq_result_log"
POISON_ROOT = "/Volumes/ap_bronze/landing/poison"

LEDGER_SCHEMA = (
    "file_path STRING, tenant_id STRING, entity STRING, config_id STRING, file_name STRING, "
    "file_date DATE, file_size BIGINT, file_mtime BIGINT, status STRING, batch_id STRING, "
    "rows_ingested BIGINT, rows_rescued BIGINT, contract_version INT, signature STRING, "
    "drift_id STRING, notes STRING, updated_at TIMESTAMP"
)
DRIFT_SCHEMA = (
    "drift_id STRING, batch_id STRING, tenant_id STRING, source_system_id STRING, entity STRING, "
    "detected_at TIMESTAMP, drift_type STRING, classification STRING, column_name STRING, "
    "expected_type STRING, observed_type STRING, sample_file STRING, action_taken STRING, "
    "status STRING, resolved_by STRING, resolved_at TIMESTAMP, notes STRING"
)
DQ_SCHEMA = (
    "batch_id STRING, rule_id STRING, tenant_id STRING, layer STRING, entity STRING, dq_dimension STRING, "
    "severity STRING, evaluated_at TIMESTAMP, records_evaluated BIGINT, records_passed BIGINT, "
    "records_failed BIGINT, pass_rate_pct DECIMAL(9,4), threshold_pct DECIMAL(5,2), outcome STRING, "
    "failed_sample STRING, action_taken STRING, rule_expression STRING"
)
_DATE_RX = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")


@dataclass
class LandedFile:
    path: str
    name: str
    size: int
    mtime: int
    file_date: date | None


@dataclass
class SourceConfig:
    """One active ingestion_config row, plus its positional names."""
    row: dict
    positional_names: list[str]

    def __getattr__(self, item):
        try:
            return self.row[item]
        except KeyError as exc:
            raise AttributeError(item) from exc

    @property
    def options(self) -> dict:
        try:
            return json.loads(self.row.get("reader_options") or "{}")
        except ValueError:
            return {}

    @property
    def encoding(self) -> str:
        return self.options.get("encoding", "UTF-8")

    @property
    def pk(self) -> list[str]:
        return [c.strip() for c in (self.row.get("primary_key_columns") or "").split(",") if c.strip()]

    @property
    def root(self) -> str:
        """The entity folder: the path pattern up to its first date token."""
        p = self.row["source_path_pattern"].format(
            tenant_id=self.row["tenant_id"], entity=self.row["entity"],
            yyyy="{yyyy}", mm="{mm}", dd="{dd}")
        return p.split("/{yyyy}")[0]


# ---------------------------------------------------------------------------
# Config, ledger and contract access
# ---------------------------------------------------------------------------

def load_sources(spark: SparkSession, tenants: list[str] | None = None,
                 entities: list[str] | None = None) -> list[SourceConfig]:
    rows = [r.asDict() for r in spark.sql("""
        SELECT * FROM ap_control.config.ingestion_config
        WHERE is_active ORDER BY tenant_id, load_priority, entity""").collect()]
    if tenants:
        rows = [r for r in rows if r["tenant_id"] in tenants]
    if entities:
        rows = [r for r in rows if r["entity"] in entities]
    pos = {}
    for r in spark.sql("""
            SELECT tenant_id, entity, source_field, ordinal FROM ap_control.config.schema_mapping
            WHERE is_active AND ordinal IS NOT NULL AND source_field IS NOT NULL""").collect():
        pos.setdefault((r.tenant_id, r.entity), {})[int(r.ordinal)] = r.source_field
    out = []
    for r in rows:
        m = pos.get((r["tenant_id"], r["entity"]), {})
        names = [m.get(i) for i in range(max(m) + 1)] if m else []
        out.append(SourceConfig(r, names))
    return out


def discover(cfg: SourceConfig) -> list[LandedFile]:
    """List landed files for one source, oldest first. Plain directory walk."""
    root = cfg.root
    if not os.path.isdir(root):
        return []
    glob_pat = cfg.row.get("file_glob") or "*"
    found = []
    for dirpath, _dirs, files in os.walk(root):
        for f in files:
            if not fnmatch.fnmatch(f, glob_pat):
                continue
            p = os.path.join(dirpath, f)
            st = os.stat(p)
            m = _DATE_RX.search(p)
            d = date(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None
            found.append(LandedFile(p, f, st.st_size, int(st.st_mtime * 1000), d))
    return sorted(found, key=lambda x: (x.file_date or date.min, x.name))


def discover_autoloader(spark: SparkSession, cfg: SourceConfig, checkpoint_root: str) -> list[LandedFile]:
    """Auto Loader file discovery. Only discovers: parsing stays in this engine.

    Uses cloudFiles with binaryFile metadata so the stream sees each new file
    once; the discovered paths are then filtered through the same ledger. If
    Auto Loader is unavailable the caller falls back to ``discover``.
    """
    ckpt = f"{checkpoint_root}/{cfg.tenant_id}/{cfg.entity}"
    sink = "ap_control.ops.autoloader_discovery"
    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "binaryFile")
              .option("pathGlobFilter", cfg.row.get("file_glob") or "*")
              .option("recursiveFileLookup", "true")
              .load(cfg.root)
              .select(F.col("path"), F.col("length").alias("size"),
                      F.col("modificationTime").alias("mtime"))
              .withColumn("source", F.lit(f"{cfg.tenant_id}/{cfg.entity}")))
    (stream.writeStream.option("checkpointLocation", ckpt).trigger(availableNow=True)
     .toTable(sink).awaitTermination())
    paths = {r.path.replace("dbfs:", "") for r in spark.table(sink)
             .filter(F.col("source") == f"{cfg.tenant_id}/{cfg.entity}").select("path").collect()}
    return [f for f in discover(cfg) if f.path in paths]


def ledger_state(spark: SparkSession, tenant_id: str, entity: str) -> dict[str, dict]:
    if not tables.exists(spark, LEDGER):
        return {}
    return {r.file_path: r.asDict() for r in spark.table(LEDGER)
            .filter((F.col("tenant_id") == tenant_id) & (F.col("entity") == entity)).collect()}


def current_contract(spark: SparkSession, tenant_id: str, entity: str) -> C.Contract | None:
    if not tables.exists(spark, CONTRACTS):
        return None
    r = (spark.table(CONTRACTS)
         .filter((F.col("tenant_id") == tenant_id) & (F.col("entity") == entity) & F.col("is_current"))
         .orderBy(F.col("version").desc()).limit(1).collect())
    return C.Contract.from_json(tenant_id, entity, r[0].version, r[0].columns_json) if r else None


def open_breaking_drift(spark: SparkSession, tenant_id: str, entity: str) -> list[dict]:
    if not tables.exists(spark, DRIFT_LOG):
        return []
    return [r.asDict() for r in spark.table(DRIFT_LOG).filter(
        (F.col("tenant_id") == tenant_id) & (F.col("entity") == entity)
        & (F.col("classification") == "BREAKING") & (F.col("status") == "OPEN")).collect()]


def known_additive(spark: SparkSession, tenant_id: str, entity: str) -> set[str]:
    if not tables.exists(spark, DRIFT_LOG):
        return set()
    return {r.column_name for r in spark.table(DRIFT_LOG).filter(
        (F.col("tenant_id") == tenant_id) & (F.col("entity") == entity)
        & (F.col("classification") == "ADDITIVE")).select("column_name").collect()}


def write_ledger(spark: SparkSession, rows: list[dict]) -> None:
    if rows:
        tables.merge(spark, tables.rows_to_df(spark, rows, LEDGER_SCHEMA), LEDGER, ["file_path"],
                     cluster_by=["tenant_id", "entity"])


def register_contract(spark: SparkSession, contract: C.Contract, source_file: str, actor: str,
                      reason: str, artifact_path: str | None = None) -> None:
    """Make ``contract`` the current version for its source."""
    if tables.exists(spark, CONTRACTS):
        tables.update(spark, CONTRACTS, "is_current = false",
                      f"tenant_id = '{contract.tenant_id}' AND entity = '{contract.entity}' AND is_current")
    row = {"tenant_id": contract.tenant_id, "entity": contract.entity, "version": contract.version,
           "columns_json": contract.to_json(), "column_count": len(contract.columns),
           "source_file": source_file, "registered_at": now(), "registered_by": actor,
           "reason": reason, "is_current": True}
    schema = ("tenant_id STRING, entity STRING, version INT, columns_json STRING, column_count INT, "
              "source_file STRING, registered_at TIMESTAMP, registered_by STRING, reason STRING, "
              "is_current BOOLEAN")
    tables.append(spark, tables.rows_to_df(spark, [row], schema), CONTRACTS, cluster_by=["tenant_id", "entity"])
    if artifact_path:
        try:
            os.makedirs(os.path.dirname(artifact_path), exist_ok=True)
            with open(artifact_path, "w") as fh:
                json.dump({"tenant_id": contract.tenant_id, "entity": contract.entity,
                           "version": contract.version, "source_file": source_file,
                           "columns": [c.as_dict() for c in contract.columns]}, fh, indent=1)
        except OSError:
            pass


def profile(cfg: SourceConfig, f: LandedFile) -> C.Profile:
    return C.profile_file(f.path, cfg.source_format, delimiter=cfg.row.get("delimiter"),
                          has_header=cfg.row.get("has_header"), quote_char=cfg.row.get("quote_char"),
                          encoding=cfg.encoding, positional_names=cfg.positional_names)


# ---------------------------------------------------------------------------
# Reading files into the bronze shape
# ---------------------------------------------------------------------------

def _norm_path(col):
    return F.regexp_replace(col, r"^[A-Za-z]+:/+", "/")


def read_group(spark: SparkSession, cfg: SourceConfig, paths: list[str], prof: C.Profile) -> DataFrame:
    """Read files that share one shape. Returns (source_file, record map, extras map, malformed)."""
    fmt = cfg.source_format.upper()
    if fmt == "JSON":
        df = (spark.read.option("encoding", cfg.encoding).text(paths)
              .select(F.col("value"), _norm_path(F.col("_metadata.file_path")).alias("source_file"))
              .filter(F.trim("value") != ""))
        parsed = F.from_json("value", "MAP<STRING,STRING>")
        df = df.select("source_file", parsed.alias("all_fields"), F.col("value").alias("_raw"))
        return df
    opts = {"header": "true" if cfg.row.get("has_header") else "false",
            "sep": cfg.row.get("delimiter") or ",", "encoding": cfg.encoding,
            "multiLine": "true" if cfg.row.get("multiline") else "false",
            "mode": "PERMISSIVE", "inferSchema": "false"}
    q = cfg.row.get("quote_char")
    opts["quote"] = q if q else ""
    for k, v in cfg.options.items():
        if k not in ("encoding",):
            opts[k] = str(v).lower() if isinstance(v, bool) else str(v)
    reader = spark.read.options(**opts)
    if not cfg.row.get("has_header"):
        # Positional: impose the profiled names so a wider (drifted) file keeps
        # its extra columns instead of Spark truncating to the first file.
        schema = ", ".join(f"`{n}` STRING" for n in prof.names)
        reader = reader.schema(schema)
    df = reader.csv(paths)
    df = df.withColumn("source_file", _norm_path(F.col("_metadata.file_path")))
    names = [c for c in df.columns if c != "source_file"]
    kv = F.map_from_arrays(F.array(*[F.lit(n) for n in names]),
                           F.array(*[F.col(f"`{n}`").cast("string") for n in names]))
    return df.select("source_file", kv.alias("all_fields"), F.lit(None).cast("string").alias("_raw"))


def to_bronze(df: DataFrame, cfg: SourceConfig, contract: C.Contract, batch_id: str) -> DataFrame:
    """Split fields into record (contract columns) and rescued data; add lineage."""
    known = contract.names
    number_cols = [c.name for c in contract.columns if c.type_class == "NUMBER"]
    known_arr = F.array(*[F.lit(n) for n in known])
    record = F.map_filter("all_fields", lambda k, v: F.array_contains(known_arr, k))
    extras = F.map_filter("all_fields", lambda k, v: ~F.array_contains(known_arr, k) & v.isNotNull())
    df = df.withColumn("record", record).withColumn("_extras", extras)
    # Values in a NUMBER contract column that do not parse as numbers are
    # rescued (kept in record too: bronze never drops what the file said).
    if number_cols:
        arr_sql = "array(" + ", ".join(json.dumps(n) for n in number_cols) + ")"
        df = df.withColumn("_badnum", F.expr(
            f"map_filter(all_fields, (k, v) -> array_contains({arr_sql}, k) AND v IS NOT NULL "
            f"AND trim(v) <> '' AND try_cast(trim(v) AS DOUBLE) IS NULL)"))
    else:
        df = df.withColumn("_badnum", F.expr("map_filter(all_fields, (k, v) -> false)"))
    malformed = F.col("all_fields").isNull()
    rescued_map = F.map_concat("_extras", "_badnum")
    rescued = (F.when(malformed, F.to_json(F.create_map(F.lit("_corrupt_record"), F.col("_raw"))))
               .when(F.size(rescued_map) > 0, F.to_json(rescued_map)))
    pk = cfg.pk
    key = F.concat_ws("|", *[F.coalesce(F.col("record").getItem(k), F.lit("")) for k in pk]) if pk else F.lit(None)
    lt = cfg.load_type
    if lt == "CDC" and cfg.row.get("cdc_operation_column"):
        op = F.upper(F.trim(F.col("record").getItem(cfg.cdc_operation_column)))
    else:
        op = F.lit({"FULL_SNAPSHOT": "SNAPSHOT", "INCREMENTAL": "UPSERT"}.get(lt, "UPSERT"))
    seq = (F.expr(f"try_cast(record[{json.dumps(cfg.cdc_sequence_column)}] AS BIGINT)")
           if cfg.row.get("cdc_sequence_column") else F.lit(None).cast("bigint"))
    wm = (F.col("record").getItem(cfg.watermark_column) if cfg.row.get("watermark_column")
          else F.lit(None).cast("string"))
    fdate = F.to_date(F.regexp_extract("source_file", r"/(\d{4}/\d{2}/\d{2})/", 1), "yyyy/MM/dd")
    from pyspark.sql.window import Window
    w = Window.partitionBy("source_file").orderBy(F.monotonically_increasing_id())
    return df.select(
        F.lit(cfg.tenant_id).alias("tenant_id"),
        F.lit(cfg.source_system_id).alias("source_system_id"),
        F.lit(cfg.entity).alias("source_entity"),
        F.lit(cfg.config_id).alias("config_id"),
        F.coalesce(F.col("record"), F.create_map().cast("map<string,string>")).alias("record"),
        key.alias("record_key"),
        F.sha2(F.coalesce(F.to_json("record"), F.col("_raw"), F.lit("")), 256).alias("record_hash"),
        op.alias("cdc_operation"),
        seq.alias("cdc_sequence"),
        wm.alias("source_watermark"),
        rescued.isNotNull().alias("is_rescued"),
        rescued.alias("_rescued_data"),
        F.col("source_file"),
        fdate.alias("source_file_date"),
        F.row_number().over(w).cast("bigint").alias("file_row_number"),
        F.lit(lt).alias("load_type"),
        F.lit(batch_id).alias("ingest_batch_id"),
        F.current_timestamp().alias("ingest_ts"),
        F.lit(contract.version).alias("contract_version"),
    )


# ---------------------------------------------------------------------------
# One source
# ---------------------------------------------------------------------------

@dataclass
class SourceResult:
    status: str
    files_pending: int = 0
    files_ingested: int = 0
    files_poison: int = 0
    rows: int = 0
    rescued: int = 0
    drift: list[dict] | None = None
    note: str = ""


def ingest_source(spark: SparkSession, cfg: SourceConfig, run: RunLog, *, dry_run: bool,
                  use_autoloader: bool = False, checkpoint_root: str | None = None,
                  actor: str = "unknown") -> SourceResult:
    tenant, entity = cfg.tenant_id, cfg.entity
    target = cfg.target_table

    halts = open_breaking_drift(spark, tenant, entity)
    if halts:
        cols = ", ".join(sorted({h["column_name"] for h in halts}))
        return SourceResult("HALTED", note=f"open BREAKING drift on {cols}; resolve with notebook 14, then re-run")

    contract = current_contract(spark, tenant, entity)
    if contract is None:
        return SourceResult("FAILED", note="no schema contract registered; run notebook 11 first")

    files = None
    if use_autoloader and checkpoint_root:
        try:
            files = discover_autoloader(spark, cfg, checkpoint_root)
        except Exception as exc:  # noqa: BLE001 - fall back, and say so
            print(f"  [{tenant}/{entity}] Auto Loader unavailable ({type(exc).__name__}); using directory listing")
    if files is None:
        files = discover(cfg)

    state = ledger_state(spark, tenant, entity)
    # Crash recovery: rows from a file left IN_PROGRESS are removed before re-ingesting it.
    stale = [p for p, s in state.items() if s["status"] == "IN_PROGRESS"]
    if stale and not dry_run and tables.exists(spark, target):
        for p in stale:
            tables.delete(spark, target, f"source_file = '{p}' AND tenant_id = '{tenant}'")
    done = {p for p, s in state.items() if s["status"] in ("INGESTED", "POISON")}
    pending = [f for f in files if f.path not in done]
    res = SourceResult("NO_NEW_DATA", files_pending=len(pending), drift=[])
    if not pending:
        return res

    additive_logged = known_additive(spark, tenant, entity)
    accepted: list[tuple[LandedFile, C.Profile]] = []
    ledger_rows: list[dict] = []
    drift_rows: list[dict] = []
    halted_note = ""
    for f in pending:
        base = {"file_path": f.path, "tenant_id": tenant, "entity": entity, "config_id": cfg.config_id,
                "file_name": f.name, "file_date": f.file_date, "file_size": f.size, "file_mtime": f.mtime,
                "batch_id": run.batch_id, "contract_version": contract.version, "updated_at": now()}
        try:
            prof = profile(cfg, f)
        except C.PoisonFile as exc:
            res.files_poison += 1
            ledger_rows.append({**base, "status": "POISON", "rows_ingested": 0, "notes": str(exc)[:500]})
            if not dry_run:
                dest = f"{POISON_ROOT}/{tenant}/{entity}/{f.name}"
                try:
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    shutil.copy2(f.path, dest)
                except OSError:
                    pass
            continue
        drifts = C.compare(contract, prof)
        breaking = [d for d in drifts if d.classification == "BREAKING"]
        for d in drifts:
            if d.classification == "ADDITIVE" and d.column_name in additive_logged:
                continue
            drift_rows.append({
                "drift_id": f"DR-{uuid.uuid4().hex[:10]}", "batch_id": run.batch_id, "tenant_id": tenant,
                "source_system_id": cfg.source_system_id, "entity": entity, "detected_at": now(),
                "drift_type": d.drift_type, "classification": d.classification, "column_name": d.column_name,
                "expected_type": d.expected_type, "observed_type": d.observed_type, "sample_file": f.path,
                "action_taken": d.action, "status": "OPEN" if d.classification == "BREAKING" else "ACCEPTED",
                "resolved_by": None if d.classification == "BREAKING" else "AUTO",
                "resolved_at": None if d.classification == "BREAKING" else now(),
                "notes": f"contract v{contract.version}; file dated {f.file_date}"})
            if d.classification == "ADDITIVE":
                additive_logged.add(d.column_name)
        if breaking:
            halted_note = (f"halted at {f.name}: " + "; ".join(f"{d.drift_type} {d.column_name}" for d in breaking)
                           + f" ({len(pending) - pending.index(f)} file(s) held back)")
            break
        accepted.append((f, prof))

    res.drift = drift_rows
    if not accepted:
        res.status = "HALTED" if halted_note else ("POISON_ONLY" if res.files_poison else "NO_NEW_DATA")
        res.note = halted_note
        if not dry_run:
            write_ledger(spark, ledger_rows)
            if drift_rows:
                tables.append(spark, tables.rows_to_df(spark, drift_rows, DRIFT_SCHEMA), DRIFT_LOG,
                              cluster_by=["tenant_id", "entity"])
        return res

    groups: dict[str, list[tuple[LandedFile, C.Profile]]] = {}
    for f, p in accepted:
        groups.setdefault(p.signature, []).append((f, p))
    frames = [to_bronze(read_group(spark, cfg, [f.path for f, _ in grp], grp[0][1]), cfg, contract, run.batch_id)
              for grp in groups.values()]
    bronze = frames[0]
    for fr in frames[1:]:
        bronze = bronze.unionByName(fr)

    per_file = {r.source_file: (r.n, r.rescued) for r in bronze.groupBy("source_file").agg(
        F.count("*").alias("n"), F.sum(F.col("is_rescued").cast("int")).alias("rescued")).collect()}
    res.rows = sum(v[0] for v in per_file.values())
    res.rescued = sum(int(v[1] or 0) for v in per_file.values())
    res.files_ingested = len(accepted)
    res.status = "HALTED" if halted_note else "SUCCEEDED"
    res.note = halted_note

    dq = bronze_dq(spark, bronze, cfg, run.batch_id, [f for f, _ in accepted])
    if any(r["outcome"] == "FAIL" and r["severity"] == "ABORT" for r in dq):
        res.status = "ABORTED"
        res.note = "bronze ABORT rule failed: " + ", ".join(r["rule_id"] for r in dq if r["outcome"] == "FAIL")
        if not dry_run:
            tables.append(spark, tables.rows_to_df(spark, dq, DQ_SCHEMA), DQ_LOG)
        return res

    if dry_run:
        return res

    # 1) mark IN_PROGRESS, 2) append rows, 3) mark INGESTED. A crash between 1
    # and 3 is repaired on the next run by the stale IN_PROGRESS cleanup above.
    inprog = [{**{"file_path": f.path, "tenant_id": tenant, "entity": entity, "config_id": cfg.config_id,
                  "file_name": f.name, "file_date": f.file_date, "file_size": f.size, "file_mtime": f.mtime,
                  "batch_id": run.batch_id, "contract_version": contract.version, "signature": p.signature,
                  "updated_at": now()}, "status": "IN_PROGRESS"} for f, p in accepted]
    write_ledger(spark, inprog + ledger_rows)
    tables.append(spark, bronze, target, cluster_by=["tenant_id", "source_file_date"])
    write_ledger(spark, [{**r, "status": "INGESTED", "rows_ingested": per_file.get(r["file_path"], (0, 0))[0],
                          "rows_rescued": int(per_file.get(r["file_path"], (0, 0))[1] or 0),
                          "updated_at": now()} for r in inprog])
    if drift_rows:
        tables.append(spark, tables.rows_to_df(spark, drift_rows, DRIFT_SCHEMA), DRIFT_LOG,
                      cluster_by=["tenant_id", "entity"])
    tables.append(spark, tables.rows_to_df(spark, dq, DQ_SCHEMA), DQ_LOG)
    return res


def bronze_dq(spark: SparkSession, bronze: DataFrame, cfg: SourceConfig, batch_id: str,
              files: list[LandedFile]) -> list[dict]:
    """The BRONZE-layer rules from dq_rule_registry, evaluated in one pass."""
    rules = {r.rule_id: r.asDict() for r in spark.sql("""
        SELECT * FROM ap_control.config.dq_rule_registry
        WHERE is_active AND layer = 'BRONZE'""").collect()}
    stats = bronze.agg(F.count("*").alias("n"),
                       F.sum(F.when(F.col("tenant_id").isNull() | (F.trim("tenant_id") == ""), 1)
                             .otherwise(0)).alias("null_tenant"),
                       F.countDistinct("tenant_id").alias("tenants")).collect()[0]
    n = int(stats.n)
    out = []

    def rec(rule_id, failed, evaluated, outcome_override=None, sample=None):
        r = rules.get(rule_id)
        if not r:
            return
        outcome = outcome_override or ("PASS" if failed == 0 else "FAIL" if r["severity"] == "ABORT" else "WARN")
        out.append({"batch_id": batch_id, "rule_id": rule_id, "tenant_id": cfg.tenant_id, "layer": "BRONZE",
                    "entity": cfg.entity, "dq_dimension": r["dq_dimension"], "severity": r["severity"],
                    "evaluated_at": now(), "records_evaluated": evaluated, "records_passed": evaluated - failed,
                    "records_failed": failed,
                    "pass_rate_pct": _pct(evaluated - failed, evaluated), "threshold_pct": r["fail_threshold_pct"],
                    "outcome": outcome, "failed_sample": sample,
                    "action_taken": "ABORTED_SOURCE" if outcome == "FAIL" else "NONE",
                    "rule_expression": r["rule_expression"]})

    rec("DQ_ALL_0001", int(stats.null_tenant or 0), n)
    rec("DQ_ALL_0002", 0 if int(stats.tenants) <= 1 else 1, 1)
    max_age = cfg.row.get("max_file_age_hours")
    if max_age:
        today = datetime.now().date()
        ages = [((today - f.file_date).days * 24) for f in files if f.file_date]
        late = [f.name for f, a in zip([f for f in files if f.file_date], ages) if a > max_age]
        rec("DQ_ALL_0601", len(late), len(files), sample=json.dumps(late[:5]) if late else None)
    return out


def _pct(a: int, b: int):
    from decimal import Decimal
    return Decimal("100.0000") if b == 0 else Decimal(f"{100.0 * a / b:.4f}")


# ---------------------------------------------------------------------------
# All sources
# ---------------------------------------------------------------------------

def run_ingestion(spark: SparkSession, *, tenants: list[str] | None = None, entities: list[str] | None = None,
                  dry_run: bool = False, use_autoloader: bool = False,
                  checkpoint_root: str = "/Volumes/ap_bronze/landing/checkpoints",
                  actor: str = "unknown") -> RunLog:
    run = RunLog(spark, "bronze_ingest", "BRONZE", tenant_id=",".join(tenants) if tenants else "ALL",
                 run_mode="INCREMENTAL", dry_run=dry_run, triggered_by=actor, prefix="BR",
                 persist=not dry_run)
    for cfg in load_sources(spark, tenants, entities):
        with run.task(cfg.config_id, cfg.tenant_id, cfg.entity) as t:
            r = ingest_source(spark, cfg, run, dry_run=dry_run, use_autoloader=use_autoloader,
                              checkpoint_root=checkpoint_root, actor=actor)
            t.status = r.status
            t.records_in = r.rows
            t.records_out = 0 if dry_run else r.rows
            t.records_quarantined = r.rescued
            t.source_files = json.dumps({"pending": r.files_pending, "ingested": r.files_ingested,
                                         "poison": r.files_poison})
            t.error_message = r.note or None
            run.files_processed += r.files_ingested
            flag = {"SUCCEEDED": "ok", "NO_NEW_DATA": "--", "HALTED": "HALT"}.get(r.status, r.status)
            print(f"  {flag:>5}  {cfg.tenant_id} {cfg.entity:22} files {r.files_ingested:>3}/{r.files_pending:<3} "
                  f"rows {r.rows:>7,}  rescued {r.rescued:>5,}  poison {r.files_poison}"
                  + (f"   {r.note}" if r.note else ""))
            for d in r.drift or []:
                print(f"         drift: {d['classification']:8} {d['drift_type']:15} {d['column_name']}"
                      f" -> {d['action_taken']}")
    run.finish()
    return run
