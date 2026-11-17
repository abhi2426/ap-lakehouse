"""
local_harness — run the real notebooks and modules on plain local Spark.

This is a TEST TOOL, not part of the platform. It exists so the logic of every
layer can be executed end to end off-platform before any Databricks compute is
spent. It changes only what local Spark cannot do, and nothing else:

* Unity Catalog three-part names (ap_control.config.x) are rewritten to
  two-part local databases (ap_control__config.x) inside spark.sql/table/
  saveAsTable. Code under test keeps its real names.
* Delta writes go through LocalBackend (Parquet + read/rewrite) instead of
  DeltaBackend. This is the only production code that is NOT exercised here.
* Databricks-only statements (CREATE CATALOG/VOLUME, GRANT, row filters) are
  skipped and recorded. /Volumes is a symlink to a local directory.
* try_to_date (Databricks Runtime 17.2+) is provided as a UDF.
* dbutils and display are stubbed.

Usage:  python -I tests/local_harness.py <repo_root> <work_dir> <plan...>
"""

from __future__ import annotations

import datetime as dt
import glob
import json
import os
import re
import shutil
import sys
import time
import traceback
import uuid

LOCAL_DB_SEP = "__"
NAME_RX = re.compile(r"(?<![\w.`/])`?(ap_(?:control|bronze|silver|gold))`?\.`?(\w+)`?\.`?(\w+)`?")
SCHEMA_RX = re.compile(r"(?<![\w.`/])(ap_(?:control|bronze|silver|gold))\.(\w+)(?![\w.])")
SKIPPED: list[str] = []


def rewrite_names(text: str) -> str:
    text = NAME_RX.sub(lambda m: f"{m.group(1)}{LOCAL_DB_SEP}{m.group(2)}.{m.group(3)}", text)
    return text


def rewrite_ddl(sql: str) -> str | None:
    """Translate one Databricks SQL statement to local Spark, or None to skip."""
    s = sql.strip()
    u = re.sub(r"\s+", " ", s.upper())
    skip_prefixes = ("CREATE CATALOG", "CREATE VOLUME", "GRANT ", "REVOKE ", "CREATE GROUP", "ALTER CATALOG",
                     "COMMENT ON VOLUME", "COMMENT ON CATALOG", "COMMENT ON SCHEMA", "USE CATALOG",
                     "ALTER VOLUME", "SHOW VOLUMES", "SHOW CATALOGS", "OPTIMIZE ", "VACUUM ", "ANALYZE ",
                     "SET ", "DESCRIBE HISTORY", "SHOW SCHEMAS")
    if u.startswith(skip_prefixes) or "SET ROW FILTER" in u or "SET MASK" in u or "DROP ROW FILTER" in u \
            or "IS_ACCOUNT_GROUP_MEMBER" in u or "ALTER TABLE" in u and "CLUSTER BY" in u:
        SKIPPED.append(s[:90].replace("\n", " "))
        return None
    if u.startswith("CREATE SCHEMA") or u.startswith("CREATE DATABASE"):
        s = SCHEMA_RX.sub(lambda m: f"{m.group(1)}{LOCAL_DB_SEP}{m.group(2)}", s)
        s = re.sub(r"\s+MANAGED LOCATION\s+'[^']*'", "", s, flags=re.I)
        return s
    if u.startswith("SHOW TABLES IN") or u.startswith("SHOW SCHEMAS"):
        s = SCHEMA_RX.sub(lambda m: f"{m.group(1)}{LOCAL_DB_SEP}{m.group(2)}", s)
    if u.startswith("CREATE FUNCTION") or u.startswith("CREATE OR REPLACE FUNCTION"):
        SKIPPED.append(s[:90].replace("\n", " "))
        return None
    s = _strip_clause(s, "CLUSTER BY")
    s = _strip_clause(s, "TBLPROPERTIES")
    s = re.sub(r"\bUSING\s+DELTA\b", "USING parquet", s, flags=re.I)
    s = re.sub(r"\bGENERATED ALWAYS AS IDENTITY(\s*\([^)]*\))?", "", s, flags=re.I)
    if not u.startswith("DECLARE"):
        s = re.sub(r"\bDEFAULT\s+(current_timestamp\(\)|current_date\(\)|'[^']*'|true|false|\d+)", "", s, flags=re.I)
    m = re.match(r"(?is)\s*CREATE\s+OR\s+REPLACE\s+TABLE\s+([\w.`]+)(.*)", s)
    if m:
        name = m.group(1)
        spark.sql(f"DROP TABLE IF EXISTS {rewrite_names(name)}")
        s = f"CREATE TABLE {name}{m.group(2)}"
    return rewrite_names(s)


def _strip_clause(s: str, kw: str) -> str:
    while True:
        m = re.search(r"\b" + kw.replace(" ", r"\s+") + r"\s*\(", s, flags=re.I)
        if not m:
            return s
        i, depth = m.end() - 1, 0
        for j in range(i, len(s)):
            if s[j] == "(":
                depth += 1
            elif s[j] == ")":
                depth -= 1
                if depth == 0:
                    s = s[:m.start()] + s[j + 1:]
                    break
        else:
            return s


# ---------------------------------------------------------------------------
# Spark session with name rewriting
# ---------------------------------------------------------------------------
spark = None


def make_spark(work: str):
    global spark
    from pyspark.sql import SparkSession, DataFrameReader, DataFrameWriter
    from pyspark.sql.catalog import Catalog
    from pyspark.sql import types as T

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.master("local[4]").appName("ap-local")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.warehouse.dir", f"{work}/warehouse")
             .config("spark.driver.memory", "3g")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
             .config("javax.jdo.option.ConnectionURL",
                     f"jdbc:derby:;databaseName={work}/metastore_db;create=true")
             .config("spark.hadoop.hive.metastore.schema.verification", "false")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    orig_sql = SparkSession.sql
    orig_table = SparkSession.table
    orig_rtable = DataFrameReader.table
    orig_save = DataFrameWriter.saveAsTable
    orig_exists = Catalog.tableExists

    def sql(self, sqlText, *a, **k):
        out = rewrite_ddl(sqlText)
        if out is None:
            return orig_sql(self, "SELECT 'skipped (Databricks-only)' AS note")
        return orig_sql(self, out, *a, **k)

    SparkSession.sql = sql
    SparkSession.table = lambda self, n: orig_table(self, rewrite_names(n))
    DataFrameReader.table = lambda self, n: orig_rtable(self, rewrite_names(n))

    def save(self, name, format=None, mode=None, partitionBy=None, **opts):
        if format == "delta":
            format = "parquet"
        try:
            self._jwrite.format("parquet")
        except Exception:
            pass
        return orig_save(self, rewrite_names(name), format=format, mode=mode, partitionBy=partitionBy, **opts)

    DataFrameWriter.saveAsTable = save
    DataFrameWriter.format = (lambda orig: (lambda self, src: orig(self, "parquet" if src == "delta" else src)))(
        DataFrameWriter.format)
    Catalog.tableExists = lambda self, tableName, dbName=None: orig_exists(self, rewrite_names(tableName), dbName)

    # try_to_date shim (Databricks Runtime 17.2+ built-in)
    java_to_py = [("yyyy", "%Y"), ("MM", "%m"), ("dd", "%d"), ("HH", "%H"), ("mm", "%M"), ("ss", "%S")]

    def try_to_date(v, fmt=None):
        if v is None:
            return None
        if isinstance(v, dt.datetime):
            return v.date()
        if isinstance(v, dt.date):
            return v
        s = str(v).strip()
        try:
            if fmt is None:
                return dt.date.fromisoformat(s[:10])
            pf = fmt
            for j, p in java_to_py:
                pf = pf.replace(j, p)
            d = dt.datetime.strptime(s, pf)
            return d.date()
        except Exception:
            return None

    spark.udf.register("try_to_date", try_to_date, T.DateType())
    return spark


# ---------------------------------------------------------------------------
# Local backend: Parquet emulation of the Delta operations in tables.py
# ---------------------------------------------------------------------------
class LocalBackend:
    name = "local-parquet"

    def __init__(self, spark):
        self.spark = spark

    def table_exists(self, spark, table):
        return spark.catalog.tableExists(table)

    def create_if_absent(self, spark, df, table, cluster_by=(), comment=None):
        if self.table_exists(spark, table):
            return False
        df.limit(0).write.format("parquet").mode("overwrite").saveAsTable(table)
        return True

    def _rewrite(self, spark, df, table):
        """Replace a table with df, which may read from that table."""
        tmp = f"{table}_tmp_{uuid.uuid4().hex[:6]}"
        df.write.format("parquet").mode("overwrite").saveAsTable(tmp)
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql(f"ALTER TABLE {rewrite_names(tmp)} RENAME TO {rewrite_names(table)}")

    def _align(self, df, cols_types):
        from pyspark.sql import functions as F
        out = []
        for name, typ in cols_types:
            out.append(F.col(f"`{name}`").cast(typ).alias(name) if name in df.columns
                       else F.lit(None).cast(typ).alias(name))
        return df.select(*out)

    def _evolved(self, spark, df, table):
        tgt = spark.table(table)
        cols = [(f.name, f.dataType) for f in tgt.schema]
        names = {c for c, _ in cols}
        cols += [(f.name, f.dataType) for f in df.schema if f.name not in names]
        return cols

    def append(self, spark, df, table):
        cols = self._evolved(spark, df, table)
        if len(cols) != len(spark.table(table).columns):
            self._rewrite(spark, self._align(spark.table(table), cols), table)
        self._align(df, cols).write.format("parquet").mode("append").saveAsTable(table)

    def overwrite(self, spark, df, table):
        if self.table_exists(spark, table):
            spark.sql(f"DROP TABLE IF EXISTS {table}")
        df.write.format("parquet").mode("overwrite").saveAsTable(table)

    def replace_where(self, spark, df, table, predicate):
        cols = self._evolved(spark, df, table)
        keep = spark.table(table).filter(f"NOT ({predicate}) OR ({predicate}) IS NULL")
        self._rewrite(spark, self._align(keep, cols).unionByName(self._align(df, cols)), table)

    def merge(self, spark, df, table, keys, update=True, insert=True, update_condition=None,
              delete_missing=None):
        from pyspark.sql import functions as F
        cols = self._evolved(spark, df, table)
        tgt = self._align(spark.table(table), cols).alias("t")
        src = self._align(df, cols).alias("s")
        cond = None
        for k in keys:
            c = F.col(f"t.`{k}`").eqNullSafe(F.col(f"s.`{k}`"))
            cond = c if cond is None else cond & c
        matched_t = tgt.join(src, cond, "left_semi")
        unmatched_t = tgt.join(src, cond, "left_anti")
        if delete_missing:
            pred = delete_missing.replace("t.", "")
            unmatched_t = unmatched_t.filter(f"NOT coalesce(({pred}), false)")
        parts = [unmatched_t]
        if update:
            upd = src.join(tgt, cond, "left_semi")
            if update_condition:
                j = tgt.join(src, cond, "inner")
                cond_df = j.filter(update_condition)
                take_src = cond_df.select("s.*")
                keep_t = j.filter(f"NOT ({update_condition}) OR ({update_condition}) IS NULL").select("t.*")
                parts += [take_src, keep_t]
            else:
                parts.append(upd)
        else:
            parts.append(matched_t)
        if insert:
            parts.append(src.join(tgt, cond, "left_anti"))
        out = parts[0]
        for p in parts[1:]:
            out = out.unionByName(p)
        self._rewrite(spark, out, table)

    def update(self, spark, table, set_sql, where):
        from pyspark.sql import functions as F
        df = spark.table(table)
        assigns = _split_assignments(set_sql)
        hit = F.expr(f"coalesce(({where}), false)")
        for col, expr in assigns:
            df = df.withColumn(col, F.when(hit, F.expr(expr)).otherwise(F.col(f"`{col}`")).cast(
                spark.table(table).schema[col].dataType))
        self._rewrite(spark, df, table)

    def delete(self, spark, table, where):
        df = spark.table(table).filter(f"NOT coalesce(({where}), false)")
        self._rewrite(spark, df, table)


def _split_assignments(set_sql: str):
    parts, depth, cur, q = [], 0, "", False
    for ch in set_sql:
        if ch == "'":
            q = not q
        if not q:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append(cur)
                cur = ""
                continue
        cur += ch
    parts.append(cur)
    out = []
    for p in parts:
        lhs, rhs = p.split("=", 1)
        out.append((lhs.strip().split(".")[-1].strip("`"), rhs.strip()))
    return out


# ---------------------------------------------------------------------------
# dbutils / display stubs and the notebook runner
# ---------------------------------------------------------------------------
class _Widgets:
    def __init__(self):
        self.defaults, self.overrides = {}, {}

    def text(self, name, default, label=None):
        self.defaults[name] = default

    def dropdown(self, name, default, choices=None, label=None):
        self.defaults[name] = default

    combobox = dropdown
    multiselect = dropdown

    def get(self, name):
        if name in self.overrides:
            return self.overrides[name]
        return self.defaults[name]

    def removeAll(self):
        self.defaults = {}


class _FileInfo:
    def __init__(self, path):
        self.path = path
        self.name = os.path.basename(path.rstrip("/")) + ("/" if os.path.isdir(path) else "")
        self.size = os.path.getsize(path) if os.path.isfile(path) else 0
        self.modificationTime = int(os.path.getmtime(path) * 1000)


class _Fs:
    def ls(self, p):
        return [_FileInfo(os.path.join(p, n)) for n in sorted(os.listdir(p))]

    def mkdirs(self, p):
        os.makedirs(p, exist_ok=True)
        return True

    def cp(self, a, b, recurse=False):
        os.makedirs(os.path.dirname(b), exist_ok=True)
        (shutil.copytree if recurse else shutil.copy2)(a, b)
        return True

    def mv(self, a, b, recurse=False):
        os.makedirs(os.path.dirname(b), exist_ok=True)
        shutil.move(a, b)
        return True

    def rm(self, p, recurse=False):
        if os.path.isdir(p):
            shutil.rmtree(p)
        elif os.path.exists(p):
            os.remove(p)
        return True

    def put(self, p, contents, overwrite=False):
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as fh:
            fh.write(contents)
        return True


class _Library:
    def restartPython(self):
        pass


class DBUtils:
    def __init__(self):
        self.widgets = _Widgets()
        self.fs = _Fs()
        self.library = _Library()


DISPLAY_LOG: list[str] = []


def display(obj, *a, **k):
    try:
        rows = obj.limit(25).collect()
        cols = obj.columns
        DISPLAY_LOG.append(f"[display {len(cols)} cols]")
        print("   | " + " | ".join(cols)[:240])
        for r in rows:
            print("   | " + " | ".join("" if v is None else str(v) for v in r)[:240])
    except Exception:
        print(obj)


def iter_cells(path: str):
    text = open(path, encoding="utf-8").read()
    sep = "-- COMMAND ----------" if path.endswith(".sql") else "# COMMAND ----------"
    for i, cell in enumerate(text.split(sep)):
        lines = cell.split("\n")
        body = [l for l in lines if not re.match(r"^\s*(--|#) MAGIC", l)
                and not l.startswith("# Databricks notebook source")
                and not l.startswith("-- Databricks notebook source")]
        yield i, "\n".join(body).strip(), cell


def split_sql(cell: str) -> list[str]:
    out, cur, q, dq, line_comment = [], "", False, False, False
    i = 0
    while i < len(cell):
        ch = cell[i]
        if line_comment:
            if ch == "\n":
                line_comment = False
                cur += ch
            i += 1
            continue
        if not q and not dq and cell[i:i + 2] == "--":
            line_comment = True
            i += 2
            continue
        if ch == "'" and not dq:
            q = not q
        elif ch == '"' and not q:
            dq = not dq
        if ch == ";" and not q and not dq:
            if cur.strip():
                out.append(cur.strip())
            cur = ""
        else:
            cur += ch
        i += 1
    if cur.strip():
        out.append(cur.strip())
    return out


def run_notebook(path: str, widgets: dict | None = None, show_sql: bool = True,
                 allow_fail: tuple = ()) -> dict:
    """Execute a Databricks .py or .sql notebook. Returns its globals."""
    dbutils = DBUtils()
    dbutils.widgets.overrides = dict(widgets or {})
    import types
    mod = types.ModuleType("__nb__")
    sys.modules["__nb__"] = mod
    g = mod.__dict__
    g.update({"spark": spark, "dbutils": dbutils, "display": display})
    name = os.path.basename(path)
    t0 = time.time()
    print(f"\n========== RUN {name} {widgets or ''}")
    for i, body, raw in iter_cells(path):
        if not body:
            continue
        is_sql = path.endswith(".sql") or body.startswith("%sql")
        if body.startswith("%sql"):
            body = body[4:]
        if body.startswith("%md") or body.startswith("%pip"):
            continue
        if is_sql:
            for stmt in split_sql(body):
                try:
                    df = spark.sql(stmt)
                    u = stmt.lstrip().upper()
                    if show_sql and (u.startswith("SELECT") or u.startswith("WITH") or u.startswith("SHOW")):
                        display(df)
                except Exception as exc:  # noqa: BLE001
                    msg = f"{name} cell {i}: {type(exc).__name__}: {str(exc).splitlines()[0][:300]}"
                    if any(a in stmt for a in allow_fail):
                        print("  (allowed failure) " + msg)
                        continue
                    print("  SQL FAILED: " + msg + "\n  STATEMENT: " + stmt[:600])
                    raise
        else:
            try:
                exec(compile(body, f"{name}#cell{i}", "exec"), g)
            except SystemExit as exc:
                print(f"  SystemExit in cell {i}: {exc}")
                raise
            except Exception:
                print(f"  PY FAILED: {name} cell {i}")
                traceback.print_exc()
                raise
    print(f"========== DONE {name} in {time.time() - t0:.1f}s")
    return g


def setup(repo: str, work: str, fresh: bool = True):
    if fresh and os.path.exists(work):
        shutil.rmtree(work)
    os.makedirs(f"{work}/vol", exist_ok=True)
    if os.path.islink("/Volumes"):
        os.unlink("/Volumes")
    if not os.path.exists("/Volumes"):
        os.symlink(f"{work}/vol", "/Volumes")
    sys.path.insert(0, f"{repo}/src")
    make_spark(work)
    from ap_platform import tables
    tables.set_backend(LocalBackend(spark))
    for d in ("ap_bronze/landing/inbox", "ap_bronze/landing/archive", "ap_bronze/landing/poison",
              "ap_control/ops/artifacts", "ap_bronze/landing/checkpoints"):
        os.makedirs(f"/Volumes/{d}", exist_ok=True)
    return spark
