"""
tables — the ONE module that knows how tables are physically written.

Every layer (config loader, bronze, silver, gold) writes through the functions
here and never calls DeltaTable, ``MERGE INTO`` or ``saveAsTable`` itself.

Why bother
----------
1. **One place for Delta specifics.** ``MERGE WITH SCHEMA EVOLUTION``, null-safe
   key joins, liquid clustering on create, and ``replaceWhere`` all live here.
   When a runtime changes a signature, there is one file to fix.
2. **Serverless-safe by construction.** Nothing here caches, touches
   ``sparkContext`` or uses RDDs, all of which serverless compute refuses.
3. **Testable off-platform.** The backend object can be swapped (see
   ``tests/local_harness.py``), so the whole pipeline's logic runs on plain
   local Spark. Only the ~100 lines of ``DeltaBackend`` are Databricks-only.

Merge semantics used everywhere
-------------------------------
Keys are compared with ``<=>`` (null-safe), because several business keys
legitimately contain nulls (an invoice line number on a header-level payment).
Plain ``=`` would treat every such row as new and duplicate it on each run.
The source frame must be unique on the keys: callers dedupe first, and
``merge`` refuses a non-unique source rather than letting Delta fail halfway.
"""

from __future__ import annotations

import re
import uuid
from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _q(col: str) -> str:
    """Backtick a column name (hyphenated source names are legal here)."""
    return "`" + col.replace("`", "``") + "`"


def _view(df: DataFrame, prefix: str = "_src") -> str:
    name = f"{prefix}_{uuid.uuid4().hex[:10]}"
    df.createOrReplaceTempView(name)
    return name


class DeltaBackend:
    """Unity Catalog + Delta. The production backend."""

    name = "delta"

    # ---- existence / creation -------------------------------------------
    def table_exists(self, spark: SparkSession, table: str) -> bool:
        try:
            return bool(spark.catalog.tableExists(table))
        except Exception:  # noqa: BLE001 - catalog not there yet
            return False

    def create_if_absent(self, spark: SparkSession, df: DataFrame, table: str,
                         cluster_by: Sequence[str] = (), comment: str | None = None) -> bool:
        """Create an empty table shaped like ``df``. Returns True if created."""
        if self.table_exists(spark, table):
            return False
        v = _view(df.limit(0), "_shape")
        cols = [c for c in cluster_by if c in df.columns]
        cluster = f"CLUSTER BY ({', '.join(_q(c) for c in cols)})" if cols else ""
        cmt = f"COMMENT '{comment.replace(chr(39), chr(39) * 2)}'" if comment else ""
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} {cluster} {cmt} AS SELECT * FROM {v} WHERE 1 = 0")
        return True

    # ---- writes ----------------------------------------------------------
    def append(self, spark: SparkSession, df: DataFrame, table: str) -> None:
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)

    def overwrite(self, spark: SparkSession, df: DataFrame, table: str) -> None:
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true").saveAsTable(table))

    def replace_where(self, spark: SparkSession, df: DataFrame, table: str, predicate: str) -> None:
        """Atomically replace the slice matching ``predicate`` (snapshots)."""
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", predicate).saveAsTable(table))

    def merge(self, spark: SparkSession, df: DataFrame, table: str, keys: Sequence[str],
              update: bool = True, insert: bool = True,
              update_condition: str | None = None, delete_missing: str | None = None) -> None:
        v = _view(df)
        on = " AND ".join(f"t.{_q(k)} <=> s.{_q(k)}" for k in keys)
        clauses = []
        if update:
            cond = f" AND ({update_condition})" if update_condition else ""
            clauses.append(f"WHEN MATCHED{cond} THEN UPDATE SET *")
        if insert:
            clauses.append("WHEN NOT MATCHED THEN INSERT *")
        if delete_missing:
            clauses.append(f"WHEN NOT MATCHED BY SOURCE AND ({delete_missing}) THEN DELETE")
        spark.sql(f"MERGE WITH SCHEMA EVOLUTION INTO {table} t USING {v} s ON {on} " + " ".join(clauses))

    def update(self, spark: SparkSession, table: str, set_sql: str, where: str) -> None:
        spark.sql(f"UPDATE {table} SET {set_sql} WHERE {where}")

    def delete(self, spark: SparkSession, table: str, where: str) -> None:
        spark.sql(f"DELETE FROM {table} WHERE {where}")


# Survives importlib.reload (the notebooks reload modules so edits take
# effect): reload re-executes this file in the same namespace, so a backend
# set earlier is kept rather than silently reset.
BACKEND = globals().get("BACKEND") or DeltaBackend()


def set_backend(backend) -> None:
    """Swap the physical backend (used only by the local test harness)."""
    global BACKEND
    BACKEND = backend


# ---------------------------------------------------------------------------
# Public API — what every other module calls
# ---------------------------------------------------------------------------

def exists(spark: SparkSession, table: str) -> bool:
    return BACKEND.table_exists(spark, table)


def ensure(spark: SparkSession, df: DataFrame, table: str,
           cluster_by: Sequence[str] = (), comment: str | None = None) -> bool:
    return BACKEND.create_if_absent(spark, df, table, cluster_by, comment)


def append(spark: SparkSession, df: DataFrame, table: str, cluster_by: Sequence[str] = ()) -> None:
    ensure(spark, df, table, cluster_by)
    BACKEND.append(spark, df, table)


def overwrite(spark: SparkSession, df: DataFrame, table: str, cluster_by: Sequence[str] = ()) -> None:
    ensure(spark, df, table, cluster_by)
    BACKEND.overwrite(spark, df, table)


def replace_where(spark: SparkSession, df: DataFrame, table: str, predicate: str,
                  cluster_by: Sequence[str] = ()) -> None:
    ensure(spark, df, table, cluster_by)
    BACKEND.replace_where(spark, df, table, predicate)


def merge(spark: SparkSession, df: DataFrame, table: str, keys: Sequence[str],
          cluster_by: Sequence[str] = (), update: bool = True, insert: bool = True,
          update_condition: str | None = None, check_unique: bool = True,
          delete_missing: str | None = None) -> None:
    """MERGE ``df`` into ``table`` on ``keys`` (null-safe).

    ``delete_missing``: a predicate on the target (alias ``t``). Target rows
    matching it that the source no longer contains are DELETED. Use only when
    the source is the complete set for that slice (a full recompute), e.g.
    ``"true"`` or ``"t.tenant_id IN ('TEN001')"``.
    """
    keys = list(keys)
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(f"merge into {table}: key column(s) {missing} not in source frame")
    if check_unique:
        dup = df.groupBy(*[F.col(_q(k)) for k in keys]).count().filter("count > 1").limit(1).collect()
        if dup:
            raise ValueError(
                f"merge into {table}: source is not unique on {keys}; example key {dup[0].asDict()}. "
                "Dedupe before merging: Delta would fail mid-merge with multiple matches.")
    ensure(spark, df, table, cluster_by)
    BACKEND.merge(spark, df, table, keys, update=update, insert=insert,
                  update_condition=update_condition, delete_missing=delete_missing)


def update(spark: SparkSession, table: str, set_sql: str, where: str) -> None:
    BACKEND.update(spark, table, set_sql, where)


def delete(spark: SparkSession, table: str, where: str) -> None:
    BACKEND.delete(spark, table, where)


def stage(spark: SparkSession, df: DataFrame, table: str) -> DataFrame:
    """Materialise ``df`` and return a frame that reads the materialised copy.

    Needed whenever a frame that READS a table is used after that table is
    WRITTEN. Spark evaluates lazily, so without this the second use silently
    recomputes against the new state of the table (or, locally, fails because
    the old files are gone). Serverless refuses ``.cache()``, so staging to a
    table is the portable way to pin a result.
    """
    schema = table.rsplit(".", 1)[0]
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    except Exception:  # noqa: BLE001 - no privilege: the write below says so clearly
        pass
    overwrite(spark, df, table)
    return spark.table(table)


def rows_to_df(spark: SparkSession, rows: Iterable[dict], schema: str) -> DataFrame:
    """Build a DataFrame from dicts with an explicit DDL schema string.

    Every value is passed through as-is; the schema decides the types. Missing
    keys become null. This is the only sanctioned way to turn Python-side
    bookkeeping (ledgers, logs) into a frame, so types never depend on inference.
    """
    names = [re.split(r"\s+", f.strip(), maxsplit=1)[0].strip("`") for f in _split_schema(schema)]
    data = [tuple(r.get(n) for n in names) for r in rows]
    return spark.createDataFrame(data, schema)


def _split_schema(schema: str) -> list[str]:
    """Split 'a STRING, b DECIMAL(18,2), c MAP<STRING,STRING>' on top-level commas."""
    out, depth, cur = [], 0, ""
    for ch in schema:
        if ch in "(<":
            depth += 1
        elif ch in ")>":
            depth -= 1
        if ch == "," and depth == 0:
            out.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur)
    return out
