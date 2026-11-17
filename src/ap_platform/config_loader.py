"""
config_loader — turn repo YAML into control-plane rows, idempotently.

Design contract
---------------
The repo is the source of truth; the control tables are a projection of it.
That gives three properties worth the small amount of machinery below:

1. **Idempotent.** Running the bootstrap twice changes nothing the second time.
2. **Converging.** A row deleted from YAML is deactivated in the table, not
   left behind. Without this, config drifts silently and an old rule keeps
   firing long after someone "removed" it.
3. **Attributable.** Every load stamps a `config_version` (SHA-256 of the file)
   and writes to `config_load_audit`, so "why did the pipeline behave
   differently last Tuesday" has a data answer.

Soft-delete rather than hard-delete is deliberate. A deleted ingestion_config
row whose bronze data still exists must remain explicable; setting `is_active`
false keeps the history and stops the behaviour.

Supported YAML shapes
---------------------
Single block::

    target_table: ap_control.config.tenant_registry
    key_columns: [tenant_id]
    rows: [...]

Multi block (several tables in one file)::

    blocks:
      - {target_table: ..., key_columns: [...], rows: [...]}

Row groups with shared fields::

    blocks:
      - apply_to_all: {tenant_id: TEN001, entity: vendor_master}
        rows: [{canonical_field: vendor_id, ...}]

Optional ``derive_key: "{tenant_id}__{entity}__{canonical_field}"`` fills the
single key column when the YAML omits it, which keeps schema_mapping readable.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# Columns the loader maintains itself. YAML must not set them.
_MANAGED = {"config_version", "updated_at", "updated_by"}


class ConfigValidationError(Exception):
    """Raised when a YAML file cannot be loaded safely. Nothing is written."""


@dataclass
class ConfigBlock:
    """One target table's worth of declared rows."""

    target_table: str
    key_columns: list[str]
    rows: list[dict[str, Any]]
    source_file: str
    derive_key: str | None = None

    @property
    def config_version(self) -> str:
        """Stable hash of the declared content, independent of dict ordering."""
        payload = json.dumps(
            {"t": self.target_table, "k": self.key_columns, "r": self.rows},
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


@dataclass
class LoadResult:
    load_id: str
    target_table: str
    source_file: str
    config_version: str
    inserted: int = 0
    updated: int = 0
    deactivated: int = 0
    status: str = "SUCCEEDED"
    validation_errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_yaml_file(path: str | Path) -> list[ConfigBlock]:
    """Read one YAML file into normalised blocks. Raises on structural problems."""
    path = Path(path)
    if not path.exists():
        raise ConfigValidationError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)

    if not isinstance(doc, dict):
        raise ConfigValidationError(f"{path}: top level must be a mapping")

    raw_blocks = doc.get("blocks")
    if raw_blocks is None:
        raw_blocks = [doc]
    elif not isinstance(raw_blocks, list):
        raise ConfigValidationError(f"{path}: 'blocks' must be a list")

    file_default_table = doc.get("target_table")
    file_default_keys = doc.get("key_columns")
    file_derive_key = doc.get("derive_key")

    blocks: list[ConfigBlock] = []
    for idx, raw in enumerate(raw_blocks):
        if not isinstance(raw, dict):
            raise ConfigValidationError(f"{path} block {idx}: must be a mapping")

        target = raw.get("target_table", file_default_table)
        keys = raw.get("key_columns", file_default_keys)
        derive = raw.get("derive_key", file_derive_key)

        if not target:
            raise ConfigValidationError(f"{path} block {idx}: no target_table")
        if not keys:
            raise ConfigValidationError(f"{path} block {idx}: no key_columns")

        rows = raw.get("rows") or []
        if not isinstance(rows, list):
            raise ConfigValidationError(f"{path} block {idx}: 'rows' must be a list")

        # `_defaults` holds YAML anchor definitions only; it is not data.
        if not rows and "_defaults" in raw:
            continue

        common = raw.get("apply_to_all") or {}
        if not isinstance(common, dict):
            raise ConfigValidationError(f"{path} block {idx}: apply_to_all must be a mapping")

        expanded: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                raise ConfigValidationError(f"{path} block {idx}: each row must be a mapping")
            merged = {**common, **row}          # row wins over the shared block
            illegal = _MANAGED & merged.keys()
            if illegal:
                raise ConfigValidationError(
                    f"{path} block {idx}: YAML must not set loader-managed columns {sorted(illegal)}"
                )
            expanded.append(merged)

        blocks.append(
            ConfigBlock(
                target_table=target,
                key_columns=list(keys),
                rows=expanded,
                source_file=str(path),
                derive_key=derive,
            )
        )

    # Several blocks can target the same table (schema_mapping does, one per
    # tenant x entity). Merge them so the soft-delete pass sees the full
    # declared set — otherwise block 2 would deactivate block 1's rows.
    return _coalesce_by_target(blocks)


def _coalesce_by_target(blocks: list[ConfigBlock]) -> list[ConfigBlock]:
    merged: dict[str, ConfigBlock] = {}
    for b in blocks:
        if b.target_table in merged:
            existing = merged[b.target_table]
            if existing.key_columns != b.key_columns:
                raise ConfigValidationError(
                    f"{b.target_table}: blocks declare different key_columns "
                    f"({existing.key_columns} vs {b.key_columns})"
                )
            existing.rows.extend(b.rows)
            existing.derive_key = existing.derive_key or b.derive_key
            if b.source_file not in existing.source_file.split(","):
                existing.source_file = f"{existing.source_file},{b.source_file}"
        else:
            merged[b.target_table] = ConfigBlock(
                target_table=b.target_table,
                key_columns=list(b.key_columns),
                rows=list(b.rows),
                source_file=b.source_file,
                derive_key=b.derive_key,
            )
    return list(merged.values())


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_block(spark: SparkSession, block: ConfigBlock) -> list[str]:
    """Check a block against the target table. Returns errors; empty means OK."""
    errors: list[str] = []

    try:
        target_schema: StructType = spark.table(block.target_table).schema
    except Exception as exc:  # noqa: BLE001 - we want the message, not the type
        return [f"{block.target_table}: cannot read target table ({exc}). Run 01_control_plane_ddl first."]

    target_cols = {f.name for f in target_schema}

    for col in block.key_columns:
        if col not in target_cols:
            errors.append(f"{block.target_table}: key column '{col}' does not exist in the table")

    seen_keys: set[tuple] = set()
    for i, row in enumerate(block.rows):
        if block.derive_key:
            row.setdefault(block.key_columns[0], _format_key(block.derive_key, row))

        unknown = set(row.keys()) - target_cols
        if unknown:
            errors.append(f"{block.target_table} row {i}: unknown columns {sorted(unknown)}")

        missing_keys = [k for k in block.key_columns if row.get(k) in (None, "")]
        if missing_keys:
            errors.append(f"{block.target_table} row {i}: null key column(s) {missing_keys}")
            continue

        key = tuple(str(row[k]) for k in block.key_columns)
        if key in seen_keys:
            errors.append(f"{block.target_table} row {i}: duplicate key {key} within the YAML")
        seen_keys.add(key)

    return errors


def _format_key(template: str, row: dict[str, Any]) -> str:
    try:
        return template.format(**row)
    except KeyError as exc:
        raise ConfigValidationError(
            f"derive_key template '{template}' needs field {exc} which the row does not have"
        ) from exc


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _to_dataframe(spark: SparkSession, block: ConfigBlock, config_version: str, actor: str) -> DataFrame:
    """Build a DataFrame shaped exactly like the target table.

    Every declared row is widened to the full target schema with nulls, then
    cast column by column. Building the DataFrame from a partial schema and
    relying on Spark inference is the usual source of "column X is string here
    and bigint there" failures across files.
    """
    target_schema = spark.table(block.target_table).schema
    target_cols = [f.name for f in target_schema]
    now = datetime.now(timezone.utc)

    widened = []
    for row in block.rows:
        if block.derive_key:
            row.setdefault(block.key_columns[0], _format_key(block.derive_key, row))
        full = {c: row.get(c) for c in target_cols}
        if "config_version" in target_cols:
            full["config_version"] = config_version
        if "updated_at" in target_cols:
            full["updated_at"] = now
        if "updated_by" in target_cols:
            full["updated_by"] = actor
        # Stringify so one Python type per column reaches Spark; cast follows.
        widened.append({c: (None if full[c] is None else str(full[c])) for c in target_cols})

    df = spark.createDataFrame(widened, schema=", ".join(f"`{c}` STRING" for c in target_cols))
    return df.select(*[F.col(f"`{f.name}`").cast(f.dataType).alias(f.name) for f in target_schema])


def load_block(
    spark: SparkSession,
    block: ConfigBlock,
    actor: str,
    dry_run: bool = False,
) -> LoadResult:
    """MERGE one block into its target table and deactivate absent rows.

    Writes go through ``ap_platform.tables`` (null-safe MERGE, no caching:
    serverless compute refuses ``.cache()``).
    """
    from . import tables

    version = block.config_version
    result = LoadResult(
        load_id=str(uuid.uuid4()),
        target_table=block.target_table,
        source_file=block.source_file,
        config_version=version,
    )

    errors = validate_block(spark, block)
    if errors:
        result.status = "VALIDATION_FAILED"
        result.validation_errors = errors
        return result

    src = _to_dataframe(spark, block, version, actor)
    tgt_cols = {f.name for f in spark.table(block.target_table).schema}

    # Counts are taken before the write so the audit reflects what changed,
    # not what the table looks like afterwards.
    existing = spark.table(block.target_table).select(*block.key_columns).distinct()
    keys_df = src.select(*block.key_columns)
    on = [keys_df[k].eqNullSafe(existing[k]) for k in block.key_columns]
    matched = keys_df.join(existing, on=on, how="left_semi").count()
    declared = keys_df.count()
    result.inserted = declared - matched
    result.updated = matched

    # Rows active in the table but no longer declared in YAML. Collected now,
    # BEFORE the merge: a lazy frame over the target re-read after the write
    # would see the new state. Config tables are small, so this is cheap.
    stale_keys: list = []
    if "is_active" in tgt_cols:
        active = spark.table(block.target_table).filter("is_active = true").select(*block.key_columns)
        on2 = [active[k].eqNullSafe(keys_df[k]) for k in block.key_columns]
        stale_keys = [tuple(r) for r in active.join(keys_df, on=on2, how="left_anti").collect()]
        result.deactivated = len(stale_keys)

    if dry_run:
        return result

    tables.merge(spark, src, block.target_table, block.key_columns)

    # Convergence pass: deactivate what YAML no longer declares. Done as a
    # MERGE of the stale keys with a literal is_active=false, which every
    # Delta version supports and which is null-safe on the keys.
    if stale_keys:
        key_schema = spark.table(block.target_table).select(*block.key_columns).schema
        sk = spark.createDataFrame(stale_keys, key_schema) \
                  .select(*[F.col(k).alias(f"_s_{k}") for k in block.key_columns])
        cond = None
        for k in block.key_columns:
            c = F.col(k).eqNullSafe(F.col(f"_s_{k}"))
            cond = c if cond is None else cond & c
        upd = (spark.table(block.target_table).join(sk, cond, "left_semi")
               .withColumn("is_active", F.lit(False)))
        if "updated_at" in tgt_cols:
            upd = upd.withColumn("updated_at", F.current_timestamp())
        tables.merge(spark, upd, block.target_table, block.key_columns, insert=False)

    return result


def write_audit(spark: SparkSession, results: Iterable[LoadResult], actor: str) -> None:
    """Append the load outcomes to ap_control.ops.config_load_audit."""
    now = datetime.now(timezone.utc)
    rows = [
        (
            r.load_id, now, actor, r.source_file, r.target_table, r.config_version,
            int(r.inserted), int(r.updated), int(r.deactivated), r.status,
            json.dumps(r.validation_errors) if r.validation_errors else None,
        )
        for r in results
    ]
    if not rows:
        return
    schema = (
        "load_id STRING, loaded_at TIMESTAMP, loaded_by STRING, config_file STRING, "
        "target_table STRING, config_version STRING, rows_inserted BIGINT, rows_updated BIGINT, "
        "rows_deactivated BIGINT, status STRING, validation_errors STRING"
    )
    from . import tables

    tables.append(spark, spark.createDataFrame(rows, schema), "ap_control.ops.config_load_audit")


def load_directory(
    spark: SparkSession,
    config_dir: str | Path,
    actor: str,
    dry_run: bool = False,
    fail_fast: bool = True,
) -> list[LoadResult]:
    """Load every YAML file under a directory, in sorted order.

    Sorted order matters: tenants.yaml must land before ingestion.yaml, because
    a validation pass in Step 3 checks ingestion rows against known tenants.
    Filenames are prefixed to control this where it is not alphabetical already.
    """
    config_dir = Path(config_dir)
    files = sorted(list(config_dir.rglob("*.yaml")) + list(config_dir.rglob("*.yml")))
    if not files:
        raise ConfigValidationError(f"No YAML files found under {config_dir}")

    # Parse EVERY file first, then merge blocks per target table ACROSS files.
    # Loading file by file was a bug: schema_mapping is declared by two files,
    # and the convergence pass of whichever loaded second deactivated every
    # row the first declared. One table = one declared set = one load.
    blocks = _coalesce_by_target([b for path in files for b in parse_yaml_file(path)])

    results: list[LoadResult] = []
    for block in blocks:
        res = load_block(spark, block, actor=actor, dry_run=dry_run)
        results.append(res)
        if res.status != "SUCCEEDED" and fail_fast:
            if not dry_run:
                write_audit(spark, results, actor)
            raise ConfigValidationError(
                f"{block.source_file} -> {block.target_table} failed validation:\n  "
                + "\n  ".join(res.validation_errors)
            )

    if dry_run:
        return results

    write_audit(spark, results, actor)
    return results
