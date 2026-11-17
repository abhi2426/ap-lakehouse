"""
operations — the four day-two operations on the bronze layer.

* ``unhalt``          accept a drifted shape: new contract version, drift RESOLVED
* ``replay_batch``    remove what one batch loaded and let the next run reload it
* ``backfill``        same, for every file in an extract-date range
* ``reset_checkpoint`` forget Auto Loader's discovery state for a source

Every operation is a dry run unless ``apply=True``, and every one prints what
it would change first. Replay and backfill remove bronze rows by source file
and reset the ledger so the files are pending again; the next run of notebook
12 reloads them through the same engine. Nothing is ever edited in place.
"""

from __future__ import annotations

import shutil
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .. import tables
from ..runlog import now
from . import contracts as C
from . import engine as E


def unhalt(spark: SparkSession, tenant_id: str, entity: str, actor: str, notes: str,
           apply: bool = False) -> dict:
    """Accept the drift halting a source: re-baseline from the drifted file."""
    halts = E.open_breaking_drift(spark, tenant_id, entity)
    if not halts:
        return {"status": "NOTHING_TO_DO", "detail": f"{tenant_id}/{entity} has no open BREAKING drift"}
    contract = E.current_contract(spark, tenant_id, entity)
    cfg = next(c for c in E.load_sources(spark, [tenant_id], [entity]))
    sample = sorted(halts, key=lambda h: h["detected_at"])[0]["sample_file"]
    f = next((x for x in E.discover(cfg) if x.path == sample), None)
    if f is None:
        return {"status": "FAILED", "detail": f"sample file {sample} is no longer in the landing zone"}
    prof = E.profile(cfg, f)
    new = C.accept(contract, prof)
    before, after = contract.names, new.names
    detail = {
        "status": "WOULD_UNHALT" if not apply else "UNHALTED",
        "tenant_id": tenant_id, "entity": entity,
        "contract": f"v{contract.version} -> v{new.version}",
        "removed_columns": [c for c in before if c not in after],
        "added_columns": [c for c in after if c not in before],
        "retyped_to_string": [c.name for c in new.columns
                              if contract.type_of(c.name) == "NUMBER" and c.type_class == "STRING"],
        "drift_ids": [h["drift_id"] for h in halts],
    }
    if apply:
        E.register_contract(spark, new, sample, actor, "ACCEPTED " + ",".join(detail["drift_ids"]))
        ids = ", ".join(f"'{d}'" for d in detail["drift_ids"])
        safe = notes.replace("'", "\\'")
        tables.update(spark, E.DRIFT_LOG,
                      f"status = 'RESOLVED', resolved_by = '{actor}', resolved_at = current_timestamp(), "
                      f"action_taken = 'ACCEPTED_BY_STEWARD', notes = '{safe}'",
                      f"drift_id IN ({ids})")
    return detail


def _reset_files(spark: SparkSession, files: list[dict], reason: str, apply: bool) -> dict:
    by_target: dict[str, list[str]] = {}
    for f in files:
        cfg = spark.sql(f"SELECT target_table FROM ap_control.config.ingestion_config "
                        f"WHERE config_id = '{f['config_id']}'").collect()
        if cfg:
            by_target.setdefault(cfg[0].target_table, []).append(f["file_path"])
    out = {"files": len(files), "rows": int(sum(f.get("rows_ingested") or 0 for f in files)),
           "targets": {k: len(v) for k, v in by_target.items()}, "applied": apply}
    if apply and files:
        for target, paths in by_target.items():
            for i in range(0, len(paths), 200):
                chunk = ", ".join("'" + p.replace("'", "\\'") + "'" for p in paths[i:i + 200])
                tables.delete(spark, target, f"source_file IN ({chunk})")
        paths = [f["file_path"] for f in files]
        for i in range(0, len(paths), 200):
            chunk = ", ".join("'" + p.replace("'", "\\'") + "'" for p in paths[i:i + 200])
            tables.delete(spark, E.LEDGER, f"file_path IN ({chunk})")
        print(f"reset {len(files)} file(s): {reason}. Re-run notebook 12 to reload them.")
    return out


def replay_batch(spark: SparkSession, batch_id: str, apply: bool = False) -> dict:
    files = [r.asDict() for r in spark.table(E.LEDGER).filter(
        (F.col("batch_id") == batch_id) & (F.col("status") == "INGESTED")).collect()]
    return _reset_files(spark, files, f"replay of {batch_id}", apply)


def backfill(spark: SparkSession, tenant_id: str, entity: str | None, date_from: date, date_to: date,
             apply: bool = False) -> dict:
    q = spark.table(E.LEDGER).filter((F.col("tenant_id") == tenant_id)
                                     & F.col("file_date").between(F.lit(date_from), F.lit(date_to))
                                     & (F.col("status") == "INGESTED"))
    if entity:
        q = q.filter(F.col("entity") == entity)
    return _reset_files(spark, [r.asDict() for r in q.collect()],
                        f"backfill {tenant_id}/{entity or '*'} {date_from}..{date_to}", apply)


def reset_checkpoint(checkpoint_root: str, tenant_id: str, entity: str, apply: bool = False) -> dict:
    path = f"{checkpoint_root}/{tenant_id}/{entity}"
    if apply:
        shutil.rmtree(path, ignore_errors=True)
    return {"checkpoint": path, "applied": apply,
            "note": "Only affects Auto Loader discovery. The file ledger still prevents double-loading."}
