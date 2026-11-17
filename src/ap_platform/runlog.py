"""
runlog — batch_run_log and task_audit_log, written the same way by every layer.

The run logs are designed so a failure is diagnosable from the control plane
without a stack trace: which batch, which task, which tenant, how many records
went in and out, which files, which watermark range, and the error text.

Usage::

    run = RunLog(spark, pipeline_name="bronze_ingest", layer="BRONZE",
                 tenant_id="ALL", run_mode="FULL", dry_run=False)
    with run.task("TEN001__vendor_master", tenant_id="TEN001", entity="vendor_master") as t:
        ...
        t.records_in, t.records_out = 120, 118
    run.finish()

Logs are written even on dry runs, with run_mode suffixed ``_DRY_RUN``: knowing
that a dry run happened, and what it would have done, is itself audit evidence.
Set ``persist=False`` to suppress that.
"""

from __future__ import annotations

import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from . import tables

BATCH_TABLE = "ap_control.ops.batch_run_log"
TASK_TABLE = "ap_control.ops.task_audit_log"

BATCH_SCHEMA = (
    "batch_id STRING, tenant_id STRING, pipeline_name STRING, layer STRING, trigger_type STRING, "
    "run_mode STRING, business_date DATE, status STRING, start_ts TIMESTAMP, end_ts TIMESTAMP, "
    "duration_seconds DOUBLE, records_read BIGINT, records_written BIGINT, records_quarantined BIGINT, "
    "records_rejected BIGINT, files_processed INT, error_message STRING, error_stack STRING, "
    "databricks_run_id STRING, triggered_by STRING, config_version STRING"
)
TASK_SCHEMA = (
    "batch_id STRING, task_id STRING, tenant_id STRING, entity STRING, task_sequence INT, status STRING, "
    "attempt_number INT, start_ts TIMESTAMP, end_ts TIMESTAMP, records_in BIGINT, records_out BIGINT, "
    "source_files STRING, watermark_from STRING, watermark_to STRING, error_message STRING"
)


def now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_batch_id(prefix: str) -> str:
    return f"{prefix}-{now():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


@dataclass
class Task:
    batch_id: str
    task_id: str
    tenant_id: str
    entity: str | None
    task_sequence: int
    status: str = "RUNNING"
    attempt_number: int = 1
    start_ts: datetime = field(default_factory=now)
    end_ts: datetime | None = None
    records_in: int = 0
    records_out: int = 0
    records_quarantined: int = 0
    source_files: str | None = None
    watermark_from: str | None = None
    watermark_to: str | None = None
    error_message: str | None = None
    error_stack: str | None = None

    def as_row(self) -> dict:
        return {k: getattr(self, k) for k in (
            "batch_id", "task_id", "tenant_id", "entity", "task_sequence", "status", "attempt_number",
            "start_ts", "end_ts", "records_in", "records_out", "source_files", "watermark_from",
            "watermark_to", "error_message")}


class RunLog:
    def __init__(self, spark: SparkSession, pipeline_name: str, layer: str, tenant_id: str = "ALL",
                 run_mode: str = "INCREMENTAL", dry_run: bool = False, triggered_by: str | None = None,
                 batch_id: str | None = None, persist: bool = True, prefix: str | None = None):
        self.spark = spark
        self.batch_id = batch_id or new_batch_id(prefix or layer[:2].upper())
        self.pipeline_name = pipeline_name
        self.layer = layer
        self.tenant_id = tenant_id
        self.run_mode = run_mode + ("_DRY_RUN" if dry_run else "")
        self.dry_run = dry_run
        self.triggered_by = triggered_by
        self.persist = persist
        self.start_ts = now()
        self.tasks: list[Task] = []
        self.files_processed = 0

    @contextmanager
    def task(self, task_id: str, tenant_id: str, entity: str | None = None, swallow: bool = True):
        """Record one unit of work. Exceptions are captured, logged and (by
        default) swallowed so one failing task cannot take the batch down —
        fail isolation is a design decision (D9), not an accident."""
        t = Task(self.batch_id, task_id, tenant_id, entity, len(self.tasks) + 1)
        self.tasks.append(t)
        try:
            yield t
            if t.status == "RUNNING":
                t.status = "SUCCEEDED"
        except Exception as exc:  # noqa: BLE001
            t.status = "FAILED"
            t.error_message = f"{type(exc).__name__}: {exc}"[:4000]
            t.error_stack = traceback.format_exc()[-8000:]
            if not swallow:
                raise
        finally:
            t.end_ts = now()

    # ---- summary --------------------------------------------------------
    @property
    def status(self) -> str:
        sts = {t.status for t in self.tasks}
        if not sts:
            return "SUCCEEDED"
        if sts <= {"SUCCEEDED", "SKIPPED", "NO_NEW_DATA"}:
            return "SUCCEEDED"
        if "SUCCEEDED" in sts or "NO_NEW_DATA" in sts:
            return "PARTIAL"
        return "FAILED"

    def finish(self, extra_error: str | None = None) -> str:
        end = now()
        failed = [t for t in self.tasks if t.status in ("FAILED", "HALTED", "ABORTED")]
        err = "; ".join(f"{t.task_id}: {t.status}: {t.error_message or ''}" for t in failed)[:4000] or extra_error
        stack = "\n----\n".join(t.error_stack for t in failed if t.error_stack)[:16000] or None
        batch = {
            "batch_id": self.batch_id, "tenant_id": self.tenant_id, "pipeline_name": self.pipeline_name,
            "layer": self.layer, "trigger_type": "MANUAL", "run_mode": self.run_mode, "business_date": end.date(),
            "status": self.status, "start_ts": self.start_ts, "end_ts": end,
            "duration_seconds": (end - self.start_ts).total_seconds(),
            "records_read": sum(t.records_in for t in self.tasks),
            "records_written": sum(t.records_out for t in self.tasks),
            "records_quarantined": sum(t.records_quarantined for t in self.tasks),
            "records_rejected": 0, "files_processed": self.files_processed,
            "error_message": err, "error_stack": stack, "databricks_run_id": None,
            "triggered_by": self.triggered_by, "config_version": None,
        }
        if self.persist:
            tables.append(self.spark, tables.rows_to_df(self.spark, [batch], BATCH_SCHEMA), BATCH_TABLE)
            if self.tasks:
                tables.append(self.spark, tables.rows_to_df(
                    self.spark, [t.as_row() for t in self.tasks], TASK_SCHEMA), TASK_TABLE)
        return self.status

    def summary_rows(self) -> list[tuple]:
        return [(t.task_sequence, t.task_id, t.tenant_id, t.entity, t.status, t.records_in, t.records_out,
                 t.records_quarantined, (t.error_message or "")[:200]) for t in self.tasks]


SUMMARY_SCHEMA = ("seq INT, task_id STRING, tenant_id STRING, entity STRING, status STRING, "
                  "records_in BIGINT, records_out BIGINT, quarantined BIGINT, error STRING")
