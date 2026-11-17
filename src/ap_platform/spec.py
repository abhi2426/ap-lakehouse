"""
spec — transformation_config rows as JobSpecs, and the DAG they imply.

The waves are DERIVED from ``depends_on``, never hand-drawn: Kahn's algorithm
over the active jobs of one layer. A dependency on a job in another layer
(gold depending on silver) is treated as already satisfied, because layers run
in order. A dependency on an unknown job, or a cycle, is a plan error reported
before anything writes.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import SparkSession


def _split(v) -> list[str]:
    return [x.strip() for x in (v or "").split(",") if x.strip()]


@dataclass
class JobSpec:
    job_id: str
    tenant_id: str
    layer: str
    target_table: str
    source_tables: list[str]
    depends_on: list[str]
    write_mode: str
    merge_keys: list[str]
    scd_type: str | None
    scd_tracked_columns: list[str]
    sequence_column: str | None
    cluster_columns: list[str]
    snapshot_grain: str | None
    raw: dict = field(default_factory=dict, repr=False)

    @property
    def entity(self) -> str | None:
        """Canonical entity for a bronze-fed job (br_<entity>), else None."""
        for s in self.source_tables:
            name = s.split(".")[-1]
            if name.startswith("br_"):
                return name[3:]
        return None

    @staticmethod
    def from_row(r: dict) -> "JobSpec":
        return JobSpec(
            job_id=r["job_id"], tenant_id=r.get("tenant_id") or "ALL", layer=r["layer"],
            target_table=r["target_table"], source_tables=_split(r.get("source_tables")),
            depends_on=_split(r.get("depends_on")), write_mode=(r.get("write_mode") or "MERGE").upper(),
            merge_keys=_split(r.get("merge_keys")), scd_type=r.get("scd_type"),
            scd_tracked_columns=_split(r.get("scd_tracked_columns")),
            sequence_column=r.get("sequence_column"), cluster_columns=_split(r.get("cluster_columns")),
            snapshot_grain=r.get("snapshot_grain"), raw=r)


def load_specs(spark: SparkSession) -> list[JobSpec]:
    rows = spark.sql("SELECT * FROM ap_control.config.transformation_config WHERE is_active").collect()
    return [JobSpec.from_row(r.asDict()) for r in rows]


class PlanError(Exception):
    pass


def waves(specs: list[JobSpec], layer: str) -> list[list[JobSpec]]:
    """Topological waves for one layer. Raises PlanError on unknown deps or cycles."""
    all_ids = {s.job_id for s in specs}
    mine = {s.job_id: s for s in specs if s.layer == layer}
    errors = []
    for s in mine.values():
        for d in s.depends_on:
            if d not in all_ids:
                errors.append(f"{s.job_id} depends on unknown or inactive job {d}")
    if errors:
        raise PlanError("; ".join(errors))
    remaining = {j: {d for d in s.depends_on if d in mine} for j, s in mine.items()}
    out: list[list[JobSpec]] = []
    while remaining:
        ready = sorted(j for j, deps in remaining.items() if not deps)
        if not ready:
            raise PlanError(f"dependency cycle among: {sorted(remaining)}")
        out.append([mine[j] for j in ready])
        for j in ready:
            del remaining[j]
        for deps in remaining.values():
            deps.difference_update(ready)
    return out
