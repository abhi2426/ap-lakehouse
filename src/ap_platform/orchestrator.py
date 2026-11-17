"""
orchestrator — ONE runner for silver and gold.

    plan = orchestrator.plan(spark, "SILVER")
    run  = orchestrator.run(spark, "SILVER", dry_run=True)

Planning has no side effects. It loads the active jobs for the layer, derives
the waves from ``depends_on``, resolves a builder for every job and validates
what can be validated without reading data: merge keys exist in the output,
cluster columns exist, the DAG is acyclic.

Builder resolution is the one deliberate asymmetry between the layers:

* SILVER: a job with a registered builder uses it; any other bronze-fed job
  falls back to the generic conformance pipeline. A mapped entity needs no code.
* GOLD: a job with no registered builder is a PLAN ERROR. Every gold table is a
  modelling decision about grain and keys, and config alone must not be able
  to conjure one.

Execution is wave by wave. A failed job marks every job that depends on it,
directly or not, as SKIPPED; independent jobs carry on (decision D9).
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from . import tables
from .runlog import RunLog
from .spec import JobSpec, PlanError, load_specs, waves


@dataclass
class PlannedJob:
    spec: JobSpec
    wave: int
    builder_name: str
    builder: object
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class Plan:
    layer: str
    jobs: list[PlannedJob]
    errors: list[str]

    @property
    def ok(self) -> bool:
        return not self.errors and not any(j.errors for j in self.jobs)

    def rows(self) -> list[tuple]:
        return [(j.wave, j.spec.job_id, j.spec.write_mode, j.builder_name, j.spec.target_table,
                 ",".join(j.spec.depends_on), "; ".join(j.errors) or None, "; ".join(j.warnings) or None)
                for j in self.jobs]


PLAN_SCHEMA = ("wave INT, job_id STRING, write_mode STRING, builder STRING, target_table STRING, "
               "depends_on STRING, errors STRING, warnings STRING")


def _registry(layer: str) -> dict:
    if layer == "SILVER":
        from .silver import builders
        return builders.REGISTRY
    from .gold import registry
    return registry.REGISTRY


def plan(spark: SparkSession, layer: str, jobs: list[str] | None = None) -> Plan:
    from .silver import conform as CF
    from .silver.pipeline import build_conformed

    specs = load_specs(spark)
    errors: list[str] = []
    try:
        ws = waves(specs, layer)
    except PlanError as exc:
        return Plan(layer, [], [str(exc)])
    reg = _registry(layer)
    planned: list[PlannedJob] = []
    for i, wave in enumerate(ws, start=1):
        for s in wave:
            if jobs and s.job_id not in jobs:
                continue
            pj = None
            if s.job_id in reg:
                b = reg[s.job_id]
                pj = PlannedJob(s, i, getattr(b, "__name__", "registered"), b)
                cols = getattr(b, "columns", None)
            elif layer == "SILVER" and s.entity:
                pj = PlannedJob(s, i, "generic_conformance", build_conformed)
                cols = CF.load_entity_config(spark, s.entity, s.source_tables[0]).output_columns()
                if not CF.load_entity_config(spark, s.entity).mappings:
                    pj.errors.append(f"no schema_mapping rows for entity {s.entity}")
            else:
                pj = PlannedJob(s, i, "MISSING", None)
                pj.errors.append("no registered builder" + (" (gold jobs must be registered)"
                                                           if layer == "GOLD" else ""))
                cols = None
            if s.write_mode in ("MERGE", "SCD2_MERGE", "SNAPSHOT") and not s.merge_keys:
                pj.errors.append(f"write_mode {s.write_mode} needs merge_keys")
            if cols:
                miss = [k for k in s.merge_keys if k not in cols and k != "valid_from"]
                if miss:
                    pj.errors.append(f"merge key(s) {miss} not produced by the builder")
            # Registered builders declare only their key columns, so cluster and
            # SCD columns can be checked only where the full output is known.
            if cols and pj.builder_name == "generic_conformance":
                cmiss = [k for k in s.cluster_columns if k not in cols]
                if cmiss:
                    pj.warnings.append(f"cluster column(s) {cmiss} not produced; ignored")
                tmiss = [k for k in s.scd_tracked_columns if k not in cols]
                if tmiss:
                    pj.warnings.append(f"SCD tracked column(s) {tmiss} not produced; ignored")
            planned.append(pj)
    return Plan(layer, planned, errors)


def _dependents(plan_: Plan, failed: str) -> set[str]:
    out, frontier = set(), {failed}
    while frontier:
        nxt = {j.spec.job_id for j in plan_.jobs if set(j.spec.depends_on) & frontier} - out
        out |= nxt
        frontier = nxt
    return out


def write_build(spark: SparkSession, ctx, b) -> None:
    from .silver import pipeline as P
    from .silver import scd2
    from .silver import dq as DQ

    spec = ctx.spec
    cluster = spec.cluster_columns
    if b.df is not None and b.records_out:
        if spec.write_mode == "SCD2_MERGE":
            scd2.apply(spark, b.df, spec.target_table, spec.merge_keys if ctx.layer == "SILVER"
                       else [k for k in spec.merge_keys if k != "valid_from"],
                       spec.scd_tracked_columns, cluster)
        elif spec.write_mode == "MERGE":
            cond = "s._version >= t._version" if "_version" in b.df.columns else None
            # A full-recompute builder produced the complete set: rows it no
            # longer produces (deleted at source) must leave the target too.
            # Scoped to the tenants in this run, so a one-tenant run cannot
            # delete another tenant's rows.
            dm = None
            if b.full_set:
                dm = ("true" if not ctx.tenants or "tenant_id" not in b.df.columns
                      else "t.tenant_id IN (" + ", ".join(f"'{t}'" for t in ctx.tenants) + ")")
            tables.merge(spark, b.df, spec.target_table, spec.merge_keys, cluster_by=cluster,
                         update_condition=cond, delete_missing=dm)
        elif spec.write_mode == "OVERWRITE":
            tables.overwrite(spark, b.df, spec.target_table, cluster_by=cluster)
        elif spec.write_mode == "SNAPSHOT":
            tables.replace_where(spark, b.df, spec.target_table, b.replace_predicate or "true",
                                 cluster_by=cluster)
        else:
            raise ValueError(f"unknown write_mode {spec.write_mode}")
    elif b.df is not None and spec.write_mode == "OVERWRITE":
        tables.overwrite(spark, b.df, spec.target_table, cluster_by=cluster)
    if b.quarantine is not None and b.records_quarantined:
        P.write_quarantine(spark, b.quarantine, spec.entity or spec.job_id.lower(), ctx.layer)
    if b.watermarks:
        P.write_watermarks(spark, spec.job_id, spec.source_tables[0], b.watermarks, ctx.batch_id)
    P.write_recon(spark, b.recon_rows)
    DQ.write_log(spark, b.dq_rows)


def run(spark: SparkSession, layer: str, *, jobs: list[str] | None = None, tenants: list[str] | None = None,
        dry_run: bool = False, full_reload: bool = False, actor: str = "unknown",
        key_filters: dict | None = None, pipeline_name: str | None = None, verbose: bool = True) -> RunLog:
    from .silver.pipeline import JobContext

    p = plan(spark, layer, jobs)
    run_ = RunLog(spark, pipeline_name or f"{layer.lower()}_run", layer,
                  tenant_id=",".join(tenants) if tenants else "ALL",
                  run_mode="FULL_RELOAD" if full_reload else ("REDRIVE" if key_filters else "INCREMENTAL"),
                  dry_run=dry_run, triggered_by=actor, prefix=layer[:2])
    run_.plan = p
    run_.last_builds = {}
    if not p.ok:
        msgs = p.errors + [f"{j.spec.job_id}: {'; '.join(j.errors)}" for j in p.jobs if j.errors]
        run_.finish(extra_error="PLAN FAILED: " + " | ".join(msgs))
        raise PlanError("plan failed:\n  " + "\n  ".join(msgs))

    skipped: set[str] = set()
    for pj in p.jobs:
        s = pj.spec
        with run_.task(s.job_id, s.tenant_id, s.entity or s.job_id) as t:
            if s.job_id in skipped:
                t.status = "SKIPPED"
                t.error_message = "an upstream job failed in this run"
                if verbose:
                    print(f"  w{pj.wave} SKIP  {s.job_id}")
                continue
            ctx = JobContext(spark, s, run_.batch_id, layer, dry_run=dry_run, full_reload=full_reload,
                             tenants=tenants, key_filter=(key_filters or {}).get(s.job_id))
            try:
                try:
                    b = pj.builder(ctx)
                except Exception as exc:  # noqa: BLE001
                    if type(exc).__name__ != "UpstreamMissing":
                        raise
                    from .silver.pipeline import Build
                    b = Build(status="SKIPPED",
                              note=f"{exc}" + (" (expected in a dry run on a fresh lake)" if dry_run else ""))
                if not dry_run and b.status not in ("NO_NEW_DATA", "SKIPPED"):
                    write_build(spark, ctx, b)
                elif dry_run and b.dq_rows:
                    pass
            except Exception as exc:  # noqa: BLE001
                skipped |= _dependents(p, s.job_id)
                t.error_stack = traceback.format_exc()[-8000:]
                raise exc
            t.status = b.status
            t.records_in, t.records_out, t.records_quarantined = b.records_in, b.records_out, b.records_quarantined
            t.error_message = b.note or None
            t.watermark_to = ",".join(f"{k}={v}" for k, v in sorted(b.watermarks.items())) or None
            run_.last_builds[s.job_id] = b
            if verbose:
                print(f"  w{pj.wave} {b.status[:7]:7} {s.job_id:28} in {b.records_in:>7,}  out {b.records_out:>7,}"
                      f"  quarantined {b.records_quarantined:>5,}  rules {b.rules_evaluated:>2}"
                      + (f"/{b.rules_evaluated + b.rules_not_evaluated}" if b.rules_not_evaluated else "")
                      + (f"   {b.note}" if b.note else ""))
        if run_.tasks[-1].status == "FAILED":
            skipped |= _dependents(p, s.job_id)
            if verbose:
                print(f"  w{pj.wave} FAILED {s.job_id}: {run_.tasks[-1].error_message}")
    run_.finish()
    return run_
