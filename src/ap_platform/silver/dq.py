"""
dq — the data-quality gate, driven by ap_control.config.dq_rule_registry.

How a rule is evaluated
-----------------------
* ROW and CROSS_ENTITY rules are boolean SQL expressions evaluated per row.
  NULL counts as PASS (SQL CHECK-constraint semantics); rules that care about
  nulls say so explicitly, e.g. ``x IS NOT NULL AND ...``.
* CROSS_ENTITY rules first get the column they reference added by a resolver
  chosen from the rule's ``parameters`` (FK_EXISTS, SUM_MATCH, tolerance...).
* AGGREGATE rules are evaluated per tenant over the rows that will be written.
* A rule whose expression cannot be resolved against the data (a column no
  mapping produces) is recorded as ``NOT_EVALUATED`` with the reason. It is
  never silently treated as a pass. That is why the scorecard's first number
  is ``rules_evaluated``: 100% of 4 rules is not 100% of 25.

Severity
--------
=========== ================================================================
ABORT       any failure stops that tenant's slice of the job: nothing written
QUARANTINE  failing rows are diverted to quarantine; the rest proceed. If the
            tenant's failure rate EXCEEDS ``fail_threshold_pct`` the outcome is
            THRESHOLD_BREACH: the rows are still quarantined, and the breach
            is an ALERT (scorecard section 5), not an abort. A NULL threshold
            never raises one. Escalating to abort was tried and rejected: the
            planted defect rates exceed several thresholds by design, so the
            invoice pipeline could never have completed. If a breach must stop
            the pipeline, make the rule ABORT severity — that is explicit.
WARN        counted and logged, nothing diverted
=========== ================================================================
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import tables
from ..runlog import now

DQ_LOG = "ap_control.ops.dq_result_log"
DQ_SCHEMA = (
    "batch_id STRING, rule_id STRING, tenant_id STRING, layer STRING, entity STRING, dq_dimension STRING, "
    "severity STRING, evaluated_at TIMESTAMP, records_evaluated BIGINT, records_passed BIGINT, "
    "records_failed BIGINT, pass_rate_pct DECIMAL(9,4), threshold_pct DECIMAL(5,2), outcome STRING, "
    "failed_sample STRING, action_taken STRING, rule_expression STRING"
)


@dataclass
class Rule:
    rule_id: str
    tenant_id: str
    layer: str
    entity: str
    scope: str
    dq_dimension: str
    rule_type: str
    expression: str
    params: dict
    severity: str
    threshold: float | None

    @staticmethod
    def from_row(r: dict) -> "Rule":
        try:
            params = json.loads(r.get("parameters") or "{}")
        except ValueError:
            params = {}
        thr = r.get("fail_threshold_pct")
        return Rule(r["rule_id"], r["tenant_id"] or "ALL", r["layer"], r["entity"], r["scope"],
                    r["dq_dimension"], r["rule_type"], r["rule_expression"], params, r["severity"],
                    float(thr) if thr is not None else None)

    def applies_to(self, tenant: str) -> bool:
        return self.tenant_id in ("ALL", tenant)

    @property
    def col(self) -> str:
        return "_r_" + re.sub(r"\W", "_", self.rule_id)


def load_rules(spark: SparkSession, layer: str, entity: str) -> list[Rule]:
    rows = spark.sql(f"""
        SELECT * FROM ap_control.config.dq_rule_registry
        WHERE is_active AND layer = '{layer}' AND (entity = '{entity}' OR entity = '*')
          AND (effective_from IS NULL OR effective_from <= current_date())
          AND (effective_to IS NULL OR effective_to >= current_date())
        ORDER BY rule_id""").collect()
    return [Rule.from_row(r.asDict()) for r in rows]


# ---------------------------------------------------------------------------
# Resolvers: add the column a CROSS_ENTITY rule's expression refers to
# ---------------------------------------------------------------------------

@dataclass
class Context:
    spark: SparkSession
    entity: str
    keys: list[str]
    conform_other: object = None     # callable(entity, keys_df) -> latest versions of another entity
    notes: dict = field(default_factory=dict)


def _exists_col(expr: str) -> str | None:
    m = re.search(r"\b([a-z_]+_(?:exists|active))\b", expr)
    return m.group(1) if m else None


def resolve(df: DataFrame, rule: Rule, ctx: Context) -> DataFrame:
    """Return df with whatever the rule needs added. Raise ValueError if it cannot."""
    p = rule.params
    spark = ctx.spark
    if rule.rule_type == "FK_EXISTS":
        out = _exists_col(rule.expression)
        ref, keys = p.get("ref_table"), p.get("ref_keys") or []
        if not out or not ref or not keys:
            raise ValueError("FK_EXISTS needs ref_table, ref_keys and a *_exists/*_active column")
        if not tables.exists(spark, ref):
            raise ValueError(f"reference table {ref} does not exist yet")
        r = spark.table(ref)
        if "is_active" in r.columns and ref.startswith("ap_control"):
            r = r.filter("is_active")
        missing = [k for k in keys if k not in df.columns or k not in r.columns]
        if missing:
            raise ValueError(f"key column(s) {missing} missing on one side")
        r = r.select(*keys).distinct().withColumn(out, F.lit(True))
        return df.join(r, keys, "left").withColumn(out, F.coalesce(F.col(out), F.lit(False)))
    if rule.rule_type == "SUM_MATCH":
        other, jk = p.get("join_entity"), p.get("join_keys") or []
        if not other or not jk or ctx.conform_other is None:
            raise ValueError("SUM_MATCH needs join_entity, join_keys and a conformer")
        lines = ctx.conform_other(other, df.select(*jk).distinct())
        if lines is None:
            raise ValueError(f"no {other} rows available to sum")
        amt = "net_amount" if "net_amount" in lines.columns else "amount"
        totals = (lines.filter(~F.col("is_deleted")).groupBy(*jk)
                  .agg(F.sum(amt).alias("line_net_total")))
        # A header with no lines at all is a different failure (orphan header);
        # leaving line_net_total NULL makes this rule pass for it rather than
        # double-reporting it as a sum mismatch.
        return df.join(totals, jk, "left")
    if p.get("ref_table") == "ap_control.ref.tax_code":
        t = (spark.table("ap_control.ref.tax_code").filter("is_active")
             .select("tenant_id", F.col("tax_code").alias("tax_code_src"), "tax_rate_pct")
             .dropDuplicates(["tenant_id", "tax_code_src"]))
        return df.join(t, ["tenant_id", "tax_code_src"], "left")
    if p.get("tolerance_source"):
        # received vs ordered quantity per PO line, against the tenant tolerance
        gr = (df.filter(~F.col("is_deleted") & (F.coalesce(F.col("movement_class"), F.lit("1")) == "1"))
              .groupBy("tenant_id", "po_number", "po_line_number")
              .agg(F.sum("quantity").alias("received_qty")))
        if not tables.exists(spark, "ap_silver.ap.sl_po_line"):
            raise ValueError("ap_silver.ap.sl_po_line does not exist yet")
        po = (spark.table("ap_silver.ap.sl_po_line").filter(~F.col("is_deleted"))
              .select("tenant_id", "po_number", "po_line_number", F.col("quantity").alias("po_qty")))
        tol = (spark.table("ap_control.config.tolerance_config")
               .filter((F.col("tolerance_type") == p.get("tolerance_type", "QUANTITY")) & F.col("is_active"))
               .groupBy("tenant_id").agg(F.max("upper_limit_pct").alias("qty_tolerance_pct")))
        return (df.join(gr, ["tenant_id", "po_number", "po_line_number"], "left")
                  .join(po, ["tenant_id", "po_number", "po_line_number"], "left")
                  .join(tol, ["tenant_id"], "left"))
    return df


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

@dataclass
class Outcome:
    rule: Rule
    tenant: str
    evaluated: int = 0
    failed: int = 0
    outcome: str = "PASS"
    action: str = "NONE"
    sample: str | None = None

    def row(self, batch_id: str, layer: str, entity: str) -> dict:
        ev, fl = self.evaluated, self.failed
        return {"batch_id": batch_id, "rule_id": self.rule.rule_id, "tenant_id": self.tenant, "layer": layer,
                "entity": entity, "dq_dimension": self.rule.dq_dimension, "severity": self.rule.severity,
                "evaluated_at": now(), "records_evaluated": ev, "records_passed": ev - fl, "records_failed": fl,
                "pass_rate_pct": Decimal("100.0000") if ev == 0 else Decimal(f"{100.0 * (ev - fl) / ev:.4f}"),
                "threshold_pct": None if self.rule.threshold is None else Decimal(f"{self.rule.threshold:.2f}"),
                "outcome": self.outcome, "failed_sample": self.sample, "action_taken": self.action,
                "rule_expression": self.rule.expression[:2000]}


@dataclass
class GateResult:
    df: DataFrame                       # input + _dq_fail_q / _dq_fail_w / _dq_abort arrays
    outcomes: list[Outcome]
    not_evaluated: list[tuple[Rule, str]]
    aborted_tenants: dict[str, str]     # tenant -> reason


def evaluate_rows(df: DataFrame, rules: list[Rule], ctx: Context, tenants: list[str]) -> GateResult:
    """Evaluate ROW and CROSS_ENTITY rules on every version in df."""
    not_eval: list[tuple[Rule, str]] = []
    active: list[Rule] = []
    for rule in [r for r in rules if r.scope in ("ROW", "CROSS_ENTITY")]:
        if not any(rule.applies_to(t) for t in tenants):
            continue
        try:
            cand = resolve(df, rule, ctx)
            test = cand.select(F.expr(rule.expression).alias("x"))
            test.schema  # forces analysis on classic and Spark Connect alike
            scope = (F.lit(True) if rule.tenant_id == "ALL" else F.col("tenant_id") == rule.tenant_id)
            df = cand.withColumn(rule.col, F.when(F.col("is_deleted") | ~scope, F.lit(True))
                                 .otherwise(F.coalesce(F.expr(rule.expression), F.lit(True))))
            active.append(rule)
        except Exception as exc:  # noqa: BLE001 - recorded, never silently passed
            msg = str(exc).split("\n")[0]
            m = re.search(r"name `([^`]+)` cannot be resolved", msg)
            not_eval.append((rule, f"column `{m.group(1)}` not available" if m else msg[:300]))

    def arr(sev):
        items = [F.when(~F.col(r.col), F.lit(r.rule_id)) for r in active if r.severity == sev]
        return F.filter(F.array(*items), lambda x: x.isNotNull()) if items else F.array().cast("array<string>")

    df = (df.withColumn("_dq_fail_q", arr("QUARANTINE"))
            .withColumn("_dq_fail_w", arr("WARN"))
            .withColumn("_dq_abort", arr("ABORT")))

    outcomes: list[Outcome] = []
    aborted: dict[str, str] = {}
    if active:
        aggs = [F.count(F.when(~F.col("is_deleted"), 1)).alias("_n")]
        aggs += [F.sum(F.when(~F.col(r.col), 1).otherwise(0)).alias(r.col) for r in active]
        stats = {row.tenant_id: row.asDict() for row in df.groupBy("tenant_id").agg(*aggs).collect()}
        for r in active:
            for t in tenants:
                if not r.applies_to(t) or t not in stats:
                    continue
                n, f = int(stats[t]["_n"] or 0), int(stats[t][r.col] or 0)
                o = Outcome(r, t, n, f)
                if f == 0:
                    o.outcome = "PASS"
                elif r.severity == "ABORT":
                    o.outcome, o.action = "FAIL", "ABORTED_TENANT_SLICE"
                    aborted[t] = f"{r.rule_id} failed on {f} row(s)"
                elif r.severity == "QUARANTINE":
                    pct = 100.0 * f / max(n, 1)
                    if r.threshold is not None and pct > r.threshold:
                        o.outcome, o.action = "THRESHOLD_BREACH", "ROWS_QUARANTINED_ALERT"
                    else:
                        o.outcome, o.action = "QUARANTINED", "ROWS_QUARANTINED"
                else:
                    pct = 100.0 * f / max(n, 1)
                    o.outcome = "WARN" if (r.threshold is None or pct > r.threshold) else "PASS_WITHIN_THRESHOLD"
                outcomes.append(o)
    return GateResult(df, outcomes, not_eval, aborted)


def add_samples(gr: GateResult, keys: list[str], limit: int = 5) -> None:
    """Attach up to ``limit`` failing business keys to each failing outcome."""
    for o in gr.outcomes:
        if o.failed and o.rule.col in gr.df.columns:
            rows = (gr.df.filter((F.col("tenant_id") == o.tenant) & ~F.col(o.rule.col))
                    .select(*[k for k in keys if k in gr.df.columns]).limit(limit).collect())
            o.sample = json.dumps([r.asDict() for r in rows], default=str)


def evaluate_aggregates(df: DataFrame, rules: list[Rule], tenants: list[str]) -> tuple[list[Outcome], list, dict]:
    """AGGREGATE rules, per tenant, on the frame about to be written."""
    outs, not_eval, aborted = [], [], {}
    for r in [x for x in rules if x.scope == "AGGREGATE" and x.rule_type != "ROW_COUNT"]:
        for t in tenants:
            if not r.applies_to(t):
                continue
            sub = df.filter((F.col("tenant_id") == t) & ~F.col("is_deleted"))
            try:
                res = sub.agg(F.expr(r.expression).alias("ok"), F.count("*").alias("n")).collect()[0]
            except Exception as exc:  # noqa: BLE001
                not_eval.append((r, str(exc).split("\n")[0][:300]))
                break
            n = int(res.n or 0)
            ok = res.ok is None or bool(res.ok) or n == 0
            o = Outcome(r, t, n, 0 if ok else 1)
            if ok:
                o.outcome = "PASS"
            elif r.severity == "ABORT":
                o.outcome, o.action = "FAIL", "ABORTED_TENANT_SLICE"
                aborted[t] = f"{r.rule_id} (aggregate) failed"
            else:
                o.outcome = "WARN"
            outs.append(o)
    return outs, not_eval, aborted


def not_evaluated_rows(items: list[tuple[Rule, str]], tenants: list[str], batch_id: str, layer: str,
                       entity: str) -> list[dict]:
    out = []
    for r, why in items:
        for t in tenants:
            if r.applies_to(t):
                o = Outcome(r, t, 0, 0, "NOT_EVALUATED", "NONE", json.dumps({"reason": why}))
                out.append(o.row(batch_id, layer, entity))
    return out


def write_log(spark: SparkSession, rows: list[dict]) -> None:
    if rows:
        tables.append(spark, tables.rows_to_df(spark, rows, DQ_SCHEMA), DQ_LOG)
