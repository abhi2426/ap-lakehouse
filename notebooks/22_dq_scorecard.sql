-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 22 — DQ scorecard
-- MAGIC
-- MAGIC Creates the DQ observability views and runs them. **Run after every 21.**
-- MAGIC
-- MAGIC **Read `rules_evaluated` before the pass rate.** A batch that evaluated
-- MAGIC 14 rules and passed them all is healthy. A batch that evaluated 4 is one
-- MAGIC where 10 rules could not resolve their columns, and its 100% means nothing.
-- MAGIC Rules that could not be evaluated are logged with outcome `NOT_EVALUATED`
-- MAGIC and the reason, never silently counted as a pass.
-- MAGIC
-- MAGIC **Section 5 is the one query to watch:** zero rows when silver is
-- MAGIC healthy, one row per problem when it is not.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ap_control.observability;

-- COMMAND ----------

-- Latest silver batch per entity and tenant: what the scorecard is "as of".
CREATE OR REPLACE VIEW ap_control.observability.vw_dq_latest AS
WITH b AS (
  SELECT entity, tenant_id, batch_id, max(evaluated_at) AS at
  FROM ap_control.ops.dq_result_log WHERE layer = 'SILVER'
  GROUP BY entity, tenant_id, batch_id
),
lb AS (SELECT entity, tenant_id, max_by(batch_id, at) AS batch_id FROM b GROUP BY entity, tenant_id)
SELECT d.*
FROM ap_control.ops.dq_result_log d
JOIN lb ON lb.entity = d.entity AND lb.tenant_id = d.tenant_id AND lb.batch_id = d.batch_id
WHERE d.layer = 'SILVER';

CREATE OR REPLACE VIEW ap_control.observability.vw_dq_scorecard AS
SELECT tenant_id, entity,
       count(DISTINCT CASE WHEN outcome <> 'NOT_EVALUATED' THEN rule_id END) AS rules_evaluated,
       count(DISTINCT CASE WHEN outcome = 'NOT_EVALUATED' THEN rule_id END)  AS rules_not_evaluated,
       count(DISTINCT CASE WHEN outcome IN ('PASS', 'PASS_WITHIN_THRESHOLD') THEN rule_id END) AS rules_passed,
       count(DISTINCT CASE WHEN outcome IN ('QUARANTINED', 'THRESHOLD_BREACH') THEN rule_id END) AS rules_quarantining,
       count(DISTINCT CASE WHEN outcome = 'WARN' THEN rule_id END) AS rules_warning,
       count(DISTINCT CASE WHEN outcome = 'FAIL' THEN rule_id END) AS rules_failed,
       max(records_evaluated) AS records_evaluated,
       round(100.0 * count(DISTINCT CASE WHEN outcome IN ('PASS', 'PASS_WITHIN_THRESHOLD') THEN rule_id END)
             / nullif(count(DISTINCT CASE WHEN outcome <> 'NOT_EVALUATED' THEN rule_id END), 0), 1) AS rule_pass_pct
FROM ap_control.observability.vw_dq_latest
GROUP BY tenant_id, entity;

CREATE OR REPLACE VIEW ap_control.observability.vw_dq_by_dimension AS
SELECT tenant_id, dq_dimension,
       sum(records_failed) AS records_failed, sum(records_evaluated) AS records_evaluated,
       round(100.0 * (1 - sum(records_failed) / nullif(sum(records_evaluated), 0)), 3) AS pass_pct
FROM ap_control.observability.vw_dq_latest
WHERE outcome <> 'NOT_EVALUATED'
GROUP BY tenant_id, dq_dimension;

CREATE OR REPLACE VIEW ap_control.observability.vw_quarantine_backlog AS
SELECT tenant_id, entity, rule_ids, status, count(*) AS rows,
       min(quarantined_at) AS oldest, max(quarantined_at) AS newest
FROM ap_control.ops.quarantine_registry
GROUP BY tenant_id, entity, rule_ids, status;

-- Planted defects vs what the gate caught: the grading query.
CREATE OR REPLACE VIEW ap_control.observability.vw_defect_detection AS
WITH caught AS (
  SELECT DISTINCT tenant_id, entity, explode(split(rule_ids, ',')) AS rule_id FROM ap_control.ops.quarantine_registry
  UNION
  SELECT DISTINCT tenant_id, entity, rule_id FROM ap_control.ops.dq_result_log
  WHERE (records_failed > 0 AND outcome <> 'NOT_EVALUATED') OR action_taken = 'EXACT_DUPLICATES_COLLAPSED'
)
SELECT l.defect_code, l.expected_rule, l.expected_severity, l.tenant_id, count(*) AS injected,
       max(CASE WHEN c.rule_id IS NOT NULL THEN 1 ELSE 0 END) = 1 AS rule_fired
FROM ap_control.ops.synthetic_defect_ledger l
LEFT JOIN caught c ON c.tenant_id = l.tenant_id AND c.rule_id = l.expected_rule
WHERE l.expected_rule IS NOT NULL
GROUP BY l.defect_code, l.expected_rule, l.expected_severity, l.tenant_id;

-- COMMAND ----------

-- MAGIC %md ## 1. Scorecard — rules_evaluated first

-- COMMAND ----------

SELECT * FROM ap_control.observability.vw_dq_scorecard ORDER BY entity, tenant_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. What could not be evaluated, and why
-- MAGIC Expected on a first run: `DQ_INV_LIN_0403` (tax consistency) — no mapping
-- MAGIC produces a line-level `tax_amount` yet. Adding one to `schema_mapping` is
-- MAGIC all it takes; the rule then evaluates with no code change.

-- COMMAND ----------

SELECT entity, rule_id, tenant_id, get_json_object(failed_sample, '$.reason') AS reason
FROM ap_control.observability.vw_dq_latest WHERE outcome = 'NOT_EVALUATED'
ORDER BY entity, rule_id, tenant_id;

-- COMMAND ----------

-- MAGIC %md ## 3. Failures by rule (latest batch)

-- COMMAND ----------

SELECT entity, rule_id, tenant_id, severity, outcome, records_failed, records_evaluated, pass_rate_pct, threshold_pct
FROM ap_control.observability.vw_dq_latest
WHERE records_failed > 0
ORDER BY entity, rule_id, tenant_id;

-- COMMAND ----------

-- MAGIC %md ## 4. Quarantine backlog and planted-defect detection

-- COMMAND ----------

SELECT * FROM ap_control.observability.vw_quarantine_backlog WHERE status = 'OPEN' ORDER BY rows DESC;

-- COMMAND ----------

SELECT defect_code, expected_rule, sum(injected) AS injected, collect_set(tenant_id) AS tenants,
       bool_and(rule_fired) AS fired_for_every_tenant
FROM ap_control.observability.vw_defect_detection
GROUP BY defect_code, expected_rule ORDER BY fired_for_every_tenant, defect_code;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. The alert query — zero rows when silver is healthy
-- MAGIC One row per problem: an aborted slice, a threshold breach, an ABORT-rule
-- MAGIC failure, a reconciliation variance, a failed or skipped silver job.
-- MAGIC On a first run expect `THRESHOLD_BREACH` rows: the planted defect rates
-- MAGIC exceed several thresholds by design, and the PCE quarantine (E1) shows
-- MAGIC up here for TEN003. Every row here should be explainable.

-- COMMAND ----------

SELECT 'DQ' AS source, tenant_id, entity, rule_id AS item, outcome AS problem,
       concat(records_failed, ' of ', records_evaluated, ' failed; threshold ', coalesce(cast(threshold_pct AS STRING), 'none')) AS detail
FROM ap_control.observability.vw_dq_latest
WHERE outcome IN ('FAIL', 'THRESHOLD_BREACH')
UNION ALL
SELECT 'RECON', tenant_id, entity, recon_type, status, explanation
FROM (SELECT *, row_number() OVER (PARTITION BY tenant_id, entity, recon_type ORDER BY checked_at DESC) rn
      FROM ap_control.ops.reconciliation_control) r
WHERE rn = 1 AND status NOT IN ('MATCHED')
UNION ALL
SELECT 'JOB', t.tenant_id, t.entity, t.task_id, t.status, t.error_message
FROM ap_control.ops.task_audit_log t
WHERE t.batch_id = (SELECT max_by(batch_id, start_ts) FROM ap_control.ops.batch_run_log
                    WHERE layer = 'SILVER' AND run_mode NOT LIKE '%DRY_RUN%')
  AND t.status NOT IN ('SUCCEEDED', 'NO_NEW_DATA')
ORDER BY source, tenant_id, entity;
