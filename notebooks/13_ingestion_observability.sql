-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 13 — Ingestion observability
-- MAGIC
-- MAGIC Five views over the run logs, then the queries that read them. Read-only
-- MAGIC and fast. This is the first place to look when something did not load.
-- MAGIC
-- MAGIC | View | Answers |
-- MAGIC |---|---|
-- MAGIC | `vw_bronze_source_status` | for every source: last batch, last status, files pending, halted or not |
-- MAGIC | `vw_bronze_file_ledger` | every file, its status and row count, newest first |
-- MAGIC | `vw_bronze_open_drift` | drift that is still halting a source |
-- MAGIC | `vw_bronze_rescue_rate` | rescued rows per source, the "is the contract still right" signal |
-- MAGIC | `vw_bronze_freshness` | latest extract date per source against its SLA tier |

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ap_control.observability COMMENT 'Read-only views over the control plane run logs.';

-- COMMAND ----------

CREATE OR REPLACE VIEW ap_control.observability.vw_bronze_file_ledger AS
SELECT tenant_id, entity, file_name, file_date, status, rows_ingested, rows_rescued,
       contract_version, batch_id, notes, updated_at
FROM ap_control.ops.file_ledger;

CREATE OR REPLACE VIEW ap_control.observability.vw_bronze_open_drift AS
SELECT tenant_id, entity, drift_type, classification, column_name, expected_type, observed_type,
       sample_file, detected_at, drift_id
FROM ap_control.ops.schema_drift_log
WHERE status = 'OPEN';

CREATE OR REPLACE VIEW ap_control.observability.vw_bronze_rescue_rate AS
SELECT tenant_id, entity,
       sum(rows_ingested) AS rows_ingested,
       sum(coalesce(rows_rescued, 0)) AS rows_rescued,
       round(100.0 * sum(coalesce(rows_rescued, 0)) / nullif(sum(rows_ingested), 0), 2) AS rescue_pct
FROM ap_control.ops.file_ledger
WHERE status = 'INGESTED'
GROUP BY tenant_id, entity;

CREATE OR REPLACE VIEW ap_control.observability.vw_bronze_source_status AS
WITH last_task AS (
  SELECT t.tenant_id, t.entity, t.status, t.error_message, t.batch_id, t.end_ts,
         row_number() OVER (PARTITION BY t.tenant_id, t.entity ORDER BY t.end_ts DESC) AS rn
  FROM ap_control.ops.task_audit_log t
  JOIN ap_control.ops.batch_run_log b ON b.batch_id = t.batch_id AND b.layer = 'BRONZE'
),
ledger AS (
  SELECT tenant_id, entity, count(*) AS files_ingested, max(file_date) AS latest_file_date,
         sum(rows_ingested) AS rows_ingested
  FROM ap_control.ops.file_ledger WHERE status = 'INGESTED' GROUP BY tenant_id, entity
),
drift AS (
  SELECT tenant_id, entity, count(*) AS open_breaking
  FROM ap_control.ops.schema_drift_log
  WHERE status = 'OPEN' AND classification = 'BREAKING' GROUP BY tenant_id, entity
)
SELECT i.tenant_id, i.entity, i.load_type,
       l.status AS last_status, l.end_ts AS last_run_at, l.error_message AS last_note,
       coalesce(g.files_ingested, 0) AS files_ingested, g.latest_file_date, coalesce(g.rows_ingested, 0) AS rows_ingested,
       coalesce(d.open_breaking, 0) > 0 AS is_halted
FROM ap_control.config.ingestion_config i
LEFT JOIN last_task l ON l.tenant_id = i.tenant_id AND l.entity = i.entity AND l.rn = 1
LEFT JOIN ledger g ON g.tenant_id = i.tenant_id AND g.entity = i.entity
LEFT JOIN drift d ON d.tenant_id = i.tenant_id AND d.entity = i.entity
WHERE i.is_active;

CREATE OR REPLACE VIEW ap_control.observability.vw_bronze_freshness AS
SELECT s.tenant_id, s.entity, s.latest_file_date, t.sla_tier, c.freshness_hours,
       datediff(current_date(), s.latest_file_date) * 24 AS age_hours,
       datediff(current_date(), s.latest_file_date) * 24 > c.freshness_hours AS is_stale
FROM ap_control.observability.vw_bronze_source_status s
JOIN ap_control.config.tenant_registry t ON t.tenant_id = s.tenant_id
JOIN ap_control.config.sla_config c ON c.sla_tier = t.sla_tier AND c.layer = 'BRONZE';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The queries
-- MAGIC **1. Is anything halted?** Two rows on a fresh run (TEN002 and TEN003
-- MAGIC `invoice_header`). Zero after you complete exercise E2.

-- COMMAND ----------

SELECT tenant_id, entity, last_status, files_ingested, latest_file_date, last_note
FROM ap_control.observability.vw_bronze_source_status
WHERE is_halted OR last_status NOT IN ('SUCCEEDED', 'NO_NEW_DATA')
ORDER BY tenant_id, entity;

-- COMMAND ----------

-- MAGIC %md **2. What drift is open, and on which file did it start?**

-- COMMAND ----------

SELECT * FROM ap_control.observability.vw_bronze_open_drift ORDER BY detected_at;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **3. Rescue rate.** A source whose rescue rate jumps has a contract that
-- MAGIC no longer describes it, even if nothing halted. TEN001 `invoice_header`
-- MAGIC is high on purpose: the appended ZUONR column is rescued on every row from day 8.

-- COMMAND ----------

SELECT * FROM ap_control.observability.vw_bronze_rescue_rate WHERE rows_rescued > 0 ORDER BY rescue_pct DESC;

-- COMMAND ----------

-- MAGIC %md **4. Batches, newest first, with what each one did.**

-- COMMAND ----------

SELECT batch_id, status, run_mode, start_ts, round(duration_seconds, 1) AS seconds,
       records_read, files_processed, left(error_message, 160) AS error_message
FROM ap_control.ops.batch_run_log
WHERE layer = 'BRONZE'
ORDER BY start_ts DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md **5. Poison files and bronze DQ outcomes that were not a clean PASS.**

-- COMMAND ----------

SELECT tenant_id, entity, file_name, notes FROM ap_control.ops.file_ledger WHERE status = 'POISON';

-- COMMAND ----------

SELECT tenant_id, entity, rule_id, outcome, records_failed, records_evaluated, failed_sample
FROM ap_control.ops.dq_result_log
WHERE layer = 'BRONZE' AND outcome <> 'PASS'
ORDER BY tenant_id, entity, rule_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `DQ_ALL_0601` (file age) WARNs on every source here, correctly: the
-- MAGIC synthetic extracts are dated July 2026, so they are all older than
-- MAGIC their SLA by the time you run this. It is a WARN, so nothing is blocked.
