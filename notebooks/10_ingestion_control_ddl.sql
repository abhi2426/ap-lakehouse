-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 10 — Bronze tables and ingestion control tables
-- MAGIC
-- MAGIC Plain DDL. Creates:
-- MAGIC
-- MAGIC * **11 bronze tables**, one per entity, shared by every tenant
-- MAGIC   (`ap_bronze.ap.br_*`). The record is a `MAP<STRING,STRING>` keyed by the
-- MAGIC   source's own field names, so three ERPs share one table and a new source
-- MAGIC   column needs no DDL. Typing happens in silver.
-- MAGIC * **`ap_control.ops.file_ledger`** — exactly-once bookkeeping, one row per landed file.
-- MAGIC * **`ap_control.ops.schema_contract`** — the versioned contract each file is compared against.
-- MAGIC
-- MAGIC `schema_drift_log` already exists from notebook 01. Safe to re-run: every
-- MAGIC statement is `IF NOT EXISTS`.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ap_bronze.ap COMMENT 'Bronze: raw records as delivered, one table per entity, all tenants.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze tables
-- MAGIC All eleven share one shape. Column meanings, once:
-- MAGIC
-- MAGIC | Column | Meaning |
-- MAGIC |---|---|
-- MAGIC | `record` | every contract field, source name → text as delivered |
-- MAGIC | `record_key` | the configured primary key values joined with `|` |
-- MAGIC | `record_hash` | SHA-256 of the record; equal hash = identical restatement |
-- MAGIC | `cdc_operation` | `I`/`U`/`D` for CDC, `SNAPSHOT` for full snapshots, `UPSERT` for incremental |
-- MAGIC | `cdc_sequence` / `source_watermark` | ordering evidence for silver's "latest wins" |
-- MAGIC | `is_rescued` / `_rescued_data` | fields not in the contract, or text in a NUMBER column, as JSON |
-- MAGIC | `source_file`, `source_file_date`, `file_row_number` | exact lineage to the landed byte range |
-- MAGIC | `ingest_batch_id`, `ingest_ts`, `contract_version` | which run, when, against which contract |

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_bronze.ap.br_vendor_master (
  tenant_id STRING NOT NULL, source_system_id STRING, source_entity STRING, config_id STRING,
  record MAP<STRING,STRING>, record_key STRING, record_hash STRING,
  cdc_operation STRING, cdc_sequence BIGINT, source_watermark STRING,
  is_rescued BOOLEAN, _rescued_data STRING,
  source_file STRING, source_file_date DATE, file_row_number BIGINT,
  load_type STRING, ingest_batch_id STRING, ingest_ts TIMESTAMP, contract_version INT
) CLUSTER BY (tenant_id, source_file_date)
COMMENT 'Bronze vendor master (LFA1 / suppliers / suppliers.csv). Append-only. Grain: one row per record per delivered file.';

CREATE TABLE IF NOT EXISTS ap_bronze.ap.br_vendor_bank (
  tenant_id STRING NOT NULL, source_system_id STRING, source_entity STRING, config_id STRING,
  record MAP<STRING,STRING>, record_key STRING, record_hash STRING,
  cdc_operation STRING, cdc_sequence BIGINT, source_watermark STRING,
  is_rescued BOOLEAN, _rescued_data STRING,
  source_file STRING, source_file_date DATE, file_row_number BIGINT,
  load_type STRING, ingest_batch_id STRING, ingest_ts TIMESTAMP, contract_version INT
) CLUSTER BY (tenant_id, source_file_date)
COMMENT 'Bronze vendor bank accounts. Sensitive: masked from Step 6. Append-only.';

CREATE TABLE IF NOT EXISTS ap_bronze.ap.br_po_header (
  tenant_id STRING NOT NULL, source_system_id STRING, source_entity STRING, config_id STRING,
  record MAP<STRING,STRING>, record_key STRING, record_hash STRING,
  cdc_operation STRING, cdc_sequence BIGINT, source_watermark STRING,
  is_rescued BOOLEAN, _rescued_data STRING,
  source_file STRING, source_file_date DATE, file_row_number BIGINT,
  load_type STRING, ingest_batch_id STRING, ingest_ts TIMESTAMP, contract_version INT
) CLUSTER BY (tenant_id, source_file_date)
COMMENT 'Bronze purchase order headers. Append-only.';

CREATE TABLE IF NOT EXISTS ap_bronze.ap.br_po_line (
  tenant_id STRING NOT NULL, source_system_id STRING, source_entity STRING, config_id STRING,
  record MAP<STRING,STRING>, record_key STRING, record_hash STRING,
  cdc_operation STRING, cdc_sequence BIGINT, source_watermark STRING,
  is_rescued BOOLEAN, _rescued_data STRING,
  source_file STRING, source_file_date DATE, file_row_number BIGINT,
  load_type STRING, ingest_batch_id STRING, ingest_ts TIMESTAMP, contract_version INT
) CLUSTER BY (tenant_id, source_file_date)
COMMENT 'Bronze purchase order lines. Append-only.';

CREATE TABLE IF NOT EXISTS ap_bronze.ap.br_po_history (
  tenant_id STRING NOT NULL, source_system_id STRING, source_entity STRING, config_id STRING,
  record MAP<STRING,STRING>, record_key STRING, record_hash STRING,
  cdc_operation STRING, cdc_sequence BIGINT, source_watermark STRING,
  is_rescued BOOLEAN, _rescued_data STRING,
  source_file STRING, source_file_date DATE, file_row_number BIGINT,
  load_type STRING, ingest_batch_id STRING, ingest_ts TIMESTAMP, contract_version INT
) CLUSTER BY (tenant_id, source_file_date)
COMMENT 'Bronze PO history: goods receipts and invoice receipts. Append-only.';

CREATE TABLE IF NOT EXISTS ap_bronze.ap.br_invoice_header (
  tenant_id STRING NOT NULL, source_system_id STRING, source_entity STRING, config_id STRING,
  record MAP<STRING,STRING>, record_key STRING, record_hash STRING,
  cdc_operation STRING, cdc_sequence BIGINT, source_watermark STRING,
  is_rescued BOOLEAN, _rescued_data STRING,
  source_file STRING, source_file_date DATE, file_row_number BIGINT,
  load_type STRING, ingest_batch_id STRING, ingest_ts TIMESTAMP, contract_version INT
) CLUSTER BY (tenant_id, source_file_date)
COMMENT 'Bronze supplier invoice headers. All three drift events land here. Append-only.';

CREATE TABLE IF NOT EXISTS ap_bronze.ap.br_invoice_line (
  tenant_id STRING NOT NULL, source_system_id STRING, source_entity STRING, config_id STRING,
  record MAP<STRING,STRING>, record_key STRING, record_hash STRING,
  cdc_operation STRING, cdc_sequence BIGINT, source_watermark STRING,
  is_rescued BOOLEAN, _rescued_data STRING,
  source_file STRING, source_file_date DATE, file_row_number BIGINT,
  load_type STRING, ingest_batch_id STRING, ingest_ts TIMESTAMP, contract_version INT
) CLUSTER BY (tenant_id, source_file_date)
COMMENT 'Bronze supplier invoice lines. Append-only.';

CREATE TABLE IF NOT EXISTS ap_bronze.ap.br_invoice_distribution (
  tenant_id STRING NOT NULL, source_system_id STRING, source_entity STRING, config_id STRING,
  record MAP<STRING,STRING>, record_key STRING, record_hash STRING,
  cdc_operation STRING, cdc_sequence BIGINT, source_watermark STRING,
  is_rescued BOOLEAN, _rescued_data STRING,
  source_file STRING, source_file_date DATE, file_row_number BIGINT,
  load_type STRING, ingest_batch_id STRING, ingest_ts TIMESTAMP, contract_version INT
) CLUSTER BY (tenant_id, source_file_date)
COMMENT 'Bronze invoice distributions (accounting grain, TEN002 only, decision D11). Append-only.';

CREATE TABLE IF NOT EXISTS ap_bronze.ap.br_payment_header (
  tenant_id STRING NOT NULL, source_system_id STRING, source_entity STRING, config_id STRING,
  record MAP<STRING,STRING>, record_key STRING, record_hash STRING,
  cdc_operation STRING, cdc_sequence BIGINT, source_watermark STRING,
  is_rescued BOOLEAN, _rescued_data STRING,
  source_file STRING, source_file_date DATE, file_row_number BIGINT,
  load_type STRING, ingest_batch_id STRING, ingest_ts TIMESTAMP, contract_version INT
) CLUSTER BY (tenant_id, source_file_date)
COMMENT 'Bronze payments (REGUH / checks / payments.csv). Append-only.';

CREATE TABLE IF NOT EXISTS ap_bronze.ap.br_payment_application (
  tenant_id STRING NOT NULL, source_system_id STRING, source_entity STRING, config_id STRING,
  record MAP<STRING,STRING>, record_key STRING, record_hash STRING,
  cdc_operation STRING, cdc_sequence BIGINT, source_watermark STRING,
  is_rescued BOOLEAN, _rescued_data STRING,
  source_file STRING, source_file_date DATE, file_row_number BIGINT,
  load_type STRING, ingest_batch_id STRING, ingest_ts TIMESTAMP, contract_version INT
) CLUSTER BY (tenant_id, source_file_date)
COMMENT 'Bronze payment-to-invoice applications: the many-to-many bridge. Append-only.';

CREATE TABLE IF NOT EXISTS ap_bronze.ap.br_approval_event (
  tenant_id STRING NOT NULL, source_system_id STRING, source_entity STRING, config_id STRING,
  record MAP<STRING,STRING>, record_key STRING, record_hash STRING,
  cdc_operation STRING, cdc_sequence BIGINT, source_watermark STRING,
  is_rescued BOOLEAN, _rescued_data STRING,
  source_file STRING, source_file_date DATE, file_row_number BIGINT,
  load_type STRING, ingest_batch_id STRING, ingest_ts TIMESTAMP, contract_version INT
) CLUSTER BY (tenant_id, source_file_date)
COMMENT 'Bronze invoice approval workflow events (TEN001, TEN002; TEN003 has none). Append-only.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion control tables

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ops.file_ledger (
  file_path        STRING NOT NULL COMMENT 'Full volume path. The ledger key: a path is ingested at most once.',
  tenant_id        STRING NOT NULL,
  entity           STRING NOT NULL,
  config_id        STRING          COMMENT 'ingestion_config row that claimed the file.',
  file_name        STRING,
  file_date        DATE            COMMENT 'Extract date parsed from the yyyy/mm/dd folder.',
  file_size        BIGINT,
  file_mtime       BIGINT          COMMENT 'Modification time (epoch ms) when first seen.',
  status           STRING NOT NULL COMMENT 'IN_PROGRESS | INGESTED | POISON. Halted files are absent: they are still pending.',
  batch_id         STRING          COMMENT 'Batch that last touched the file.',
  rows_ingested    BIGINT,
  rows_rescued     BIGINT,
  contract_version INT             COMMENT 'Contract the file was validated against.',
  signature        STRING          COMMENT 'Observed column list, pipe-joined.',
  drift_id         STRING,
  notes            STRING          COMMENT 'Poison reason, or replay note.',
  updated_at       TIMESTAMP
) CLUSTER BY (tenant_id, entity)
COMMENT 'Exactly-once bookkeeping for bronze. Replaces a stream checkpoint when Auto Loader is not used, and makes every file traceable to the batch that loaded it.';

CREATE TABLE IF NOT EXISTS ap_control.ops.schema_contract (
  tenant_id     STRING NOT NULL,
  entity        STRING NOT NULL,
  version       INT    NOT NULL COMMENT 'Starts at 1; +1 each time a steward accepts drift.',
  columns_json  STRING NOT NULL COMMENT 'JSON list of {name, type_class, ordinal}. type_class is NUMBER, STRING or BOOLEAN.',
  column_count  INT,
  source_file   STRING          COMMENT 'File the contract was captured from.',
  registered_at TIMESTAMP,
  registered_by STRING,
  reason        STRING          COMMENT 'BASELINE, or the drift_id that was accepted.',
  is_current    BOOLEAN NOT NULL
) CLUSTER BY (tenant_id, entity)
COMMENT 'Versioned schema contract per tenant x entity. Every landed file is profiled and compared to the current version: that comparison is the drift detector.';

CREATE TABLE IF NOT EXISTS ap_control.ops.autoloader_discovery (
  path   STRING,
  size   BIGINT,
  mtime  TIMESTAMP,
  source STRING
) COMMENT 'Files discovered by Auto Loader (only when use_autoloader=true). Parsing still happens in the engine.';

-- COMMAND ----------

SHOW TABLES IN ap_bronze.ap;
