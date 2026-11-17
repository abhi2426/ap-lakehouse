-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 20 — Silver schemas, watermark table, quarantine schema
-- MAGIC
-- MAGIC Silver **tables** are created by the writer on first write, shaped by
-- MAGIC the conformed DataFrame and clustered by `transformation_config.cluster_columns`.
-- MAGIC Declaring 16 wide tables by hand here would duplicate `schema_mapping`
-- MAGIC and drift from it the first time a mapping changes. What this notebook
-- MAGIC creates is what the writer does not: the schemas and the control tables.
-- MAGIC
-- MAGIC `quarantine_registry` and `reconciliation_control` already exist from notebook 01.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ap_silver.ap COMMENT 'Silver: conformed, typed, deduplicated, DQ-gated. One table per entity, all tenants.';
CREATE SCHEMA IF NOT EXISTS ap_silver.quarantine COMMENT 'Rows diverted by the DQ gate, one table per entity (q_<entity>), with the rule ids that failed.';
CREATE SCHEMA IF NOT EXISTS ap_silver.staging COMMENT 'Scratch: each job pins its DQ verdicts here before writing, so the rows written and the rows quarantined come from one evaluation. Overwritten every run; never read by anything else.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ops.silver_watermark (
  job_id        STRING NOT NULL COMMENT 'transformation_config.job_id.',
  tenant_id     STRING NOT NULL,
  source_table  STRING          COMMENT 'Bronze table the watermark refers to.',
  watermark_ts  TIMESTAMP       COMMENT 'Highest bronze ingest_ts processed. The next run reads ingest_ts > this.',
  batch_id      STRING          COMMENT 'Batch that advanced it.',
  updated_at    TIMESTAMP
) COMMENT 'Per job, per tenant high watermark into bronze. full_reload=true on notebook 21 ignores it; nothing else should.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appendix — every silver table's write mode and keys
-- MAGIC Read this once. It is the same information as `transformation_config`,
-- MAGIC with the reason for each choice.
-- MAGIC
-- MAGIC | Job | Table | Write mode | Merge keys | Why |
-- MAGIC |---|---|---|---|---|
-- MAGIC | SL_VENDOR | sl_vendor | SCD2_MERGE | tenant_id, vendor_id | payment terms and block status are needed as at invoice date (D5) |
-- MAGIC | SL_VENDOR_BANK | sl_vendor_bank | SCD2_MERGE | tenant, vendor, bank country/key/account | bank change history is the fraud evidence |
-- MAGIC | SL_VENDOR_CHANGE_EVENT | sl_vendor_change_event | MERGE | tenant, vendor, change_type, valid_from | one row per change, derived from the SCD2 history |
-- MAGIC | SL_PO_HEADER / SL_PO_LINE | sl_po_* | MERGE | PO number (+ line) | latest state is all matching needs |
-- MAGIC | SL_GOODS_RECEIPT | sl_goods_receipt | MERGE | tenant, receipt doc, receipt line | receipts are immutable events |
-- MAGIC | SL_INVOICE_HEADER | sl_invoice_header | MERGE | tenant, company, doc, **fiscal year** | SAP doc numbers restart each fiscal year (trap 3) |
-- MAGIC | SL_INVOICE_LINE | sl_invoice_line | MERGE | header key + line | reporting grain |
-- MAGIC | SL_INVOICE_DISTRIBUTION | sl_invoice_distribution | MERGE | line key + distribution | accounting grain, TEN002 only (D11) |
-- MAGIC | SL_APPROVAL_EVENT | sl_approval_event | MERGE | header key + event_seq | workflow events |
-- MAGIC | SL_PAYMENT / _APPLICATION | sl_payment* | MERGE | see config | application is the many-to-many bridge |
-- MAGIC | SL_THREE_WAY_MATCH | sl_three_way_match | MERGE | invoice line key | recomputed each run from silver |
-- MAGIC | SL_DUPLICATE_CANDIDATE | sl_duplicate_candidate | MERGE | candidate_pair_id | stable pair hash, so dispositions survive re-scoring |
-- MAGIC | SL_GRIR_POSITION | sl_grir_position | OVERWRITE | — | a position, not history: always as of now |
-- MAGIC | SL_AP_ITEM | sl_ap_item | MERGE | company, doc, fiscal year, line item | the open-item sub-ledger |

-- COMMAND ----------

SHOW SCHEMAS IN ap_silver;
