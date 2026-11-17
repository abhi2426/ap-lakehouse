-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 30 — Gold schemas and the model catalogue
-- MAGIC
-- MAGIC Gold tables (13 dimensions, 13 facts, 4 aggregates) are created by their
-- MAGIC builders on first write, clustered by `transformation_config.cluster_columns`.
-- MAGIC What this notebook creates is the **model catalogue**: one row per gold
-- MAGIC table stating its grain, key and history behaviour. Keep it open while you
-- MAGIC read `src/ap_platform/gold/`. It is the appendix, as data you can query.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ap_gold.ap COMMENT 'Gold: dimensional model for AP. All tenants, row-filtered by tenant from Step 6.';
CREATE SCHEMA IF NOT EXISTS ap_gold.semantic COMMENT 'Gold: KPI views (the semantic layer) and the model catalogue.';

-- COMMAND ----------

CREATE OR REPLACE TABLE ap_gold.semantic.model_catalogue (
  table_name STRING, kind STRING, grain STRING, business_key STRING, surrogate_key STRING,
  history STRING, built_by STRING, notes STRING
) COMMENT 'One row per gold table: grain, keys, history behaviour, and the builder that writes it.';

INSERT INTO ap_gold.semantic.model_catalogue VALUES
('dim_date','DIMENSION','calendar day 2020-2030','calendar_date','date_key (yyyymmdd)','static','dimensions.dim_date',NULL),
('dim_fiscal_calendar','DIMENSION','tenant x day','tenant_id, date_key',NULL,'static','dimensions.dim_fiscal_calendar','CALENDAR, JUL_JUN, simplified 4-4-5'),
('dim_tenant','DIMENSION','tenant','tenant_id',NULL,'SCD1','dimensions.dim_tenant','has_approval_stream drives the touchless caveat'),
('dim_company','DIMENSION','tenant x company code','tenant_id, company_code','company_key','SCD1','dimensions.dim_company',NULL),
('dim_vendor','DIMENSION','vendor version','tenant_id, vendor_id, valid_from','vendor_key','SCD2 (from silver)','dimensions.dim_vendor','no tax id or VAT in gold'),
('dim_vendor_bank','DIMENSION','bank account version','tenant, vendor, bank key, valid_from','vendor_bank_key','SCD2 (from silver)','dimensions.dim_vendor_bank','account masked to last 4'),
('dim_currency','DIMENSION','currency','currency_code',NULL,'SCD1','dimensions.dim_currency',NULL),
('dim_payment_terms','DIMENSION','canonical terms code','payment_terms_code',NULL,'SCD1','dimensions.dim_payment_terms',NULL),
('dim_gl_account','DIMENSION','tenant x GL account','tenant_id, gl_account','gl_account_key','SCD1','dimensions.dim_gl_account',NULL),
('dim_cost_center','DIMENSION','tenant x cost centre','tenant_id, cost_center','cost_center_key','SCD1','dimensions.dim_cost_center','derived from silver: no cost-centre master is ingested'),
('dim_doc_type','DIMENSION','canonical document type','doc_type_code',NULL,'SCD1','dimensions.dim_doc_type',NULL),
('dim_capture_channel','DIMENSION','canonical capture channel','capture_channel_code',NULL,'SCD1','dimensions.dim_capture_channel',NULL),
('dim_dq_rule','DIMENSION','DQ rule','rule_id',NULL,'SCD1','dimensions.dim_dq_rule',NULL),
('fact_ap_invoice_header','FACT','invoice','tenant, company, doc, fiscal year','invoice_key','transaction, MERGE','facts.fact_invoice_header','vendor_key resolved as at invoice_date'),
('fact_ap_invoice_line','FACT','invoice line','header key + line_number',NULL,'transaction, MERGE','facts.fact_invoice_line','reporting grain; distributions stay in silver (D11)'),
('fact_ap_payment','FACT','payment document','tenant, payment doc, fiscal year','payment_key','transaction, MERGE','facts.fact_payment',NULL),
('fact_ap_payment_application','FACT','payment x invoice application','see transformation_config','application_key','transaction, MERGE','facts.fact_payment_application','the many-to-many bridge: partial payments live here'),
('fact_ap_open_item_snapshot','FACT','open item x snapshot date','tenant, snapshot_date, company, doc, FY, line',NULL,'periodic snapshot','facts.fact_open_item_snapshot','recomputed as of the date, never copied from current state'),
('fact_grir_snapshot','FACT','PO line x snapshot date','tenant, snapshot_date, PO, line',NULL,'periodic snapshot','facts.fact_grir_snapshot',NULL),
('fact_invoice_lifecycle','FACT','invoice','header key','invoice_key','accumulating snapshot','facts.fact_invoice_lifecycle','approval_evidence = NONE means touchless is unknowable'),
('fact_approval_event','FACT','approval event','header key + event_seq',NULL,'transaction','facts.fact_approval_event',NULL),
('fact_duplicate_candidate','FACT','scored invoice pair','tenant, candidate_pair_id',NULL,'MERGE; dispositions preserved','facts.fact_duplicate_candidate',NULL),
('fact_vendor_change_event','FACT','vendor or bank change','tenant, vendor, change_type, valid_from',NULL,'transaction','facts.fact_vendor_change_event','is_fraud_signal: unverified bank change then payment within 14 days'),
('fact_match_exception','FACT','invoice line not cleanly matched','line key',NULL,'rebuilt each run','facts.fact_match_exception',NULL),
('fact_dq_result','FACT','rule x tenant x entity x batch','batch, rule, tenant, entity',NULL,'rebuilt each run','facts.fact_dq_result',NULL),
('fact_reconciliation','FACT','silver table x tenant','tenant, gold_table',NULL,'latest check','facts.fact_reconciliation','history in ap_control.ops.reconciliation_control'),
('agg_ap_aging_daily','AGGREGATE','tenant x date x company x currency x bucket','all columns',NULL,'snapshot','aggregates.agg_ap_aging_daily',NULL),
('agg_dpo_monthly','AGGREGATE','tenant x month','tenant, year_month',NULL,'rebuilt','aggregates.agg_dpo_monthly',NULL),
('agg_vendor_spend_monthly','AGGREGATE','tenant x vendor x month','tenant, vendor, year_month',NULL,'rebuilt','aggregates.agg_vendor_spend_monthly',NULL),
('agg_dq_scorecard_daily','AGGREGATE','tenant x layer x entity x day','all columns',NULL,'rebuilt','aggregates.agg_dq_scorecard_daily',NULL);

-- COMMAND ----------

SELECT kind, count(*) AS tables FROM ap_gold.semantic.model_catalogue GROUP BY kind ORDER BY kind;
