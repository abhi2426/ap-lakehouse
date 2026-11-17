-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 33 — Dashboards: every tile as named SQL
-- MAGIC
-- MAGIC Five dashboards. Each tile is one SQL cell named `D.T — title`. They read
-- MAGIC the semantic layer (notebook 32) and gold, nothing lower.
-- MAGIC
-- MAGIC **Parameters** are SQL session variables, declared once below:
-- MAGIC `tenant` (`'ALL'` or a tenant id) and `months` (look-back for trends).
-- MAGIC Change the `DEFAULT` and re-run. This works as-is in a notebook on Free
-- MAGIC Edition; when you build a Lakeview dashboard, replace each variable with a
-- MAGIC dashboard parameter of the same name.
-- MAGIC
-- MAGIC **Start with tile 1.4** — the executive summary strip. It exercises most
-- MAGIC of the semantic layer in one query, so it is a good smoke test.
-- MAGIC
-- MAGIC **Dashboard 5 does not isolate anything yet.** Its queries deliberately carry
-- MAGIC no tenant filter, because isolation belongs in the governance layer, not in
-- MAGIC the query. Until Step 6 applies row filters, tile 5.2 reports three tenants
-- MAGIC visible. That is the expected reading today; exercise E2 of Step 5 turns it
-- MAGIC into a real test.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE tenant STRING DEFAULT 'ALL';
DECLARE OR REPLACE VARIABLE months INT DEFAULT 24;

-- COMMAND ----------

-- MAGIC %md ## Dashboard 1 — Executive

-- COMMAND ----------

-- 1.1 — Open AP and past due, by tenant
SELECT tenant_id, snapshot_date, open_items, open_ap_group, past_due_group, past_due_pct
FROM ap_gold.semantic.vw_kpi_ap_balance
WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id;

-- COMMAND ----------

-- 1.2 — Aging profile
SELECT tenant_id, aging_bucket, open_items, open_amount_group, share_pct
FROM ap_gold.semantic.vw_kpi_aging
WHERE tenant = 'ALL' OR tenant_id = tenant
ORDER BY tenant_id, CASE aging_bucket WHEN 'CURRENT' THEN 0 WHEN '1-30' THEN 1 WHEN '31-60' THEN 2 WHEN '61-90' THEN 3 ELSE 4 END;

-- COMMAND ----------

-- 1.3 — Monthly spend trend
SELECT tenant_id, year_month, invoices, spend_group
FROM ap_gold.semantic.vw_kpi_spend_monthly
WHERE (tenant = 'ALL' OR tenant_id = tenant)
  AND year_month >= date_format(add_months((SELECT max(snapshot_date) FROM ap_gold.semantic.vw_latest_snapshot), -months), 'yyyy-MM')
ORDER BY tenant_id, year_month;

-- COMMAND ----------

-- 1.4 — Executive summary strip: five numbers plus a data-trust indicator
SELECT tenant_id, snapshot_date,
       open_ap_group        AS open_ap,
       past_due_pct         AS past_due_pct,
       dpo_days             AS dpo,
       on_time_pct          AS paid_on_time_pct,
       first_pass_match_pct AS first_pass_match_pct,
       trust_level          AS data_trust
FROM ap_gold.semantic.vw_executive_summary
WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id;

-- COMMAND ----------

-- MAGIC %md ## Dashboard 2 — Working capital

-- COMMAND ----------

-- 2.1 — DPO
SELECT * FROM ap_gold.semantic.vw_kpi_dpo WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id;

-- COMMAND ----------

-- 2.2 — Payment timeliness
SELECT * FROM ap_gold.semantic.vw_kpi_payment_timeliness WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id;

-- COMMAND ----------

-- 2.3 — Discount capture
SELECT * FROM ap_gold.semantic.vw_kpi_discount_capture WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id;

-- COMMAND ----------

-- 2.4 — Days to pay by month (from the DPO aggregate)
SELECT tenant_id, year_month, applications, round(weighted_days_to_pay, 1) AS weighted_days_to_pay,
       round(100 * on_time_rate, 1) AS on_time_pct
FROM ap_gold.ap.agg_dpo_monthly
WHERE (tenant = 'ALL' OR tenant_id = tenant) AND applications IS NOT NULL
ORDER BY tenant_id, year_month;

-- COMMAND ----------

-- MAGIC %md ## Dashboard 3 — Operations

-- COMMAND ----------

-- 3.1 — Cycle time
SELECT * FROM ap_gold.semantic.vw_kpi_cycle_time WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id;

-- COMMAND ----------

-- 3.2 — Touchless rate, labelled where it cannot be measured
SELECT tenant_id, invoices,
       CASE WHEN approval_evidence = 'NONE' THEN NULL ELSE touchless_pct END AS touchless_pct,
       coalesce(caveat, '') AS note
FROM ap_gold.semantic.vw_kpi_touchless_rate
WHERE tenant = 'ALL' OR tenant_id = tenant
ORDER BY CASE WHEN approval_evidence = 'NONE' THEN 1 ELSE 0 END, touchless_pct DESC;

-- COMMAND ----------

-- 3.3 — Capture channel mix
SELECT * FROM ap_gold.semantic.vw_kpi_capture_channel_mix WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id, share_pct DESC;

-- COMMAND ----------

-- 3.4 — Blocked invoices
SELECT * FROM ap_gold.semantic.vw_kpi_blocked WHERE (tenant = 'ALL' OR tenant_id = tenant) AND block_reason <> 'NONE' ORDER BY tenant_id, invoices DESC;

-- COMMAND ----------

-- MAGIC %md ## Dashboard 4 — Controls and procurement

-- COMMAND ----------

-- 4.1 — Three-way match
SELECT * FROM ap_gold.semantic.vw_kpi_match_rate WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id;

-- COMMAND ----------

-- 4.2 — Duplicate exposure
SELECT * FROM ap_gold.semantic.vw_kpi_duplicate_exposure WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id, score_band;

-- COMMAND ----------

-- 4.3 — Bank-change fraud signals, with the events behind them
SELECT * FROM ap_gold.semantic.vw_kpi_fraud_signals WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id;

-- COMMAND ----------

SELECT tenant_id, vendor_id, change_date, changed_by, is_verified, bank_account_last4, next_payment_date, days_to_next_payment
FROM ap_gold.ap.fact_vendor_change_event
WHERE is_fraud_signal AND (tenant = 'ALL' OR tenant_id = tenant) ORDER BY tenant_id, change_date;

-- COMMAND ----------

-- 4.4 — PO coverage and GR/IR
SELECT * FROM ap_gold.semantic.vw_kpi_po_coverage WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id;

-- COMMAND ----------

SELECT * FROM ap_gold.semantic.vw_kpi_grir WHERE tenant = 'ALL' OR tenant_id = tenant ORDER BY tenant_id, grir_status;

-- COMMAND ----------

-- MAGIC %md ## Dashboard 5 — Platform and isolation

-- COMMAND ----------

-- 5.1 — Data trust
SELECT * FROM ap_gold.semantic.vw_kpi_data_trust ORDER BY tenant_id;

-- COMMAND ----------

-- 5.2 — Tenants visible to the current user. Expect 3 until Step 6 row filters.
SELECT current_user() AS user_name, count(DISTINCT tenant_id) AS tenants_visible,
       array_sort(collect_set(tenant_id)) AS tenant_ids
FROM ap_gold.ap.fact_ap_invoice_header;

-- COMMAND ----------

-- 5.3 — Freshness: latest extract loaded per tenant
SELECT tenant_id, max(file_date) AS latest_extract, count(*) AS files_ingested
FROM ap_control.ops.file_ledger WHERE status = 'INGESTED' GROUP BY tenant_id ORDER BY tenant_id;

-- COMMAND ----------

-- 5.4 — Silver-to-gold reconciliation
SELECT tenant_id, silver_table, gold_table, silver_count, gold_count, count_variance, amount_variance, status
FROM ap_gold.ap.fact_reconciliation ORDER BY tenant_id, gold_table;
