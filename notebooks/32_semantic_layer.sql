-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 32 — Semantic layer: the KPI dictionary as views
-- MAGIC
-- MAGIC Pure DDL over gold. Fast. Run once after notebook 31, and again after any
-- MAGIC metric change. **Start with `vw_metric_catalogue`**: it publishes the
-- MAGIC dictionary as data — every metric, the view that serves it, owner, target,
-- MAGIC and for the seven that are deliberately NOT implemented, why.
-- MAGIC
-- MAGIC A view returning a plausible number from data that cannot support it is
-- MAGIC worse than a documented gap.
-- MAGIC
-- MAGIC Conventions: amounts in group currency (`*_group`), as of the latest
-- MAGIC snapshot date. No view filters by tenant: isolation belongs in the
-- MAGIC governance layer (row filters, Step 6), not in every query.
-- MAGIC
-- MAGIC **One result that looks wrong and is not:** touchless rate is 100% for
-- MAGIC TEN003. TEN003 supplies no approval stream, so `touch_count` is always
-- MAGIC zero. `vw_kpi_touchless_rate.approval_evidence = 'NONE'` says so, and the
-- MAGIC operations dashboard labels it rather than ranking TEN003 first.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ap_gold.semantic;

-- COMMAND ----------

CREATE OR REPLACE VIEW ap_gold.semantic.vw_latest_snapshot AS
SELECT tenant_id, max(snapshot_date) AS snapshot_date
FROM ap_gold.ap.fact_ap_open_item_snapshot GROUP BY tenant_id;

-- 1. AP balance
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_ap_balance AS
SELECT s.tenant_id, s.snapshot_date, count(*) AS open_items,
       round(sum(s.open_amount_group), 2) AS open_ap_group,
       round(sum(CASE WHEN s.days_past_due > 0 THEN s.open_amount_group END), 2) AS past_due_group,
       round(100.0 * sum(CASE WHEN s.days_past_due > 0 THEN s.open_amount_group END) / nullif(sum(s.open_amount_group), 0), 2) AS past_due_pct,
       sum(CASE WHEN s.open_amount_group_fx_missing THEN 1 ELSE 0 END) AS items_missing_fx
FROM ap_gold.ap.fact_ap_open_item_snapshot s
JOIN ap_gold.semantic.vw_latest_snapshot l USING (tenant_id, snapshot_date)
GROUP BY s.tenant_id, s.snapshot_date;

-- 2. Aging profile
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_aging AS
SELECT a.tenant_id, a.snapshot_date, a.aging_bucket,
       sum(a.open_items) AS open_items, round(sum(a.open_amount_group), 2) AS open_amount_group,
       round(100.0 * sum(a.open_amount_group) / sum(sum(a.open_amount_group)) OVER (PARTITION BY a.tenant_id), 2) AS share_pct
FROM ap_gold.ap.agg_ap_aging_daily a
JOIN ap_gold.semantic.vw_latest_snapshot l USING (tenant_id, snapshot_date)
GROUP BY a.tenant_id, a.snapshot_date, a.aging_bucket;

-- 3. DPO (count-back on the last 90 days of posted spend)
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_dpo AS
SELECT b.tenant_id, b.snapshot_date, b.open_ap_group,
       round(sum(i.gross_amount_group), 2) AS spend_90d_group,
       round(b.open_ap_group / nullif(sum(i.gross_amount_group), 0) * 90, 1) AS dpo_days
FROM ap_gold.semantic.vw_kpi_ap_balance b
JOIN ap_gold.ap.fact_ap_invoice_header i
  ON i.tenant_id = b.tenant_id AND i.posting_date > date_sub(b.snapshot_date, 90) AND i.posting_date <= b.snapshot_date
GROUP BY b.tenant_id, b.snapshot_date, b.open_ap_group;

-- 4. Days to pay and on-time / early payment
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_payment_timeliness AS
SELECT tenant_id, count(*) AS applications,
       round(sum(days_to_pay * abs(applied_amount)) / sum(abs(applied_amount)), 1) AS weighted_days_to_pay,
       round(100.0 * avg(CASE WHEN paid_on_time THEN 1.0 ELSE 0.0 END), 2) AS on_time_pct,
       round(100.0 * avg(CASE WHEN days_after_due < -5 THEN 1.0 ELSE 0.0 END), 2) AS early_pct,
       round(100.0 * avg(CASE WHEN days_after_due > 30 THEN 1.0 ELSE 0.0 END), 2) AS late_over_30d_pct
FROM ap_gold.ap.fact_ap_payment_application
WHERE NOT is_void AND days_to_pay IS NOT NULL
GROUP BY tenant_id;

-- 5. Discount capture: of invoices on discount terms that were paid, how many inside the window
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_discount_capture AS
SELECT i.tenant_id,
       count(DISTINCT i.invoice_key) AS discount_eligible_paid,
       count(DISTINCT CASE WHEN a.discount_taken <> 0 THEN i.invoice_key END) AS discount_taken,
       round(100.0 * count(DISTINCT CASE WHEN a.discount_taken <> 0 THEN i.invoice_key END)
             / nullif(count(DISTINCT i.invoice_key), 0), 2) AS capture_pct
FROM ap_gold.ap.fact_ap_invoice_header i
JOIN ap_gold.ap.dim_payment_terms t ON t.payment_terms_code = i.payment_terms_code AND t.discount_pct > 0
JOIN ap_gold.ap.fact_ap_payment_application a ON a.invoice_key = i.invoice_key AND NOT a.is_void
GROUP BY i.tenant_id;

-- 6. Cycle times
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_cycle_time AS
SELECT tenant_id,
       percentile_approx(days_capture_to_approval, 0.5) AS median_days_capture_to_approval,
       percentile_approx(days_capture_to_pay, 0.5) AS median_days_capture_to_pay,
       percentile_approx(days_capture_to_pay, 0.9) AS p90_days_capture_to_pay,
       count(days_capture_to_pay) AS paid_invoices
FROM ap_gold.ap.fact_invoice_lifecycle
GROUP BY tenant_id;

-- 7. Touchless rate, with the evidence caveat carried as data
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_touchless_rate AS
SELECT tenant_id, max(approval_evidence) AS approval_evidence, count(*) AS invoices,
       round(100.0 * avg(CASE WHEN is_touchless THEN 1.0 ELSE 0.0 END), 2) AS touchless_pct,
       CASE WHEN max(approval_evidence) = 'NONE'
            THEN 'NO APPROVAL STREAM: touch count is unknown, not zero' END AS caveat
FROM ap_gold.ap.fact_invoice_lifecycle GROUP BY tenant_id;

-- 8. Three-way match: first-pass match rate and exception mix
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_match_rate AS
SELECT tenant_id, count(*) AS po_lines,
       round(100.0 * avg(CASE WHEN match_status IN ('MATCHED', 'MATCHED_TWO_WAY') THEN 1.0 ELSE 0.0 END), 2) AS first_pass_match_pct,
       sum(CASE WHEN match_status = 'PRICE_VARIANCE' THEN 1 ELSE 0 END) AS price_variance,
       sum(CASE WHEN match_status = 'QTY_VARIANCE' THEN 1 ELSE 0 END) AS qty_variance,
       sum(CASE WHEN match_status = 'NOT_RECEIVED' THEN 1 ELSE 0 END) AS not_received,
       sum(CASE WHEN match_status IN ('PO_NOT_FOUND', 'PO_LINE_NOT_FOUND') THEN 1 ELSE 0 END) AS po_not_found
FROM ap_gold.ap.fact_ap_invoice_line
WHERE match_status IS NOT NULL AND match_status <> 'NO_PO'
GROUP BY tenant_id;

-- 9. PO coverage
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_po_coverage AS
SELECT tenant_id, count(*) AS invoices,
       round(100.0 * avg(CASE WHEN has_po THEN 1.0 ELSE 0.0 END), 2) AS po_coverage_pct,
       round(100.0 * sum(CASE WHEN has_po THEN gross_amount_group END) / nullif(sum(gross_amount_group), 0), 2) AS po_spend_pct
FROM ap_gold.ap.fact_ap_invoice_header GROUP BY tenant_id;

-- 10. Duplicate exposure
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_duplicate_exposure AS
SELECT tenant_id, score_band, count(*) AS pairs, round(sum(abs(gross_amount)), 2) AS exposure_txn_ccy,
       sum(CASE WHEN disposition IS NULL THEN 1 ELSE 0 END) AS undispositioned
FROM ap_gold.ap.fact_duplicate_candidate GROUP BY tenant_id, score_band;

-- 11. Fraud signal: unverified bank change followed by a payment within 14 days
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_fraud_signals AS
SELECT tenant_id, count(*) AS bank_changes,
       sum(CASE WHEN NOT coalesce(is_verified, false) THEN 1 ELSE 0 END) AS unverified,
       sum(CASE WHEN is_fraud_signal THEN 1 ELSE 0 END) AS fraud_signals
FROM ap_gold.ap.fact_vendor_change_event WHERE change_type = 'BANK_ACCOUNT' GROUP BY tenant_id;

-- 12. Blocked invoices
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_blocked AS
SELECT tenant_id, coalesce(payment_block_reason, 'NONE') AS block_reason, count(*) AS invoices,
       round(sum(open_amount), 2) AS open_amount_txn
FROM ap_gold.ap.fact_ap_invoice_header WHERE is_open GROUP BY tenant_id, coalesce(payment_block_reason, 'NONE');

-- 13. GR/IR
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_grir AS
SELECT g.tenant_id, g.snapshot_date, g.grir_status, count(*) AS po_lines, round(sum(g.grir_value), 2) AS grir_value_txn,
       percentile_approx(g.days_since_receipt, 0.5) AS median_days_since_receipt
FROM ap_gold.ap.fact_grir_snapshot g
JOIN (SELECT tenant_id, max(snapshot_date) snapshot_date FROM ap_gold.ap.fact_grir_snapshot GROUP BY tenant_id) l
  USING (tenant_id, snapshot_date)
GROUP BY g.tenant_id, g.snapshot_date, g.grir_status;

-- 14. Spend: monthly, vendor concentration, channel mix
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_spend_monthly AS
SELECT tenant_id, year_month, sum(invoices) AS invoices, round(sum(spend_group), 2) AS spend_group
FROM ap_gold.ap.agg_vendor_spend_monthly GROUP BY tenant_id, year_month;

CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_vendor_concentration AS
WITH v AS (SELECT tenant_id, vendor_id, sum(spend_group) AS spend FROM ap_gold.ap.agg_vendor_spend_monthly GROUP BY tenant_id, vendor_id),
r AS (SELECT *, row_number() OVER (PARTITION BY tenant_id ORDER BY spend DESC) AS rk, sum(spend) OVER (PARTITION BY tenant_id) AS total FROM v)
SELECT tenant_id, count(*) AS vendors, round(100.0 * sum(CASE WHEN rk <= 10 THEN spend END) / max(total), 2) AS top10_share_pct
FROM r GROUP BY tenant_id;

CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_capture_channel_mix AS
SELECT tenant_id, coalesce(capture_channel_code, 'UNMAPPED') AS channel, count(*) AS invoices,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY tenant_id), 2) AS share_pct
FROM ap_gold.ap.fact_ap_invoice_header GROUP BY tenant_id, coalesce(capture_channel_code, 'UNMAPPED');

-- 15. Data trust: is the data good enough to believe the numbers above?
CREATE OR REPLACE VIEW ap_gold.semantic.vw_kpi_data_trust AS
WITH dq AS (
  SELECT tenant_id, sum(rules_evaluated) AS rules_evaluated, sum(rules_not_evaluated) AS rules_not_evaluated,
         sum(rules_alerting) AS rules_alerting
  FROM ap_gold.ap.agg_dq_scorecard_daily
  WHERE eval_date = (SELECT max(eval_date) FROM ap_gold.ap.agg_dq_scorecard_daily) AND layer = 'SILVER'
  GROUP BY tenant_id
),
q AS (SELECT tenant_id, count(*) AS quarantine_open FROM ap_control.ops.quarantine_registry WHERE status = 'OPEN' GROUP BY tenant_id),
rc AS (SELECT tenant_id, sum(CASE WHEN status <> 'MATCHED' THEN 1 ELSE 0 END) AS recon_variances FROM ap_gold.ap.fact_reconciliation GROUP BY tenant_id),
h AS (SELECT tenant_id, count(*) AS invoices FROM ap_gold.ap.fact_ap_invoice_header GROUP BY tenant_id)
SELECT h.tenant_id, dq.rules_evaluated, dq.rules_not_evaluated, dq.rules_alerting,
       coalesce(q.quarantine_open, 0) AS quarantine_open, coalesce(rc.recon_variances, 0) AS recon_variances,
       CASE WHEN coalesce(rc.recon_variances, 0) > 0 THEN 'RED'
            WHEN coalesce(q.quarantine_open, 0) > 0.05 * h.invoices OR dq.rules_not_evaluated > 0 THEN 'AMBER'
            ELSE 'GREEN' END AS trust_level
FROM h LEFT JOIN dq USING (tenant_id) LEFT JOIN q USING (tenant_id) LEFT JOIN rc USING (tenant_id);

-- 16. Executive summary strip: five numbers plus trust, one row per tenant
CREATE OR REPLACE VIEW ap_gold.semantic.vw_executive_summary AS
SELECT b.tenant_id, b.snapshot_date, b.open_ap_group, b.past_due_pct, d.dpo_days, p.on_time_pct,
       m.first_pass_match_pct, t.trust_level
FROM ap_gold.semantic.vw_kpi_ap_balance b
LEFT JOIN ap_gold.semantic.vw_kpi_dpo d USING (tenant_id)
LEFT JOIN ap_gold.semantic.vw_kpi_payment_timeliness p USING (tenant_id)
LEFT JOIN ap_gold.semantic.vw_kpi_match_rate m USING (tenant_id)
LEFT JOIN ap_gold.semantic.vw_kpi_data_trust t USING (tenant_id);

-- COMMAND ----------

-- MAGIC %md ## The metric catalogue

-- COMMAND ----------

CREATE OR REPLACE VIEW ap_gold.semantic.vw_metric_catalogue AS
SELECT * FROM VALUES
 ('Open AP balance',            'vw_kpi_ap_balance',           'CFO',            'trend',          'IMPLEMENTED', NULL),
 ('Past-due share',             'vw_kpi_ap_balance',           'AP lead',        '< 10%',          'IMPLEMENTED', NULL),
 ('Aging profile',              'vw_kpi_aging',                'AP lead',        '90+ < 2%',       'IMPLEMENTED', NULL),
 ('Days payable outstanding',   'vw_kpi_dpo',                  'Treasury',       '45-60 days',     'IMPLEMENTED', 'count-back on 90 days of posted spend'),
 ('Weighted days to pay',       'vw_kpi_payment_timeliness',   'Treasury',       '= terms',        'IMPLEMENTED', NULL),
 ('On-time payment rate',       'vw_kpi_payment_timeliness',   'AP lead',        '> 95%',          'IMPLEMENTED', NULL),
 ('Early payment rate',         'vw_kpi_payment_timeliness',   'Treasury',       '< 5%',           'IMPLEMENTED', 'paying early with no discount is lost float'),
 ('Discount capture rate',      'vw_kpi_discount_capture',     'Treasury',       '> 90%',          'IMPLEMENTED', NULL),
 ('Invoice cycle time',         'vw_kpi_cycle_time',           'AP lead',        'median < 10 days','IMPLEMENTED', NULL),
 ('Touchless rate',             'vw_kpi_touchless_rate',       'AP transformation','> 60%',        'IMPLEMENTED', 'approved in one step with no routing; unknowable where approval_evidence = NONE'),
 ('First-pass match rate',      'vw_kpi_match_rate',           'Procurement',    '> 85%',          'IMPLEMENTED', NULL),
 ('PO coverage',                'vw_kpi_po_coverage',          'Procurement',    '> 80% of spend', 'IMPLEMENTED', NULL),
 ('Duplicate exposure',         'vw_kpi_duplicate_exposure',   'Controller',     '0 undispositioned HIGH', 'IMPLEMENTED', NULL),
 ('Bank-change fraud signals',  'vw_kpi_fraud_signals',        'Controller',     '0',              'IMPLEMENTED', 'unverified change + payment within 14 days'),
 ('Blocked invoices',           'vw_kpi_blocked',              'AP lead',        'trend',          'IMPLEMENTED', NULL),
 ('GR/IR position',             'vw_kpi_grir',                 'Controller',     'aged > 90d = 0', 'IMPLEMENTED', NULL),
 ('Monthly spend',              'vw_kpi_spend_monthly',        'Procurement',    'trend',          'IMPLEMENTED', NULL),
 ('Vendor concentration',       'vw_kpi_vendor_concentration', 'Procurement',    'top-10 share',   'IMPLEMENTED', NULL),
 ('Capture channel mix',        'vw_kpi_capture_channel_mix',  'AP transformation','EDI+portal > 70%','IMPLEMENTED', NULL),
 ('Data trust indicator',       'vw_kpi_data_trust',           'Data owner',     'GREEN',          'IMPLEMENTED', NULL),
 ('Cost per invoice',           NULL, 'CFO',        NULL, 'NOT_IMPLEMENTED', 'needs a finance cost base (AP team cost) that is not ingested'),
 ('AP-to-GL variance',          NULL, 'Controller', NULL, 'NOT_IMPLEMENTED', 'needs the general ledger AP control account balance, not ingested'),
 ('Invoices per FTE',           NULL, 'AP lead',    NULL, 'NOT_IMPLEMENTED', 'needs headcount from HR, not ingested'),
 ('Payment error rate',         NULL, 'Treasury',   NULL, 'NOT_IMPLEMENTED', 'needs bank returns / rejected payments, not ingested'),
 ('Supplier satisfaction',      NULL, 'Procurement',NULL, 'NOT_IMPLEMENTED', 'survey data, not a transaction measure'),
 ('Maverick spend',             NULL, 'Procurement',NULL, 'NOT_IMPLEMENTED', 'needs the contract repository to know what was off-contract'),
 ('Cash forecast accuracy',     NULL, 'Treasury',   NULL, 'NOT_IMPLEMENTED', 'needs several weeks of stored forecasts to score; see notebook 34')
AS t(metric, served_by, owner, target, status, note);

-- COMMAND ----------

SELECT status, count(*) AS metrics FROM ap_gold.semantic.vw_metric_catalogue GROUP BY status;

-- COMMAND ----------

SELECT * FROM ap_gold.semantic.vw_executive_summary ORDER BY tenant_id;

-- COMMAND ----------

SELECT * FROM ap_gold.semantic.vw_kpi_touchless_rate ORDER BY tenant_id;
