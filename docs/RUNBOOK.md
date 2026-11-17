# AP Lakehouse — Implementation Runbook (Steps 2–5)

Session-by-session guide from an empty Databricks Free Edition workspace to a
working gold layer. Every notebook has a checkpoint and the numbers to expect.
The numbers are not estimates: they come from running this exact repository end
to end at `scale=0.04` with `tests/run_local_e2e.py` (local Spark 4.0, 17 Nov 2026).

> **What has and has not been run.** Every notebook and every module ran end to
> end locally, including exercises E1 and E2. What has **not** run is the
> Databricks-specific write path (`tables.DeltaBackend`: `MERGE WITH SCHEMA
> EVOLUTION`, `CLUSTER BY`, `replaceWhere`) and Unity Catalog / volume
> behaviour. Expect a small number of platform fixes; they will be in
> `src/ap_platform/tables.py` or a SQL statement, not in the design.
> `docs/CHANGES.md` lists every bug the local run found and fixed.

---

## 0. Before you start (15 min)

### Get the code into the workspace
1. Unzip `ap-lakehouse.zip`.
2. **Recommended:** push the `ap-lakehouse/` folder to a new GitHub repository, then
   in Databricks: **Workspace → Create → Git folder**, paste the URL. You will edit
   `config/` during the exercises; a Git folder gives you history for that.
3. Alternative: **Workspace → Import**, upload the folder.

### Find your `repo_root` — the one setting that breaks everything
Open any notebook and run:
```python
print(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
```
Everything before `/notebooks/...` is your **repo_root**, e.g.
`/Workspace/Users/you@example.com/ap-lakehouse`. Set the `repo_root` widget on
**every** Python notebook. Symptom of getting it wrong:
`ModuleNotFoundError: No module named 'ap_platform'`.

### Two habits that save hours
* **After editing a module, restart Python** (`dbutils.library.restartPython()`)
  or re-run the whole notebook. The notebooks also `importlib.reload` the modules.
* **Use `dry_run` first** wherever there is one. It runs the whole path and writes nothing.

### Run order (not negotiable)
```
00 → 01 → 02 → 03 → 04        Session 1  foundation
10 → 11 → 12 → 13 (→ 14)      Session 2  bronze          (+ exercise E2: unhalt)
20 → 21 → 22 (→ 23 → 24)      Session 3  silver          (+ exercise E1: re-drive)
30 → 31 → 32 → 33 (→ 34)      Session 4  gold
                              Session 5  scale up to 0.2 and re-run
```
Each layer reads the one before it and the control plane underneath. Skipping
ahead produces empty tables and confusing errors, not a shortcut.

---

## Session 1 — Foundation (notebooks 00–04, ~60 min)

### 00_unity_catalog_setup.sql
Creates 4 catalogs, their schemas, and the volumes `inbox`, `archive`, `poison`,
`artifacts`. **`CREATE GROUP` and some `GRANT`s are expected to fail** on Free
Edition: note them and move on; nothing before Step 6 depends on them.
```sql
SHOW CATALOGS LIKE 'ap_*';           -- 4
SHOW VOLUMES IN ap_bronze.landing;   -- inbox, archive, poison
SHOW VOLUMES IN ap_control.ops;      -- artifacts
```
**Row-filter probe** — run now, note the result, it decides Step 6:
```sql
CREATE OR REPLACE FUNCTION ap_control.config.fn_tenant_filter(t STRING) RETURN t = 'TEN001';
```

### 01_control_plane_ddl.sql
24 control tables (config, ref, ops). Pure DDL.

### 02_bootstrap_config.py
`dry_run=true` first (every block `OK`), then `dry_run=false`, then run it again:
the second run must show 0 inserted, 0 deactivated.
```sql
SELECT 'schema_mapping' t, count(*) n, sum(CASE WHEN is_active THEN 1 ELSE 0 END) active FROM ap_control.config.schema_mapping
UNION ALL SELECT 'dq_rule_registry',      count(*), sum(CASE WHEN is_active THEN 1 ELSE 0 END) FROM ap_control.config.dq_rule_registry
UNION ALL SELECT 'transformation_config', count(*), sum(CASE WHEN is_active THEN 1 ELSE 0 END) FROM ap_control.config.transformation_config
UNION ALL SELECT 'value_domain_mapping',  count(*), sum(CASE WHEN is_active THEN 1 ELSE 0 END) FROM ap_control.config.value_domain_mapping
UNION ALL SELECT 'domain_binding',        count(*), sum(CASE WHEN is_active THEN 1 ELSE 0 END) FROM ap_control.config.domain_binding
UNION ALL SELECT 'ingestion_config',      count(*), sum(CASE WHEN is_active THEN 1 ELSE 0 END) FROM ap_control.config.ingestion_config;
```
| table | expect (n = active) |
|---|---|
| schema_mapping | 374 |
| dq_rule_registry | 27 |
| transformation_config | 46 (16 silver + 30 gold) |
| value_domain_mapping | 55 |
| domain_binding | 9 |
| ingestion_config | 30 |

`active` must equal `n` everywhere. If it does not, stop: that is the
convergence bug, and the config loader has been fixed for it — send the counts.

### 03_generate_synthetic_data.py
Leave `scale = 0.04`. Expect **375 files, 45,532 rows** (TEN001 28,909 · TEN002
12,668 · TEN003 3,955), 1,273 defects logged across all 30 defect types, and three
drift events. Read the defect ledger it prints; you will want it in Session 3.
Do not re-run it unless you change the scale: it is deterministic.

### 04_validate_foundation.py
Every **blocking** check must pass.

**Done when:** 4 catalogs and the volumes exist · config loaded with active = total ·
375 files under TEN001/TEN002/TEN003 · notebook 04 clean · row-filter result noted.

---

## Session 2 — Bronze (notebooks 10–14, ~60 min)

### 10 and 11
10 creates 11 bronze tables plus `file_ledger` and `schema_contract`.
11 registers the baseline contracts from the **earliest** file of each source:
**30 contracts**. Run it before ingesting; re-baselining after drift has been
ingested would bake the drift into the contract.

### 12_ingest_bronze.py
First `tenants=TEN001, dry_run=true`, then `dry_run=false`, then blank (all tenants).

**Three things that look like failures and are the pipeline working:**

| | Source | Expect |
|---|---|---|
| ADDITIVE drift | TEN001 invoice_header | `COLUMN_ADDED _c18` (ZUONR, positional) → accepted; 1,858 rescued rows |
| BREAKING drift | TEN002 invoice_header | `TYPE_CHANGED INVOICE_AMOUNT` → **HALT**, 12/21 files loaded, 9 held back |
| BREAKING drift | TEN003 invoice_header | `COLUMN_RENAMED po-number -> purchase-order-number` → **HALT**, 18/26 loaded |

First-run totals from `file_ledger`: TEN001 28,909 rows / 30 files · TEN002
12,604 / 182 · TEN003 3,916 / 146. Every other source loads in full.

**Idempotency:** run 12 again unchanged. Every line must say `--` (no new data).

### 13_ingestion_observability.sql
Section 1 should list exactly the two halted sources.

### 14 — Exercise E2: unhalt both sources (do it)
1. `operation=unhalt, tenant=TEN003, entity=invoice_header` — read the diff
   (`po-number` removed, `purchase-order-number` added), then `apply=true`.
2. Fix the meaning, not just the shape. In `config/schema_mapping.yaml`, in the
   **TEN003 invoice_header** block, change the `po_number` row's `transform_expr` to
   ``coalesce(nullif(upper(trim(`purchase-order-number`)),''), nullif(upper(trim(`po-number`)),''))``
   (it is the first `po-number` row in the file; keep `source_field: po-number`).
3. `operation=unhalt, tenant=TEN002, entity=invoice_header, apply=true`.
   No mapping change: the amount mappings already parse `"$1,009,737.72"`.
4. Re-run 02 (`generate_fx=false`), then 12.

Expect 12 to load TEN002 **9/9** files (64 rows) and TEN003 **8/8** (39 rows).
Bronze is now complete: **375 files, 45,532 rows**, exactly what was generated.

---

## Session 3 — Silver (notebooks 20–24, ~75 min)

### 20_silver_control_ddl.sql
Schemas `ap_silver.ap`, `ap_silver.quarantine`, `ap_silver.staging` and the
watermark table. Silver tables are created by the writer on first write.

### 21_run_silver.py
`mode=plan` → `mode=dry_run` → `mode=run`.

* **plan:** 16 jobs in 5 waves, and "silver code is tenant-agnostic".
* **dry_run** on a fresh lake: the 11 entity jobs run the full gate; the 5 derived
  jobs show `SKIPPED ... does not exist yet (expected in a dry run on a fresh lake)`.
* **run** — expect:

| Wave | Job | Rows in (versions) | Written | Quarantined | Rules |
|---|---|---|---|---|---|
| 1 | SL_VENDOR (SCD2) | 152 | 80 | 0 | 2 |
| 2 | SL_INVOICE_HEADER | 4,006 | 1,931 | 142 | 14 |
| 2 | SL_PAYMENT | 3,126 | 1,600 | 0 | 1 |
| 2 | SL_PO_HEADER | 1,576 | 800 | 0 | 1 |
| 2 | SL_VENDOR_BANK (SCD2) | 203 | 103 | 0 | 2 |
| 3 | SL_APPROVAL_EVENT | 7,573 | 3,488 | 0 | 1 |
| 3 | SL_DUPLICATE_CANDIDATE | 44 | 44 | – | – |
| 3 | SL_INVOICE_LINE | 14,436 | 6,797 | 620 | 4 of 5 |
| 3 | SL_PAYMENT_APPLICATION | 3,126 | 1,600 | 0 | 1 |
| 3 | SL_PO_LINE | 3,877 | 1,904 | 58 | 1 |
| 3 | SL_VENDOR_CHANGE_EVENT | 23 | 23 | – | – |
| 4 | SL_AP_ITEM | 1,931 | 1,931 | – | – |
| 4 | SL_GOODS_RECEIPT | 3,487 | 1,728 | 45 | 2 |
| 4 | SL_INVOICE_DISTRIBUTION | 3,970 | 3,410 | 0 | 1 |
| 5 | SL_GRIR_POSITION | 1,904 | 1,904 | – | – |
| 5 | SL_THREE_WAY_MATCH | 4,974 | 4,974 | – | – |

"Rows in" counts every version (TEN001's three snapshots repeat most rows;
TEN002/TEN003 restatements add more), so "written" is the number of distinct
records. Invoice headers per tenant: TEN001 873 · TEN002 670 · TEN003 388.

* **Run 21 again:** every entity job `NO_NEW_DATA (watermarks current)`; the five
  derived jobs recompute.

### 22_dq_scorecard.sql
Read `rules_evaluated` first. Section 2 should list exactly one rule as not
evaluated, `DQ_INV_LIN_0403` (no line-level `tax_amount` mapping: Step 4 exercise
E3). Section 5 (alerts) will show `THRESHOLD_BREACH` rows: the planted defect
rates exceed several thresholds by design. Every row there should be explainable.

What the quarantine backlog looks like after the first run (largest buckets):

| Tenant | Entity | Rule(s) | Rows | Why |
|---|---|---|---|---|
| TEN003 | invoice_line | DQ_ALL_0504 | 207 | **PCE unit of measure — exercise E1** |
| TEN002 | invoice_line | DQ_INV_LIN_0502 | 174 | lines whose header is quarantined (orphans by cascade) |
| TEN001 | invoice_line | DQ_INV_LIN_0502 | 150 | same |
| TEN003 | po_line / po_history | DQ_ALL_0504 | 58 / 45 | PCE again |
| TEN003 | invoice_header | DQ_INV_0401 + DQ_INV_HDR_0602 | 26 | planted late corrections: amount changed > 14 days after posting |
| TEN002 | invoice_header | DQ_INV_0401 | 22 | header ≠ lines + tax (planted, plus CDC line deletions the header never reflected) |
| TEN001 | invoice_header | DQ_INV_HDR_0201 | 13 | unparseable invoice dates (planted) |

The grading view (`vw_defect_detection`) shows 18 of the 24 planted defect types
firing for every tenant they were planted in. The rest, and why:
`TAX_AMOUNT_INCONSISTENT` (rule not evaluable until exercise E3),
`BANK_CHANGE_UNVERIFIED` (a GOLD rule, evaluated in Session 4),
`BANK_ACCOUNT_MALFORMED` (TEN003's mapping strips the formatting),
`NULL_MANDATORY_SUPPLIER_INV` (TEN003's file cannot express it: its invoice
number IS the supplier's number), and `EXACT_DUP_PK` / `NEGATIVE_NON_CREDIT`
(fire for some tenants, not all).

### 23 — Exercise E1: the TEN003 `PCE` unit of measure (do it)
1. Run with `entity` blank: the unmapped-domain report lists
   `TEN003 | UOM | PCE` with the YAML line to add (and planted payment-terms codes
   like `ZZ99` — leave those quarantined).
2. In `config/value_domains_silver.yaml` add
   `- {tenant_id: TEN003, domain: UOM, source_value: PCE, canonical_value: EACH, canonical_label: Each, sort_order: 1, is_active: true}`
   and re-run 02.
3. Re-run 23 with `entity=invoice_line, dry_run=false`, then `po_line`, then `po_history`.
4. Re-run 21 (`mode=run`) to refresh three-way match and GR/IR.

Expected re-drive results:

| Entity | Open before | Resolved | Still open | Why some stay open |
|---|---|---|---|---|
| invoice_line | 620 | 207 | 413 | their **header** is quarantined; fixing UOM does not fix that |
| po_line | 58 | 58 | 0 | |
| po_history | 45 | 45 | 0 | |

After E1, `SELECT uom, count(*) FROM ap_silver.ap.sl_invoice_line WHERE tenant_id='TEN003' GROUP BY uom`
shows no NULL, and the registry shows `RESOLVED_CORRECTED` with a `redrive_batch_id`.

### 24_silver_maintenance.py
Section 4 must print "SCD2 integrity: clean".

---

## Session 4 — Gold (notebooks 30–34, ~60 min)

### 30 and 31
30 creates the schemas and `ap_gold.semantic.model_catalogue` (30 rows).
31: `mode=plan` (30 jobs, 5 waves, gold code tenant-agnostic), then `mode=run`:

| | Rows | | Rows |
|---|---|---|---|
| dim_date | 4,018 | fact_ap_invoice_header | 1,931 |
| dim_fiscal_calendar | 12,054 | fact_ap_invoice_line | 6,955 |
| dim_tenant | 3 | fact_ap_payment | 1,600 |
| dim_company | 7 | fact_ap_payment_application | 1,600 |
| dim_vendor (SCD2) | 80 | fact_ap_open_item_snapshot | 687 |
| dim_vendor_bank (SCD2) | 103 | fact_grir_snapshot | 1,952 |
| dim_currency | 6 | fact_invoice_lifecycle | 1,931 |
| dim_payment_terms | 10 | fact_approval_event | 3,470 |
| dim_gl_account | 17 | fact_duplicate_candidate | 44 |
| dim_cost_center | 2,177 | fact_vendor_change_event | 23 |
| dim_doc_type | 3 | fact_match_exception | 1,124 |
| dim_capture_channel | 5 | fact_dq_result | 255 |
| dim_dq_rule | 27 | fact_reconciliation | 12 (all MATCHED) |
| agg_ap_aging_daily | 83 | agg_dpo_monthly | 81 |
| agg_vendor_spend_monthly | 978 | agg_dq_scorecard_daily | 60 |

Integrity: orphan `vendor_key` on invoices 0, on payments 9 (payments to vendors
that never passed the gate); applications 720 / 560 / 320 against invoices
settled 689 / 471 / 304 (partial payments present: 34 / 31 / 23); point-in-time
duplicates **0**; reconciliation 12 of 12 `MATCHED`. Running 31 a second time
changes nothing.

Section 4 integrity checks: orphan vendor keys near zero; applications ≥ payments
and > invoices settled; **point-in-time duplicates = 0** (the notebook asserts it);
reconciliation all `MATCHED`.

### 32_semantic_layer.sql
Start with `vw_metric_catalogue` (20 implemented, 7 documented gaps), then
`vw_executive_summary`:

| tenant | open AP (USD) | past due % | DPO | paid on time % | first-pass match % | trust |
|---|---|---|---|---|---|---|
| TEN001 | 89.2 M | 78.4 | 72.1 | 42.1 | 70.4 | AMBER |
| TEN002 | 87.4 M | 84.8 | 115.6 | 36.5 | 85.2 | AMBER |
| TEN003 | 9.1 M | 62.6 | 39.3 | 48.5 | 85.5 | AMBER |

High past-due shares are a property of the synthetic data (the generator pays
only a subset of invoices). AMBER trust is correct: open quarantine exceeds 5% of
invoices and one rule is not evaluated. Touchless reads 100% for TEN003 with the
caveat "NO APPROVAL STREAM" — expected, see the notebook's header. For the other
two, `vw_kpi_touchless_rate` gives TEN001 14.3% and TEN002 17.3% (approved in one
step, no routing).

### 33_dashboards.sql
Five dashboards as named SQL tiles. Change `DECLARE OR REPLACE VARIABLE tenant`
to filter. **Tile 1.4** is the smoke test. **Tile 5.2 shows 3 tenants visible**:
correct until Step 6 applies row filters.

### 34_analytics_extras.py
Cash forecast stored; forecast accuracy says "too few"; duplicate calibration
**REFUSES**. All three are the correct output on day one.

---

## Session 5 — Scale up (~60 min)

Re-run 03 with `scale=0.2` (415 files, 226,140 rows: TEN001 143,048 · TEN002
63,162 · TEN003 19,930), then the whole chain again. Because the generator
purges and rewrites the inbox, start from a clean bronze/silver/gold: drop the
three data catalogs' tables (or re-run 00 after dropping `ap_bronze`,
`ap_silver`, `ap_gold`), keep `ap_control` but truncate `ops.file_ledger`,
`ops.schema_contract`, `ops.schema_drift_log`, `ops.silver_watermark` and
`ops.quarantine_registry`. Then 10 → 34 as above. Expect roughly 5× the
silver and gold row counts.

---

## Troubleshooting (in the order you will hit them)

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: ap_platform` | `repo_root` widget wrong | print the notebook path, take everything above `/notebooks/` |
| You fixed a module and the bug is still there | Python cached the import | `dbutils.library.restartPython()` or re-run the whole notebook |
| `CREATE GROUP` / `GRANT` fails in 00 | Free Edition | expected; continue |
| A config table near-empty after 02 | convergence | `SELECT count(*), sum(int(is_active))` — send the counts |
| A source is halted | breaking drift, by design | `vw_bronze_open_drift`, then notebook 14 `unhalt` |
| 12 reports `FAILED ... no schema contract` | 11 not run | run notebook 11 |
| `MERGE WITH SCHEMA EVOLUTION` syntax error | older runtime | in `tables.py` `DeltaBackend.merge`, drop `WITH SCHEMA EVOLUTION` (silver schemas are stable after the first write) |
| `CLUSTER BY` refused on create | edition | in `tables.py` `DeltaBackend.create_if_absent`, drop the cluster clause |
| A silver/gold job fails on a missing column | a builder reads a column silver does not produce | `spark.table('<silver table>').columns` vs the builder; fix the builder, re-run (MERGEs are idempotent) |
| Job succeeded but wrote nothing | watermarks current, or inactive config, or tenant widget | `full_reload=true`; check `is_active`; clear `tenants` |
| DQ aborts a slice | an ABORT rule failed | `dq_result_log` for the rule; change severity in `config/dq_rules.yaml` + 02 if it should not abort |
| Silver-to-gold variance | quarantine (explained) → failed upstream job → builder bug | `fact_reconciliation.explanation`, then `task_audit_log` |
| Compute quota exhausted | | wait for reset; stay at `scale=0.04` |

**When you ask for help, send:** notebook and cell, the error text, and the
control-plane rows for that batch — `batch_run_log` and `task_audit_log` (or
`dq_result_log` for a DQ abort). The run logs are designed to make a failure
diagnosable without the stack trace.
