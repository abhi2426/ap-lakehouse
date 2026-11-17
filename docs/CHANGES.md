# What changed, and why (17 Nov 2026)

This repository started from the Step 2 archive (`ap-lakehouse.tar.gz`, 19 Sep
2026) and the hands-on runbook that described Steps 2–5. The archive contained
only Step 2, mixed with an older generation of files. Steps 3–5 (notebooks
10–34 and the ingestion, silver and gold engines) were written new for this
release, to the runbook's design, and the whole chain was then executed end to
end on local Spark with `tests/local_harness.py`. Every item below was found by
running it, not by reading it.

## Bugs fixed in the Step 2 code

| # | Where | Bug | Effect if unfixed |
|---|---|---|---|
| 1 | archive | Two generations of files mixed (`conf/` and `config/`, clashing notebook numbers 00–03) | confusing; old notebooks import modules that no longer exist |
| 2 | `config_loader.py` | Convergence pass ran per FILE; `schema_mapping` is declared by two files | the second file deactivated every row of the first; silver would map nothing for vendor, bank and invoice entities |
| 3 | `config_loader.py` | Used `.cache()` and `DeltaTable` directly | `.cache()` is refused on serverless compute |
| 4 | notebook 01 | `''` used to escape quotes inside `COMMENT '...'` | parse error: notebook 01 stops at `schema_mapping` |
| 5 | notebook 02 | FX rates passed as Python floats into a `DECIMAL` column | `createDataFrame` raises; FX table never written |
| 6 | notebook 02 | FX rates ended 2026-06-30 | invoices and payments in July 2026 have no rate |
| 7 | notebook 04 | Three checks used `HAVING` without `GROUP BY` | analysis error; checks report ERROR |
| 8 | notebook 04 | DQ check read `threshold_pct` (column is `fail_threshold_pct`) | check always errors |
| 9 | notebook 04 | Tenant-guard check ignored rules with `tenant_id = 'ALL'` | blocking failure for every tenant |
| 10 | notebook 04 | Delivery-series check required deltas for vendor master data | blocking failure on correct data |
| 11 | `datagen/writers.py` | `int()` on formatted bank accounts (`" 844-323"`) | generation crashes at `scale=0.2` |
| 12 | `datagen` | TEN003 lines and payment applications carried the internal doc id; the header carries the supplier's number; line accounting date was one fiscal year off | **no TEN003 invoice line could join its header** |
| 13 | `datagen/writers.py` | Daily restatement rescaled header amounts ±2% without touching lines | ~5% of invoices failed header-equals-lines for a reason that was not a planted defect |
| 14 | notebook 02 | FX rates written with `overwrite` + `overwriteSchema` | replaces the table schema and drops the column comments: notebook 04's blocking comment check fails on Databricks. Now a MERGE |
| 15 | notebook 04 | New `domain_binding` table added to the checked list | (completeness) |

## Configuration fixes

| # | File | Fix |
|---|---|---|
| 16 | `schema_mapping*.yaml` | **37 amount mappings now parse every format the sources send.** The TEN003 mapping stripped everything but digits and dots, so `738,06` became `73806` and `1.234,56` became `1.23` — a silent 100x error. TEN002 could not parse `$5,559.23` at all. |
| 17 | `reference_data.yaml` | TEN001 doc type `RG` (credit memo) was missing: every TEN001 credit memo was quarantined as unmapped |
| 18 | `dq_rules.yaml` DQ_INV_HDR_0602 | Referenced an `ingest_date` no layer produces (could never evaluate). Now: a restatement that changes the amount and arrives more than 14 days after posting |
| 19 | `dq_rules.yaml` DQ_INV_HDR_0202 | "Future posting date" compared with `current_date()`, so it stopped firing once the data was a few months old. Now: more than 14 days after the extract date that delivered it |
| 20 | `dq_rules.yaml` DQ_INV_HDR_0402 | Only caught positive credit memos; now also negative invoices |
| 21 | `transformations.yaml` | `SL_PAYMENT_APPLICATION` merge keys named a `fiscal_year` that does not exist; two jobs clustered on columns that do not exist; `SL_APPROVAL_EVENT` (the 16th silver job) was missing |
| 22 | new: `transformations_gold.yaml` | the 30 gold jobs (only 7 existed) |
| 23 | new: `domain_bindings.yaml`, `value_domains_silver.yaml` | which fields are translated through value domains; the UOM domain (with the deliberate TEN003 `PCE` gap that is exercise E1) |
| 24 | notebook 03 | `scale` defaults to 0.04 for the first pass |

## Design decisions made while building Steps 3–5

* **Bronze keeps the record as `MAP<STRING,STRING>`.** Three ERPs share one table
  per entity and a new source column needs no DDL. Typing is silver's job.
* **Exactly-once via a file ledger**, not a stream checkpoint. Auto Loader is an
  optional discovery mechanism on top (`use_autoloader`), off by default.
* **Type drift needs evidence**: a NUMBER column has changed type when fewer than
  50% of a file's values parse and at least 3 do not. A couple of injected
  `1.234,56` values in a six-row delta file are a DQ matter, not drift.
* **Accepting a rename tolerates both names** during the transition (both
  columns become optional in the new contract), because TEN003's corrections
  export still uses the old name after the main export moved.
* **Silver holds the latest version that passed the gate**; newer failing versions
  are quarantined. A bad correction cannot overwrite good data.
* **A threshold breach is an alert, not an abort.** The planted defect rates
  exceed several thresholds by design; escalating to abort meant the invoice
  pipeline could never complete. ABORT severity is explicit and still aborts.
* **Rules that cannot be evaluated are recorded as `NOT_EVALUATED`** with the
  reason, never counted as passes.
* **Exact duplicate keys within one file are collapsed and reported**, so the
  uniqueness rule shows what it caught instead of a silent pass.
* **Business date**: gold snapshots and aging use the latest extract date loaded
  (2026-07-21), not the wall clock, so synthetic items are not aged by months.
* **All writes go through `tables.py`**, so the only Databricks-only code is the
  ~100-line `DeltaBackend`; everything else was executed locally.
* **Verdicts are pinned before writes.** Spark is lazy: a frame that reads a table
  and is used after that table is written gets recomputed against the new state.
  Silver stages its DQ verdicts (`ap_silver.staging`), the SCD2 writer stages its
  new and closing versions, and re-drive pins the backlog, so what is written and
  what is quarantined always come from one evaluation. (The local run caught
  this; on Delta it would have produced silently wrong SCD2 dates instead.)
* **Full-recompute builders keep targets exact.** Derived silver tables and gold
  facts are rebuilt each run and MERGEd with `WHEN NOT MATCHED BY SOURCE ... DELETE`,
  scoped to the run's tenants, so a record deleted at source leaves gold too.
* **Three-way match is per invoice line** (line quantity vs received quantity
  within tolerance). A cumulative check across invoices flagged most lines,
  because the synthetic invoices bill the same PO line several times.
* **Touchless = approved in one step, no routing.** The sources send no
  auto-approval event, so "no human approval at all" is not measurable; the
  metric catalogue says so.
