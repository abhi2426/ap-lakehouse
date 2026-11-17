# Multi-Tenant Accounts Payable Lakehouse

A metadata-driven, multi-tenant AP lakehouse on Databricks with Unity Catalog.
Three tenants, three different ERPs, one canonical model, one codebase.

| Tenant | Business | Source shape | Load type | Quirks that shape the design |
|---|---|---|---|---|
| TEN001 | Northwind Manufacturing GmbH | SAP-style, pipe-delimited, headerless, ISO-8859-1 | FULL_SNAPSHOT | Positional layout, `yyyyMMdd` dates, gross amount with a separate debit/credit indicator, fiscal year inside the key |
| TEN002 | Cascade Retail Holdings Inc | Oracle-style, newline-delimited JSON | CDC | Surrogate ids everywhere, a distribution grain below the line grain, signed amounts, 4-4-5 fiscal calendar |
| TEN003 | Meridian Services Pty Ltd | Coupa-style, quoted CSV, hyphenated headers | INCREMENTAL | Supplier's invoice number is the key, mixed date and amount formats in one column, July–June fiscal year, no approval stream, no way to express a deletion |

TEN004 is deliberately absent. Onboarding it, using only config and no new code,
is the Step 6 capstone.

**Start here: [`docs/RUNBOOK.md`](docs/RUNBOOK.md)** — the session-by-session
implementation guide with the checkpoint after every notebook and the numbers
to expect.

---

## Repo layout

```
ap-lakehouse/
├── README.md                         this file
├── EXERCISES.md                      Step 2 exercises (Steps 3–5 exercises are in docs/)
├── config/                           THE SOURCE OF TRUTH for all behaviour (YAML -> ap_control)
│   ├── tenants.yaml                  tenant_registry
│   ├── source_systems.yaml           source systems, SLA tiers, match tolerances
│   ├── ingestion.yaml                30 ingestion_config rows: format, path, load type, keys
│   ├── schema_mapping.yaml           vendor, bank, invoice header/line: source field -> canonical
│   ├── schema_mapping_po_payment.yaml PO, receipt, payment, approval, distribution
│   ├── domain_bindings.yaml          which canonical fields go through value_domain_mapping
│   ├── value_domains_silver.yaml     UOM domain (TEN003 PCE gap = exercise E1)
│   ├── dq_rules.yaml                 27 rules: ABORT / QUARANTINE / WARN
│   ├── transformations.yaml          16 silver jobs with a depends_on DAG
│   ├── transformations_gold.yaml     30 gold jobs (13 dims, 13 facts, 4 aggregates)
│   └── reference/reference_data.yaml company codes, terms, tax, GL, value domains
├── notebooks/                        run in numeric order — see the runbook
│   ├── 00–04                         Step 2  foundation: catalogs, control plane, config, data, validation
│   ├── 10–14                         Step 3  bronze: DDL, contracts, ingest, observability, replay/unhalt
│   ├── 20–24                         Step 4  silver: DDL, run, DQ scorecard, re-drive, maintenance
│   └── 30–34                         Step 5  gold: DDL, run, semantic layer, dashboards, analytics
├── src/ap_platform/
│   ├── tables.py                     the ONLY module that knows how tables are written (Delta MERGE etc.)
│   ├── runlog.py                     batch_run_log / task_audit_log
│   ├── spec.py, orchestrator.py      transformation_config -> DAG waves; one runner for silver AND gold
│   ├── lint.py                       asserts engine code contains no tenant or ERP literal
│   ├── config_loader.py              converging YAML -> Delta loader
│   ├── datagen/                      synthetic source files with a defect ledger
│   ├── ingestion/                    contracts (drift), engine (generic bronze), operations (unhalt/replay)
│   ├── silver/                       conform, dq, scd2, pipeline, builders, redrive, maintenance
│   └── gold/                         keys, dimensions, facts, aggregates, registry, analytics
├── jobs/                             3 Jobs API definitions (reference only on Free Edition)
├── docs/                             RUNBOOK.md, STEP3_INGESTION.md, STEP4_SILVER.md, STEP5_GOLD.md, CHANGES.md
└── tests/local_harness.py            runs every notebook on plain local Spark (see below)
```

Two directories carry the whole argument of the project. `config/` is data that
describes behaviour; `src/` is code that does not know which tenant it is
serving. Notebooks 21 and 31 assert the second half: they scan the engine code
and fail if a tenant id or ERP name appears in it.

---

## How the layers fit

| Layer | One engine | Driven by | Writes |
|---|---|---|---|
| Bronze | `ingestion/engine.py` | `ingestion_config`, schema contracts | `ap_bronze.ap.br_<entity>`: the record as a `MAP<STRING,STRING>`, plus lineage |
| Silver | `silver/pipeline.py` (+5 derived builders) | `schema_mapping`, `domain_binding`, `dq_rule_registry`, `transformation_config` | `ap_silver.ap.sl_*`, quarantine in `ap_silver.quarantine.q_*` |
| Gold | `orchestrator.py` + `gold/registry.py` | `transformation_config` (GOLD) | `ap_gold.ap.dim_* / fact_* / agg_*`, views in `ap_gold.semantic` |

Silver's rule: **the latest version of each record that passed the DQ gate.**
Newer versions that fail are quarantined, so a bad late correction cannot
overwrite good data, and a re-drive (notebook 23) puts quarantined records back
through the same gate with the current config.

---

## Testing off-platform

`tests/local_harness.py` executes the real notebooks on plain local Spark
(PySpark 4.0): it rewrites three-part Unity Catalog names to local databases,
swaps the Delta backend in `tables.py` for a Parquet emulation, stubs
`dbutils`, and links `/Volumes` to a local folder. Everything except the ~100
lines of `tables.DeltaBackend` runs exactly as it will on Databricks. The whole
chain 00 → 34, including exercises E1 and E2, was run end to end this way;
`docs/RUNBOOK.md` quotes the numbers it produced.

---

## What is deliberately wrong with the data

30 defect classes, all logged to `ap_control.ops.synthetic_defect_ledger` with
the rule expected to catch each one. Grouped by what they teach:

* **Duplicates** — near-duplicates with punctuation variance on the supplier's
  invoice number, the same invoice arriving via two channels under different
  document ids, and identical primary keys emitted twice in one extract.
* **Fraud signals** — bank account changed with no verification event and a
  payment following within 14 days; an approver who also created the vendor.
* **Sign and grain** — credit memos with positive amounts, invoices with negative
  ones, header gross not equal to lines plus tax, over-receipts beyond tolerance,
  unmatched GR/IR on both sides.
* **Format chaos** — three date formats in one column, the 31st of February,
  SAP's eight-zero null date, `1.234,56` next to `$1,234.56`, blank currency
  codes, trailing spaces and lower-case join keys.
* **Completeness** — null vendor, null supplier invoice number, null capture
  timestamp (which quietly makes cycle time uncomputable), missing tax codes.
* **Referential** — invoice lines whose header was withheld from the extract,
  invoices referencing a vendor that is not in the master, postings to an
  inactive GL account, payment terms absent from `value_domain_mapping`.
* **Timeliness** — corrections to closed periods arriving past TEN003's 14-day
  lateness window, in a file that looks exactly like any other.

Plus three schema-drift events, one per tenant, each a different class:

| Event | Tenant | Type | Class | Expected handling |
|---|---|---|---|---|
| DRIFT_001 | TEN001 | column added (`ZUONR`, appended) | ADDITIVE | auto-accept, log, rescue the value |
| DRIFT_002 | TEN002 | `INVOICE_AMOUNT` becomes a string with a currency symbol | BREAKING | halt this source only |
| DRIFT_003 | TEN003 | `po-number` renamed to `purchase-order-number` | BREAKING | halt; resolution is a config change |

The two breaking drifts are dangerous in opposite ways. DRIFT_002 makes
`try_cast` return null, so totals collapse silently. DRIFT_003 makes a mapped
column vanish, so PO coverage drops to zero overnight. Neither raises an error on
read, which is exactly why `schema_evolution: RESCUE` plus a schema contract is
the design rather than trusting inference.

---

## Design decisions this code assumes

Set in Step 1 and confirmed since. Downstream code depends on all twelve; these
are the five that shape the foundation most.

* **D1 — isolation.** Shared tables with a `tenant_id` row filter. Logical
  separation only, confirmed 18 Sep 2026: no plumbing for a future physical
  split. Row filters plus a negative isolation test are therefore the *whole* of
  the control, and both are mandatory release gates in Step 6.
* **D2 — keys.** `tenant_id` is the first element of every primary and join key,
  never nullable, and set from config rather than read from the data. Notebook 04
  fails the release if any tenant lacks an ABORT-severity `tenant_id` rule.
* **D6 — DQ severities.** Three, each with a threshold: `ABORT` stops the batch,
  `QUARANTINE` diverts the rows, `WARN` records and continues. A framework with
  one severity is a framework that gets switched off.
* **D9 — schema drift.** Additive drift auto-accepts; breaking drift halts that
  one source and nothing else. A rename is resolved by a steward accepting the
  new shape (notebook 14) **and** a mapping change in `config/` with an audit
  trail — never by defensive SQL shipped in advance. During the transition the
  accepted contract tolerates both names, because a second feed (TEN003's
  corrections export) may move later than the first.
* **D12 — duplicates.** Scored, never suppressed, never written back to the
  source. AP duplicate detection that auto-deletes is how a real payment goes
  missing.

---

## Free Edition limits, and what production would do instead

| Here | Why | Production |
|---|---|---|
| `CREATE CATALOG` may be blocked; notebook 00 offers a `schema_fallback` layout | Free Edition restricts catalog creation | Four catalogs, created by Terraform, with environment separation |
| Groups created via the SDK with a graceful fallback, grants that log and skip missing principals | No `CREATE GROUP` in SQL, no account console access | SCIM-provisioned groups from the identity provider, entitlements as code |
| Managed volumes for the landing zone | No external locations or storage credentials | External locations on cloud storage with a storage credential per environment |
| Row filters and column masks documented in `access_model`, applied in Step 6 | Feature availability varies on Free Edition | Row filters and column masks on every silver and gold table, tested by a negative isolation test in CI |
| TEN001 materialises 3 full snapshots, not 21 | 21 snapshots of the invoice ledger is minutes of writing for no extra learning | One snapshot per day; the ingestion framework is identical either way |
| `scale=0.04` by default (0.2 for the full pass) | Serverless compute limits | No such knob: you get whatever the source sends |
| Test-harness tables live in `ap_control.ops` | Fewer catalogs to create | A separate `ap_test` catalog, so nothing in production can depend on knowing where the defects are |

Everywhere the code simplifies, the comment next to it says what production would
do. That habit is worth more than the code.

---

## Onboarding a tenant

The short version, and the thing Step 6 grades:

1. A row in `config/tenants.yaml`.
2. A row in `config/source_systems.yaml`, plus SLA and tolerance rows.
3. One `ingestion.yaml` row per entity the source supplies.
4. `schema_mapping` rows mapping that source's fields to canonical fields, with
   `transform_expr` doing any format repair.
5. `value_domain_mapping` rows reconciling the tenant's vocabulary.
6. Run notebook 02, then 04.

No new code. If a new tenant needs a code change, the metadata model has a gap,
and finding that gap is the point of the exercise.
