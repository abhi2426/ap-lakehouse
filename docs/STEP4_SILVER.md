# Step 4 — Silver

Notebooks 20–24. Sixteen jobs: eleven bronze-fed entities through **one generic
conformance pipeline** (`silver/pipeline.py`), five business-logic builders
(`silver/builders.py`). None of them contains a tenant id or an ERP name;
notebook 21 asserts it before running.

## How one entity job works

```
bronze rows newer than the job's watermark (per tenant)
  │  conform: pull source fields out of `record`, evaluate every transform_expr,
  │           try_cast ONCE to the canonical type, translate code fields through
  │           value_domain_mapping (domain_binding), attach lineage and _version
  │  FULL_SNAPSHOT tenants: keys missing from the latest snapshot get a delete version
  ▼
every VERSION in scope ─► collapse exact duplicates (and count in-file ones)
  ▼
changed_fields vs the previous version (in batch, or the row already in silver)
  ▼
DQ gate: ROW + CROSS_ENTITY rules on every version  ─► pinned to ap_silver.staging
  ▼
choose: the latest version of each key that PASSED   ─► MERGE (SCD2 for vendor, bank)
        newer versions that failed                    ─► ap_silver.quarantine.q_<entity>
                                                         + quarantine_registry (OPEN)
  ▼
AGGREGATE rules on what will be written; reconciliation (DQ_REC_0702); watermark
```

**Latest that passed, not latest.** A late correction that fails the gate does not
overwrite the good version already in silver; on a first full load that contains
both, the good one still lands.

**Severity.** ABORT stops that tenant's slice of the job. QUARANTINE diverts rows;
if the tenant's failure rate exceeds `fail_threshold_pct` the outcome is
`THRESHOLD_BREACH` — an alert on the scorecard, not an abort (the planted defect
rates exceed several thresholds by design). WARN only records. A rule that cannot
resolve its columns is `NOT_EVALUATED`, with the reason, never a silent pass.

## Expected numbers (scale 0.04, first real run)

See `docs/RUNBOOK.md` Session 3 for the table produced by the last end-to-end run.
The shape to expect:

* all 16 jobs `SUCCEED`; the dry run on a fresh lake shows five derived jobs as
  `SKIPPED` because their inputs do not exist yet — that is correct;
* `SL_INVOICE_HEADER` evaluates 14 rules; `SL_INVOICE_LINE` evaluates 4 of 5 —
  `DQ_INV_LIN_0403` is NOT_EVALUATED because no mapping produces a line-level
  `tax_amount` (exercise E3);
* the planted TEN003 `PCE` finding quarantines invoice lines, PO lines and
  receipts (exercise E1), and three-way match under-reports TEN003 until it is
  fixed;
* re-running notebook 21 straight away: every entity job reports
  `NO_NEW_DATA (watermarks current)`; only the five derived jobs recompute.

## The planted finding you must not mistake for a bug

TEN003 sends `PCE` for "each" and `value_domain_mapping` has no row for it. The
domain lookup maps it to NULL and `DQ_ALL_0504` quarantines those rows. Two
knock-on effects are also correct: TEN003 invoice lines whose header is fine but
whose lines are quarantined, and a three-way match that cannot match a line that
never reached silver.

## Exercises

**E1 — Fix a domain gap without touching code (★, do this one).** Notebook 23:
run with `entity` blank and read the unmapped-domain report — it prints the YAML
line. Add `TEN003 / UOM / PCE → EACH` to `config/value_domains_silver.yaml` (the
header has it ready), re-run 02, then 23 with `entity=invoice_line, dry_run=false`,
then `po_line`, then `po_history`. Re-run 21 to refresh the derived tables.
*Verify:* no NULL `uom` for TEN003 in `sl_invoice_line`; registry rows
`RESOLVED_CORRECTED` with a `redrive_batch_id`. *Grading:* the payment-terms
values in the same report (`ZZ99`, `TBC`, ...) are planted defects. Why must they
NOT be mapped to something plausible?

**E2 — Change a severity in config (★★).** Make `DQ_INV_HDR_0104` (null capture
timestamp) QUARANTINE instead of WARN in `config/dq_rules.yaml`, re-run 02 and 21
with `full_reload=true`. Watch the scorecard and the cycle-time KPI in Step 5.
Then put it back. *Grading:* which downstream metric would silently improve if
this rule were QUARANTINE, and why is that a reason to keep it a WARN?

**E3 — Make a rule evaluable with one mapping row (★★).** `DQ_INV_LIN_0403` is
NOT_EVALUATED: no `tax_amount` on lines. Add a `tax_amount` canonical field for
TEN002 (`TAX_AMOUNT`) and TEN003 (`tax-amount`) invoice lines in
`config/schema_mapping.yaml`, re-run 02 and 21 with `full_reload=true`. The rule
now evaluates for those tenants, with no code change. *Grading:* why does it
stay NOT_EVALUATED for TEN001, and what would you need from the source?

**E4 — Break SCD2 and catch it (★★★).** In a scratch copy of `sl_vendor`, set
`is_current = true` on one closed version, point notebook 24's check at it, and
confirm it reports "more than one current version". *Grading:* write the gold
query that would silently double that vendor's spend.
