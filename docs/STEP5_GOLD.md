# Step 5 — Gold and consumption

Notebooks 30–34. Thirty tables — 13 dimensions, 13 facts, 4 aggregates — each
written by a **registered** builder (`gold/registry.py`) through the same
orchestrator as silver. An unregistered gold job fails the plan: every gold table
is a modelling decision about grain and keys, and config alone must not be able
to conjure one. `ap_gold.semantic.model_catalogue` states every table's grain,
key and history behaviour.

## Waves (derived from `depends_on`)

| Wave | Content |
|---|---|
| 1 | 13 dimensions, plus DQ results, approval events, duplicate candidates, GR/IR snapshot |
| 2 | invoice header, payment, vendor change event, invoice lifecycle, DQ scorecard aggregate |
| 3 | invoice line, payment application, vendor spend aggregate |
| 4 | open-item snapshot, match exception, DPO aggregate, silver-to-gold reconciliation |
| 5 | aging aggregate |

## Things that will look wrong and are not

* **Business date.** Snapshots and aging use the latest extract date loaded
  (2026-07-21), not the wall clock. Override with the `as_of` widget.
* **Touchless 100% for TEN003.** It has no approval stream, so `touch_count` is
  always zero. `fact_invoice_lifecycle.approval_evidence = 'NONE'` and the
  operations tile labels it instead of ranking TEN003 first.
* **Orphan `vendor_key`s.** An invoice whose vendor is not in the master (planted
  ORPHAN_INVOICE_VENDOR, quarantined in silver) cannot reach gold; but a vendor
  quarantined or late leaves a NULL key, kept deliberately rather than dropping
  real spend. Integrity check 2 counts them.
* **Seven metrics are not implemented** (`vw_metric_catalogue` says why). A view
  returning a plausible number from data that cannot support it is worse than a
  documented gap.
* **Duplicate calibration refuses to run** (notebook 34): there are almost no
  dispositions. Weights fitted to 30 examples look rigorous and are noise.

## The four integrity checks (notebook 31, section 4)

1. **Row counts** — the obvious one.
2. **Orphan dimension keys** — NULL `vendor_key` in invoice and payment facts.
3. **Fan-out** — `fact_ap_payment_application` must exceed both payments and
   invoices settled; equal to one of them means the bridge collapsed to 1:1 and
   partial payments are lost.
4. **Point-in-time** — more than one fact row per invoice key means
   `resolve_scd2` matched two dimension versions. Uses half-open intervals
   (`valid_from <= d < valid_to`); a closed interval doubles facts on the day of a
   change. The notebook asserts zero.

## Exercises

**E1 — Read a builder against its silver table (★).** For `fact_invoice_line`,
list every silver column it reads, then `DESCRIBE ap_silver.ap.sl_invoice_line`.
*Grading:* which column does the builder have to fetch from the header because
lines do not carry it, and what would happen if it fetched it from the gold
header instead of silver?

**E2 — Make dashboard 5 a real isolation test (★★, prepares Step 6).** Tile 5.2
reports three tenants visible. Create a row-filter function on
`fact_ap_invoice_header` that admits only `TEN001` for your user (or, if row
filters are unavailable on your edition, a secure view `vw_invoice_isolated` with
the same predicate) and re-run tile 5.2. *Grading:* why does the tile itself
carry no tenant filter?

**E3 — Point-in-time (★★★).** Pick a TEN001 vendor with two versions in
`dim_vendor`. Find an invoice dated between the two `valid_from`s and show that
`fact_ap_invoice_header.vendor_key` resolves to the older version. Then change
`resolve_scd2`'s `<` to `<=` in a scratch copy and find the invoice that doubles.

**E4 — Store a forecast for a week (★★).** Run notebook 34 with `as_of` set to
seven consecutive dates (via notebook 31's `as_of` and a re-run of the snapshot
jobs) and watch forecast accuracy go from "too few" to a number. *Grading:* what
does the accuracy figure assume about payments made outside the forecast?
