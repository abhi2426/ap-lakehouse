# Step 3 — Ingestion to bronze

Notebooks 10–14. One generic engine (`src/ap_platform/ingestion/engine.py`)
ingests every tenant and entity from `ingestion_config`. There is no per-tenant
code; `notebooks/21` and `31` assert the same of silver and gold.

## What bronze looks like and why

One table per entity (`ap_bronze.ap.br_<entity>`), shared by all tenants. The
record is a `MAP<STRING,STRING>` of every contract field under its **source**
name, as the text the file contained:

```sql
SELECT tenant_id, record['INVOICE_AMOUNT'], record['total'], record['RMWWR']
FROM ap_bronze.ap.br_invoice_header LIMIT 5;
```

Three ERPs with three vocabularies share one table, a new source column needs
no DDL, and nothing is lost to an early cast. Typing is silver's job.

## The control loop for one source

```
discover files ─► skip what the ledger says is INGESTED/POISON
              ─► open BREAKING drift for this source?  ─► HALTED (nothing read)
              ─► for each pending file, oldest first:
                    profile it (header + 400-row sample, standard library)
                    unreadable?           ─► POISON: copy to poison volume, skip
                    compare to contract:
                      ADDITIVE drift      ─► log once, accept, values rescued
                      BREAKING drift      ─► log, HALT here; this and later files stay pending
              ─► read accepted files, grouped by identical shape
              ─► bronze DQ (DQ_ALL_0001/0002/0601)
              ─► ledger IN_PROGRESS ─► append rows ─► ledger INGESTED
```

**Exactly-once** comes from the ledger, not a stream checkpoint: a re-run with
nothing new changes nothing, and a file left IN_PROGRESS by a crash has its
partial rows deleted before it is read again. Auto Loader is optional
(`use_autoloader=true`) and only discovers files; parsing stays in the engine.

**Type drift needs evidence.** A contract column is NUMBER when ≥ 90% of the
baseline sample parses. A later file has changed type only when fewer than 50%
of its values parse *and* at least 3 do not. Without that, two injected
`1.234,56` values in a six-row delta file halted a source on day 3.

## Expected numbers (scale 0.04)

| | TEN001 | TEN002 | TEN003 | Total |
|---|---|---|---|---|
| Files landed | 30 | 191 | 154 | 375 |
| Rows landed | 28,909 | 12,668 | 3,955 | 45,532 |
| First run of 12: files / rows ingested | 30 / 28,909 | 182 / 12,604 | 146 / 3,916 | |
| After exercise E2 | 30 / 28,909 | 191 / 12,668 | 154 / 3,955 | 375 / 45,532 |

At scale 0.2: 415 files and 226,140 rows (TEN001 143,048 · TEN002 63,162 ·
TEN003 19,930).

The first run halts two sources and that is correct:

| Drift | Source | Detected as | Action |
|---|---|---|---|
| DRIFT_001 | TEN001 invoice_header, day 8 | `COLUMN_ADDED _c18` (ZUONR; headerless, so positional) | ADDITIVE → accepted; values rescued |
| DRIFT_002 | TEN002 invoice_header, day 12 | `TYPE_CHANGED INVOICE_AMOUNT` | BREAKING → halted, 9 files held back |
| DRIFT_003 | TEN003 invoice_header, day 15 | `COLUMN_RENAMED po-number -> purchase-order-number` | BREAKING → halted, 8 files held back |

Rescued rows on the first run: TEN001 `invoice_header` 1,858 (every row of the
two snapshots after day 8 carries `_c18`), plus a handful in `vendor_bank`
(formatted account numbers in a NUMBER column).

## Exercises

**E1 — Idempotency (★).** Re-run notebook 12 unchanged. Every source must report
`--` and no table's row count may move. *Grading:* also explain what would have
happened if the ledger were written only after the append and the job died in
between — then find the code that makes that safe.

**E2 — Unhalt a drifted source (★★, do this one).** Notebook 14 walks through it:
read the drift, accept TEN003's rename (`operation=unhalt`, check the printed
diff, then `apply=true`), change the TEN003 `po_number` mapping to
`coalesce(nullif(upper(trim(\`purchase-order-number\`)),''), nullif(upper(trim(\`po-number\`)),''))`,
re-run notebook 02, then notebook 12. Then unhalt TEN002. *Grading:* why does the
accepted contract keep `po-number` as an optional column? (Hint: look at what
the `invoices_corrections_*` files contain after day 15.) Why did TEN002 need no
mapping change, and what would have reached silver if it had?

**E3 — Poison a file (★★).** Copy any TEN002 JSON file to a new name in the same
folder, truncate it to half a line, and re-run 12. It must land in
`/Volumes/ap_bronze/landing/poison/` with status `POISON` in the ledger, and every
other file must load. *Grading:* where is "unreadable" decided, and why is it
decided before Spark reads anything?

**E4 — Replay (★★★).** Use notebook 14 `replay_batch` on the batch that loaded
TEN002's day-5 to day-7 files (find it in `file_ledger`), then re-run 12.
Row counts must come back identical. *Grading:* what would go wrong in silver
if bronze were not append-only?
