# Step 2 exercises — the foundation

Twelve exercises against the code in this repo. They are ordered so that each one
leans on the previous, and every one of them is answerable **from the control
plane and the landed files alone** — no ingestion code exists yet, which is the
point. If you can't answer a question here, Step 3 will build on sand.

Each exercise states what to do, what "done" looks like, and a grading note
saying what separates a passing answer from a good one. Solutions are not
printed: the control plane is queryable, so every answer is checkable.

Run notebooks `00 → 01 → 02 → 03 → 04` first, with `scale=0.2`.

---

## Group A — read the control plane

### E1. Answer a question without reading a file (★)

Without opening any file in the landing volume, write one SQL statement against
`ap_control.config` that returns, for every tenant, the number of entities it
supplies, whether it supplies approval events, and its load type.

**Done:** three rows, and TEN003 shows `approval_event = false`.

**Grading note.** A passing answer joins `ingestion_config` to `tenant_registry`.
A good answer notices that "supplies approval events" is not a column anywhere —
it is the *absence* of a row — and uses a left join or a `filter(...)` over the
entity list rather than inventing a flag. The absence of a row is data. Most
people reach for a boolean column that isn't there and then propose adding one;
adding it would create two sources of truth for the same fact.

### E2. The fiscal calendar trap (★★)

`tenant_registry` says TEN003 runs JUL–JUN and TEN002 runs a 4-4-5 calendar.
Write the query that returns, for an arbitrary posting date, the fiscal year and
period for each of the three tenants.

**Done:** for `2026-03-15`, TEN001 returns FY2026/P03, TEN003 returns FY2026/P09.

**Grading note.** TEN002 is the interesting one: 4-4-5 cannot be computed from a
date with arithmetic, because period boundaries are *declared*, not derived. A
passing answer computes TEN001 and TEN003 and hardcodes or fudges TEN002. A good
answer states plainly that 4-4-5 needs a period table, notes that
`GD_DIM_DATE` in `transformation_config` is where it will live, and writes the
query against that future table's shape rather than pretending arithmetic works.
Recognising which problems need a table is most of dimensional modelling.

### E3. Config referential integrity, your own version (★★)

Notebook 02 ends with a referential-integrity check. Add two checks it does not
currently make, and make them fail on purpose before you make them pass.

**Done:** two new assertions, each demonstrated failing.

**Grading note.** Weak additions restate existing checks with different SQL.
Strong additions find a class of error the current check cannot see — for
example, a `dq_rule_registry` rule whose `scope_entity` is not in
`ingestion_config` for any tenant (a rule that can never fire), or a
`transformation_config` job whose `depends_on` names a job that doesn't exist (a
DAG with a dangling edge). Both are silent failures: nothing errors, the work
just never happens. A check that can only fire on a typo is worth less than a
check that fires on a design mistake.

---

## Group B — read the landed files

### E4. Reconcile the manifest to the volume (★)

Notebook 03 writes a manifest. Independently walk the inbox volume and prove the
manifest is complete and accurate: every file listed exists, every file present
is listed, and the row counts agree.

**Done:** three zero-row discrepancy sets.

**Grading note.** A passing answer compares paths. A good answer also compares
row counts and catches the one genuinely awkward case: TEN001's `snapshot_days`
means most days have *no* file, and "no file" must be distinguishable from
"missing file". If your check can't tell those apart, it will page someone at
3am for a source that is behaving correctly.

### E5. Find the drift without being told (★★)

Three schema-drift events are in the data. Find all three by inspecting landed
files only — do not read `defects.py` or the drift table.

**Done:** tenant, entity, date, and drift class for each.

**Grading note.** The TEN001 one is findable by field count. TEN003's is findable
by header diff. TEN002's is the one that matters: nothing about the file is
malformed, the JSON parses, the field is present on every record, and only the
*type* changed. A good answer explains why this is the dangerous class — a
downstream `try_cast` yields null rather than an error, so the invoice total
silently drops out of every sum — and proposes a detection method that would
have caught it (per-field type profiling per batch, compared to the previous
batch's profile).

### E6. Date and number archaeology (★★)

Enumerate every distinct date format and every distinct number format present in
the TEN003 files. Write the `transform_expr` that parses each, and prove your
expression returns null for exactly the values that are genuinely unparseable and
for no others.

**Done:** a coalesce chain, plus the count of true unparseables, matching the
`DATE_UNPARSEABLE` count in the defect ledger.

**Grading note.** The trap is a silent over-catch. `try_to_date(x,'dd/MM/yyyy')`
and `try_to_date(x,'MM/dd/yyyy')` both succeed on `03/04/2026` and produce
different dates, so a coalesce chain ordered wrongly parses everything and is
wrong about a twelfth of it. A good answer either establishes format from a
locale declared in config rather than guessing per value, or flags the ambiguous
subset for review. "It parsed" is not "it is correct" — this is the single most
common source of quiet wrongness in cross-ERP date handling.

### E7. Which defects can you see from bronze alone? (★★★)

Take the 30 defect codes in `defects.py`. Partition them into: detectable from a
single file; detectable only across files within a tenant; detectable only across
tenants or against reference data; not detectable at bronze at all.

**Done:** a four-way partition with a one-line justification each.

**Grading note.** The fourth bucket is the one to get right. `BANK_CHANGE_UNVERIFIED`
needs a vendor history *and* a payment, so it cannot exist before silver builds
SCD2 — meaning no bronze rule can ever catch it, and writing one is wasted work.
Conversely `EXACT_DUP_PK` is visible in a single file and should be caught at the
earliest possible moment. A good answer draws the conclusion: the partition tells
you which layer each of your 25 DQ rules belongs in, and a rule placed in the
wrong layer either can't fire or fires too late to be actionable.

---

## Group C — break it

### E8. Make the loader lie (★★★)

`config_loader.py` claims to be idempotent and converging. Find an edit to a YAML
file that it applies incorrectly, or prove no such edit exists.

**Done:** either a reproducible defect, or an argument covering add, modify,
delete, rename-key, and reorder.

**Grading note.** Renaming a key is the interesting case: the loader sees a
delete plus an insert, so anything referencing the old key breaks, and the
soft-delete pass makes this survivable rather than catastrophic — but only if
you check. A good answer also tests two rows in different files that resolve to
the same target key, and says which file wins and whether that's deterministic
across filesystem ordering. "Idempotent" is a claim about repeated runs;
"converging" is a claim about the end state matching the YAML. They are different
properties and the second is harder.

### E9. Onboard a fourth tenant, badly (★★)

Add TEN004 to `tenants.yaml` only — no source system, no ingestion rows, no
mappings. Run 02, then 04.

**Done:** notebook 04 fails with a message that names what is missing.

**Grading note.** If 04 passes, the validation is decorative and you should fix
it. A good answer goes further and asks whether failing is even correct: a tenant
in `ONBOARDING` status legitimately has no ingestion config yet, so the check
should be conditioned on `onboarding_status`, not on existence. Validation that
can't express "not yet" forces people to disable it, and disabled validation is
worse than none.

### E10. Poison the landing zone (★★)

Put a file in the inbox that matches the path contract but is unreadable: wrong
encoding, truncated mid-record, and zero bytes. Predict what Step 3 will do with
each, and write the assertion you want to hold.

**Done:** three files, three predictions, three assertions.

**Grading note.** The zero-byte file is the one people get wrong. It is not an
error in most readers — it is a successful read of nothing, which looks exactly
like a quiet day. A good answer requires row counts to be reconciled against an
expectation rather than merely recorded, which is what `reconciliation_control`
is for, and notes that this is why `DQ_REC_0702` is ABORT rather than WARN.

---

## Group D — argue

### E11. Attack D1 (★★★)

The decision log fixes logical tenant separation via `tenant_id` row filters.
Write the strongest case against it, then the rebuttal.

**Done:** both sides, with the specific failure mode named.

**Grading note.** The strong case is not "someone might see another tenant's
data" — it is that the control is a *predicate*, so every new access path is a
new place to forget it, and a single view created without the filter, or a job
running as a service principal that bypasses it, silently defeats the whole
model. The rebuttal is not "we'll be careful": it is that the control is
testable, the negative isolation test is a release gate, and physical separation
buys a weaker guarantee than people assume because backups, logs, and the control
plane itself stay shared. A good answer ends with what would change its mind — a
named contractual or regulatory requirement, not a feeling about risk.

### E12. The metadata-model gap hunt (★★★)

Find something a plausible fourth ERP would need that this metadata model cannot
express without a code change. One real gap is enough.

**Done:** the gap, the code change it would force, and the schema change that
would avoid it.

**Grading note.** Cheap answers name a missing enum value, which is a data change
and not a gap. Real gaps are structural: a source that supplies invoices split
across two files that must be joined before mapping (`ingestion_config` assumes
one file per entity); a source where the primary key is only unique within a
fiscal year (expressible, but only because the SAP trap forced it — check); a
source needing a mapping conditional on a *row value* rather than a field name
(`schema_mapping` has no predicate column). Finding one of these now is cheaper
than finding it in Step 6 under the capstone's time pressure, which is exactly
why this exercise is here rather than there.

---

## Scoring

| Band | Meaning |
|---|---|
| E1–E4 only | You can operate the foundation. |
| Through E7 | You understand what the data actually contains. |
| Through E10 | You can find where it breaks before it breaks in front of someone. |
| E11–E12 | You can defend and extend the design, which is the job. |

Do E5, E7 and E12 even if you skip the rest. Those three are the ones whose
answers change what you build in Steps 3 and 4.
