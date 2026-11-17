-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 — Control Plane DDL
-- MAGIC
-- MAGIC Creates every table in `ap_control`. No data is loaded here — that is
-- MAGIC `02_bootstrap_config`, which reads the repo YAML. This separation is the whole
-- MAGIC point of config-as-code: **structure is deployed, content is declared.**
-- MAGIC
-- MAGIC All DDL is `CREATE TABLE IF NOT EXISTS`, so this notebook is safe to re-run.
-- MAGIC Schema changes to control tables are made by editing this file and running an
-- MAGIC explicit `ALTER TABLE` in Cmd 20, never by dropping and recreating — the
-- MAGIC operational tables hold audit history that must survive a deployment.
-- MAGIC
-- MAGIC **Free Edition notes.** `CLUSTER BY` (liquid clustering) is used on the high-volume
-- MAGIC operational tables. If your runtime rejects it, remove the clause — these tables
-- MAGIC are small in the POC and clustering is a performance concern, not a correctness one.
-- MAGIC `TBLPROPERTIES (delta.enableChangeDataFeed = true)` is set on the config tables so
-- MAGIC Step 6 can prove *who changed which rule when* without a separate audit trigger.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 1 — Tenant and source registries
-- MAGIC Everything in the platform starts by resolving a `tenant_id` against
-- MAGIC `tenant_registry`. If a tenant is not here, no pipeline will run for it. That is
-- MAGIC deliberate: it makes onboarding an explicit, reviewable act.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.config.tenant_registry (
  tenant_id            STRING  NOT NULL COMMENT 'Immutable tenant key, e.g. TEN001. First element of every business key in the platform.',
  tenant_name          STRING  NOT NULL COMMENT 'Human-readable tenant name for display.',
  legal_entity_group   STRING           COMMENT 'Parent group, where several tenants belong to one corporate group.',
  erp_type             STRING  NOT NULL COMMENT 'SAP_ECC | SAP_S4 | ORACLE_FUSION | COUPA | NETSUITE | OTHER. Drives which reader and mapping set applies.',
  erp_version          STRING           COMMENT 'Source version, because field availability differs across releases.',
  source_timezone      STRING  NOT NULL COMMENT 'IANA timezone of the source system. All source timestamps are interpreted in this zone before conversion to UTC.',
  functional_currency  STRING  NOT NULL COMMENT 'ISO 4217 currency the tenant books in. Second of the three amounts stored on every monetary fact (decision D3).',
  group_currency       STRING  NOT NULL COMMENT 'ISO 4217 reporting currency for group consolidation. Third of the three amounts.',
  fiscal_calendar      STRING  NOT NULL COMMENT 'CALENDAR | APR_MAR | JUL_JUN | CUSTOM. Determines period derivation in dim_date.',
  fiscal_year_offset   INT              COMMENT 'Months the fiscal year leads the calendar year. 0 for calendar, 3 for Apr-Mar.',
  isolation_tier       STRING  NOT NULL COMMENT 'SHARED_ROW_FILTER | DEDICATED_SCHEMA | DEDICATED_CATALOG. Per decision D1 all POC tenants are SHARED_ROW_FILTER.',
  onboarding_status    STRING  NOT NULL COMMENT 'PENDING | IN_PROGRESS | ACTIVE | SUSPENDED | OFFBOARDING. Only ACTIVE tenants are picked up by scheduled jobs.',
  sla_tier             STRING  NOT NULL COMMENT 'GOLD | SILVER | BRONZE. Joined to sla_config for freshness targets and alert routing.',
  retention_years      INT     NOT NULL COMMENT 'Retention for transactional records, per decision D8. Drives VACUUM and archival policy.',
  pii_jurisdiction     STRING           COMMENT 'Governing privacy regime, e.g. GDPR, CCPA, DPDP. Drives erasure handling.',
  is_active            BOOLEAN NOT NULL COMMENT 'Soft-delete flag. Deactivating a tenant halts its pipelines without destroying its data or history.',
  effective_from       DATE    NOT NULL COMMENT 'Date the tenant became contractually live.',
  effective_to         DATE             COMMENT 'Offboarding date. NULL while active.',
  config_version       STRING           COMMENT 'Hash of the YAML block this row was loaded from, for drift detection against the repo.',
  updated_at           TIMESTAMP        COMMENT 'Last write by the bootstrap notebook.',
  updated_by           STRING           COMMENT 'Identity that ran the bootstrap.'
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'One row per tenant. The authoritative answer to "does this tenant exist, and what are its conventions?"';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.config.source_system_registry (
  tenant_id          STRING  NOT NULL COMMENT 'FK to tenant_registry.',
  source_system_id   STRING  NOT NULL COMMENT 'Unique id of a source system within a tenant, e.g. TEN001_SAP_PRD.',
  source_system_name STRING  NOT NULL COMMENT 'Display name.',
  system_type        STRING  NOT NULL COMMENT 'ERP | PROCUREMENT | OCR | BANK | MASTER_DATA. An AP tenant usually has more than one.',
  delivery_mechanism STRING  NOT NULL COMMENT 'VOLUME_FILE | SFTP | API | JDBC | CDC_STREAM. Free Edition supports VOLUME_FILE only.',
  file_encoding      STRING           COMMENT 'Source file encoding. UTF-8 is not a safe assumption for SAP extracts.',
  contact_owner      STRING           COMMENT 'Named person accountable for the extract at the tenant end. Required before go-live.',
  extract_schedule   STRING           COMMENT 'Cron expression describing when the tenant delivers, used to detect a missed delivery.',
  is_active          BOOLEAN NOT NULL COMMENT 'Soft-delete flag.',
  config_version     STRING           COMMENT 'Config hash.',
  updated_at         TIMESTAMP        COMMENT 'Last bootstrap write.'
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Source systems per tenant. A tenant with two ERPs has two rows and two mapping sets.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 2 — Ingestion configuration
-- MAGIC This table is the reason Step 3 needs no per-tenant code. One row here produces
-- MAGIC one ingestion task. Onboarding a new source is an `INSERT`.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.config.ingestion_config (
  config_id            STRING  NOT NULL COMMENT 'Surrogate key: <tenant_id>__<source_system_id>__<entity>.',
  tenant_id            STRING  NOT NULL COMMENT 'FK to tenant_registry.',
  source_system_id     STRING  NOT NULL COMMENT 'FK to source_system_registry.',
  entity               STRING  NOT NULL COMMENT 'Canonical entity name, e.g. vendor_master, invoice_header. Determines the bronze target.',
  source_format        STRING  NOT NULL COMMENT 'CSV | DELIMITED | JSON | PARQUET | AVRO | XML.',
  delimiter            STRING           COMMENT 'Field delimiter for DELIMITED/CSV. Pipe for SAP-style extracts.',
  has_header           BOOLEAN          COMMENT 'Whether row 1 is a header. SAP extracts frequently have none.',
  quote_char           STRING           COMMENT 'Quote character. Empty string disables quoting, which some ERP extracts require.',
  multiline            BOOLEAN          COMMENT 'Whether records span physical lines. Expensive; enable only when the source demands it.',
  source_path_pattern  STRING  NOT NULL COMMENT 'Volume path with {tenant_id}/{yyyy}/{mm}/{dd} tokens. Resolved at runtime, never hardcoded.',
  file_glob            STRING           COMMENT 'Filename pattern, e.g. INVOICE_HEADER_*.dat. Guards against picking up a neighbouring entity.',
  target_table         STRING  NOT NULL COMMENT 'Fully qualified bronze table.',
  load_type            STRING  NOT NULL COMMENT 'FULL_SNAPSHOT | INCREMENTAL | CDC. Drives whether bronze appends, and how silver merges.',
  watermark_column     STRING           COMMENT 'Source column carrying last-changed time, for INCREMENTAL. NULL for FULL_SNAPSHOT.',
  cdc_sequence_column  STRING           COMMENT 'Monotonic ordering column for CDC. Without it, out-of-order deltas apply wrongly (decision D4).',
  cdc_operation_column STRING           COMMENT 'Column holding I/U/D. Deletes become tombstones, never physical deletes in bronze.',
  primary_key_columns  STRING  NOT NULL COMMENT 'Comma-separated SOURCE column names forming the natural key. tenant_id is added by the framework.',
  schema_evolution     STRING  NOT NULL COMMENT 'RESCUE | ADDNEWCOLUMNS | FAILONNEWCOLUMNS. Per decision D9 the default is RESCUE.',
  schema_contract_ref  STRING           COMMENT 'Path to the registered JSON schema contract used for drift classification.',
  expected_frequency   STRING           COMMENT 'DAILY | WEEKLY | MONTHLY | ON_DEMAND. A missed window raises a freshness alert.',
  max_file_age_hours   INT              COMMENT 'Freshness SLA. Older newest-file than this means the tenant has stopped delivering.',
  late_arrival_days    INT              COMMENT 'Bounded lateness window from decision D4. Corrections older than this go to review, not straight through.',
  reader_options       STRING           COMMENT 'JSON blob of extra reader options, so a new option never needs a code change.',
  is_active            BOOLEAN NOT NULL COMMENT 'Set FALSE to stop a single source without touching the rest of the tenant.',
  load_priority        INT              COMMENT 'Lower runs first. Reference data loads before transactions.',
  config_version       STRING           COMMENT 'Config hash.',
  updated_at           TIMESTAMP        COMMENT 'Last bootstrap write.'
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'One row per tenant x source x entity. The ingestion framework iterates this table and nothing else.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 3 — Schema and value mapping
-- MAGIC `schema_mapping` maps field names and types. `value_domain_mapping` maps the
-- MAGIC *contents* of a field. Both are needed: TEN001 calls the field `ZTERM` and puts
-- MAGIC `Z030` in it; TEN002 calls it `TERMS_NAME` and puts `30 Net`. Renaming the column
-- MAGIC is only half the job.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.config.schema_mapping (
  mapping_id          STRING  NOT NULL COMMENT 'Surrogate key: <tenant_id>__<entity>__<canonical_field>.',
  tenant_id           STRING  NOT NULL COMMENT 'FK to tenant_registry.',
  source_system_id    STRING  NOT NULL COMMENT 'FK to source_system_registry.',
  entity              STRING  NOT NULL COMMENT 'Canonical entity name.',
  source_field        STRING           COMMENT 'Field name as it appears in the source. NULL when the canonical field is derived rather than mapped.',
  source_datatype     STRING           COMMENT 'Declared source type, used to build the read schema and to detect type drift.',
  canonical_field     STRING  NOT NULL COMMENT 'Target field in the silver canonical model.',
  canonical_datatype  STRING  NOT NULL COMMENT 'Target Spark type, e.g. DECIMAL(18,2), DATE, TIMESTAMP, STRING.',
  transform_expr      STRING           COMMENT 'Spark SQL expression evaluated against the bronze row. This is where format chaos is repaired, e.g. to_date(BLDAT, "yyyyMMdd").',
  is_mandatory        BOOLEAN NOT NULL COMMENT 'TRUE means a null here is a DQ failure, not an absence. Drives auto-generated completeness rules.',
  is_primary_key      BOOLEAN NOT NULL COMMENT 'Part of the canonical natural key.',
  is_sensitive        BOOLEAN NOT NULL COMMENT 'TRUE for bank account, tax id, personal contact. Drives masking in Step 6 (decision D7).',
  default_value       STRING           COMMENT 'Applied when the source omits the field entirely, e.g. a currency the source assumes.',
  ordinal             INT              COMMENT 'Position in positional/headerless files. Required when has_header is FALSE.',
  is_active           BOOLEAN NOT NULL COMMENT 'Soft-delete flag.',
  config_version      STRING           COMMENT 'Config hash.',
  updated_at          TIMESTAMP        COMMENT 'Last bootstrap write.'
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Source field to canonical field mapping. Three ERPs land in one model because of this table.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.config.value_domain_mapping (
  tenant_id        STRING  NOT NULL COMMENT 'FK to tenant_registry.',
  domain           STRING  NOT NULL COMMENT 'Logical domain: PAYMENT_TERMS | DOC_TYPE | BLOCK_REASON | TAX_CODE | UOM | MATCH_STATUS.',
  source_value     STRING  NOT NULL COMMENT 'Value exactly as it appears in the source, before trimming or case folding.',
  canonical_value  STRING  NOT NULL COMMENT 'Canonical value used throughout silver and gold.',
  canonical_label  STRING           COMMENT 'Display label for reporting.',
  sort_order       INT              COMMENT 'Presentation order for reports.',
  attributes       STRING           COMMENT 'JSON of domain-specific attributes, e.g. {"net_days":30,"disc_pct":2.0,"disc_days":10} for payment terms.',
  is_active        BOOLEAN NOT NULL COMMENT 'Soft-delete flag.',
  config_version   STRING           COMMENT 'Config hash.',
  updated_at       TIMESTAMP        COMMENT 'Last bootstrap write.'
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Tenant-specific code translation. An unmapped source value is a DQ failure, never a silent pass-through.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 4 — Data quality rules
-- MAGIC Three severities per decision D6. Every rule carries a threshold so a 0.01%
-- MAGIC failure and a 40% failure are not treated identically, and effective dates so a
-- MAGIC rule can be introduced without retroactively failing history.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.config.dq_rule_registry (
  rule_id           STRING  NOT NULL COMMENT 'Stable rule identifier, e.g. DQ_INV_HDR_0007. Never reused after retirement.',
  rule_name         STRING  NOT NULL COMMENT 'Short human-readable name shown on the DQ scorecard.',
  tenant_id         STRING  NOT NULL COMMENT 'Specific tenant_id, or the literal ALL for a rule every tenant must satisfy.',
  layer             STRING  NOT NULL COMMENT 'BRONZE | SILVER | GOLD. Determines where in the pipeline the rule is evaluated.',
  entity            STRING  NOT NULL COMMENT 'Canonical entity the rule applies to.',
  scope             STRING  NOT NULL COMMENT 'ROW | AGGREGATE | CROSS_ENTITY. ROW rules can quarantine; AGGREGATE rules can only abort or warn.',
  dq_dimension      STRING  NOT NULL COMMENT 'COMPLETENESS | VALIDITY | UNIQUENESS | CONSISTENCY | ACCURACY | TIMELINESS | REFERENTIAL_INTEGRITY.',
  rule_type         STRING  NOT NULL COMMENT 'NOT_NULL | UNIQUE | RANGE | REGEX | ALLOWED_VALUES | FK_EXISTS | CUSTOM_SQL | ROW_COUNT | SUM_MATCH.',
  rule_expression   STRING  NOT NULL COMMENT 'Spark SQL boolean expression that is TRUE for a PASSING record. Written positively so pass rate is a simple average.',
  parameters        STRING           COMMENT 'JSON for parameterised rule types, e.g. {"min":0,"max":100000000} or {"ref_table":"ap_silver.ap.sl_vendor"}.',
  severity          STRING  NOT NULL COMMENT 'ABORT | QUARANTINE | WARN. ABORT fails the batch; QUARANTINE diverts the row; WARN records and passes.',
  fail_threshold_pct DECIMAL(5,2)     COMMENT 'Failure percentage above which the severity escalates. NULL means any single failure triggers the severity.',
  business_impact   STRING           COMMENT 'What breaks downstream if this rule fails. Required: a rule nobody can justify gets disabled during the first incident.',
  remediation_hint  STRING           COMMENT 'What the on-call engineer should actually do. Goes into the alert payload.',
  owner             STRING           COMMENT 'Accountable role, e.g. AP_TRANSFORMATION_LEAD, DATA_STEWARD.',
  effective_from    DATE    NOT NULL COMMENT 'Rule applies to batches on or after this date. Lets a new rule skip historical data.',
  effective_to      DATE             COMMENT 'Retirement date. NULL while live.',
  is_active         BOOLEAN NOT NULL COMMENT 'Emergency off switch, separate from effective dates so disabling is auditable and reversible.',
  config_version    STRING           COMMENT 'Config hash.',
  updated_at        TIMESTAMP        COMMENT 'Last bootstrap write.'
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Every data quality rule in the platform. Change data feed is enabled so we can prove who changed a rule and when.';

-- COMMAND ----------

-- MAGIC %md ## Section 5 — Transformation, tolerance and SLA configuration

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.config.transformation_config (
  job_id             STRING  NOT NULL COMMENT 'Unique job identifier, e.g. SL_INVOICE_HEADER.',
  tenant_id          STRING  NOT NULL COMMENT 'Specific tenant_id or ALL for a job that runs once across tenants.',
  layer              STRING  NOT NULL COMMENT 'SILVER | GOLD.',
  target_table       STRING  NOT NULL COMMENT 'Fully qualified target table.',
  source_tables      STRING  NOT NULL COMMENT 'Comma-separated inputs, used to build the dependency graph automatically.',
  depends_on         STRING           COMMENT 'Comma-separated job_ids that must complete first. Enables topological scheduling.',
  write_mode         STRING  NOT NULL COMMENT 'MERGE | APPEND | OVERWRITE | SCD2_MERGE | SNAPSHOT.',
  merge_keys         STRING           COMMENT 'Comma-separated keys for MERGE. Must begin with tenant_id (decision D2).',
  scd_type           STRING           COMMENT 'NONE | SCD1 | SCD2. Per decision D5, SCD2 only on vendor, vendor_bank, gl_account, cost_center, payment_terms.',
  scd_tracked_columns STRING          COMMENT 'Columns whose change opens a new SCD2 version. Everything else updates in place.',
  sequence_column    STRING           COMMENT 'Ordering column for deduplication within a batch, e.g. source sequence or ingest timestamp.',
  partition_columns  STRING           COMMENT 'Partition columns. Usually empty because we prefer liquid clustering.',
  cluster_columns    STRING           COMMENT 'Liquid clustering keys. tenant_id first on every shared table.',
  snapshot_grain     STRING           COMMENT 'DAILY | MONTHLY for periodic snapshot facts. NULL otherwise.',
  optimize_frequency STRING           COMMENT 'How often OPTIMIZE runs: DAILY | WEEKLY | NEVER.',
  is_active          BOOLEAN NOT NULL COMMENT 'Soft-delete flag.',
  config_version     STRING           COMMENT 'Config hash.',
  updated_at         TIMESTAMP        COMMENT 'Last bootstrap write.'
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Silver and gold job registry. The orchestrator derives its DAG from depends_on rather than from a hand-drawn workflow.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### domain_binding
-- MAGIC Which canonical fields silver translates through `value_domain_mapping`,
-- MAGIC and into which column. Loaded from `config/domain_bindings.yaml`.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.config.domain_binding (
  binding_id     STRING  NOT NULL COMMENT 'Derived: <entity>__<source_field>.',
  entity         STRING  NOT NULL COMMENT 'Canonical entity whose silver job applies the lookup.',
  source_field   STRING  NOT NULL COMMENT 'Canonical field holding the source code, e.g. uom_src.',
  domain         STRING  NOT NULL COMMENT 'value_domain_mapping.domain to look the code up in.',
  target_field   STRING  NOT NULL COMMENT 'Silver column that receives the canonical value, e.g. uom.',
  on_unmapped    STRING  NOT NULL COMMENT 'QUARANTINE: DQ_ALL_0504 diverts the row. WARN: counted only.',
  is_active      BOOLEAN NOT NULL COMMENT 'Soft-delete flag.',
  config_version STRING           COMMENT 'Config hash.',
  updated_at     TIMESTAMP        COMMENT 'Last bootstrap write.'
)
USING DELTA
COMMENT 'Binds canonical code fields to value domains. Per entity, never per tenant: tenants differ only in their mapping rows.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.config.tolerance_config (
  tenant_id            STRING  NOT NULL COMMENT 'FK to tenant_registry.',
  company_code         STRING           COMMENT 'Optional narrowing to one company code. ALL applies tenant-wide.',
  tolerance_type       STRING  NOT NULL COMMENT 'PRICE | QUANTITY | AMOUNT | DATE | TAX.',
  lower_limit_pct      DECIMAL(9,4)     COMMENT 'Allowed negative variance as a percentage.',
  upper_limit_pct      DECIMAL(9,4)     COMMENT 'Allowed positive variance as a percentage.',
  lower_limit_abs      DECIMAL(18,2)    COMMENT 'Allowed negative variance as an absolute amount in functional currency.',
  upper_limit_abs      DECIMAL(18,2)    COMMENT 'Allowed positive variance as an absolute amount.',
  combine_rule         STRING           COMMENT 'AND means both the percentage and absolute test must pass; OR means either suffices.',
  block_on_breach      BOOLEAN NOT NULL COMMENT 'Whether breaching this tolerance sets a payment block or merely flags an exception.',
  effective_from       DATE    NOT NULL COMMENT 'Tolerances change; historical matches must be explainable against the tolerance in force at the time.',
  effective_to         DATE             COMMENT 'NULL while current.',
  is_active            BOOLEAN NOT NULL COMMENT 'Soft-delete flag.',
  config_version       STRING           COMMENT 'Config hash.',
  updated_at           TIMESTAMP        COMMENT 'Last bootstrap write.'
)
USING DELTA
COMMENT 'Three-way match tolerances. Effective-dated because a match outcome must be reproducible against the rules that applied on the day.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.config.sla_config (
  sla_tier            STRING  NOT NULL COMMENT 'GOLD | SILVER | BRONZE. Joined from tenant_registry.',
  layer               STRING  NOT NULL COMMENT 'BRONZE | SILVER | GOLD.',
  max_latency_minutes INT     NOT NULL COMMENT 'Maximum minutes from file arrival to data being queryable at this layer.',
  freshness_hours     INT     NOT NULL COMMENT 'Data older than this is considered stale and raises a freshness alert.',
  alert_channel       STRING           COMMENT 'Where the alert goes: EMAIL | SLACK | PAGERDUTY.',
  escalate_after_min  INT              COMMENT 'Minutes before an unacknowledged alert escalates.',
  is_active           BOOLEAN NOT NULL COMMENT 'Soft-delete flag.',
  updated_at          TIMESTAMP        COMMENT 'Last bootstrap write.'
)
USING DELTA
COMMENT 'Freshness and latency targets by tier. A tenant paying for GOLD gets a tighter target, and the platform can prove whether it met it.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 6 — Reference data
-- MAGIC Currency rates are effective-dated and typed, because per decision D3 every
-- MAGIC monetary fact stores the rate it used and must be reproducible.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ref.currency_rate (
  tenant_id       STRING  NOT NULL COMMENT 'ALL for a shared rate table, or a tenant_id where the tenant supplies its own rates.',
  from_currency   STRING  NOT NULL COMMENT 'ISO 4217 source currency.',
  to_currency     STRING  NOT NULL COMMENT 'ISO 4217 target currency.',
  rate_type       STRING  NOT NULL COMMENT 'SPOT | DAILY_MID | MONTH_END | BUDGET | REVALUATION. Using the wrong type is a classic silent error.',
  rate_date       DATE    NOT NULL COMMENT 'Date the rate applies to.',
  exchange_rate   DECIMAL(18,8) NOT NULL COMMENT 'Multiply the from_currency amount by this to get to_currency.',
  rate_source     STRING           COMMENT 'Provenance, e.g. ECB, tenant ERP TCURR extract. Auditors ask.',
  is_active       BOOLEAN NOT NULL COMMENT 'Soft-delete flag.',
  updated_at      TIMESTAMP        COMMENT 'Last load time.'
)
USING DELTA
COMMENT 'Effective-dated FX rates by type. Stored rate plus rate date on every fact makes historical conversions reproducible.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ref.payment_terms (
  tenant_id        STRING  NOT NULL COMMENT 'FK to tenant_registry, or ALL.',
  terms_code       STRING  NOT NULL COMMENT 'Canonical terms code after value-domain mapping.',
  terms_label      STRING           COMMENT 'Display label, e.g. "2/10 Net 30".',
  net_days         INT     NOT NULL COMMENT 'Days from baseline date to the due date.',
  discount_pct     DECIMAL(6,3)     COMMENT 'Early-payment discount percentage. NULL where no discount is offered.',
  discount_days    INT              COMMENT 'Days from baseline date within which the discount applies.',
  baseline_date_rule STRING         COMMENT 'INVOICE_DATE | POSTING_DATE | RECEIPT_DATE | EOM. Getting this wrong shifts every due date.',
  is_active        BOOLEAN NOT NULL COMMENT 'Soft-delete flag.',
  updated_at       TIMESTAMP        COMMENT 'Last load time.'
)
USING DELTA
COMMENT 'Canonical payment terms with the arithmetic needed to derive due date and discount date.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ref.gl_account (
  tenant_id       STRING  NOT NULL COMMENT 'FK to tenant_registry.',
  company_code    STRING           COMMENT 'Chart of accounts can differ by company code.',
  gl_account      STRING  NOT NULL COMMENT 'Account number as used in the source.',
  gl_account_name STRING           COMMENT 'Account description.',
  account_type    STRING           COMMENT 'ASSET | LIABILITY | EQUITY | REVENUE | EXPENSE.',
  account_group   STRING           COMMENT 'Grouping used for reporting rollups.',
  is_recon_account BOOLEAN         COMMENT 'TRUE for the AP reconciliation account. This is what the subledger must tie to (KPI 31).',
  is_grir_account BOOLEAN          COMMENT 'TRUE for the GR/IR clearing account, the basis of the GR/IR position.',
  is_active       BOOLEAN NOT NULL COMMENT 'Soft-delete flag. Posting to an inactive account is a DQ failure.',
  updated_at      TIMESTAMP        COMMENT 'Last load time.'
)
USING DELTA
COMMENT 'Chart of accounts, with the two flags the AP domain actually needs: reconciliation account and GR/IR clearing account.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ref.tax_code (
  tenant_id       STRING  NOT NULL COMMENT 'FK to tenant_registry.',
  country_code    STRING  NOT NULL COMMENT 'ISO country the tax code belongs to.',
  tax_code        STRING  NOT NULL COMMENT 'Canonical tax code after value-domain mapping.',
  tax_description STRING           COMMENT 'Description.',
  tax_rate_pct    DECIMAL(6,3)     COMMENT 'Rate percentage. Used to validate that tax amount is consistent with base amount.',
  tax_type        STRING           COMMENT 'VAT | GST | WITHHOLDING | SALES_USE | REVERSE_CHARGE | EXEMPT.',
  is_recoverable  BOOLEAN          COMMENT 'Whether input tax is recoverable. Drives the unrecovered-input-tax value lever.',
  effective_from  DATE    NOT NULL COMMENT 'Rates change by statute; historical invoices must validate against the rate then in force.',
  effective_to    DATE             COMMENT 'NULL while current.',
  is_active       BOOLEAN NOT NULL COMMENT 'Soft-delete flag.',
  updated_at      TIMESTAMP        COMMENT 'Last load time.'
)
USING DELTA
COMMENT 'Effective-dated tax codes with rates, so tax amount validity is checkable rather than assumed.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ref.company_code (
  tenant_id           STRING  NOT NULL COMMENT 'FK to tenant_registry.',
  company_code        STRING  NOT NULL COMMENT 'Legal entity code as used in the source.',
  company_name        STRING           COMMENT 'Legal entity name.',
  country_code        STRING           COMMENT 'ISO country of registration.',
  functional_currency STRING  NOT NULL COMMENT 'Booking currency. May differ from the tenant default for foreign subsidiaries.',
  fiscal_calendar     STRING           COMMENT 'Overrides the tenant calendar where a subsidiary differs.',
  is_active           BOOLEAN NOT NULL COMMENT 'Soft-delete flag.',
  updated_at          TIMESTAMP        COMMENT 'Last load time.'
)
USING DELTA
COMMENT 'Legal entities within a tenant. Reconciliation is performed per company code per currency, so this table is load-bearing.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 7 — Operational tables
-- MAGIC These are written by pipelines at runtime, not by the bootstrap. They are
-- MAGIC append-only by convention: correcting a run log by updating it destroys the
-- MAGIC evidence trail that makes the platform auditable.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ops.batch_run_log (
  batch_id          STRING  NOT NULL COMMENT 'UUID generated at batch start. Stamped onto every row the batch writes, so any record traces back to its run.',
  tenant_id         STRING  NOT NULL COMMENT 'Tenant the batch ran for, or ALL.',
  pipeline_name     STRING  NOT NULL COMMENT 'Logical pipeline, e.g. BRONZE_INGEST, SILVER_CONFORM.',
  layer             STRING           COMMENT 'BRONZE | SILVER | GOLD.',
  trigger_type      STRING           COMMENT 'SCHEDULED | MANUAL | BACKFILL | RETRY. Backfills must be distinguishable from normal runs.',
  run_mode          STRING           COMMENT 'NORMAL | REPLAY | DRY_RUN.',
  business_date     DATE             COMMENT 'Logical date the batch processes, which is not necessarily the day it ran.',
  status            STRING  NOT NULL COMMENT 'RUNNING | SUCCEEDED | FAILED | ABORTED_DQ | PARTIAL.',
  start_ts          TIMESTAMP NOT NULL COMMENT 'Batch start, UTC.',
  end_ts            TIMESTAMP        COMMENT 'Batch end, UTC. NULL while running.',
  duration_seconds  DOUBLE           COMMENT 'Derived duration for trend analysis.',
  records_read      BIGINT           COMMENT 'Rows read from source.',
  records_written   BIGINT           COMMENT 'Rows written to target.',
  records_quarantined BIGINT         COMMENT 'Rows diverted by QUARANTINE rules.',
  records_rejected  BIGINT           COMMENT 'Rows dropped, which should normally be zero.',
  files_processed   INT              COMMENT 'File count for file-based sources.',
  error_message     STRING           COMMENT 'Truncated exception message on failure.',
  error_stack       STRING           COMMENT 'Stack trace for diagnosis.',
  databricks_run_id STRING           COMMENT 'Workspace job run id, to jump from this log to the platform UI.',
  triggered_by      STRING           COMMENT 'Identity that started the run.',
  config_version    STRING           COMMENT 'Config hash in force during the run, so behaviour is explainable after a config change.'
)
USING DELTA
CLUSTER BY (tenant_id, business_date)
COMMENT 'One row per batch. The spine of observability: every other operational table joins back to batch_id.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ops.task_audit_log (
  batch_id         STRING  NOT NULL COMMENT 'FK to batch_run_log.',
  task_id          STRING  NOT NULL COMMENT 'Task identifier within the batch, usually the ingestion config_id or transformation job_id.',
  tenant_id        STRING  NOT NULL COMMENT 'Tenant scope of the task.',
  entity           STRING           COMMENT 'Canonical entity processed.',
  task_sequence    INT              COMMENT 'Execution order within the batch.',
  status           STRING  NOT NULL COMMENT 'PENDING | RUNNING | SUCCEEDED | FAILED | SKIPPED.',
  attempt_number   INT              COMMENT 'Retry attempt, starting at 1.',
  start_ts         TIMESTAMP        COMMENT 'Task start, UTC.',
  end_ts           TIMESTAMP        COMMENT 'Task end, UTC.',
  records_in       BIGINT           COMMENT 'Input row count.',
  records_out      BIGINT           COMMENT 'Output row count.',
  source_files     STRING           COMMENT 'JSON array of files consumed, for exactly-once verification and replay.',
  watermark_from   STRING           COMMENT 'Low watermark of the increment processed.',
  watermark_to     STRING           COMMENT 'High watermark, which becomes the next run\'s low watermark.',
  error_message    STRING           COMMENT 'Failure detail scoped to this task, so one tenant\'s failure is diagnosable in isolation.'
)
USING DELTA
CLUSTER BY (tenant_id, batch_id)
COMMENT 'Per-task detail within a batch. Fail-isolation means a task can fail while its siblings succeed, so status lives here not only at batch level.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ops.dq_result_log (
  batch_id           STRING  NOT NULL COMMENT 'FK to batch_run_log.',
  rule_id            STRING  NOT NULL COMMENT 'FK to dq_rule_registry.',
  tenant_id          STRING  NOT NULL COMMENT 'Tenant evaluated.',
  layer              STRING           COMMENT 'Layer the rule ran at.',
  entity             STRING           COMMENT 'Entity evaluated.',
  dq_dimension       STRING           COMMENT 'Denormalised from the registry so the scorecard needs no join.',
  severity           STRING           COMMENT 'Severity in force at evaluation time, which may differ from the registry today.',
  evaluated_at       TIMESTAMP NOT NULL COMMENT 'Evaluation timestamp, UTC.',
  records_evaluated  BIGINT  NOT NULL COMMENT 'Denominator of the pass rate.',
  records_passed     BIGINT  NOT NULL COMMENT 'Numerator.',
  records_failed     BIGINT  NOT NULL COMMENT 'Failing row count.',
  pass_rate_pct      DECIMAL(9,4)     COMMENT 'Derived pass percentage, stored to keep the scorecard cheap.',
  threshold_pct      DECIMAL(5,2)     COMMENT 'Threshold in force at evaluation time.',
  outcome            STRING  NOT NULL COMMENT 'PASS | WARN | QUARANTINE | ABORT. The action actually taken.',
  failed_sample      STRING           COMMENT 'JSON array of up to 10 failing keys. Keys only, never full rows, so this table carries no sensitive payload.',
  action_taken       STRING           COMMENT 'Human-readable description of what the framework did.',
  rule_expression    STRING           COMMENT 'The expression as evaluated, captured so a later rule edit does not rewrite history.'
)
USING DELTA
CLUSTER BY (tenant_id, evaluated_at)
COMMENT 'Every rule evaluation, every run. This is the evidence that the DQ control operated, which is what an auditor asks for.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ops.quarantine_registry (
  quarantine_id    STRING  NOT NULL COMMENT 'UUID for the quarantined record instance.',
  batch_id         STRING  NOT NULL COMMENT 'Batch that quarantined it.',
  tenant_id        STRING  NOT NULL COMMENT 'Tenant scope.',
  layer            STRING           COMMENT 'Layer at which it was diverted.',
  entity           STRING  NOT NULL COMMENT 'Entity the record belongs to.',
  business_key     STRING  NOT NULL COMMENT 'JSON of the record\'s natural key. The payload itself stays in the quarantine table in ap_silver.quarantine.',
  rule_ids         STRING  NOT NULL COMMENT 'Comma-separated rules the record violated. A record can fail several at once.',
  quarantined_at   TIMESTAMP NOT NULL COMMENT 'When it was diverted.',
  status           STRING  NOT NULL COMMENT 'OPEN | RESOLVED_CORRECTED | RESOLVED_ACCEPTED | RESOLVED_REJECTED | EXPIRED.',
  assigned_to      STRING           COMMENT 'Who owns the fix. An unassigned quarantine is one nobody will fix.',
  resolution_notes STRING           COMMENT 'What was done and why. Required to close.',
  resolved_at      TIMESTAMP        COMMENT 'Closure time.',
  redrive_batch_id STRING           COMMENT 'Batch that successfully reprocessed the corrected record, closing the loop.'
)
USING DELTA
CLUSTER BY (tenant_id, status)
COMMENT 'Index of quarantined records and their disposition. Quarantine without a re-drive path is just silent data loss with extra steps.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ops.reconciliation_control (
  recon_id          STRING  NOT NULL COMMENT 'UUID for the reconciliation instance.',
  batch_id          STRING  NOT NULL COMMENT 'Batch that performed the check.',
  tenant_id         STRING  NOT NULL COMMENT 'Tenant scope.',
  recon_type        STRING  NOT NULL COMMENT 'BRONZE_TO_SILVER | SILVER_TO_GOLD | SUBLEDGER_TO_GL | SOURCE_CONTROL_TOTAL.',
  entity            STRING           COMMENT 'Entity reconciled.',
  company_code      STRING           COMMENT 'Reconciliation is per company code per currency, never in aggregate.',
  currency          STRING           COMMENT 'Currency of the amounts compared.',
  business_date     DATE             COMMENT 'Logical date reconciled.',
  source_count      BIGINT           COMMENT 'Row count on the source side.',
  target_count      BIGINT           COMMENT 'Row count on the target side.',
  count_variance    BIGINT           COMMENT 'Derived difference. Non-zero needs an explanation, not a tolerance.',
  source_amount     DECIMAL(20,2)    COMMENT 'Control total on the source side.',
  target_amount     DECIMAL(20,2)    COMMENT 'Control total on the target side.',
  amount_variance   DECIMAL(20,2)    COMMENT 'Derived difference.',
  variance_pct      DECIMAL(9,6)     COMMENT 'Variance relative to source, for trending.',
  status            STRING  NOT NULL COMMENT 'MATCHED | VARIANCE_WITHIN_TOLERANCE | VARIANCE_BREACH | NOT_RUN.',
  explanation       STRING           COMMENT 'Known cause of an accepted variance, e.g. quarantined rows excluded by design.',
  checked_at        TIMESTAMP NOT NULL COMMENT 'When the check ran.'
)
USING DELTA
CLUSTER BY (tenant_id, business_date)
COMMENT 'Row and amount reconciliation between layers and against the GL. The control that makes the lakehouse trustworthy rather than merely populated.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ops.schema_drift_log (
  drift_id          STRING  NOT NULL COMMENT 'UUID for the drift event.',
  batch_id          STRING           COMMENT 'Batch that detected it.',
  tenant_id         STRING  NOT NULL COMMENT 'Tenant scope.',
  source_system_id  STRING           COMMENT 'Source system that drifted.',
  entity            STRING  NOT NULL COMMENT 'Entity affected.',
  detected_at       TIMESTAMP NOT NULL COMMENT 'Detection time.',
  drift_type        STRING  NOT NULL COMMENT 'COLUMN_ADDED | COLUMN_REMOVED | TYPE_CHANGED | COLUMN_RENAMED | ORDER_CHANGED | ENCODING_CHANGED.',
  classification    STRING  NOT NULL COMMENT 'ADDITIVE | BREAKING. Additive auto-accepts and logs; breaking halts this source only (decision D9).',
  column_name       STRING           COMMENT 'Column involved.',
  expected_type     STRING           COMMENT 'Type in the registered contract.',
  observed_type     STRING           COMMENT 'Type actually seen.',
  sample_file       STRING           COMMENT 'File that exhibited the drift, for reproduction.',
  action_taken      STRING  NOT NULL COMMENT 'AUTO_ACCEPTED | HALTED | RESCUED | IGNORED.',
  status            STRING  NOT NULL COMMENT 'OPEN | ACKNOWLEDGED | CONTRACT_UPDATED | REJECTED.',
  resolved_by       STRING           COMMENT 'Who dispositioned it.',
  resolved_at       TIMESTAMP        COMMENT 'Disposition time.',
  notes             STRING           COMMENT 'Context, e.g. tenant confirmed an ERP upgrade added the field.'
)
USING DELTA
CLUSTER BY (tenant_id, status)
COMMENT 'Schema drift events and their disposition. An unnoticed new column is data loss you discover months later.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ops.config_load_audit (
  load_id          STRING  NOT NULL COMMENT 'UUID for the bootstrap run.',
  loaded_at        TIMESTAMP NOT NULL COMMENT 'When the bootstrap ran.',
  loaded_by        STRING           COMMENT 'Identity that ran it.',
  config_file      STRING  NOT NULL COMMENT 'Repo path of the YAML file loaded.',
  target_table     STRING  NOT NULL COMMENT 'Control table written.',
  config_version   STRING  NOT NULL COMMENT 'SHA-256 of the file content. Detects drift between repo and platform.',
  rows_inserted    BIGINT           COMMENT 'New config rows.',
  rows_updated     BIGINT           COMMENT 'Changed config rows.',
  rows_deactivated BIGINT           COMMENT 'Rows present in the table but absent from the YAML, soft-deleted rather than dropped.',
  status           STRING  NOT NULL COMMENT 'SUCCEEDED | FAILED | VALIDATION_FAILED.',
  validation_errors STRING          COMMENT 'JSON array of validation failures when the YAML was rejected.'
)
USING DELTA
COMMENT 'Audit of every configuration deployment. Answers "why did the pipeline behave differently last Tuesday" without guesswork.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ap_control.ops.onboarding_checklist (
  tenant_id      STRING  NOT NULL COMMENT 'Tenant being onboarded.',
  step_no        INT     NOT NULL COMMENT 'Ordinal position in the runbook.',
  step_name      STRING  NOT NULL COMMENT 'What must be done.',
  category       STRING           COMMENT 'CONTRACT | CONFIG | DATA | VALIDATION | GOVERNANCE | SIGNOFF.',
  is_blocking    BOOLEAN NOT NULL COMMENT 'TRUE means the tenant cannot go live until this passes.',
  status         STRING  NOT NULL COMMENT 'NOT_STARTED | IN_PROGRESS | COMPLETE | BLOCKED | WAIVED.',
  owner          STRING           COMMENT 'Accountable person.',
  evidence_link  STRING           COMMENT 'Link to the artefact proving completion, e.g. a validation notebook run.',
  completed_at   TIMESTAMP        COMMENT 'Completion time.',
  notes          STRING           COMMENT 'Context, including the justification for any WAIVED step.'
)
USING DELTA
COMMENT 'Per-tenant onboarding state. Step 6 generates this automatically for TEN004 as the capstone exercise.';

-- COMMAND ----------

-- MAGIC %md ## Section 8 — Verify

-- COMMAND ----------

-- MAGIC %python
-- MAGIC EXPECTED = {
-- MAGIC     "ap_control.config": ["access_model", "tenant_registry", "source_system_registry",
-- MAGIC                           "ingestion_config", "schema_mapping", "value_domain_mapping",
-- MAGIC                           "dq_rule_registry", "transformation_config", "tolerance_config",
-- MAGIC                           "sla_config"],
-- MAGIC     "ap_control.ref":    ["currency_rate", "payment_terms", "gl_account", "tax_code",
-- MAGIC                           "company_code"],
-- MAGIC     "ap_control.ops":    ["batch_run_log", "task_audit_log", "dq_result_log",
-- MAGIC                           "quarantine_registry", "reconciliation_control",
-- MAGIC                           "schema_drift_log", "config_load_audit", "onboarding_checklist"],
-- MAGIC }
-- MAGIC
-- MAGIC rows, missing = [], []
-- MAGIC for schema, tables in EXPECTED.items():
-- MAGIC     actual = {r.tableName for r in spark.sql(f"SHOW TABLES IN {schema}").collect()}
-- MAGIC     for t in tables:
-- MAGIC         present = t in actual
-- MAGIC         if not present:
-- MAGIC             missing.append(f"{schema}.{t}")
-- MAGIC         rows.append((f"{schema}.{t}", "OK" if present else "MISSING"))
-- MAGIC
-- MAGIC display(spark.createDataFrame(rows, ["table", "status"]))
-- MAGIC assert not missing, f"Missing control tables: {missing}"
-- MAGIC print(f"Control plane ready: {len(rows)} tables. Next: 02_bootstrap_config")
