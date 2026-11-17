"""
run_local_e2e — every notebook, 00 to 34, on plain local Spark, with the
Step 3 and Step 4 exercises applied exactly as the runbook describes.

    pip install pyspark==4.0.1 pyyaml      # needs Java 17 or 21
    python tests/run_local_e2e.py          # about 30 minutes at scale 0.04

Works on a COPY of the repo (the exercises edit config there), in /tmp/ap_local.
Needs permission to create the /Volumes symlink. Stages can be picked:
    python tests/run_local_e2e.py fresh,s2,s3,e2,s4
"""
import sys, os, shutil, re, time
SRC_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPO = "/tmp/ap_local/repo/ap-lakehouse"
stages = sys.argv[1].split(",") if len(sys.argv) > 1 else ["all"]
scale = os.environ.get("SCALE", "0.04")
fresh = "fresh" in stages or "all" in stages
if fresh:
    shutil.rmtree("/tmp/ap_local/repo", ignore_errors=True)
    shutil.copytree(SRC_REPO, REPO)
else:
    # sync code (not config) so iterative fixes apply without a full reset
    for d in ("src", "notebooks", "tests"):
        shutil.rmtree(f"{REPO}/{d}"); shutil.copytree(f"{SRC_REPO}/{d}", f"{REPO}/{d}")
sys.path.insert(0, f"{REPO}/tests")
import local_harness as H
H.setup(REPO, "/tmp/ap_local/lake", fresh=fresh)
nb = f"{REPO}/notebooks"; W = {"repo_root": REPO}
def want(*names): return "all" in stages or any(n in stages for n in names)
T0 = time.time()

def edit(path, old, new, count=-1):
    p = f"{REPO}/{path}"; s = open(p).read()
    if new in s:
        return
    assert old in s, f"edit anchor not found in {path}: {old[:80]}"
    open(p, "w").write(s.replace(old, new, count))

if want("fresh", "s2"):
    H.run_notebook(f"{nb}/00_unity_catalog_setup.sql", show_sql=False)
    H.run_notebook(f"{nb}/01_control_plane_ddl.sql", show_sql=False)
    H.run_notebook(f"{nb}/02_bootstrap_config.py", {**W, "dry_run": "true"})
    H.run_notebook(f"{nb}/02_bootstrap_config.py", W)
    H.run_notebook(f"{nb}/03_generate_synthetic_data.py", {**W, "scale": scale})
    H.run_notebook(f"{nb}/04_validate_foundation.py", W) if False else None   # schema/volume/comment checks need Unity Catalog
if want("fresh", "s3"):
    H.run_notebook(f"{nb}/10_ingestion_control_ddl.sql", show_sql=False)
    H.run_notebook(f"{nb}/11_register_schema_contracts.py", W)
    H.run_notebook(f"{nb}/12_ingest_bronze.py", W)
    H.run_notebook(f"{nb}/12_ingest_bronze.py", W)          # idempotency
    H.run_notebook(f"{nb}/13_ingestion_observability.sql")
if want("fresh", "e2"):
    # Exercise E2 as the runbook describes it
    if "skipunhalt3" not in stages:
        H.run_notebook(f"{nb}/14_replay_and_backfill.py", {**W, "operation": "unhalt", "tenant": "TEN003", "entity": "invoice_header", "apply": "true"})
    # first occurrence = TEN003 invoice_header (the invoice_line row comes later)
    edit("config/schema_mapping.yaml", "transform_expr: \"nullif(upper(trim(`po-number`)),'')\"",
         "transform_expr: \"coalesce(nullif(upper(trim(`purchase-order-number`)),''), nullif(upper(trim(`po-number`)),''))\"", 1)
    H.run_notebook(f"{nb}/14_replay_and_backfill.py", {**W, "operation": "unhalt", "tenant": "TEN002", "entity": "invoice_header", "notes": "Oracle patch formats amounts as text; mapping strips symbols", "apply": "true"})
    # TEN002: no mapping edit needed — amount mappings parse '$1,009,737.72' since Step 4
    H.run_notebook(f"{nb}/02_bootstrap_config.py", {**W, "generate_fx": "false"})
    H.run_notebook(f"{nb}/12_ingest_bronze.py", W)
if want("fresh", "s4"):
    H.run_notebook(f"{nb}/20_silver_control_ddl.sql", show_sql=False)
    H.run_notebook(f"{nb}/21_run_silver.py", {**W, "mode": "plan"})
    H.run_notebook(f"{nb}/21_run_silver.py", {**W, "mode": "dry_run"})
    H.run_notebook(f"{nb}/21_run_silver.py", {**W, "mode": "run"})
if want("s4again"):
    H.run_notebook(f"{nb}/21_run_silver.py", {**W, "mode": "run"})
if want("s22"):
    H.run_notebook(f"{nb}/22_dq_scorecard.sql")
if want("e1"):
    H.run_notebook(f"{nb}/23_quarantine_redrive.py", W)
    edit("config/value_domains_silver.yaml", "  # TEN003: no PCE row. See the header.\n",
         "  # TEN003: no PCE row. See the header.\n  - {tenant_id: TEN003, domain: UOM, source_value: PCE, canonical_value: EACH, canonical_label: Each, sort_order: 1, is_active: true}\n")
    H.run_notebook(f"{nb}/02_bootstrap_config.py", {**W, "generate_fx": "false"})
    for e in ("invoice_line", "po_line", "po_history"):
        H.run_notebook(f"{nb}/23_quarantine_redrive.py", {**W, "entity": e, "dry_run": "false"})
    H.run_notebook(f"{nb}/23_quarantine_redrive.py", W)             # verify
    H.run_notebook(f"{nb}/21_run_silver.py", {**W, "mode": "run"})  # refresh derived tables
    H.run_notebook(f"{nb}/22_dq_scorecard.sql")
if want("s24"):
    H.run_notebook(f"{nb}/24_silver_maintenance.py", W)
if want("s5"):
    H.run_notebook(f"{nb}/30_gold_ddl.sql", show_sql=False)
    H.run_notebook(f"{nb}/31_run_gold.py", {**W, "mode": "plan"})
    H.run_notebook(f"{nb}/31_run_gold.py", {**W, "mode": "run"})
if want("s5again"):
    H.run_notebook(f"{nb}/31_run_gold.py", {**W, "mode": "run"})
if want("s32"):
    H.run_notebook(f"{nb}/32_semantic_layer.sql")
if want("s33"):
    H.run_notebook(f"{nb}/33_dashboards.sql")
if want("s34"):
    H.run_notebook(f"{nb}/34_analytics_extras.py", W)
print(f"\nE2E stages {stages} finished in {time.time()-T0:.0f}s")
