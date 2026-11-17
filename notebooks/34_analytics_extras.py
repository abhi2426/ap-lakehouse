# Databricks notebook source
# MAGIC %md
# MAGIC # 34 — Analytics extras (on demand): cash forecast and duplicate scoring
# MAGIC
# MAGIC Both sections are worth reading for their reasoning even when the output
# MAGIC is thin, which on a fresh build it will be.
# MAGIC
# MAGIC * **Cash forecast** — expected cash out per week for the next 8 weeks:
# MAGIC   each open item on its due date, shifted by the vendor's historical
# MAGIC   median days-late. Stored with the date it was made.
# MAGIC * **Forecast accuracy** — returns little: scoring a forecast needs several
# MAGIC   weeks of stored forecasts and you have one day.
# MAGIC * **Duplicate calibration** — will REFUSE to run, because
# MAGIC   `fact_duplicate_candidate` has almost no dispositions. That refusal is
# MAGIC   correct behaviour, not a failure.

# COMMAND ----------

dbutils.widgets.text("repo_root", "/Workspace/Repos/ap-lakehouse", "Repo root")
dbutils.widgets.text("weeks", "8", "Forecast horizon (weeks)")
dbutils.widgets.dropdown("store_forecast", "true", ["true", "false"], "Store the forecast")

REPO_ROOT = dbutils.widgets.get("repo_root")
WEEKS = int(dbutils.widgets.get("weeks"))
STORE = dbutils.widgets.get("store_forecast") == "true"

import sys
SRC = f"{REPO_ROOT}/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import importlib
import ap_platform.tables as tables
import ap_platform.gold.keys as keys
import ap_platform.gold.analytics as an
for _m in (tables, keys, an):
    importlib.reload(_m)

# COMMAND ----------

# MAGIC %md ## 1. Cash forecast

# COMMAND ----------

fc = an.cash_forecast(spark, weeks=WEEKS, store=STORE)
display(fc.orderBy("tenant_id", "week_start"))

# COMMAND ----------

# MAGIC %md ## 2. Forecast accuracy

# COMMAND ----------

acc, msg = an.forecast_accuracy(spark)
print(msg)
if acc is not None:
    display(acc)

# COMMAND ----------

# MAGIC %md ## 3. Duplicate calibration — expect a refusal

# COMMAND ----------

cal, msg = an.duplicate_calibration(spark)
print(msg)
if cal is not None:
    display(cal)
display(spark.sql("""
    SELECT tenant_id, score_band, count(*) AS pairs, sum(CASE WHEN disposition IS NULL THEN 1 ELSE 0 END) AS undispositioned
    FROM ap_gold.ap.fact_duplicate_candidate GROUP BY tenant_id, score_band ORDER BY tenant_id, score_band"""))
