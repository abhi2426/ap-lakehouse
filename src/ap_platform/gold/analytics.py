"""
analytics — two on-demand analyses that know when to refuse.

Cash forecast
-------------
Expected cash out per week for the next N weeks: each open item lands on its
due date, shifted by that vendor's historical median days-late (falling back
to the tenant median). Forecasts are stored with the date they were made, so
accuracy can be scored later against what was actually paid. Scoring needs
several weeks of stored forecasts; with one day of history it returns little,
and says so.

Duplicate calibration
---------------------
Fits the duplicate-score weights to reviewer dispositions (CONFIRMED / NOT_DUP).
It REFUSES with fewer than ``MIN_DISPOSITIONS`` dispositions: weights fitted
to 30 examples look rigorous and are noise. Refusing is the correct output.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import tables
from .keys import G, as_of_date

MIN_DISPOSITIONS = 200
FORECAST_TABLE = "ap_gold.ap.fc_cash_forecast"


def cash_forecast(spark: SparkSession, weeks: int = 8, as_of: str | None = None, store: bool = True) -> DataFrame:
    asof = as_of_date(spark, as_of)
    snap = spark.table(G + "fact_ap_open_item_snapshot")
    latest = snap.agg(F.max("snapshot_date").alias("d")).collect()[0].d
    items = snap.filter(F.col("snapshot_date") == F.lit(latest))
    app = spark.table(G + "fact_ap_payment_application").filter(~F.col("is_void") & F.col("days_after_due").isNotNull())
    vendor_lag = app.groupBy("tenant_id", "vendor_key").agg(F.percentile_approx("days_after_due", 0.5).alias("v_lag"))
    tenant_lag = app.groupBy("tenant_id").agg(F.percentile_approx("days_after_due", 0.5).alias("t_lag"))
    f = (items.join(vendor_lag, ["tenant_id", "vendor_key"], "left").join(tenant_lag, "tenant_id", "left")
         .withColumn("expected_lag_days", F.coalesce("v_lag", "t_lag", F.lit(0)))
         .withColumn("expected_pay_date", F.greatest(F.date_add("due_date", F.col("expected_lag_days").cast("int")), asof))
         .withColumn("week_start", F.date_trunc("week", "expected_pay_date").cast("date")))
    horizon = F.date_add(asof, weeks * 7)
    out = (f.filter(F.col("expected_pay_date") < horizon)
             .groupBy("tenant_id", "week_start")
             .agg(F.count("*").alias("items"), F.round(F.sum("open_amount_group"), 2).alias("expected_cash_out_group"))
             .withColumn("forecast_made_on", asof).withColumn("horizon_weeks", F.lit(weeks)))
    if store:
        tables.merge(spark, out, FORECAST_TABLE, ["tenant_id", "forecast_made_on", "week_start"],
                     cluster_by=["tenant_id", "forecast_made_on"])
    return out


def forecast_accuracy(spark: SparkSession) -> tuple[DataFrame | None, str]:
    if not tables.exists(spark, FORECAST_TABLE):
        return None, "no forecasts stored yet"
    fc = spark.table(FORECAST_TABLE)
    made = fc.select("forecast_made_on").distinct().count()
    paid = (spark.table(G + "fact_ap_payment_application").filter(~F.col("is_void"))
            .withColumn("week_start", F.date_trunc("week", "payment_date").cast("date"))
            .groupBy("tenant_id", "week_start").agg(F.sum(F.abs("applied_amount")).alias("actual_txn")))
    j = fc.join(paid, ["tenant_id", "week_start"], "inner").filter(F.col("week_start") < F.current_date())
    n = j.count()
    msg = (f"{made} forecast date(s) stored, {n} forecast week(s) with actuals. "
           + ("Too few to score meaningfully: store a forecast every week for 6+ weeks." if made < 6 or n < 12
              else "Enough history to score."))
    return (j if n else None), msg


def duplicate_calibration(spark: SparkSession) -> tuple[DataFrame | None, str]:
    d = spark.table(G + "fact_duplicate_candidate")
    labelled = d.filter(F.col("disposition").isin("CONFIRMED", "NOT_DUP"))
    n = labelled.count()
    if n < MIN_DISPOSITIONS:
        return None, (f"REFUSED: {n} dispositioned pairs (need {MIN_DISPOSITIONS}). Calibrating weights on so few "
                      "examples produces numbers that look rigorous and are noise. Record dispositions in "
                      "fact_duplicate_candidate.disposition as reviewers work the queue, then re-run.")
    feats = ["same_normalised_number", "same_exact_number", "same_amount"]
    rows = []
    for f_ in feats:
        r = labelled.groupBy(f_).agg(F.avg((F.col("disposition") == "CONFIRMED").cast("double")).alias("p")).collect()
        rows.append((f_, {str(x[f_]): round(x.p, 3) for x in r}))
    return spark.createDataFrame([(a, str(b)) for a, b in rows], "feature STRING, confirm_rate_by_value STRING"), \
        f"{n} dispositions: per-feature confirmation rates computed"
