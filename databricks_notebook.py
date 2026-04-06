# Databricks notebook source
# MAGIC %md
# MAGIC # MoveSignal AI — Databricks Notebook
# MAGIC
# MAGIC **Snowflake Korea Hackathon 2026 — Databricks Cross-Platform Extension**
# MAGIC
# MAGIC This notebook demonstrates the Databricks leg of MoveSignal AI:
# MAGIC 1. Load Feature Mart from Delta Lake (Unity Catalog)
# MAGIC 2. Run demand forecast with MLflow experiment tracking
# MAGIC 3. Build gold KPI Delta table
# MAGIC 4. Budget allocation recommendation via Databricks SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Verify Delta Tables

# COMMAND ----------

catalog = "movesignal_ai"
schema = "analytics"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Verify feature mart
fm = spark.table("feature_mart_final")
print(f"Feature Mart: {fm.count()} rows, {len(fm.columns)} columns")
fm.groupBy("DISTRICT").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Mart EDA

# COMMAND ----------

import pandas as pd

pdf = fm.toPandas()

# District-level summary
summary = (
    pdf.groupby("DISTRICT")
    .agg(
        months=("YM", "count"),
        avg_sales=("TOTAL_SALES", "mean"),
        avg_pop=("TOTAL_POP", "mean"),
        avg_sales_per_pop=("SALES_PER_POP", "mean"),
    )
    .round(0)
    .sort_values("avg_sales", ascending=False)
)
display(summary)

# COMMAND ----------

# Time series plot per district
import matplotlib.pyplot as plt

fig, axes = plt.subplots(1, 3, figsize=(18, 5), sharey=True)
districts = ["서초구", "영등포구", "중구"]

for ax, district in zip(axes, districts):
    dist_df = pdf[pdf["DISTRICT"] == district].sort_values("YM")
    ax.plot(dist_df["YM"], dist_df["TOTAL_SALES"] / 1e8, marker="o", markersize=2)
    ax.set_title(f"{district} — Monthly Card Sales")
    ax.set_ylabel("Sales (억원)")
    ax.set_xlabel("YM")
    ax.tick_params(axis="x", rotation=90)
    ax.set_xticks(ax.get_xticks()[::6])

plt.tight_layout()
plt.savefig("/tmp/movesignal_eda.png", dpi=150)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Demand Forecast with MLflow

# COMMAND ----------

import mlflow
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import datetime

mlflow.set_experiment("/movesignal-ai/demand-forecast")

forecast_months = 3
all_forecasts = []

with mlflow.start_run(run_name=f"notebook_run_{datetime.now():%Y%m%d_%H%M}"):
    mlflow.log_param("forecast_months", forecast_months)
    mlflow.log_param("model_type", "ExponentialSmoothing")
    mlflow.log_param("districts", districts)
    mlflow.log_param("seasonal_periods", 12)

    for district in districts:
        dist_df = pdf[pdf["DISTRICT"] == district].sort_values("YM").copy()
        dist_df["DS"] = pd.to_datetime(dist_df["YM"] + "01", format="%Y%m%d")
        ts = dist_df.set_index("DS")["TOTAL_SALES"].dropna()

        model = ExponentialSmoothing(
            ts.values, trend="add", seasonal="add", seasonal_periods=12
        ).fit(optimized=True)

        forecast = model.forecast(forecast_months)
        residuals = ts.values - model.fittedvalues
        rmse = (residuals**2).mean() ** 0.5
        mape = (abs(residuals / ts.values)).mean() * 100

        mlflow.log_metric(f"rmse_{district}", round(rmse, 2))
        mlflow.log_metric(f"mape_{district}", round(mape, 2))

        last_date = ts.index[-1]
        forecast_dates = pd.date_range(
            start=last_date + pd.DateOffset(months=1),
            periods=forecast_months,
            freq="MS",
        )

        for dt, val in zip(forecast_dates, forecast):
            all_forecasts.append({
                "DS": dt,
                "DISTRICT": district,
                "FORECAST": round(val, 2),
                "MODEL": "ExponentialSmoothing",
                "RMSE": round(rmse, 2),
                "MAPE_PCT": round(mape, 2),
            })

        print(f"  {district}: RMSE={rmse:,.0f}, MAPE={mape:.1f}%")

    forecast_pdf = pd.DataFrame(all_forecasts)
    mlflow.log_metric("total_forecast_rows", len(forecast_pdf))
    mlflow.log_artifact("/tmp/movesignal_eda.png")

print(f"\nForecast complete: {len(forecast_pdf)} rows")
display(forecast_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Forecast to Delta Lake

# COMMAND ----------

forecast_sdf = spark.createDataFrame(forecast_pdf)
forecast_sdf = forecast_sdf.withColumn("_created_at", F.current_timestamp())

(
    forecast_sdf.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.forecast_results_databricks")
)

print("Forecast written to Delta Lake.")
spark.table(f"{catalog}.{schema}.forecast_results_databricks").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold KPI Table

# COMMAND ----------

from pyspark.sql import functions as F

gold = (
    fm.groupBy("DISTRICT")
    .agg(
        F.sum("TOTAL_SALES").alias("TOTAL_REVENUE"),
        F.avg("TOTAL_SALES").alias("AVG_MONTHLY_REVENUE"),
        F.sum("TOTAL_POP").alias("TOTAL_FOOTFALL"),
        F.avg("SALES_PER_POP").alias("AVG_REVENUE_PER_PERSON"),
        F.avg("AVG_MEME_PRICE").alias("AVG_PROPERTY_PRICE"),
        F.sum("NET_MOVE").alias("CUMULATIVE_NET_MIGRATION"),
        F.count("YM").alias("MONTHS_COVERED"),
        F.min("YM").alias("START_YM"),
        F.max("YM").alias("END_YM"),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

(
    gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.gold_district_kpi")
)

display(spark.table(f"{catalog}.{schema}.gold_district_kpi"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Budget Allocation Recommendation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DISTRICT,
# MAGIC     ROUND(FORECAST, 0) AS FORECAST,
# MAGIC     ROUND(FORECAST / SUM(FORECAST) OVER () * 100, 1) AS SHARE_PCT,
# MAGIC     ROUND(50000000 * FORECAST / SUM(FORECAST) OVER (), 0) AS BUDGET_5000만원
# MAGIC FROM movesignal_ai.analytics.forecast_results_databricks
# MAGIC ORDER BY SHARE_PCT DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. MLflow Experiment Summary

# COMMAND ----------

experiment = mlflow.get_experiment_by_name("/movesignal-ai/demand-forecast")
if experiment:
    runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id], max_results=5)
    display(runs[["run_id", "start_time", "params.model_type", "params.forecast_months",
                   "metrics.rmse_서초구", "metrics.rmse_영등포구", "metrics.rmse_중구",
                   "metrics.mape_서초구", "metrics.mape_영등포구", "metrics.mape_중구"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC Snowflake (Primary)                    Databricks (Cross-Platform)
# MAGIC ┌─────────────────────┐               ┌─────────────────────────┐
# MAGIC │  Marketplace Data   │               │  Unity Catalog          │
# MAGIC │  SPH/Richgo/AJD     │──── CSV ────→ │  Delta Lake tables      │
# MAGIC │                     │    export     │                         │
# MAGIC │  Feature Mart       │               │  Feature Mart (Delta)   │
# MAGIC │  ML FORECAST        │               │  Forecast (Delta)       │
# MAGIC │  Cortex AI          │               │  Gold KPI (Delta)       │
# MAGIC │  Streamlit          │               │  Audit Log (Delta)      │
# MAGIC └─────────────────────┘               │                         │
# MAGIC                                       │  MLflow Tracking        │
# MAGIC                                       │  Databricks SQL         │
# MAGIC                                       └─────────────────────────┘
# MAGIC ```
