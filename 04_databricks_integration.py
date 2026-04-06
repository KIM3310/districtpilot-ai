"""
MoveSignal AI — Databricks Integration Layer
Exports Snowflake Feature Mart to Databricks Delta Lake,
runs MLflow-tracked demand forecast, and writes gold KPIs back.

Prerequisites:
    pip install databricks-sdk mlflow delta-spark pyspark pandas
    Environment vars: DATABRICKS_HOST, DATABRICKS_TOKEN (or service-principal OAuth)
"""

import os
import logging
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType
)

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
CATALOG = os.getenv("DATABRICKS_CATALOG", "movesignal_ai")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "analytics")
MLFLOW_EXPERIMENT = "/movesignal-ai/demand-forecast"

DISTRICTS = ["서초구", "영등포구", "중구"]


def get_spark() -> SparkSession:
    """Create or return a SparkSession with Delta Lake support."""
    return (
        SparkSession.builder
        .appName("MoveSignal-AI-Databricks")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.preview.enabled", "true")
        .getOrCreate()
    )


# ──────────────────────────────────────────────
# 1. Feature Mart → Delta Lake
# ──────────────────────────────────────────────
def export_feature_mart_to_delta(
    feature_mart_csv: str = "data/feature_mart_final.csv",
) -> str:
    """
    Read the Snowflake-exported Feature Mart CSV and write it
    as a Delta table in Unity Catalog.

    Returns the Delta table path.
    """
    spark = get_spark()

    df = spark.read.option("header", True).option("inferSchema", True).csv(feature_mart_csv)

    # Standardize column names to upper-case for Snowflake parity
    for col in df.columns:
        df = df.withColumnRenamed(col, col.upper())

    # Add ingestion metadata
    df = df.withColumn("_ingested_at", F.current_timestamp())
    df = df.withColumn("_source", F.lit("snowflake_feature_mart_v4"))

    table_name = f"{CATALOG}.{SCHEMA}.feature_mart_final"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable(table_name)
    )

    row_count = spark.table(table_name).count()
    logger.info("Wrote %d rows to %s", row_count, table_name)
    return table_name


# ──────────────────────────────────────────────
# 2. Demand Forecast with MLflow Tracking
# ──────────────────────────────────────────────
def run_demand_forecast(
    forecast_months: int = 3,
    confidence_interval: float = 0.95,
) -> pd.DataFrame:
    """
    Run a simple demand forecast per district using exponential
    smoothing, log parameters and metrics to MLflow, and persist
    results as a Delta table.
    """
    import mlflow
    from statsmodels.tsa.holtwinters import ExponentialSmoothing

    spark = get_spark()
    table_name = f"{CATALOG}.{SCHEMA}.feature_mart_final"
    pdf = spark.table(table_name).toPandas()

    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    all_forecasts = []

    with mlflow.start_run(run_name=f"demand_forecast_{datetime.now():%Y%m%d_%H%M}"):
        mlflow.log_param("forecast_months", forecast_months)
        mlflow.log_param("confidence_interval", confidence_interval)
        mlflow.log_param("districts", DISTRICTS)
        mlflow.log_param("model_type", "ExponentialSmoothing")
        mlflow.log_param("source_table", table_name)

        for district in DISTRICTS:
            dist_df = (
                pdf[pdf["DISTRICT"] == district]
                .sort_values("YM")
                .copy()
            )
            dist_df["DS"] = pd.to_datetime(dist_df["YM"] + "01", format="%Y%m%d")
            ts = dist_df.set_index("DS")["TOTAL_SALES"].dropna()

            if len(ts) < 12:
                logger.warning("Skipping %s — fewer than 12 data points", district)
                continue

            model = ExponentialSmoothing(
                ts.values,
                trend="add",
                seasonal="add",
                seasonal_periods=12,
            ).fit(optimized=True)

            forecast = model.forecast(forecast_months)
            residuals = ts.values - model.fittedvalues
            rmse = (residuals ** 2).mean() ** 0.5
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

        forecast_pdf = pd.DataFrame(all_forecasts)
        mlflow.log_metric("total_forecast_rows", len(forecast_pdf))

        # Log forecast as artifact
        artifact_path = "/tmp/movesignal_forecast.csv"
        forecast_pdf.to_csv(artifact_path, index=False)
        mlflow.log_artifact(artifact_path)

    # Write to Delta
    forecast_sdf = spark.createDataFrame(forecast_pdf)
    forecast_sdf = forecast_sdf.withColumn("_created_at", F.current_timestamp())

    forecast_table = f"{CATALOG}.{SCHEMA}.forecast_results_databricks"
    (
        forecast_sdf.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(forecast_table)
    )
    logger.info("Forecast written to %s", forecast_table)

    return forecast_pdf


# ──────────────────────────────────────────────
# 3. Gold KPI Rollup (Delta Table)
# ──────────────────────────────────────────────
def build_gold_kpi_table() -> str:
    """
    Aggregate Feature Mart into a gold-layer KPI Delta table
    with district-level monthly rollups.
    """
    spark = get_spark()
    feature_mart = spark.table(f"{CATALOG}.{SCHEMA}.feature_mart_final")

    gold = (
        feature_mart
        .groupBy("DISTRICT")
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

    gold_table = f"{CATALOG}.{SCHEMA}.gold_district_kpi"
    (
        gold.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_table)
    )

    logger.info("Gold KPI table written to %s", gold_table)
    return gold_table


# ──────────────────────────────────────────────
# 4. Allocation Recommendation (Databricks SQL)
# ──────────────────────────────────────────────
ALLOCATION_SQL = f"""
SELECT
    DISTRICT,
    FORECAST,
    ROUND(FORECAST / SUM(FORECAST) OVER () * 100, 1) AS SHARE_PCT,
    ROUND({{total_budget}} * FORECAST / SUM(FORECAST) OVER (), 0) AS BUDGET_ALLOC
FROM {CATALOG}.{SCHEMA}.forecast_results_databricks
"""


def get_allocation_recommendation(total_budget: float = 50_000_000) -> pd.DataFrame:
    """Query the Databricks forecast table for budget allocation."""
    spark = get_spark()
    sql = ALLOCATION_SQL.format(total_budget=total_budget)
    return spark.sql(sql).toPandas()


# ──────────────────────────────────────────────
# 5. Audit Trail (Delta append-only)
# ──────────────────────────────────────────────
def log_audit_event(
    event_type: str,
    detail: str,
    user: str = "system",
) -> None:
    """Append an audit event to the Delta audit log."""
    spark = get_spark()
    audit_table = f"{CATALOG}.{SCHEMA}.audit_log"

    audit_df = spark.createDataFrame([{
        "event_ts": datetime.utcnow().isoformat(),
        "event_type": event_type,
        "detail": detail,
        "user": user,
    }])

    audit_df.write.format("delta").mode("append").saveAsTable(audit_table)


# ──────────────────────────────────────────────
# Main — full pipeline
# ──────────────────────────────────────────────
def run_full_pipeline(
    feature_mart_csv: str = "data/feature_mart_final.csv",
    total_budget: float = 50_000_000,
) -> dict:
    """
    End-to-end Databricks pipeline:
    1. Export Feature Mart to Delta Lake
    2. Run MLflow-tracked demand forecast
    3. Build gold KPI table
    4. Generate allocation recommendation
    5. Log audit event
    """
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("MoveSignal AI — Databricks Pipeline")
    print("=" * 60)

    # Step 1: Feature Mart → Delta
    print("\n[1/5] Exporting Feature Mart to Delta Lake...")
    fm_table = export_feature_mart_to_delta(feature_mart_csv)
    print(f"  -> {fm_table}")

    # Step 2: Demand Forecast + MLflow
    print("\n[2/5] Running demand forecast with MLflow tracking...")
    forecast_df = run_demand_forecast()
    print(f"  -> {len(forecast_df)} forecast rows")
    print(forecast_df.to_string(index=False))

    # Step 3: Gold KPI
    print("\n[3/5] Building gold KPI table...")
    gold_table = build_gold_kpi_table()
    print(f"  -> {gold_table}")

    # Step 4: Allocation
    print(f"\n[4/5] Budget allocation (total: {total_budget/10000:,.0f}만원)...")
    alloc_df = get_allocation_recommendation(total_budget)
    print(alloc_df.to_string(index=False))

    # Step 5: Audit
    print("\n[5/5] Logging audit event...")
    log_audit_event("pipeline_run", f"Full pipeline completed at {datetime.now():%Y-%m-%d %H:%M}")
    print("  -> Audit logged")

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print("=" * 60)

    return {
        "feature_mart_table": fm_table,
        "forecast_rows": len(forecast_df),
        "gold_table": gold_table,
        "allocation": alloc_df.to_dict(orient="records"),
    }


if __name__ == "__main__":
    run_full_pipeline()
