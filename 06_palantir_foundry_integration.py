"""
MoveSignal AI — Palantir Foundry Integration Layer

Maps the MoveSignal Feature Mart and forecast pipeline into Foundry's
Ontology-first paradigm: typed objects, link definitions, pipeline
transforms, and action-backed decision workflows.

Prerequisites:
    pip install palantir-sdk pandas
    Environment: FOUNDRY_URL, FOUNDRY_TOKEN
"""

import os
import json
import logging
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
FOUNDRY_URL = os.getenv("FOUNDRY_URL", "")
FOUNDRY_TOKEN = os.getenv("FOUNDRY_TOKEN", "")
DATASET_RID_PREFIX = os.getenv("FOUNDRY_DATASET_PREFIX", "ri.foundry.main.dataset")
PROJECT_FOLDER = "/MoveSignal-AI"

DISTRICTS = ["서초구", "영등포구", "중구"]


# ═══════════════════════════════════════════════════════════════
# 1. Ontology Object Definitions
# ═══════════════════════════════════════════════════════════════

ONTOLOGY_SCHEMA = {
    "objectTypes": [
        {
            "apiName": "District",
            "displayName": "Seoul District",
            "description": "서울시 행정구 — 상권 분석 단위",
            "primaryKey": "districtId",
            "properties": {
                "districtId": {"type": "string", "description": "구 코드 (서초구/영등포구/중구)"},
                "name": {"type": "string"},
                "latestPopulation": {"type": "long", "description": "최신 월간 유동인구"},
                "latestSales": {"type": "double", "description": "최신 월간 카드소비 (KRW)"},
                "avgPropertyPrice": {"type": "double", "description": "평균 아파트 매매가 (KRW)"},
                "netMigration": {"type": "long", "description": "누적 순이동"},
            },
        },
        {
            "apiName": "MonthlyFeature",
            "displayName": "Monthly Feature Record",
            "description": "Feature Mart 월별 레코드 (구 × 월)",
            "primaryKey": "featureId",
            "properties": {
                "featureId": {"type": "string", "description": "district_ym composite key"},
                "districtId": {"type": "string"},
                "yearMonth": {"type": "string"},
                "totalPop": {"type": "long"},
                "totalSales": {"type": "double"},
                "salesPerPop": {"type": "double"},
                "avgMemePrice": {"type": "double"},
                "netMove": {"type": "long"},
                "salesChgPct": {"type": "double"},
            },
        },
        {
            "apiName": "DemandForecast",
            "displayName": "Demand Forecast",
            "description": "3개월 수요 예측 결과",
            "primaryKey": "forecastId",
            "properties": {
                "forecastId": {"type": "string"},
                "districtId": {"type": "string"},
                "forecastDate": {"type": "date"},
                "forecastValue": {"type": "double"},
                "model": {"type": "string"},
                "rmse": {"type": "double"},
                "mapePct": {"type": "double"},
            },
        },
        {
            "apiName": "BudgetAllocation",
            "displayName": "Budget Allocation Decision",
            "description": "예산 배분 의사결정 레코드",
            "primaryKey": "allocationId",
            "properties": {
                "allocationId": {"type": "string"},
                "districtId": {"type": "string"},
                "totalBudget": {"type": "double"},
                "sharePct": {"type": "double"},
                "allocatedBudget": {"type": "double"},
                "decisionDate": {"type": "datetime"},
                "approver": {"type": "string"},
                "status": {"type": "string", "description": "PENDING / APPROVED / REJECTED"},
            },
        },
    ],
    "linkTypes": [
        {
            "apiName": "district_features",
            "displayName": "District → Monthly Features",
            "from": "District",
            "to": "MonthlyFeature",
            "cardinality": "ONE_TO_MANY",
        },
        {
            "apiName": "district_forecasts",
            "displayName": "District → Demand Forecasts",
            "from": "District",
            "to": "DemandForecast",
            "cardinality": "ONE_TO_MANY",
        },
        {
            "apiName": "district_allocations",
            "displayName": "District → Budget Allocations",
            "from": "District",
            "to": "BudgetAllocation",
            "cardinality": "ONE_TO_MANY",
        },
    ],
}


def export_ontology_schema(output_path: str = "foundry/ontology_schema.json") -> str:
    """Export the Ontology schema definition to JSON for Foundry import."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(ONTOLOGY_SCHEMA, f, indent=2, ensure_ascii=False)
    logger.info("Ontology schema exported to %s", output_path)
    return output_path


# ═══════════════════════════════════════════════════════════════
# 2. Pipeline Transforms (Foundry-style)
# ═══════════════════════════════════════════════════════════════

def transform_feature_mart(feature_mart_csv: str = "data/feature_mart_final.csv") -> pd.DataFrame:
    """
    Foundry Pipeline Transform: Raw Feature Mart → Typed MonthlyFeature objects.
    Maps to Foundry's @transform decorator pattern.
    """
    df = pd.read_csv(feature_mart_csv)

    # Normalize column names
    df.columns = [c.upper() for c in df.columns]

    # Build Ontology-typed records
    records = []
    for _, row in df.iterrows():
        records.append({
            "featureId": f"{row['DISTRICT']}_{row['YM']}",
            "districtId": row["DISTRICT"],
            "yearMonth": str(row["YM"]),
            "totalPop": int(row.get("TOTAL_POP", 0)),
            "totalSales": float(row.get("TOTAL_SALES", 0)),
            "salesPerPop": float(row.get("SALES_PER_POP", 0)),
            "avgMemePrice": float(row.get("AVG_MEME_PRICE", 0)),
            "netMove": int(row.get("NET_MOVE", 0)),
            "salesChgPct": float(row.get("SALES_CHG_PCT", 0)),
        })

    result = pd.DataFrame(records)
    logger.info("Transformed %d MonthlyFeature objects", len(result))
    return result


def transform_districts(feature_mart: pd.DataFrame) -> pd.DataFrame:
    """
    Foundry Pipeline Transform: MonthlyFeature → District summary objects.
    Aggregates latest state per district for Ontology District objects.
    """
    latest = (
        feature_mart
        .sort_values("yearMonth")
        .groupby("districtId")
        .last()
        .reset_index()
    )

    districts = []
    for _, row in latest.iterrows():
        districts.append({
            "districtId": row["districtId"],
            "name": row["districtId"],
            "latestPopulation": int(row["totalPop"]),
            "latestSales": float(row["totalSales"]),
            "avgPropertyPrice": float(row["avgMemePrice"]),
            "netMigration": int(row["netMove"]),
        })

    result = pd.DataFrame(districts)
    logger.info("Built %d District objects", len(result))
    return result


def transform_forecasts(forecast_csv: str = "data/forecast_results.csv") -> pd.DataFrame:
    """
    Foundry Pipeline Transform: Forecast results → DemandForecast objects.
    """
    df = pd.read_csv(forecast_csv)
    df.columns = [c.upper() for c in df.columns]

    records = []
    for _, row in df.iterrows():
        records.append({
            "forecastId": f"{row['DISTRICT']}_{row['DS']}",
            "districtId": row["DISTRICT"],
            "forecastDate": str(row["DS"]),
            "forecastValue": float(row["FORECAST"]),
            "model": row.get("MODEL", "ExponentialSmoothing"),
            "rmse": float(row.get("RMSE", 0)),
            "mapePct": float(row.get("MAPE_PCT", 0)),
        })

    result = pd.DataFrame(records)
    logger.info("Transformed %d DemandForecast objects", len(result))
    return result


# ═══════════════════════════════════════════════════════════════
# 3. Action-backed Decision Workflows
# ═══════════════════════════════════════════════════════════════

def create_allocation_decision(
    district_id: str,
    total_budget: float,
    share_pct: float,
    approver: str = "system",
) -> dict:
    """
    Foundry Action: Create a BudgetAllocation decision object.
    Maps to Foundry's Action Type pattern — creates an Ontology object
    and triggers downstream workflows (notification, audit, approval).
    """
    allocation = {
        "allocationId": f"alloc_{district_id}_{datetime.now():%Y%m%d_%H%M%S}",
        "districtId": district_id,
        "totalBudget": total_budget,
        "sharePct": share_pct,
        "allocatedBudget": round(total_budget * share_pct / 100, 0),
        "decisionDate": datetime.now().isoformat(),
        "approver": approver,
        "status": "PENDING",
    }
    logger.info("Created allocation decision: %s", allocation["allocationId"])
    return allocation


def approve_allocation(allocation: dict, approver: str) -> dict:
    """
    Foundry Action: Approve a pending BudgetAllocation.
    Updates status and triggers execution workflow.
    """
    allocation["status"] = "APPROVED"
    allocation["approver"] = approver
    allocation["approvedAt"] = datetime.now().isoformat()
    logger.info("Approved: %s by %s", allocation["allocationId"], approver)
    return allocation


def generate_allocation_decisions(
    forecasts: pd.DataFrame,
    total_budget: float = 50_000_000,
) -> list[dict]:
    """
    Generate BudgetAllocation decisions from forecast results.
    Each district gets a PENDING decision proportional to forecast demand.
    """
    total_forecast = forecasts["forecastValue"].sum()
    decisions = []

    for _, row in forecasts.groupby("districtId")["forecastValue"].mean().reset_index().iterrows():
        share = round(row["forecastValue"] / total_forecast * 100, 1) if total_forecast > 0 else 33.3
        decision = create_allocation_decision(
            district_id=row["districtId"],
            total_budget=total_budget,
            share_pct=share,
        )
        decisions.append(decision)

    return decisions


# ═══════════════════════════════════════════════════════════════
# 4. Foundry Dataset Export
# ═══════════════════════════════════════════════════════════════

def export_to_foundry_datasets(
    feature_mart: pd.DataFrame,
    districts: pd.DataFrame,
    forecasts: pd.DataFrame,
    decisions: list[dict],
    output_dir: str = "foundry/datasets",
) -> dict:
    """
    Export all Ontology objects as Foundry-compatible datasets (Parquet).
    In production, these would be synced via Foundry's Data Connection or API.
    """
    os.makedirs(output_dir, exist_ok=True)

    paths = {}
    for name, df in [
        ("monthly_features", feature_mart),
        ("districts", districts),
        ("demand_forecasts", forecasts),
        ("budget_allocations", pd.DataFrame(decisions)),
    ]:
        path = os.path.join(output_dir, f"{name}.parquet")
        df.to_parquet(path, index=False)
        paths[name] = path
        logger.info("Exported %s: %d rows → %s", name, len(df), path)

    return paths


# ═══════════════════════════════════════════════════════════════
# 5. Contour-style Analytics Queries
# ═══════════════════════════════════════════════════════════════

CONTOUR_QUERIES = {
    "district_ranking": """
        -- Contour Board: District Ranking by Forecast Demand
        SELECT
            d.districtId,
            d.name,
            d.latestSales,
            d.latestPopulation,
            AVG(f.forecastValue) AS avg_forecast,
            ROUND(AVG(f.forecastValue) / SUM(AVG(f.forecastValue)) OVER () * 100, 1) AS share_pct
        FROM districts d
        JOIN demand_forecasts f ON d.districtId = f.districtId
        GROUP BY d.districtId, d.name, d.latestSales, d.latestPopulation
        ORDER BY avg_forecast DESC
    """,
    "yoy_growth": """
        -- Contour Board: Year-over-Year Sales Growth
        SELECT
            districtId,
            LEFT(yearMonth, 4) AS year,
            SUM(totalSales) AS annual_sales,
            LAG(SUM(totalSales)) OVER (PARTITION BY districtId ORDER BY LEFT(yearMonth, 4)) AS prev_year,
            ROUND((SUM(totalSales) - LAG(SUM(totalSales)) OVER (
                PARTITION BY districtId ORDER BY LEFT(yearMonth, 4)
            )) / NULLIF(LAG(SUM(totalSales)) OVER (
                PARTITION BY districtId ORDER BY LEFT(yearMonth, 4)
            ), 0) * 100, 1) AS yoy_pct
        FROM monthly_features
        GROUP BY districtId, LEFT(yearMonth, 4)
        ORDER BY districtId, year
    """,
    "allocation_status": """
        -- Contour Board: Budget Allocation Decision Tracker
        SELECT
            districtId,
            totalBudget,
            sharePct,
            allocatedBudget,
            status,
            approver,
            decisionDate
        FROM budget_allocations
        ORDER BY decisionDate DESC
    """,
}


def export_contour_queries(output_path: str = "foundry/contour_queries.json") -> str:
    """Export Contour-style analytics queries for Foundry import."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(CONTOUR_QUERIES, f, indent=2)
    logger.info("Contour queries exported to %s", output_path)
    return output_path


# ═══════════════════════════════════════════════════════════════
# Main — Full Foundry Pipeline
# ═══════════════════════════════════════════════════════════════

def run_foundry_pipeline(
    feature_mart_csv: str = "data/feature_mart_final.csv",
    forecast_csv: str = "data/forecast_results.csv",
    total_budget: float = 50_000_000,
) -> dict:
    """
    End-to-end Palantir Foundry pipeline:
    1. Export Ontology schema
    2. Transform Feature Mart → typed objects
    3. Build District objects
    4. Transform forecast results
    5. Generate allocation decisions (Action-backed)
    6. Export as Foundry datasets (Parquet)
    7. Export Contour analytics queries
    """
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("MoveSignal AI — Palantir Foundry Pipeline")
    print("=" * 60)

    # 1. Ontology Schema
    print("\n[1/7] Exporting Ontology schema...")
    schema_path = export_ontology_schema()
    print(f"  → {schema_path}")

    # 2. Transform Feature Mart
    print("\n[2/7] Transforming Feature Mart → MonthlyFeature objects...")
    features = transform_feature_mart(feature_mart_csv)
    print(f"  → {len(features)} records")

    # 3. Build Districts
    print("\n[3/7] Building District objects...")
    districts = transform_districts(features)
    print(districts.to_string(index=False))

    # 4. Transform Forecasts
    print("\n[4/7] Transforming forecast results...")
    forecasts = transform_forecasts(forecast_csv)
    print(f"  → {len(forecasts)} forecast records")

    # 5. Generate Decisions
    print(f"\n[5/7] Generating allocation decisions (budget: {total_budget/10000:,.0f}만원)...")
    decisions = generate_allocation_decisions(forecasts, total_budget)
    for d in decisions:
        print(f"  → {d['districtId']}: {d['sharePct']}% = {d['allocatedBudget']/10000:,.0f}만원 [{d['status']}]")

    # 6. Export Datasets
    print("\n[6/7] Exporting Foundry datasets (Parquet)...")
    paths = export_to_foundry_datasets(features, districts, forecasts, decisions)
    for name, path in paths.items():
        print(f"  → {name}: {path}")

    # 7. Contour Queries
    print("\n[7/7] Exporting Contour analytics queries...")
    queries_path = export_contour_queries()
    print(f"  → {queries_path}")

    print("\n" + "=" * 60)
    print("Foundry pipeline complete.")
    print("=" * 60)

    return {
        "ontology_schema": schema_path,
        "features": len(features),
        "districts": len(districts),
        "forecasts": len(forecasts),
        "decisions": len(decisions),
        "datasets": paths,
        "contour_queries": queries_path,
    }


if __name__ == "__main__":
    run_foundry_pipeline()
