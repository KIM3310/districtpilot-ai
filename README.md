# MoveSignal AI

> **Snowflake Korea Hackathon 2026 — Tech Track**
> 서초·영등포·중구 렌탈/마케팅 예산 배분 의사결정 엔진

## Overview

MoveSignal AI는 서울 3개 핵심 상권(서초구, 영등포구, 중구)의 렌탈 및 마케팅 예산을 **데이터 기반으로 최적 배분**하는 의사결정 엔진입니다.

**100% Snowflake 네이티브** — 데이터 수집부터 ML, AI, 시각화까지 외부 서비스 없이 Snowflake 안에서 동작합니다.

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Snowflake Marketplace               │
│  SPH (유동인구/카드소비/자산)                      │
│  Richgo (부동산시세/인구이동)                      │
│  AJD (통신가입)                                   │
└──────────────┬──────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────┐
│          Feature Mart (3구 × 60개월)              │
│  STG_POP → STG_CARD → STG_ASSET                  │
│  STG_PRICE → STG_MOVE → FEATURE_MART_FINAL       │
└──────────┬──────────────────────┬───────────────┘
           ▼                      ▼
┌──────────────────────┐  ┌───────────────────────┐  ┌───────────────────────┐
│  Snowflake ML/AI     │  │  Databricks (Cross)   │  │  Palantir Foundry     │
│  ML FORECAST (3개월)  │  │  Delta Lake tables    │  │  Ontology objects     │
│  Cortex mistral-large2│  │  MLflow tracking      │  │  Pipeline transforms  │
│  Streamlit dashboard │  │  Gold KPI (Delta)     │  │  Action workflows     │
└──────────────────────┘  │  Databricks SQL       │  │  Contour analytics    │
                          │  Audit log (Delta)    │  │  Decision tracking    │
                          └───────────────────────┘  └───────────────────────┘
```

### Tri-Platform Strategy

| Layer | Snowflake (Primary) | Databricks (Cross-Platform) | Palantir Foundry (Ontology) |
|-------|--------------------|-----------------------------|----------------------------|
| Data Ingestion | Marketplace (SPH, Richgo, AJD) | CSV export → Delta Lake | Parquet datasets |
| Feature Store | `FEATURE_MART_FINAL` table | Delta table (Unity Catalog) | MonthlyFeature objects |
| ML Forecast | Snowflake ML FORECAST | ExponentialSmoothing + MLflow | DemandForecast objects |
| AI Agent | Cortex mistral-large2 | — | — |
| Gold KPI | Snowflake views | Delta gold table | District objects |
| Decisions | Stored procedures | SQL views | Action-backed workflows |
| Visualization | Streamlit in Snowflake | Databricks SQL dashboard | Contour boards |
| Audit | Task log | Delta append-only audit log | Decision tracking |

## Key Features

| Feature | Description |
|---------|------------|
| **ML Forecast** | Snowflake ML FORECAST로 3개월 수요 예측 (95% 신뢰구간) |
| **AI Agent** | Cortex mistral-large2 기반 한국어 데이터 분석 에이전트 |
| **Budget Allocation** | 예측 기반 자동 예산 배분 추천 |
| **What-if Simulation** | 시나리오별 배분 시뮬레이션 |
| **One Engine, Two Impacts** | 동일 엔진으로 민간(렌탈/마케팅) + 공공(상권활성화) 활용 |

## Data Sources (Snowflake Marketplace)

- **SPH**: 유동인구 (거주/직장/방문), 카드소비 (8개 카테고리), 자산소득
- **Richgo**: 아파트 매매/전세 시세, 인구이동 (전입/전출/순이동)
- **AJD**: 통신 가입 데이터 (시/군 단위)

## Project Structure

```
movesignal-ai/
├── README.md
├── 02_feature_mart_v4.sql        # Feature Mart SQL (5개 STG → 통합)
├── 03_ml_and_cortex_v2.sql       # ML Forecast + Cortex SQL
├── 04_databricks_integration.py  # Databricks pipeline (Delta + MLflow + Gold KPI)
├── 05_databricks_sql_analytics.sql # Databricks SQL analytics queries
├── 06_palantir_foundry_integration.py # Foundry pipeline (Ontology + Actions + Contour)
├── databricks_notebook.py        # Databricks notebook (end-to-end)
├── streamlit_app_v4.py           # Streamlit 앱 (최신, GPT Pro 리뷰 반영)
├── streamlit_app_v3.py           # Streamlit 앱 (이전 버전)
├── DEMO_SCRIPT.md                # 10분 데모 발표 스크립트
├── CODEX_HANDOFF.md              # 프로젝트 문서
└── MoveSignal_AI_Hackathon.pptx  # 발표 PPT (13슬라이드)
```

## Snowflake Objects

| Object | Type | Description |
|--------|------|-------------|
| `FEATURE_MART_FINAL` | Table | 통합 Feature Mart (3구 × 60개월) |
| `MOVESIGNAL_FORECAST` | ML Model | Snowflake ML FORECAST 모델 |
| `FORECAST_RESULTS` | Table | 3개월 예측 결과 |
| `RECOMMEND_ALLOCATION` | Procedure | 예산 배분 추천 |
| `SIMULATE_WHATIF` | Procedure | What-if 시뮬레이션 |
| `MOVESIGNAL_APP` | Streamlit | 배포된 Streamlit 앱 |

## Palantir Foundry Objects

| Object | Type | Description |
|--------|------|-------------|
| `District` | Ontology Object | 서울시 행정구 (서초/영등포/중구) |
| `MonthlyFeature` | Ontology Object | Feature Mart 월별 레코드 |
| `DemandForecast` | Ontology Object | 3개월 수요 예측 결과 |
| `BudgetAllocation` | Ontology Object | 예산 배분 의사결정 (Action-backed) |
| `district_features` | Link Type | District → MonthlyFeature (ONE_TO_MANY) |
| `district_forecasts` | Link Type | District → DemandForecast (ONE_TO_MANY) |
| Contour Queries | Analytics | District ranking, YoY growth, allocation tracker |

## Databricks Objects (Unity Catalog)

| Object | Type | Description |
|--------|------|-------------|
| `feature_mart_final` | Delta Table | Snowflake Feature Mart mirror |
| `forecast_results_databricks` | Delta Table | MLflow-tracked forecast results |
| `gold_district_kpi` | Delta Table | Gold-layer district KPI rollups |
| `audit_log` | Delta Table | Append-only pipeline audit trail |
| `v_budget_allocation` | View | Parametric budget allocation |
| `/movesignal-ai/demand-forecast` | MLflow Experiment | Forecast model tracking |

## Palantir Foundry Quick Start

```bash
# Set environment variables
export FOUNDRY_URL="https://your-stack.palantirfoundry.com"
export FOUNDRY_TOKEN="..."

# Run full Foundry pipeline (Ontology + Transforms + Actions + Contour)
python 06_palantir_foundry_integration.py

# Exports: foundry/ontology_schema.json, foundry/datasets/*.parquet, foundry/contour_queries.json
```

## Databricks Quick Start

```bash
# Set environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_CATALOG="movesignal_ai"

# Run full pipeline (Feature Mart → Delta → MLflow Forecast → Gold KPI)
python 04_databricks_integration.py

# Or import databricks_notebook.py into Databricks workspace
```

## Cost

~**$80/month** Snowflake (Compute WH X-Small + Cortex LLM + Streamlit hosting)
Databricks leg runs on serverless SQL warehouse; cost depends on usage.

## Author

**Doeon Kim** — [GitHub](https://github.com/KIM3310)
