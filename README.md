# MoveSignal AI

[![Snowflake](https://img.shields.io/badge/Snowflake-Native_App-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-in_Snowflake-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Status](https://img.shields.io/badge/Status-Competition_Ready-brightgreen)]()

> **Snowflake Korea Hackathon 2026 — Tech Track**
> 서초·영등포·중구 렌탈/마케팅 배분 의사결정 에이전트

## Problem

"다음 달 서초·영등포·중구 어디에 어떤 렌탈 상품과 마케팅 예산을 얼마나 넣어야 하는가?"

## Solution

MoveSignal AI는 Snowflake Marketplace 데이터 위에 Semantic View와 Cortex Analyst로 구조화 질의를 처리하고, Snowflake ML Forecast의 검증 지표와 Feature Importance를 근거로 제시하며, AI_COMPLETE Structured Output이 최종 배분 액션을 생성하고, Dynamic Tables와 Tasks로 운영 가능성을 증명하는 **의사결정 에이전트**입니다.

**100% Snowflake Native** — 외부 서비스 없이 Snowflake 안에서 동작합니다.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
│  Marketplace: SPH + Richgo + AJD (스폰서)                    │
│  External: 공휴일 + 연령구조 + 관광수요 + 상권변화 (공개)       │
└────────────────────────┬────────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────────┐
│            DT_FEATURE_MART (Dynamic Table, 1h lag)           │
│  STG_POP + STG_CARD + STG_ASSET + STG_PRICE + STG_MOVE      │
│  + STG_HOLIDAY + STG_DEMOGRAPHICS + STG_TOURISM + STG_COMMERCIAL │
│  → FEATURE_MART_V2 (3구 × 60개월, 50+ features)              │
└──────────┬──────────────────────────┬───────────────────────┘
           ▼                          ▼
┌────────────────────┐    ┌──────────────────────────────────┐
│  ML FORECAST       │    │  Semantic View (MOVESIGNAL_SV)   │
│  Ablation A→E      │    │  Cortex Analyst (SQL generation) │
│  MAPE/SMAPE/MAE    │    │  Cortex Search (policy docs)     │
│  Feature Importance│    │  AI_COMPLETE (structured output)  │
└──────────┬─────────┘    └──────────────┬───────────────────┘
           ▼                             ▼
┌─────────────────────────────────────────────────────────────┐
│              Streamlit in Snowflake (5 tabs)                 │
│  Allocation | Analysis | AI Agent | Simulation | Ops/Trust  │
└─────────────────────────────────────────────────────────────┘
```

## Scoring Coverage

| Criteria | Max | How We Cover It |
|----------|-----|-----------------|
| Creativity 25 | Forecast + Allocation + Simulation + Dual use case | Ablation, What-if, 민간+공공 |
| Snowflake expertise 25 | Semantic View, Analyst, Search, Dynamic Tables, Tasks, AI_COMPLETE | Full native stack |
| AI expertise 25 | Exogenous Forecast + Eval metrics + Feature importance + Grounded AI | Evidence-based agent |
| Realism 15 | V_APP_HEALTH, target lag, retry, RBAC, query tags | Ops/Trust panel |
| Presentation 10 | 10-min demo, ROI, execution roadmap | Demo script + PPT |

## Data Sources

### Sponsor (Snowflake Marketplace)
| Source | Data | Level |
|--------|------|-------|
| **SPH** | 유동인구 (거주/직장/방문), 카드소비 (8 카테고리), 자산/소득 | 법정동 → 구 집계 |
| **Richgo** | 아파트 매매/전세 시세, 인구이동 (전입/전출/순이동) | 시군구 |
| **AJD** | 통신 가입/계약, 렌탈, 마케팅, CS | 시/군 |

### External (Public Open Data)
| Source | Data | License | URL |
|--------|------|---------|-----|
| 한국천문연구원 | 공휴일/특일 캘린더 | 공공누리 1유형 | data.go.kr |
| 행정안전부 | 연령·성별 주민등록 인구 | 공공누리 1유형 | jumin.mois.go.kr |
| 한국관광 데이터랩 | 관광수요/외래객 지수 | 공공누리 1유형 | datalab.visitkorea.or.kr |
| 서울시 상권분석서비스 | 상권변화지표 | 공공누리 1유형 | golmok.seoul.go.kr |

## Project Structure

```
movesignal-ai/
├── README.md
│
│ ── Snowflake SQL Pipeline ──
├── 02_feature_mart_v4.sql          # Feature Mart (5 STG → FEATURE_MART_FINAL)
├── 03_ml_and_cortex_v2.sql         # ML Forecast + Cortex LLM
├── 06_semantic_view.sql            # Semantic View (Cortex Analyst)
├── 07_dynamic_tables_tasks.sql     # Dynamic Tables + Tasks + V_APP_HEALTH
├── 08_ajd_integration.sql          # AJD 통신 데이터 통합
├── 09_cortex_search_agent.sql      # Cortex Search + Agent
├── 10_external_data.sql            # 4개 외부 데이터 + FEATURE_MART_V2
├── 11_ablation_study.sql           # Ablation A→E (5 모델 비교)
│
│ ── Streamlit App ──
├── streamlit_app_v7.py             # 최종 앱 (5탭, 경쟁용)
├── streamlit_app_v6.py             # 4탭 버전
├── streamlit_app_v5.py             # AI_COMPLETE + structured output
│
│ ── Cross-Platform (Portfolio) ──
├── 04_databricks_integration.py    # Databricks pipeline
├── 05_databricks_sql_analytics.sql # Databricks SQL
├── 06_palantir_foundry_integration.py # Palantir Foundry
├── databricks_notebook.py          # Databricks notebook
│
│ ── Presentation ──
├── DEMO_SCRIPT.md                  # 10분 데모 스크립트
├── CODEX_HANDOFF.md                # 프로젝트 문서
└── MoveSignal_AI_Hackathon.pptx    # 발표 PPT
```

## Snowflake Objects

| Object | Type | Description |
|--------|------|-------------|
| `FEATURE_MART_V2` | Table | 확장 Feature Mart (50+ features) |
| `DT_FEATURE_MART` | Dynamic Table | 자동 갱신 (1h lag) |
| `DT_ALLOCATION_INPUT` | Dynamic Table | 배분 비율 자동 계산 |
| `MOVESIGNAL_FORECAST_V2` | ML Model | Ablation 최종 모델 (외생변수 포함) |
| `ABLATION_RESULTS` | Table | 5 모델 MAPE/SMAPE/MAE 비교 |
| `MOVESIGNAL_SV` | Semantic View | Cortex Analyst용 비즈니스 메트릭 |
| `MOVESIGNAL_SEARCH_SVC` | Cortex Search | 정책/룰북 문서 검색 |
| `POLICY_DOCUMENTS` | Table | AI 근거용 정책 문서 |
| `TASK_REFRESH_PIPELINE` | Task | 매일 06:00 KST 갱신 |
| `TASK_RETRAIN_FORECAST` | Task | 매주 월 07:00 KST 재학습 |
| `V_APP_HEALTH` | View | 운영 상태 모니터링 |
| `V_ABLATION_SUMMARY` | View | Ablation MAPE 개선 요약 |

## Prerequisites

- Snowflake account with **ACCOUNTADMIN** role
- Marketplace datasets: **SPH**, **Richgo**, **AJD** (sponsor data)
- Warehouse: `COMPUTE_WH` (X-Small)
- Streamlit in Snowflake enabled
- Database/Schema: `MOVESIGNAL_AI.ANALYTICS`

```sql
-- Initial setup
CREATE DATABASE IF NOT EXISTS MOVESIGNAL_AI;
CREATE SCHEMA IF NOT EXISTS MOVESIGNAL_AI.ANALYTICS;
CREATE STAGE IF NOT EXISTS MOVESIGNAL_AI.ANALYTICS.STREAMLIT_STAGE
  DIRECTORY = (ENABLE = TRUE);
```

## Execution Order (Snowflake)

```sql
-- 1. Base pipeline
02_feature_mart_v4.sql
03_ml_and_cortex_v2.sql

-- 2. External data + extended features
10_external_data.sql

-- 3. Ablation study
11_ablation_study.sql

-- 4. AI layer
06_semantic_view.sql
09_cortex_search_agent.sql

-- 5. Operations
07_dynamic_tables_tasks.sql

-- 6. (Optional) AJD deep integration
08_ajd_integration.sql

-- 7. Deploy Streamlit
streamlit_app_v7.py
```

## Key Differentiators

- **Ablation Study**: Sponsor-only → +Holiday → +Age → +Tourism → +Commercial, MAPE 개선 정량화
- **Evidence Chain**: Forecast metrics → Feature importance → Search-grounded rules → Structured action
- **Dual Use**: 민간 (렌탈/마케팅 배분) + 공공 (상권 활성화/행정 배분)
- **Production-Ready**: Dynamic Tables (1h lag) + Tasks (daily/weekly) + Health monitoring

## Cost

~**$80/month** (Compute WH X-Small + Cortex LLM + Streamlit + Dynamic Tables)

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `FEATURE_MART_V2 not found` | Pipeline not executed | Run `02_feature_mart_v4.sql` → `10_external_data.sql` |
| AI response timeout | Warehouse suspended | `ALTER WAREHOUSE COMPUTE_WH RESUME` |
| Cortex Search unavailable | Region limitation | Verify Cortex Search availability in your region |
| Streamlit blank screen | Stage file missing | Re-upload `streamlit_app_v7.py` to `@STREAMLIT_STAGE` |
| Dynamic Table stale | Task paused | `ALTER TASK TASK_REFRESH_PIPELINE RESUME` |

## Author

**Doeon Kim** — [GitHub](https://github.com/KIM3310)

---

*Submission deadline: 2026-04-12 23:59 KST*
*Finals: 2026-04-29, Seoul (10min presentation + 3min Q&A)*
