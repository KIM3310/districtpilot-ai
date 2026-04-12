# DistrictPilot AI

[![Snowflake](https://img.shields.io/badge/Snowflake-Native_App-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-in_Snowflake-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Status](https://img.shields.io/badge/Status-Competition_Ready-brightgreen)]()

> **Snowflake Korea Hackathon 2026 - Tech Track**
> 이사 직후 72시간 골든타임 — 홈서비스 수요를 예측하고 구별 집행 액션을 추천하는 Snowflake-native AI 에이전트

## Problem

"다음 달 서초/영등포/중구 중 어디에 홈서비스 설치 인력을 먼저 배치하고, 어떤 강도로 집행해야 하는가?"

한국에서 연간 700만 건의 이사가 일어나고, 이사 직후 72시간이 홈서비스 수요의 골든타임입니다.

## Solution

DistrictPilot AI는 **이사 직후 골든타임을 예측하고 구별 집행 액션을 추천하는** 100% Snowflake Native AI 에이전트입니다.

- **Semantic View + Cortex Analyst** -- 전입·이사/소비/관광 신호를 자연어에서 정확한 SQL로 변환
- **ML FORECAST + Ablation** -- 외생변수 효과를 정량 증명한 지역 수요 예측
- **AI_COMPLETE Structured Output** -- 추천 지역, 집행 강도, 리스크, 다음 액션을 JSON으로 반환
- **Dynamic Tables + Tasks** -- 운영 가능한 월간 집행 시스템을 증명 (1h lag, daily/weekly refresh)

외부 서비스 없이 Snowflake 안에서 데이터 수집부터 전입 신호 해석, 집행 추천, 운영 모니터링까지 완결됩니다.

## Judge Fast Path

1. Snowsight에서 [`14_judge_fastpath.sql`](14_judge_fastpath.sql)을 실행해 라이브 스택과 증거 체인을 확인합니다.
2. Streamlit 앱에서 `Capture Plan -> Move-in Signals -> AI Playbook -> Ops / Trust` 순서로 클릭합니다.
3. 레포에서는 [`DOMAIN_POSITIONING.md`](DOMAIN_POSITIONING.md), [`JUDGE_FASTPATH.md`](JUDGE_FASTPATH.md), [`DEMO_SCRIPT.md`](DEMO_SCRIPT.md) 순서로 보면 됩니다.

## Architecture

```
Data Sources
  Marketplace: SPH (유동인구/카드소비) + Richgo (부동산/인구이동)
  Public: 공휴일 + 연령구조 + 관광수요 + 상권변화
                         |
                         v
DT_FEATURE_MART (Dynamic Table, 1h lag) -> FEATURE_MART_V2
  3구 x 60개월, 50+ features
         |                    |
         v                    v
  DISTRICTPILOT_FORECAST_V2  Semantic View (DISTRICTPILOT_SV)
  Ablation A->E          Cortex Analyst (VQR 10개)
  Feature Importance     Cortex Search (정책 문서)
         |                    |
         v                    v
  Streamlit in Snowflake (5 tabs)
  Capture Plan | Move-in Signals | AI Playbook | Scenario Lab | Ops/Trust
```

Current live alignment:

- Feature pipeline: `DT_FEATURE_MART` -> `FEATURE_MART_V2`
- Final production model: `DISTRICTPILOT_FORECAST_V2`
- `AJD` is optional and not required for the base demo flow

## Evidence Chain

| Step | Snowflake Object | Evidence |
|------|------------------|----------|
| Forecast | `DISTRICTPILOT_FORECAST_V2` | 3구 수요 예측 + evaluation metrics + feature importance |
| Feature Importance | `FEATURE_IMPORTANCE` + `ABLATION_RESULTS` | 전입·이사/소비 신호별 Top-5 기여도, ablation 전후 MAPE 개선 |
| Semantic View | `DISTRICTPILOT_SV` | VQR 10개, SQL 규칙 13개, synonym 100+, 카테고리 7개 |
| Search Grounding | `CORTEX.SEARCH()` | 설치/온보딩/집행 룰북 컨텍스트로 hallucination 방지 |
| AI Structured Output | `AI_COMPLETE()` + `SNOWFLAKE.CORTEX.COMPLETE()` | JSON action card (recommended_district / allocation_pct / risk / next_action) |
| Refresh State | `V_APP_HEALTH` | 실시간 LAG_SEC, task 상태, query_tag 감사 |

## Engineering Signals

- **Version-tolerant app**: Streamlit 앱이 `DISTRICTPILOT_FORECAST_V2`와 레거시 모델명을 자동으로 흡수해 라이브 데모 리스크를 낮춥니다.
- **Grounded move-in engine**: Feature Mart JSON 컨텍스트와 Search grounding으로 전입·이사 시그널을 설명 가능한 추천으로 바꿉니다.
- **Partner-data intersection**: SPH, Richgo, optional AJD가 가장 자연스럽게 만나는 도메인 축으로 문제를 정의했습니다.
- **Observable ops**: `V_APP_HEALTH`, query tag, 실행 컨텍스트, Dynamic Table/Task 상태를 앱과 SQL에서 확인할 수 있습니다.

## Scoring Coverage (해커톤 공식 심사 기준)

| 심사 항목 | 배점 | DistrictPilot AI 대응 |
|----------|------|----------------------|
| **비즈니스 임팩트** | **25%** | 연 700만 이사 × 72시간 골든타임. SPH+Richgo+AJD 3사 데이터가 이사 밸류체인 완벽 커버 |
| **기술 구현** | **25%** | Marketplace 3사 + ML FORECAST(외생변수) + Ablation 5모델 + AI_COMPLETE + DT + Tasks + Streamlit = 8개 Snowflake 기능 통합 |
| **솔루션 완성도** | **20%** | 데이터 수집 → 예측 → 액션 추천 → 시뮬레이션 → 운영 모니터링 End-to-End 완결 |
| **데이터 분석 & 인사이트** | **20%** | Feature Importance(외생변수 기여도) + Ablation(5모델 MAPE 비교) + 구별 인사이트 콜아웃 |
| **발표 품질** | **10%** | 5탭 라이브 데모 + Judge Fast Path + Q&A 7문항 대비 |

**보너스 포인트**: Snowpark, Cortex AI (Complete + Search), Marketplace (SPH + Richgo + AJD), Streamlit 모두 활용

## Data Sources

### Sponsor (Snowflake Marketplace)
| Source | Data | Level |
|--------|------|-------|
| **SPH** | 유동인구 (거주/직장/방문), 카드소비 (8 카테고리), 자산/소득 | 법정동 -> 구 집계 |
| **Richgo** | 아파트 매매/전세 시세, 인구이동 (전입/전출/순이동) | 시군구 |
| **AJD (Optional)** | 통신 가입 / 렌탈 / 마케팅 / CS | 별도 통합 시 사용 |

### External (Public Open Data — 합성 시뮬레이션)
| Source | Data | License | 비고 |
|--------|------|---------|------|
| 한국천문연구원 | 공휴일/특일 캘린더 | 공공누리 1유형 | 합성: 공개 캘린더 기반 |
| 행정안전부 | 연령/성별 주민등록 인구 | 공공누리 1유형 | 합성: KOSIS 통계 기반 트렌드 |
| 한국관광 데이터랩 | 관광수요/외래객 지수 | 공공누리 1유형 | 합성: 계절 패턴 모사 |
| 서울시 상권분석서비스 | 상권변화지표 | 공공누리 1유형 | 합성: 상권 안정성 트렌드 |

> **투명성 노트**: 외부 공개 데이터와 AJD 통신/렌탈 데이터는 해커톤 환경에서 공개 통계와
> 도메인 지식 기반으로 현실적인 패턴을 재현한 합성 데이터입니다.
> Production에서는 각 API/Marketplace 실데이터로 자동 교체되도록 파이프라인이 설계되어 있습니다.
> 스폰서 데이터(SPH, Richgo)는 Marketplace 실데이터입니다.

## Project Structure

```
districtpilot-ai/
|-- README.md
|
|-- Snowflake SQL Pipeline --
|-- 00_rename_database.sql          # DB rename (MOVESIGNAL -> DISTRICTPILOT)
|-- 02_feature_mart_v4.sql          # Feature Mart (5 STG -> FEATURE_MART_FINAL)
|-- 03_ml_and_cortex_v2.sql         # ML Forecast + Cortex LLM
|-- 06_semantic_view.sql            # Semantic View + VQR 10개 (Cortex Analyst)
|-- 07_dynamic_tables_tasks.sql     # Dynamic Tables + Tasks + V_APP_HEALTH
|-- 08_ajd_integration.sql          # AJD 통신/렌탈 데이터 통합 + FEATURE_MART_V3
|-- 09_cortex_search_agent.sql      # Cortex Search + Agent
|-- 10_external_data.sql            # 4개 외부 데이터 + FEATURE_MART_V2
|-- 11_ablation_study.sql           # Ablation A->E (5 모델 비교)
|
|-- Streamlit App --
|-- streamlit_app_v8.py             # 최종 앱 (5탭)
|-- snowflake.yml                   # SiS 배포 설정
|
|-- Docs --
|-- DEMO_SCRIPT.md                  # 10분 데모 스크립트
|-- evidence_chain_slide.md         # Evidence Chain 슬라이드 + 30초 멘트
|-- DATA_SOURCES_AND_LICENSES.md    # 데이터 소스 + 라이선스
|-- SUBMISSION_CHECKLIST.md         # 제출 체크리스트
|-- DOMAIN_POSITIONING.md           # 도메인 정의와 심사 메시지
|-- FINAL_PRE_SUBMISSION_RUNBOOK.md # 제출 직전 15분 런북
|-- LIVE_SUBMISSION_NOTES.md        # 라이브 오브젝트 정합성 기준
|-- 12_final_precheck.sql           # Snowsight 최종 점검 SQL
|-- 14_judge_fastpath.sql           # 심사자용 빠른 검증 SQL
|-- 13_live_app_compatibility_patch.sql # 라이브 앱 호환 패치
|-- JUDGE_FASTPATH.md               # 심사자용 3분 가이드
|-- generate_pptx.py                # 최종 PPT 생성 스크립트
|-- build_demo_video.py             # 한국어 TTS 데모 영상 생성 스크립트
```

## Snowflake Objects

| Object | Type | Description |
|--------|------|-------------|
| `FEATURE_MART_V3` | Table | AJD 통합 Feature Mart (60+ features, 렌탈/CS 포함) |
| `FEATURE_MART_V2` | Table | 확장 Feature Mart (50+ features) |
| `STG_TELECOM` | Table | AJD 통신/렌탈 스테이징 (구별 월별) |
| `DT_FEATURE_MART` | Dynamic Table | 자동 갱신 (1h lag) |
| `DISTRICTPILOT_FORECAST_V2` | ML Model | Ablation 최종 모델 (외생변수 포함) |
| `ABLATION_RESULTS` | Table/View | 5 모델 MAPE/SMAPE/MAE 비교 |
| `DISTRICTPILOT_SV` | Semantic View | Cortex Analyst용 비즈니스 메트릭 (VQR 10개) |
| `DISTRICTPILOT_SEARCH_SVC` | Cortex Search | 정책/룰북 문서 검색 |
| `V_APP_HEALTH` | View | 운영 상태 모니터링 |

## Prerequisites

- Snowflake account with **ACCOUNTADMIN** role
- Marketplace datasets: **SPH**, **Richgo**
- Warehouse: `COMPUTE_WH` (X-Small)
- Streamlit in Snowflake enabled

```sql
CREATE DATABASE IF NOT EXISTS DISTRICTPILOT_AI;
CREATE SCHEMA IF NOT EXISTS DISTRICTPILOT_AI.ANALYTICS;
CREATE STAGE IF NOT EXISTS DISTRICTPILOT_AI.ANALYTICS.STREAMLIT_STAGE
  DIRECTORY = (ENABLE = TRUE);
```

## Execution Order

```sql
-- 1. Base pipeline
02_feature_mart_v4.sql
03_ml_and_cortex_v2.sql

-- 2. External data + extended features
10_external_data.sql

-- 2.5. AJD 통신/렌탈 통합
08_ajd_integration.sql

-- 3. Ablation study
11_ablation_study.sql

-- 4. AI layer
06_semantic_view.sql
09_cortex_search_agent.sql

-- 5. Operations
07_dynamic_tables_tasks.sql

-- 6. Optional live hardening
12_final_precheck.sql
13_live_app_compatibility_patch.sql

-- 7. Deploy Streamlit
streamlit_app_v8.py
```

## Deploy Streamlit

From the repo root, deploy the app with a Snowflake CLI connection that points
to `DISTRICTPILOT_AI.ANALYTICS`:

```bash
snow streamlit deploy DISTRICTPILOT_APP --replace --open
```

Smoke-test deployment immediately after:

- `SHOW DATABASES LIKE 'DISTRICTPILOT_AI';`
- `SHOW STREAMLITS LIKE 'DISTRICTPILOT_APP';`
- if the opened URL says `App not found`, verify the same account / database /
  schema / role was used for deploy and for browser access

## Submission Artifacts

- Final PPT: `deliverables/DistrictPilot_AI_Hackathon_Final.pptx`
- Narrated demo video: `deliverables/DistrictPilot_AI_Demo_Narrated.mp4`
- Source ZIP: `deliverables/DistrictPilot_AI_Submission.zip`

To regenerate the presentation and video locally:

```bash
python3 generate_pptx.py
python3 build_demo_video.py
```

## Key Differentiators

- **Ablation Study**: Sponsor-only -> +Holiday -> +Age -> +Tourism -> +Commercial, MAPE 개선 정량화
- **Evidence Chain**: Forecast -> Feature Importance -> Semantic View -> Search Grounding -> Structured Output -> Refresh State
- **Domain Fit**: SPH(소비) + Richgo(이동) + AJD(렌탈/CS) 3사 데이터가 전입·이사 → 홈서비스 수요로 자연스럽게 수렴
- **데이터 투명성**: 스폰서 실데이터 + 합성 외부 데이터를 명시적으로 구분, Production 교체 경로 제공
- **Production-Ready**: Dynamic Tables (1h lag) + Tasks (daily/weekly) + Health monitoring + Query tags

## Cost

~**$80/month** (Compute WH X-Small + Cortex LLM + Streamlit + Dynamic Tables)

## Author

**Doeon Kim** - [GitHub](https://github.com/KIM3310)

---

*Snowflake Korea Hackathon 2026 Tech Track*
*Submission deadline: 2026-04-12 23:59 KST*
*Finals: 2026-04-29, Seoul (10min presentation + 3min Q&A)*
