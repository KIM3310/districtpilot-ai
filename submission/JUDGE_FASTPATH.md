# DistrictPilot AI - Judge Fast Path

이 문서는 심사자가 레포와 Snowflake 환경을 빠르게 훑을 때, 이 프로젝트가 `지역 대시보드`가 아니라 `전입·이사 시그널을 홈서비스 액션으로 번역하는 Snowflake-native 운영 시스템`이라는 점을 바로 확인하도록 돕기 위한 안내서입니다.

## 3분 확인 순서

### 1. 레포에서 먼저 볼 것

1. [`DOMAIN_POSITIONING.md`](DOMAIN_POSITIONING.md): 도메인 정의와 왜 이 문제가 맞는지
2. [`README.md`](README.md): 문제, 아키텍처, Snowflake 오브젝트, 실행 순서
3. [`DEMO_SCRIPT.md`](DEMO_SCRIPT.md): 10분 발표 흐름과 Q&A

### 2. Snowsight에서 바로 확인할 것

[`14_judge_fastpath.sql`](14_judge_fastpath.sql)을 실행해 아래를 확인합니다.

- `FEATURE_MART_V2`, `FORECAST_RESULTS`, `ACTUAL_VS_FORECAST`, `FEATURE_IMPORTANCE`, `ABLATION_RESULTS`가 비어 있지 않음
- `DISTRICTPILOT_FORECAST_V2` 또는 `DISTRICTPILOT_FORECAST` 모델이 존재함
- `DISTRICTPILOT_SV`가 유효하게 검증됨
- `DISTRICTPILOT_SEARCH_SVC`와 Streamlit 앱이 배포돼 있음
- `V_APP_HEALTH` 또는 Dynamic Table/Task 상태가 조회됨

### 3. 앱에서 클릭할 순서

1. `Capture Plan`
   다음 달 집행 강도, Actual vs Forecast + **95% 신뢰구간**, Ablation **MAPE 개선 delta**
2. `Move-in Signals`
   **구별 핵심 인사이트 콜아웃**, 전입/소비/관광/상권 신호, Feature Importance
3. `AI Playbook`
   grounded recommendation + structured output + **Cortex Search 인용 문서 표시**
4. `Scenario Lab`
   AI vs 사용자 비교 + **편차 경고 알림** (15%p 이상 차이 시)
5. `Ops / Trust`
   freshness, 실행 컨텍스트, semantic validation, 보안/거버넌스

## 심사 항목별 핵심 증거 (공식 배점 기준)

| 심사 항목 | 배점 | 핵심 증거 |
|----------|------|----------|
| **비즈니스 임팩트** | **25%** | 연 700만 이사 × 72시간 골든타임. SPH+Richgo+AJD 3사 데이터가 이사 밸류체인 완벽 커버 |
| **기술 구현** | **25%** | Marketplace 3사 + ML FORECAST(외생변수) + Ablation 5모델 + AI_COMPLETE + DT + Tasks + Streamlit = 8개 Snowflake 기능 |
| **솔루션 완성도** | **20%** | 데이터 수집 → 예측 → 액션 추천 → 시뮬레이션 → 운영 모니터링 End-to-End 완결 |
| **데이터 분석 & 인사이트** | **20%** | Feature Importance(외생변수 기여도) + Ablation(5모델 MAPE 비교) + 구별 인사이트 콜아웃 |
| **발표 품질** | **10%** | 5탭 라이브 데모, Q&A 9문항 대비 |

## 이 레포가 강하게 보이는 이유

- **골든타임 72시간 (비즈니스 임팩트 25%)**: 연 700만 이사 시장, 이사 직후가 홈서비스 전환 최적 시점
- **기술 스택 (25%)**: 8개 Snowflake 기능 통합, 외생변수 ML FORECAST + Feature Importance
- **End-to-End (20%)**: 데이터 수집 → ML → AI → 시뮬레이션 → 운영 모니터링 완결
- **데이터 교차 (20%)**: SPH(소비) + Richgo(이동) + AJD(렌탈) 3사 데이터가 이사 밸류체인을 완벽 커버
