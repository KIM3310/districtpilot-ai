# DistrictPilot AI - Judge Fast Path

이 문서는 심사자가 레포와 Snowflake 환경을 빠르게 훑을 때, 이 프로젝트가 `대시보드`가 아니라 `운영 가능한 Snowflake-native 의사결정 시스템`이라는 점을 바로 확인하도록 돕기 위한 안내서입니다.

## 3분 확인 순서

### 1. 레포에서 먼저 볼 것

1. [`README.md`](README.md): 문제, 아키텍처, Snowflake 오브젝트, 실행 순서
2. [`JUDGE_FASTPATH.md`](JUDGE_FASTPATH.md): 심사용 빠른 동선
3. [`DEMO_SCRIPT.md`](DEMO_SCRIPT.md): 10분 발표 흐름과 Q&A

### 2. Snowsight에서 바로 확인할 것

[`14_judge_fastpath.sql`](14_judge_fastpath.sql)을 실행해 아래를 확인합니다.

- `FEATURE_MART_V2`, `FORECAST_RESULTS`, `ACTUAL_VS_FORECAST`, `FEATURE_IMPORTANCE`, `ABLATION_RESULTS`가 비어 있지 않음
- `DISTRICTPILOT_FORECAST_V2` 또는 `DISTRICTPILOT_FORECAST` 모델이 존재함
- `DISTRICTPILOT_SV`가 유효하게 검증됨
- `DISTRICTPILOT_SEARCH_SVC`와 Streamlit 앱이 배포돼 있음
- `V_APP_HEALTH` 또는 Dynamic Table/Task 상태가 조회됨

### 3. 앱에서 클릭할 순서

1. `Allocation`
   다음 달 배분 비중, Actual vs Forecast, Ablation 개선
2. `Analysis`
   구별 KPI, 연령/관광/상권 신호, Feature Importance
3. `AI Agent`
   grounded recommendation + structured output
4. `Ops / Trust`
   freshness, 실행 컨텍스트, semantic validation, 보안/거버넌스

## 심사 포인트별 한 줄 요약

- Creativity: 예측에서 끝나지 않고, 예산 배분 액션까지 연결했습니다.
- Snowflake depth: Marketplace, ML Forecast, Semantic View, Cortex, Dynamic Tables, Tasks, Streamlit을 한 계정 안에서 연결했습니다.
- AI rigor: Ablation, evaluation metrics, feature importance로 추천의 근거를 수치로 보여줍니다.
- Realism: `V_APP_HEALTH`, query tag, 운영 주기, 비용, 거버넌스를 앱 안에서 확인할 수 있습니다.
- Presentation: 심사자가 따라오기 쉬운 탭 순서와 증거 체인을 문서와 앱에서 동일하게 유지했습니다.

## 이 레포가 강하게 보이는 이유

- 최신 모델명과 레거시 모델명을 앱이 자동으로 흡수하도록 설계했습니다.
- 추천 결과가 `Forecast -> Importance -> Semantic -> AI action`으로 이어져 설명력이 높습니다.
- 외부 AI 서비스에 화면 밖 데이터 파이프라인을 붙이지 않고 Snowflake 안에서 완결합니다.
- 제출 전 점검 문서, 런북, 빠른 검증 SQL이 함께 있어 운영 감각이 드러납니다.
