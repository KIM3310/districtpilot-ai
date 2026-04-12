# DistrictPilot AI — Final Pre-Submission Runbook

## Goal

Submit a stable build that scores well on (공식 심사 기준):

- 문제 정의 및 접근법 (20점)
- 기술 구현 (30점)
- 결과 품질 및 인사이트 (30점)
- 발표 품질 (20점)

## 15-Minute Preflight

### 1. Snowflake object health

Run [`12_final_precheck.sql`](12_final_precheck.sql) in Snowsight and confirm:

- `SHOW DATABASES LIKE 'DISTRICTPILOT_AI'` returns the target database
- `DISTRICTPILOT_FORECAST_V2` or `DISTRICTPILOT_FORECAST` exists
- `FEATURE_MART_V3` (또는 V2), `FORECAST_RESULTS`, `FEATURE_IMPORTANCE`, `ABLATION_RESULTS`, `STG_TELECOM` row counts are non-zero
- `DISTRICTPILOT_SV` validates successfully
- `V_APP_HEALTH` returns rows
- Streamlit app is listed in `SHOW STREAMLITS LIKE 'DISTRICTPILOT_APP'`
- for a judge-facing smoke test, run [`14_judge_fastpath.sql`](14_judge_fastpath.sql) right after and keep that Snowsight tab open
- if the app is missing, redeploy from the repo root with `snow streamlit deploy DISTRICTPILOT_APP --replace --open`

### 2. Streamlit app click-through

Open the app and verify all five tabs:

- `Capture Plan`: next-month capture intensity, overlay chart, ablation chart
- `Move-in Signals`: 인사이트 콜아웃, move-in / spending / AJD 렌탈 신호 / tourism / commercial, feature importance
- `AI Playbook`: one Korean prompt returns structured answer
- `Scenario Lab`: slider comparison and AI comment both work
- `Ops / Trust`: health evidence and semantic-view validation display
- app banner shows the live Feature Mart and Forecast model names you are actually using

### 3. Submission package

Confirm the following are ready:

- source ZIP from `submission/`
- PPT
- demo video link
- final README
- final Korean demo script

### 4. Rule compliance

- sponsor raw data is not included in GitHub or ZIP
- public data sources and licenses are documented
- AJD는 합성 fallback으로 통합됨 (Production에서 실데이터 교체)
- all submission artifacts are in Korean

## Demo Fail-Safe Order

If time is short or a screen is slow, keep this order:

1. `Capture Plan`
2. `Move-in Signals`
3. `AI Playbook`
4. `Ops / Trust`
5. `Scenario Lab`

This sequence maximizes judging evidence even if you must skip one tab.

## Judge-Facing One-Liners

- Creativity: "We did not stop at prediction; we turned move-in signals into an actioning agent."
- Snowflake depth: "Marketplace, ML Forecast, Semantic View, Cortex, Dynamic Tables, Tasks, and Streamlit all run in one account."
- AI rigor: "Ablation and feature-importance prove why the recommendation exists."
- Realism: "The app shows freshness, governance, operating constraints, and cost, not just charts."
- Presentation: "One engine connects move-in detection, recommendation, and operator action."

## Final Rule

If anything is inconsistent between slides, README, and app, trust the live Snowflake app and fix the docs to match it before submission.
