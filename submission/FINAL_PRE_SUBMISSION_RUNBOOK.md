# DistrictPilot AI — Final Pre-Submission Runbook

## Goal

Submit a stable build that scores well on:

- Creativity / originality
- Snowflake platform depth
- AI / data rigor
- Real-world operability
- Presentation clarity

## 15-Minute Preflight

### 1. Snowflake object health

Run [`12_final_precheck.sql`](12_final_precheck.sql) in Snowsight and confirm:

- `DISTRICTPILOT_FORECAST_V2` or `DISTRICTPILOT_FORECAST` exists
- `FEATURE_MART_V2`, `FORECAST_RESULTS`, `FEATURE_IMPORTANCE`, `ABLATION_RESULTS` row counts are non-zero
- `DISTRICTPILOT_SV` validates successfully
- `V_APP_HEALTH` returns rows
- Streamlit app is listed in `SHOW STREAMLITS`

### 2. Streamlit app click-through

Open the app and verify all five tabs:

- `Allocation`: next-month allocation, overlay chart, ablation chart
- `Analysis`: KPI cards, age/tourism/commercial signals, feature importance
- `AI Agent`: one Korean prompt returns structured answer
- `Simulation`: slider comparison and AI comment both work
- `Ops / Trust`: health evidence and semantic-view validation display

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
- AJD is described as optional unless fully integrated
- all submission artifacts are in Korean

## Demo Fail-Safe Order

If time is short or a screen is slow, keep this order:

1. `Allocation`
2. `Analysis`
3. `AI Agent`
4. `Ops / Trust`
5. `Simulation`

This sequence maximizes judging evidence even if you must skip one tab.

## Judge-Facing One-Liners

- Creativity: "We did not stop at prediction; we turned forecast into an actioning agent."
- Snowflake depth: "Marketplace, ML Forecast, Semantic View, Cortex, Dynamic Tables, Tasks, and Streamlit all run in one account."
- AI rigor: "Ablation and feature-importance prove why the recommendation exists."
- Realism: "The app shows freshness, governance, and operating cost, not just charts."
- Presentation: "One engine serves both private rental allocation and public resource planning."

## Final Rule

If anything is inconsistent between slides, README, and app, trust the live Snowflake app and fix the docs to match it before submission.
