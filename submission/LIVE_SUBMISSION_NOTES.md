# DistrictPilot AI - Live Submission Notes

This document is the authoritative submission note for the live Snowflake build.

## Canonical Snowflake Objects

- Database / schema: `DISTRICTPILOT_AI.ANALYTICS`
- Feature mart: `DT_FEATURE_MART` -> `FEATURE_MART_V2` -> `FEATURE_MART_V3` (AJD 포함)
- AJD staging: `STG_TELECOM` (렌탈/계약/CS)
- Final forecast model: `DISTRICTPILOT_FORECAST_V2`
- Search service: `DISTRICTPILOT_SEARCH_SVC`
- Semantic view: `DISTRICTPILOT_SV`
- Health view: `V_APP_HEALTH`

## Demo-Before-Submission Order

1. Run [`12_final_precheck.sql`](12_final_precheck.sql)
2. Open the deployed Streamlit app and test all five tabs
3. Run [`14_judge_fastpath.sql`](14_judge_fastpath.sql) and keep the results ready for demo-day verification
4. If `SHOW STREAMLITS LIKE 'DISTRICTPILOT_APP'` returns nothing, redeploy with `snow streamlit deploy DISTRICTPILOT_APP --replace --open`
5. If the deployed app still expects legacy object names, run [`13_live_app_compatibility_patch.sql`](13_live_app_compatibility_patch.sql) as a last resort
6. Re-test `Allocation`, `Analysis`, and `Ops / Trust`
7. Record the final demo or submit the final package

## Compatibility Rule

The current app prefers canonical live objects first and only falls back when needed.

Legacy names that may still appear in older deployments:

- `DISTRICTPILOT_FORECAST`
- `ABLATION_RESULTS.MODEL`
- `ABLATION_RESULTS.DISTRICT`

If an older deployed build still breaks on those names, use [`13_live_app_compatibility_patch.sql`](13_live_app_compatibility_patch.sql) to restore compatibility without redeploying the app.

## Judge Message

DistrictPilot AI is not only a forecasting dashboard. It combines 3 Snowflake Marketplace datasets (SPH + Richgo + AJD), ML Forecast with ablation, Dynamic Tables, Tasks, Semantic View, Cortex Search, AI_COMPLETE structured output, and Streamlit into a single move-in driven home-service decision system. External data uses realistic synthetic patterns with production API replacement paths documented.
