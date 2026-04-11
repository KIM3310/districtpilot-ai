# DistrictPilot AI - Live Submission Notes

This document is the authoritative submission note for the live Snowflake build.

## Canonical Snowflake Objects

- Database / schema: `DISTRICTPILOT_AI.ANALYTICS`
- Feature mart: `DT_FEATURE_MART` -> `FEATURE_MART_V2`
- Final forecast model: `DISTRICTPILOT_FORECAST_V2`
- Search service: `DISTRICTPILOT_SEARCH_SVC`
- Semantic view: `DISTRICTPILOT_SV`
- Health view: `V_APP_HEALTH`

## Demo-Before-Submission Order

1. Run [`12_final_precheck.sql`](12_final_precheck.sql)
2. Open the deployed Streamlit app and test all five tabs
3. If the deployed app still expects legacy object names, run [`13_live_app_compatibility_patch.sql`](13_live_app_compatibility_patch.sql)
4. Re-test `Allocation`, `Analysis`, and `Ops / Trust`
5. Record the final demo or submit the final package

## Compatibility Rule

The live app may still reference legacy names such as:

- `DISTRICTPILOT_FORECAST`
- `ABLATION_RESULTS.MODEL`
- `ABLATION_RESULTS.DISTRICT`

If that happens, use [`13_live_app_compatibility_patch.sql`](13_live_app_compatibility_patch.sql) to restore compatibility without redeploying the app.

## Judge Message

DistrictPilot AI is not only a forecasting dashboard. It combines Snowflake Marketplace data, ML Forecast, Dynamic Tables, Tasks, Semantic View, Cortex Search, Cortex completion, and Streamlit into a single operational decision system.
