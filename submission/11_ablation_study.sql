-- ============================================================
-- MoveSignal AI: Ablation Study
-- Compares forecast accuracy across incremental feature sets
-- Models A through E with progressive feature addition
-- Target districts: 서초구, 영등포구, 중구
-- ============================================================

-- ============================================================
-- 0. Session Setup
-- ============================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MOVESIGNAL_AI;
USE SCHEMA ANALYTICS;

ALTER SESSION SET QUERY_TAG = '{"app":"movesignal_ai","module":"ablation_study","version":"v2"}';

-- Ablation study design:
--   Model A: Y-only baseline (TOTAL_SALES, no exogenous variables)
--   Model B: + Holiday features (HOLIDAY_DAYS, LONG_WEEKEND_CNT, BUSINESS_DAYS)
--   Model C: + Demographics (AGE_20_39_SHARE, SENIOR_60P_SHARE, FAMILY_30_49_SHARE)
--   Model D: + Tourism (TOURISM_DEMAND_IDX, FOREIGN_VISITOR_IDX)
--   Model E: Full model (+ STABILITY_SCORE, NET_STORE_CHANGE) → production


-- ============================================================
-- 1. Model A: Sponsor-only (Y-only baseline)
--    No exogenous features — pure autoregressive time-series
-- ============================================================

-- 1a. Prepare input: only timestamp, series key, and target
CREATE OR REPLACE TABLE FORECAST_INPUT_A AS
SELECT
    TO_DATE(YM || '01', 'YYYYMMDD')  AS DS,
    DISTRICT,
    TOTAL_SALES                       AS Y
FROM FEATURE_MART_FINAL
WHERE TOTAL_SALES IS NOT NULL
  AND TOTAL_SALES > 0
  AND DISTRICT IN ('서초구', '영등포구', '중구')
ORDER BY DISTRICT, DS;

-- 1b. Verify row counts per district
SELECT DISTRICT, MIN(DS) AS START_DT, MAX(DS) AS END_DT, COUNT(*) AS MONTHS
FROM FORECAST_INPUT_A
GROUP BY DISTRICT
ORDER BY DISTRICT;

-- 1c. Train Model A
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST MODEL_A(
    INPUT_DATA      => SYSTEM$REFERENCE('TABLE', 'FORECAST_INPUT_A'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME    => 'Y',
    SERIES_COLNAME    => 'DISTRICT'
);

-- 1d. Generate 3-month forecast
CALL MODEL_A!FORECAST(
    FORECASTING_PERIODS => 3,
    CONFIG_OBJECT       => {'prediction_interval': 0.95}
);

CREATE OR REPLACE TABLE FORECAST_RESULTS_A AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 1e. Retrieve evaluation metrics for Model A
CALL MODEL_A!SHOW_EVALUATION_METRICS();

CREATE OR REPLACE TABLE EVAL_METRICS_A AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT 'Model A (Y-only baseline) — training complete' AS STATUS;


-- ============================================================
-- 2. Model B: Sponsor + Holiday features
--    Adds calendar/holiday signals as exogenous variables
-- ============================================================

-- 2a. Prepare input with holiday features
CREATE OR REPLACE TABLE FORECAST_INPUT_B AS
SELECT
    TO_DATE(YM || '01', 'YYYYMMDD')  AS DS,
    DISTRICT,
    TOTAL_SALES                       AS Y,
    HOLIDAY_DAYS,
    LONG_WEEKEND_CNT,
    BUSINESS_DAYS
FROM FEATURE_MART_FINAL
WHERE TOTAL_SALES IS NOT NULL
  AND TOTAL_SALES > 0
  AND DISTRICT IN ('서초구', '영등포구', '중구')
ORDER BY DISTRICT, DS;

-- 2b. Verify
SELECT DISTRICT, COUNT(*) AS MONTHS,
       SUM(CASE WHEN HOLIDAY_DAYS IS NULL THEN 1 ELSE 0 END) AS NULL_HOLIDAY
FROM FORECAST_INPUT_B
GROUP BY DISTRICT
ORDER BY DISTRICT;

-- 2c. Train Model B
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST MODEL_B(
    INPUT_DATA      => SYSTEM$REFERENCE('TABLE', 'FORECAST_INPUT_B'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME    => 'Y',
    SERIES_COLNAME    => 'DISTRICT'
);

-- 2d. Prepare future exogenous values for prediction period
--     Holiday values are known in advance (calendar-based)
CREATE OR REPLACE TABLE FORECAST_INPUT_FUTURE_B AS
WITH future_months AS (
    SELECT DATEADD(MONTH, seq, (SELECT DATEADD(MONTH, 1, MAX(DS)) FROM FORECAST_INPUT_B)) AS DS
    FROM TABLE(GENERATOR(ROWCOUNT => 3)) t,
         LATERAL (SELECT SEQ4() AS seq) s
),
districts AS (
    SELECT DISTINCT DISTRICT FROM FORECAST_INPUT_B
)
SELECT
    f.DS,
    d.DISTRICT,
    -- Known future holiday counts (reasonable defaults for Seoul)
    CASE MONTH(f.DS)
        WHEN 1  THEN 3   -- 신정 + 설날
        WHEN 2  THEN 2   -- 설날 연휴
        WHEN 3  THEN 2   -- 삼일절
        WHEN 4  THEN 1   -- 식목일 area
        WHEN 5  THEN 3   -- 어린이날, 석가탄신일, 근로자의날
        WHEN 6  THEN 1   -- 현충일
        WHEN 7  THEN 0
        WHEN 8  THEN 1   -- 광복절
        WHEN 9  THEN 3   -- 추석
        WHEN 10 THEN 2   -- 개천절, 한글날
        WHEN 11 THEN 0
        WHEN 12 THEN 1   -- 크리스마스
    END AS HOLIDAY_DAYS,
    CASE MONTH(f.DS)
        WHEN 1  THEN 1
        WHEN 5  THEN 2
        WHEN 9  THEN 1
        WHEN 10 THEN 1
        ELSE 0
    END AS LONG_WEEKEND_CNT,
    -- Approximate business days per month
    DATEDIFF(DAY, f.DS, DATEADD(MONTH, 1, f.DS))
        - (DATEDIFF(DAY, f.DS, DATEADD(MONTH, 1, f.DS)) / 7) * 2
        - CASE MONTH(f.DS)
              WHEN 1 THEN 3 WHEN 2 THEN 2 WHEN 3 THEN 2 WHEN 5 THEN 3
              WHEN 9 THEN 3 WHEN 10 THEN 2 ELSE 1
          END AS BUSINESS_DAYS
FROM future_months f
CROSS JOIN districts d
ORDER BY d.DISTRICT, f.DS;

-- 2e. Generate forecast with known future exogenous values
CALL MODEL_B!FORECAST(
    FORECASTING_PERIODS => 3,
    EXOGENOUS_DATA      => SYSTEM$REFERENCE('TABLE', 'FORECAST_INPUT_FUTURE_B'),
    CONFIG_OBJECT       => {'prediction_interval': 0.95}
);

CREATE OR REPLACE TABLE FORECAST_RESULTS_B AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 2f. Evaluation metrics
CALL MODEL_B!SHOW_EVALUATION_METRICS();

CREATE OR REPLACE TABLE EVAL_METRICS_B AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT 'Model B (+ Holiday) — training complete' AS STATUS;


-- ============================================================
-- 3. Model C: Sponsor + Holiday + Demographics
--    Adds age/family demographic composition shares
-- ============================================================

-- 3a. Prepare input
CREATE OR REPLACE TABLE FORECAST_INPUT_C AS
SELECT
    TO_DATE(YM || '01', 'YYYYMMDD')  AS DS,
    DISTRICT,
    TOTAL_SALES                       AS Y,
    -- Holiday
    HOLIDAY_DAYS,
    LONG_WEEKEND_CNT,
    BUSINESS_DAYS,
    -- Demographics
    AGE_20_39_SHARE,
    SENIOR_60P_SHARE,
    FAMILY_30_49_SHARE
FROM FEATURE_MART_FINAL
WHERE TOTAL_SALES IS NOT NULL
  AND TOTAL_SALES > 0
  AND DISTRICT IN ('서초구', '영등포구', '중구')
ORDER BY DISTRICT, DS;

-- 3b. Train Model C
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST MODEL_C(
    INPUT_DATA      => SYSTEM$REFERENCE('TABLE', 'FORECAST_INPUT_C'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME    => 'Y',
    SERIES_COLNAME    => 'DISTRICT'
);

-- 3c. Generate forecast (demographics are slow-moving, use last known values)
CALL MODEL_C!FORECAST(
    FORECASTING_PERIODS => 3,
    CONFIG_OBJECT       => {'prediction_interval': 0.95}
);

CREATE OR REPLACE TABLE FORECAST_RESULTS_C AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 3d. Evaluation metrics
CALL MODEL_C!SHOW_EVALUATION_METRICS();

CREATE OR REPLACE TABLE EVAL_METRICS_C AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT 'Model C (+ Demographics) — training complete' AS STATUS;


-- ============================================================
-- 4. Model D: Sponsor + Holiday + Demographics + Tourism
--    Adds tourism demand and foreign visitor indices
-- ============================================================

-- 4a. Prepare input
CREATE OR REPLACE TABLE FORECAST_INPUT_D AS
SELECT
    TO_DATE(YM || '01', 'YYYYMMDD')  AS DS,
    DISTRICT,
    TOTAL_SALES                       AS Y,
    -- Holiday
    HOLIDAY_DAYS,
    LONG_WEEKEND_CNT,
    BUSINESS_DAYS,
    -- Demographics
    AGE_20_39_SHARE,
    SENIOR_60P_SHARE,
    FAMILY_30_49_SHARE,
    -- Tourism
    TOURISM_DEMAND_IDX,
    FOREIGN_VISITOR_IDX
FROM FEATURE_MART_FINAL
WHERE TOTAL_SALES IS NOT NULL
  AND TOTAL_SALES > 0
  AND DISTRICT IN ('서초구', '영등포구', '중구')
ORDER BY DISTRICT, DS;

-- 4b. Train Model D
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST MODEL_D(
    INPUT_DATA      => SYSTEM$REFERENCE('TABLE', 'FORECAST_INPUT_D'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME    => 'Y',
    SERIES_COLNAME    => 'DISTRICT'
);

-- 4c. Generate forecast
CALL MODEL_D!FORECAST(
    FORECASTING_PERIODS => 3,
    CONFIG_OBJECT       => {'prediction_interval': 0.95}
);

CREATE OR REPLACE TABLE FORECAST_RESULTS_D AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 4d. Evaluation metrics
CALL MODEL_D!SHOW_EVALUATION_METRICS();

CREATE OR REPLACE TABLE EVAL_METRICS_D AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT 'Model D (+ Tourism) — training complete' AS STATUS;


-- ============================================================
-- 5. Model E: Full (All features)
--    Adds business stability signals → production model
-- ============================================================

-- 5a. Prepare input — all feature groups
CREATE OR REPLACE TABLE FORECAST_INPUT_E AS
SELECT
    TO_DATE(YM || '01', 'YYYYMMDD')  AS DS,
    DISTRICT,
    TOTAL_SALES                       AS Y,
    -- Holiday
    HOLIDAY_DAYS,
    LONG_WEEKEND_CNT,
    BUSINESS_DAYS,
    -- Demographics
    AGE_20_39_SHARE,
    SENIOR_60P_SHARE,
    FAMILY_30_49_SHARE,
    -- Tourism
    TOURISM_DEMAND_IDX,
    FOREIGN_VISITOR_IDX,
    -- Business stability
    STABILITY_SCORE,
    NET_STORE_CHANGE
FROM FEATURE_MART_FINAL
WHERE TOTAL_SALES IS NOT NULL
  AND TOTAL_SALES > 0
  AND DISTRICT IN ('서초구', '영등포구', '중구')
ORDER BY DISTRICT, DS;

-- 5b. Verify full feature coverage
SELECT
    DISTRICT,
    COUNT(*)                                                          AS MONTHS,
    SUM(CASE WHEN STABILITY_SCORE    IS NULL THEN 1 ELSE 0 END)      AS NULL_STABILITY,
    SUM(CASE WHEN NET_STORE_CHANGE   IS NULL THEN 1 ELSE 0 END)      AS NULL_NET_STORE,
    SUM(CASE WHEN TOURISM_DEMAND_IDX IS NULL THEN 1 ELSE 0 END)      AS NULL_TOURISM,
    SUM(CASE WHEN FOREIGN_VISITOR_IDX IS NULL THEN 1 ELSE 0 END)     AS NULL_FOREIGN
FROM FORECAST_INPUT_E
GROUP BY DISTRICT
ORDER BY DISTRICT;

-- 5c. Train Model E (full model)
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST MODEL_E(
    INPUT_DATA      => SYSTEM$REFERENCE('TABLE', 'FORECAST_INPUT_E'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME    => 'Y',
    SERIES_COLNAME    => 'DISTRICT'
);

-- 5d. Generate forecast
CALL MODEL_E!FORECAST(
    FORECASTING_PERIODS => 3,
    CONFIG_OBJECT       => {'prediction_interval': 0.95}
);

CREATE OR REPLACE TABLE FORECAST_RESULTS_E AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 5e. Evaluation metrics
CALL MODEL_E!SHOW_EVALUATION_METRICS();

CREATE OR REPLACE TABLE EVAL_METRICS_E AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 5f. Alias Model E as production model MOVESIGNAL_FORECAST_V2
--     Snowflake ML models do not support ALTER RENAME, so we re-create
--     under the production alias using the same input/config
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST MOVESIGNAL_FORECAST_V2(
    INPUT_DATA      => SYSTEM$REFERENCE('TABLE', 'FORECAST_INPUT_E'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME    => 'Y',
    SERIES_COLNAME    => 'DISTRICT'
);

SELECT 'Model E (Full / Production) — training complete' AS STATUS;


-- ============================================================
-- 6. Collect all evaluation metrics into ABLATION_RESULTS
--    Unified comparison table across all five models
-- ============================================================

-- The evaluation metrics tables from SHOW_EVALUATION_METRICS()
-- contain columns: SERIES, METRIC, VALUE (structure may vary).
-- We pivot them into a single flat table for easy comparison.

CREATE OR REPLACE TABLE ABLATION_RESULTS (
    MODEL_NAME    VARCHAR(20),
    FEATURE_SET   VARCHAR(200),
    SERIES        VARCHAR(50),
    MAPE          FLOAT,
    SMAPE         FLOAT,
    MAE           FLOAT
);

-- Helper: extract metrics from each model's evaluation table
-- Model A
INSERT INTO ABLATION_RESULTS
SELECT
    'MODEL_A'                            AS MODEL_NAME,
    'Y-only (baseline)'                  AS FEATURE_SET,
    SERIES,
    MAX(CASE WHEN UPPER(METRIC) = 'MAPE'  THEN VALUE END) AS MAPE,
    MAX(CASE WHEN UPPER(METRIC) = 'SMAPE' THEN VALUE END) AS SMAPE,
    MAX(CASE WHEN UPPER(METRIC) = 'MAE'   THEN VALUE END) AS MAE
FROM EVAL_METRICS_A
GROUP BY SERIES;

-- Model B
INSERT INTO ABLATION_RESULTS
SELECT
    'MODEL_B'                                           AS MODEL_NAME,
    '+ Holiday (HOLIDAY_DAYS, LONG_WEEKEND, BIZ_DAYS)' AS FEATURE_SET,
    SERIES,
    MAX(CASE WHEN UPPER(METRIC) = 'MAPE'  THEN VALUE END) AS MAPE,
    MAX(CASE WHEN UPPER(METRIC) = 'SMAPE' THEN VALUE END) AS SMAPE,
    MAX(CASE WHEN UPPER(METRIC) = 'MAE'   THEN VALUE END) AS MAE
FROM EVAL_METRICS_B
GROUP BY SERIES;

-- Model C
INSERT INTO ABLATION_RESULTS
SELECT
    'MODEL_C'                                                       AS MODEL_NAME,
    '+ Demographics (AGE_20_39, SENIOR_60P, FAMILY_30_49)'          AS FEATURE_SET,
    SERIES,
    MAX(CASE WHEN UPPER(METRIC) = 'MAPE'  THEN VALUE END) AS MAPE,
    MAX(CASE WHEN UPPER(METRIC) = 'SMAPE' THEN VALUE END) AS SMAPE,
    MAX(CASE WHEN UPPER(METRIC) = 'MAE'   THEN VALUE END) AS MAE
FROM EVAL_METRICS_C
GROUP BY SERIES;

-- Model D
INSERT INTO ABLATION_RESULTS
SELECT
    'MODEL_D'                                                    AS MODEL_NAME,
    '+ Tourism (TOURISM_DEMAND_IDX, FOREIGN_VISITOR_IDX)'        AS FEATURE_SET,
    SERIES,
    MAX(CASE WHEN UPPER(METRIC) = 'MAPE'  THEN VALUE END) AS MAPE,
    MAX(CASE WHEN UPPER(METRIC) = 'SMAPE' THEN VALUE END) AS SMAPE,
    MAX(CASE WHEN UPPER(METRIC) = 'MAE'   THEN VALUE END) AS MAE
FROM EVAL_METRICS_D
GROUP BY SERIES;

-- Model E
INSERT INTO ABLATION_RESULTS
SELECT
    'MODEL_E'                                                    AS MODEL_NAME,
    '+ Stability (STABILITY_SCORE, NET_STORE_CHANGE) = FULL'     AS FEATURE_SET,
    SERIES,
    MAX(CASE WHEN UPPER(METRIC) = 'MAPE'  THEN VALUE END) AS MAPE,
    MAX(CASE WHEN UPPER(METRIC) = 'SMAPE' THEN VALUE END) AS SMAPE,
    MAX(CASE WHEN UPPER(METRIC) = 'MAE'   THEN VALUE END) AS MAE
FROM EVAL_METRICS_E
GROUP BY SERIES;

-- Review consolidated results
SELECT
    MODEL_NAME,
    FEATURE_SET,
    SERIES,
    ROUND(MAPE, 4)   AS MAPE,
    ROUND(SMAPE, 4)  AS SMAPE,
    ROUND(MAE, 2)    AS MAE
FROM ABLATION_RESULTS
ORDER BY SERIES, MODEL_NAME;


-- ============================================================
-- 7. Feature Importance comparison
--    Extract from full Model E to show which features matter most
-- ============================================================

CALL MODEL_E!EXPLAIN_FEATURE_IMPORTANCE();

CREATE OR REPLACE TABLE ABLATION_FEATURE_IMPORTANCE AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Review feature rankings
SELECT *
FROM ABLATION_FEATURE_IMPORTANCE
ORDER BY RANK;


-- ============================================================
-- 8. Summary view: V_ABLATION_SUMMARY
--    Pivot showing MAPE and step-over-step improvement
--    from Model A → B → C → D → E per district
-- ============================================================

CREATE OR REPLACE VIEW V_ABLATION_SUMMARY AS
WITH ranked AS (
    SELECT
        MODEL_NAME,
        FEATURE_SET,
        SERIES AS DISTRICT,
        MAPE,
        SMAPE,
        MAE,
        -- Order models A through E
        CASE MODEL_NAME
            WHEN 'MODEL_A' THEN 1
            WHEN 'MODEL_B' THEN 2
            WHEN 'MODEL_C' THEN 3
            WHEN 'MODEL_D' THEN 4
            WHEN 'MODEL_E' THEN 5
        END AS MODEL_ORDER
    FROM ABLATION_RESULTS
),
with_deltas AS (
    SELECT
        r.*,
        -- MAPE of the baseline (Model A) for each district
        FIRST_VALUE(MAPE) OVER (
            PARTITION BY DISTRICT ORDER BY MODEL_ORDER
        ) AS BASELINE_MAPE,
        -- Previous model's MAPE (for incremental delta)
        LAG(MAPE) OVER (
            PARTITION BY DISTRICT ORDER BY MODEL_ORDER
        ) AS PREV_MAPE
    FROM ranked r
)
SELECT
    MODEL_NAME,
    FEATURE_SET,
    DISTRICT,
    ROUND(MAPE, 4)                                              AS MAPE,
    ROUND(SMAPE, 4)                                             AS SMAPE,
    ROUND(MAE, 2)                                               AS MAE,
    -- Improvement vs baseline (Model A), in percentage points
    ROUND(BASELINE_MAPE - MAPE, 4)                              AS MAPE_IMPROVE_VS_BASELINE,
    -- Improvement vs baseline as relative %
    CASE WHEN BASELINE_MAPE > 0
         THEN ROUND((BASELINE_MAPE - MAPE) / BASELINE_MAPE * 100, 2)
         ELSE NULL
    END                                                          AS MAPE_IMPROVE_PCT_VS_BASELINE,
    -- Incremental improvement from previous model step
    CASE WHEN PREV_MAPE IS NOT NULL
         THEN ROUND(PREV_MAPE - MAPE, 4)
         ELSE NULL
    END                                                          AS MAPE_STEP_IMPROVE,
    -- Incremental improvement as relative %
    CASE WHEN PREV_MAPE IS NOT NULL AND PREV_MAPE > 0
         THEN ROUND((PREV_MAPE - MAPE) / PREV_MAPE * 100, 2)
         ELSE NULL
    END                                                          AS MAPE_STEP_IMPROVE_PCT,
    MODEL_ORDER
FROM with_deltas
ORDER BY DISTRICT, MODEL_ORDER;

-- Preview the summary
SELECT * FROM V_ABLATION_SUMMARY ORDER BY DISTRICT, MODEL_ORDER;

-- District-level best model identification
SELECT
    DISTRICT,
    MODEL_NAME                                        AS BEST_MODEL,
    MAPE                                              AS BEST_MAPE,
    MAPE_IMPROVE_PCT_VS_BASELINE                      AS TOTAL_IMPROVEMENT_PCT
FROM V_ABLATION_SUMMARY
WHERE MODEL_ORDER = 5
ORDER BY DISTRICT;


-- ============================================================
-- 9. Final production model outputs
--    Update production tables from Model E (full / best model)
-- ============================================================

-- 9a. Production forecast results
--     Replace the main FORECAST_RESULTS with Model E output
CREATE OR REPLACE TABLE FORECAST_RESULTS AS
SELECT * FROM FORECAST_RESULTS_E;

SELECT 'FORECAST_RESULTS updated from Model E (full model)' AS STATUS;

-- 9b. Production actual-vs-forecast comparison
CREATE OR REPLACE TABLE ACTUAL_VS_FORECAST AS
SELECT
    a.DS,
    a.DISTRICT,
    a.Y            AS ACTUAL,
    NULL           AS FORECAST_VAL,
    NULL           AS LOWER_BOUND,
    NULL           AS UPPER_BOUND
FROM FORECAST_INPUT_E a
UNION ALL
SELECT
    f.TS           AS DS,
    f.SERIES       AS DISTRICT,
    NULL           AS ACTUAL,
    f.FORECAST     AS FORECAST_VAL,
    f.LOWER_BOUND,
    f.UPPER_BOUND
FROM FORECAST_RESULTS_E f
ORDER BY DISTRICT, DS;

SELECT 'ACTUAL_VS_FORECAST updated from Model E' AS STATUS;

-- 9c. Production feature importance
CALL MOVESIGNAL_FORECAST_V2!EXPLAIN_FEATURE_IMPORTANCE();

CREATE OR REPLACE TABLE FEATURE_IMPORTANCE AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT 'FEATURE_IMPORTANCE updated from MOVESIGNAL_FORECAST_V2' AS STATUS;

-- Final verification: show production tables
SELECT 'FORECAST_RESULTS' AS TBL, COUNT(*) AS ROWS FROM FORECAST_RESULTS
UNION ALL
SELECT 'ACTUAL_VS_FORECAST', COUNT(*) FROM ACTUAL_VS_FORECAST
UNION ALL
SELECT 'FEATURE_IMPORTANCE', COUNT(*) FROM FEATURE_IMPORTANCE
UNION ALL
SELECT 'ABLATION_RESULTS', COUNT(*) FROM ABLATION_RESULTS
UNION ALL
SELECT 'ABLATION_FEATURE_IMPORTANCE', COUNT(*) FROM ABLATION_FEATURE_IMPORTANCE;


-- ============================================================
-- Ablation study complete.
-- Key artifacts:
--   Models:  MODEL_A, MODEL_B, MODEL_C, MODEL_D, MODEL_E
--   Alias:   MOVESIGNAL_FORECAST_V2  (= Model E, production)
--   Tables:  ABLATION_RESULTS, ABLATION_FEATURE_IMPORTANCE
--   View:    V_ABLATION_SUMMARY  (MAPE progression per district)
--   Updated: FORECAST_RESULTS, ACTUAL_VS_FORECAST, FEATURE_IMPORTANCE
-- ============================================================
SELECT 'Ablation study complete — see V_ABLATION_SUMMARY for results' AS STATUS;
