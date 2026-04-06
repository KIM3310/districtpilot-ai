-- ============================================================
-- MoveSignal AI: Dynamic Tables, Tasks & Health Monitoring
-- Automated pipeline orchestration for production readiness
-- ============================================================

-- ============================================================
-- 0. Session Setup
-- ============================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MOVESIGNAL_AI;
USE SCHEMA ANALYTICS;

ALTER SESSION SET QUERY_TAG = '{"app":"movesignal_ai","version":"v1"}';

-- ============================================================
-- 1. Dynamic Table: DT_FEATURE_MART
--    Replaces FEATURE_MART_FINAL with auto-refresh (1 hour lag)
--    Joins all 5 STG tables with derived features
-- ============================================================
CREATE OR REPLACE DYNAMIC TABLE DT_FEATURE_MART
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    p.YM,
    p.DISTRICT,
    -- 유동인구 (Floating Population)
    p.TOTAL_POP, p.RES_POP, p.WORK_POP, p.VISIT_POP,
    -- 카드소비 (Card Sales)
    c.TOTAL_SALES, c.FOOD, c.COFFEE, c.ENTERTAIN, c.CLOTHING, c.CULTURE,
    c.ACCOMMODATION, c.BEAUTY, c.MEDICAL, c.EDUCATION, c.TOTAL_TX_COUNT,
    -- 자산소득 (Asset & Income)
    a.CUSTOMERS, a.AVG_INCOME, a.AVG_ASSET, a.AVG_CREDIT_SCORE, a.AVG_HH_INCOME,
    -- 부동산 시세 (Real Estate Prices)
    r.AVG_MEME_PRICE, r.AVG_JEONSE_PRICE, r.AVG_PRICE_PER_PYEONG, r.TOTAL_HOUSEHOLDS,
    -- 인구이동 (Population Movement)
    m.MOVE_IN, m.MOVE_OUT, m.NET_MOVE,
    -- 파생 피처 (Derived Features)
    CASE WHEN p.TOTAL_POP > 0
         THEN c.TOTAL_SALES / p.TOTAL_POP
         ELSE 0
    END AS SALES_PER_POP,
    CASE WHEN a.AVG_ASSET > 0
         THEN r.AVG_MEME_PRICE / a.AVG_ASSET
         ELSE NULL
    END AS PRICE_TO_ASSET_RATIO,
    -- 전월 대비 변화율 (Month-over-Month Change %)
    LAG(c.TOTAL_SALES) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) AS PREV_SALES,
    LAG(p.TOTAL_POP)   OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) AS PREV_POP,
    CASE WHEN LAG(c.TOTAL_SALES) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) > 0
         THEN (c.TOTAL_SALES - LAG(c.TOTAL_SALES) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM))
              / LAG(c.TOTAL_SALES) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) * 100
         ELSE 0
    END AS SALES_CHG_PCT,
    CASE WHEN LAG(p.TOTAL_POP) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) > 0
         THEN (p.TOTAL_POP - LAG(p.TOTAL_POP) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM))
              / LAG(p.TOTAL_POP) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) * 100
         ELSE 0
    END AS POP_CHG_PCT,
    CASE WHEN LAG(r.AVG_MEME_PRICE) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) > 0
         THEN (r.AVG_MEME_PRICE - LAG(r.AVG_MEME_PRICE) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM))
              / LAG(r.AVG_MEME_PRICE) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) * 100
         ELSE 0
    END AS PRICE_CHG_PCT
FROM STG_POP p
LEFT JOIN STG_CARD  c ON p.YM = c.YM AND p.DISTRICT = c.DISTRICT
LEFT JOIN STG_ASSET a ON p.YM = a.YM AND p.DISTRICT = a.DISTRICT
LEFT JOIN STG_PRICE r ON p.YM = r.YM AND p.DISTRICT = r.DISTRICT
LEFT JOIN STG_MOVE  m ON p.YM = m.YM AND p.DISTRICT = m.DISTRICT;

-- ============================================================
-- 2. Dynamic Table: DT_ALLOCATION_INPUT
--    Downstream of FORECAST_RESULTS
--    Computes budget allocation % per district
-- ============================================================
CREATE OR REPLACE DYNAMIC TABLE DT_ALLOCATION_INPUT
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    SERIES                              AS DISTRICT,
    AVG(FORECAST)                       AS AVG_FORECAST,
    RATIO_TO_REPORT(AVG(FORECAST))
        OVER ()                         AS ALLOCATION_PCT,
    RANK() OVER (
        ORDER BY AVG(FORECAST) DESC
    )                                   AS RANK
FROM FORECAST_RESULTS
GROUP BY SERIES;

-- ============================================================
-- 3. Task: TASK_REFRESH_PIPELINE
--    Daily 06:00 KST — refreshes STG tables from source
--    Uses EXECUTE IMMEDIATE for multi-statement pattern
-- ============================================================
CREATE OR REPLACE TASK TASK_REFRESH_PIPELINE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 6 * * * Asia/Seoul'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT   = 'MoveSignal AI: Daily STG table refresh from Marketplace sources'
AS
BEGIN
    -- STG_POP: SPH Floating Population
    CREATE OR REPLACE TABLE STG_POP AS
    SELECT
        FP.STANDARD_YEAR_MONTH AS YM,
        SM.CITY_KOR_NAME       AS DISTRICT,
        SUM(FP.RESIDENTIAL_POPULATION) AS RES_POP,
        SUM(FP.WORKING_POPULATION)     AS WORK_POP,
        SUM(FP.VISITING_POPULATION)    AS VISIT_POP,
        SUM(FP.RESIDENTIAL_POPULATION + FP.WORKING_POPULATION + FP.VISITING_POPULATION) AS TOTAL_POP
    FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.FLOATING_POPULATION_INFO FP
    JOIN SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.M_SCCO_MST SM
        ON FP.DISTRICT_CODE = SM.DISTRICT_CODE
    WHERE SM.CITY_KOR_NAME IN ('서초구','영등포구','중구')
    GROUP BY FP.STANDARD_YEAR_MONTH, SM.CITY_KOR_NAME;

    -- STG_CARD: SPH Card Sales
    CREATE OR REPLACE TABLE STG_CARD AS
    SELECT
        CS.STANDARD_YEAR_MONTH AS YM,
        SM.CITY_KOR_NAME       AS DISTRICT,
        SUM(CS.TOTAL_SALES)    AS TOTAL_SALES,
        SUM(CS.FOOD_SALES)     AS FOOD,
        SUM(CS.COFFEE_SALES)   AS COFFEE,
        SUM(CS.ENTERTAINMENT_SALES)          AS ENTERTAIN,
        SUM(CS.CLOTHING_ACCESSORIES_SALES)   AS CLOTHING,
        SUM(CS.SPORTS_CULTURE_LEISURE_SALES) AS CULTURE,
        SUM(CS.ACCOMMODATION_SALES)          AS ACCOMMODATION,
        SUM(CS.BEAUTY_SALES)                 AS BEAUTY,
        SUM(CS.MEDICAL_SALES)                AS MEDICAL,
        SUM(CS.EDUCATION_ACADEMY_SALES)      AS EDUCATION,
        SUM(CS.TOTAL_COUNT)                  AS TOTAL_TX_COUNT
    FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.CARD_SALES_INFO CS
    JOIN SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.M_SCCO_MST SM
        ON CS.DISTRICT_CODE = SM.DISTRICT_CODE
    WHERE SM.CITY_KOR_NAME IN ('서초구','영등포구','중구')
    GROUP BY CS.STANDARD_YEAR_MONTH, SM.CITY_KOR_NAME;

    -- STG_ASSET: SPH Asset & Income
    CREATE OR REPLACE TABLE STG_ASSET AS
    SELECT
        AI.STANDARD_YEAR_MONTH AS YM,
        SM.CITY_KOR_NAME       AS DISTRICT,
        SUM(AI.CUSTOMER_COUNT)       AS CUSTOMERS,
        AVG(AI.AVERAGE_INCOME)       AS AVG_INCOME,
        AVG(AI.AVERAGE_ASSET_AMOUNT) AS AVG_ASSET,
        AVG(AI.AVERAGE_SCORE)        AS AVG_CREDIT_SCORE,
        AVG(AI.AVERAGE_HOUSEHOLD_INCOME) AS AVG_HH_INCOME
    FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.ASSET_INCOME_INFO AI
    JOIN SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.M_SCCO_MST SM
        ON AI.DISTRICT_CODE = SM.DISTRICT_CODE
    WHERE SM.CITY_KOR_NAME IN ('서초구','영등포구','중구')
    GROUP BY AI.STANDARD_YEAR_MONTH, SM.CITY_KOR_NAME;

    -- STG_PRICE: Richgo Market Price
    CREATE OR REPLACE TABLE STG_PRICE AS
    SELECT
        LEFT(YYYYMMDD, 6) AS YM,
        SGG               AS DISTRICT,
        AVG(MEAN_MEME_PRICE)              AS AVG_MEME_PRICE,
        AVG(MEAN_JEONSE_PRICE)            AS AVG_JEONSE_PRICE,
        AVG(MEME_PRICE_PER_SUPPLY_PYEONG) AS AVG_PRICE_PER_PYEONG,
        SUM(TOTAL_HOUSEHOLDS)             AS TOTAL_HOUSEHOLDS
    FROM KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE.HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H
    WHERE SGG IN ('서초구','영등포구','중구')
      AND REGION_LEVEL = 'sgg'
    GROUP BY LEFT(YYYYMMDD, 6), SGG;

    -- STG_MOVE: Richgo Population Movement
    CREATE OR REPLACE TABLE STG_MOVE AS
    SELECT
        LEFT(YYYYMMDD, 6) AS YM,
        SGG               AS DISTRICT,
        SUM(CASE WHEN MOVEMENT_TYPE = '전입' THEN POPULATION ELSE 0 END) AS MOVE_IN,
        SUM(CASE WHEN MOVEMENT_TYPE = '전출' THEN POPULATION ELSE 0 END) AS MOVE_OUT,
        SUM(CASE WHEN MOVEMENT_TYPE = '전입' THEN POPULATION ELSE 0 END) -
        SUM(CASE WHEN MOVEMENT_TYPE = '전출' THEN POPULATION ELSE 0 END) AS NET_MOVE
    FROM KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE.HACKATHON_2026.REGION_POPULATION_MOVEMENT
    WHERE SGG IN ('서초구','영등포구','중구')
    GROUP BY LEFT(YYYYMMDD, 6), SGG;
END;

-- ============================================================
-- 4. Task: TASK_RETRAIN_FORECAST
--    Weekly Monday 07:00 KST — retrains ML FORECAST model
--    Depends on TASK_REFRESH_PIPELINE completion
-- ============================================================
CREATE OR REPLACE TASK TASK_RETRAIN_FORECAST
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 7 * * 1 Asia/Seoul'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT   = 'MoveSignal AI: Weekly model retrain (Monday 07:00 KST)'
AS
BEGIN
    -- Step 1: Rebuild forecast input from latest feature mart
    CREATE OR REPLACE TABLE FORECAST_INPUT AS
    SELECT
        TO_DATE(YM || '01', 'YYYYMMDD') AS DS,
        DISTRICT,
        TOTAL_SALES   AS Y,
        TOTAL_POP,
        AVG_MEME_PRICE,
        NET_MOVE,
        AVG_ASSET,
        SALES_PER_POP
    FROM DT_FEATURE_MART
    WHERE TOTAL_SALES IS NOT NULL
      AND TOTAL_SALES > 0
    ORDER BY DISTRICT, DS;

    -- Step 2: Retrain the Snowflake ML FORECAST model
    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST MOVESIGNAL_FORECAST(
        INPUT_DATA      => SYSTEM$REFERENCE('TABLE', 'FORECAST_INPUT'),
        TIMESTAMP_COLNAME => 'DS',
        TARGET_COLNAME    => 'Y',
        SERIES_COLNAME    => 'DISTRICT'
    );

    -- Step 3: Generate 3-month forecast
    CALL MOVESIGNAL_FORECAST!FORECAST(
        FORECASTING_PERIODS => 3,
        CONFIG_OBJECT       => {'prediction_interval': 0.95}
    );

    CREATE OR REPLACE TABLE FORECAST_RESULTS AS
    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    -- Step 4: Refresh feature importance
    CALL MOVESIGNAL_FORECAST!EXPLAIN_FEATURE_IMPORTANCE();

    CREATE OR REPLACE TABLE FEATURE_IMPORTANCE AS
    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
END;

-- Add dependency: retrain waits for pipeline refresh
ALTER TASK TASK_RETRAIN_FORECAST ADD AFTER TASK_REFRESH_PIPELINE;

-- ============================================================
-- 5. Leave tasks SUSPENDED (demo safety)
--    To activate in production:
--      ALTER TASK TASK_RETRAIN_FORECAST RESUME;
--      ALTER TASK TASK_REFRESH_PIPELINE RESUME;
-- ============================================================
ALTER TASK TASK_RETRAIN_FORECAST SUSPEND;
ALTER TASK TASK_REFRESH_PIPELINE SUSPEND;

-- ============================================================
-- 6. Health View: V_APP_HEALTH
--    Unified monitoring for dynamic tables, tasks, model
-- ============================================================
CREATE OR REPLACE VIEW V_APP_HEALTH AS
WITH dt_health AS (
    SELECT
        'DT_FEATURE_MART'           AS COMPONENT,
        'DYNAMIC_TABLE'             AS COMPONENT_TYPE,
        REFRESH_END_TIME            AS LAST_REFRESH_TIME,
        TARGET_LAG_SEC || 's'       AS TARGET_LAG,
        STATE                       AS REFRESH_STATE,
        NULL                        AS TASK_STATE,
        NULL                        AS MODEL_VERSION
    FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
        NAME => 'MOVESIGNAL_AI.ANALYTICS.DT_FEATURE_MART'
    ))
    ORDER BY REFRESH_END_TIME DESC
    LIMIT 1
),
dt_alloc_health AS (
    SELECT
        'DT_ALLOCATION_INPUT'       AS COMPONENT,
        'DYNAMIC_TABLE'             AS COMPONENT_TYPE,
        REFRESH_END_TIME            AS LAST_REFRESH_TIME,
        'DOWNSTREAM'                AS TARGET_LAG,
        STATE                       AS REFRESH_STATE,
        NULL                        AS TASK_STATE,
        NULL                        AS MODEL_VERSION
    FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
        NAME => 'MOVESIGNAL_AI.ANALYTICS.DT_ALLOCATION_INPUT'
    ))
    ORDER BY REFRESH_END_TIME DESC
    LIMIT 1
),
task_refresh_health AS (
    SELECT
        'TASK_REFRESH_PIPELINE'     AS COMPONENT,
        'TASK'                      AS COMPONENT_TYPE,
        COMPLETED_TIME              AS LAST_REFRESH_TIME,
        'CRON 0 6 * * * KST'       AS TARGET_LAG,
        NULL                        AS REFRESH_STATE,
        STATE                       AS TASK_STATE,
        NULL                        AS MODEL_VERSION
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'TASK_REFRESH_PIPELINE',
        SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
    ))
    ORDER BY COMPLETED_TIME DESC
    LIMIT 1
),
task_retrain_health AS (
    SELECT
        'TASK_RETRAIN_FORECAST'     AS COMPONENT,
        'TASK'                      AS COMPONENT_TYPE,
        COMPLETED_TIME              AS LAST_REFRESH_TIME,
        'CRON 0 7 * * 1 KST'       AS TARGET_LAG,
        NULL                        AS REFRESH_STATE,
        STATE                       AS TASK_STATE,
        NULL                        AS MODEL_VERSION
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'TASK_RETRAIN_FORECAST',
        SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
    ))
    ORDER BY COMPLETED_TIME DESC
    LIMIT 1
),
model_health AS (
    SELECT
        'MOVESIGNAL_FORECAST'       AS COMPONENT,
        'ML_MODEL'                  AS COMPONENT_TYPE,
        CURRENT_TIMESTAMP()         AS LAST_REFRESH_TIME,
        NULL                        AS TARGET_LAG,
        NULL                        AS REFRESH_STATE,
        NULL                        AS TASK_STATE,
        'MOVESIGNAL_FORECAST@v1'    AS MODEL_VERSION
    FROM DUAL
)
SELECT * FROM dt_health
UNION ALL SELECT * FROM dt_alloc_health
UNION ALL SELECT * FROM task_refresh_health
UNION ALL SELECT * FROM task_retrain_health
UNION ALL SELECT * FROM model_health;

-- ============================================================
-- 7. Verify Setup
-- ============================================================

-- Check dynamic tables
SHOW DYNAMIC TABLES IN SCHEMA MOVESIGNAL_AI.ANALYTICS;

-- Check tasks
SHOW TASKS IN SCHEMA MOVESIGNAL_AI.ANALYTICS;

-- Preview health view
SELECT * FROM V_APP_HEALTH;

-- Check query tag
SELECT CURRENT_SESSION(),
       PARSE_JSON(SYSTEM$GET_QUERY_TAG()) AS QUERY_TAG;
