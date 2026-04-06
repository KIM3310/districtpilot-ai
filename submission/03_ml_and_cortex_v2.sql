-- ============================================================
-- MoveSignal AI: ML Forecast + Cortex v2
-- Feature Mart v4 컬럼명에 맞춤
-- ============================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MOVESIGNAL_AI;
USE SCHEMA ANALYTICS;

-- ============================================================
-- 1. 예측용 시계열 테이블 (타깃: TOTAL_SALES)
-- ============================================================
CREATE OR REPLACE TABLE FORECAST_INPUT AS
SELECT
    TO_DATE(YM || '01', 'YYYYMMDD') AS DS,
    DISTRICT,
    TOTAL_SALES AS Y,
    TOTAL_POP,
    AVG_MEME_PRICE,
    NET_MOVE,
    AVG_ASSET,
    SALES_PER_POP
FROM FEATURE_MART_FINAL
WHERE TOTAL_SALES IS NOT NULL
  AND TOTAL_SALES > 0
ORDER BY DISTRICT, DS;

-- 확인
SELECT DISTRICT, MIN(DS) AS START_DT, MAX(DS) AS END_DT, COUNT(*) AS MONTHS
FROM FORECAST_INPUT GROUP BY DISTRICT ORDER BY DISTRICT;

-- ============================================================
-- 2. Snowflake ML FORECAST 모델 생성
-- ============================================================
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST MOVESIGNAL_FORECAST(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'FORECAST_INPUT'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y',
    SERIES_COLNAME => 'DISTRICT'
);

-- ============================================================
-- 3. 예측 실행 (3개월)
-- ============================================================
CALL MOVESIGNAL_FORECAST!FORECAST(
    FORECASTING_PERIODS => 3,
    CONFIG_OBJECT => {'prediction_interval': 0.95}
);

CREATE OR REPLACE TABLE FORECAST_RESULTS AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM FORECAST_RESULTS ORDER BY SERIES, TS;

-- ============================================================
-- 4. Feature Importance
-- ============================================================
CALL MOVESIGNAL_FORECAST!EXPLAIN_FEATURE_IMPORTANCE();

CREATE OR REPLACE TABLE FEATURE_IMPORTANCE AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM FEATURE_IMPORTANCE ORDER BY RANK;

-- ============================================================
-- 5. 실제 vs 예측 비교 테이블
-- ============================================================
CREATE OR REPLACE TABLE ACTUAL_VS_FORECAST AS
SELECT
    a.DS,
    a.DISTRICT,
    a.Y AS ACTUAL,
    NULL AS FORECAST_VAL,
    NULL AS LOWER_BOUND,
    NULL AS UPPER_BOUND
FROM FORECAST_INPUT a
UNION ALL
SELECT
    f.TS AS DS,
    f.SERIES AS DISTRICT,
    NULL AS ACTUAL,
    f.FORECAST AS FORECAST_VAL,
    f.LOWER_BOUND,
    f.UPPER_BOUND
FROM FORECAST_RESULTS f
ORDER BY DISTRICT, DS;

SELECT * FROM ACTUAL_VS_FORECAST WHERE DISTRICT = '서초구' ORDER BY DS DESC LIMIT 10;

-- ============================================================
-- 6. 배분 추천 프로시저
-- ============================================================
CREATE OR REPLACE PROCEDURE RECOMMEND_ALLOCATION(TOTAL_BUDGET FLOAT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
BEGIN
    SELECT OBJECT_CONSTRUCT(
        'total_budget', :TOTAL_BUDGET,
        'allocations', ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'district', DISTRICT,
                'forecast', FORECAST_VAL,
                'share_pct', ROUND(FORECAST_VAL / SUM(FORECAST_VAL) OVER () * 100, 1),
                'budget', ROUND(:TOTAL_BUDGET * FORECAST_VAL / SUM(FORECAST_VAL) OVER (), 0)
            )
        )
    ) INTO result
    FROM (
        SELECT SERIES AS DISTRICT, AVG(FORECAST) AS FORECAST_VAL
        FROM FORECAST_RESULTS
        GROUP BY SERIES
    );
    RETURN result;
END;
$$;

CALL RECOMMEND_ALLOCATION(50000000);

-- ============================================================
-- 7. What-if 시뮬레이션 프로시저
-- ============================================================
CREATE OR REPLACE PROCEDURE SIMULATE_WHATIF(
    SEOCHO_PCT FLOAT, YEONGDEUNGPO_PCT FLOAT, JUNGGU_PCT FLOAT, TOTAL_BUDGET FLOAT
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
BEGIN
    SELECT OBJECT_CONSTRUCT(
        'scenario', OBJECT_CONSTRUCT(
            'seocho_pct', :SEOCHO_PCT,
            'yeongdeungpo_pct', :YEONGDEUNGPO_PCT,
            'junggu_pct', :JUNGGU_PCT
        ),
        'results', ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'district', DISTRICT,
                'budget', CASE
                    WHEN DISTRICT = '서초구' THEN ROUND(:TOTAL_BUDGET * :SEOCHO_PCT / 100, 0)
                    WHEN DISTRICT = '영등포구' THEN ROUND(:TOTAL_BUDGET * :YEONGDEUNGPO_PCT / 100, 0)
                    WHEN DISTRICT = '중구' THEN ROUND(:TOTAL_BUDGET * :JUNGGU_PCT / 100, 0)
                END,
                'forecast_demand', FORECAST_VAL,
                'expected_contracts', ROUND(FORECAST_VAL * CASE
                    WHEN DISTRICT = '서초구' THEN :SEOCHO_PCT / 100
                    WHEN DISTRICT = '영등포구' THEN :YEONGDEUNGPO_PCT / 100
                    WHEN DISTRICT = '중구' THEN :JUNGGU_PCT / 100
                END * 0.001, 0)
            )
        )
    ) INTO result
    FROM (
        SELECT SERIES AS DISTRICT, AVG(FORECAST) AS FORECAST_VAL
        FROM FORECAST_RESULTS
        GROUP BY SERIES
    );
    RETURN result;
END;
$$;

CALL SIMULATE_WHATIF(45, 35, 20, 50000000);

-- ============================================================
-- 8. Cortex LLM 테스트
-- ============================================================
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    '서초구의 렌탈 수요가 증가하는 이유를 한국어로 3줄로 설명해주세요.'
) AS LLM_RESPONSE;

-- ============================================================
-- 9. 운영용 Task (데모용)
-- ============================================================
CREATE OR REPLACE TASK DAILY_REFRESH
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * * Asia/Seoul'
    COMMENT = 'MoveSignal AI 일일 데이터 갱신'
AS
    SELECT 'refresh_placeholder';

ALTER TASK DAILY_REFRESH SUSPEND;

SELECT 'ML Forecast + Cortex setup complete!' AS STATUS;
