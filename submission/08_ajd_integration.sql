-- ============================================================
-- DistrictPilot AI: AJD 통신/렌탈 데이터 통합
-- Snowflake Marketplace: AJD 통신 가입/계약/마케팅/콜센터
--
-- 이 스크립트는 2가지 경로를 제공합니다:
--   Path A: AJD Marketplace 실데이터 (Section 1-3)
--   Path B: 합성 데이터 Fallback (Section 4) — 실데이터 미확보 시
--
-- 어느 경로든 결과는 STG_TELECOM 테이블이며,
-- FEATURE_MART_V2에 LEFT JOIN으로 통합됩니다.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE DISTRICTPILOT_AI;
USE SCHEMA ANALYTICS;

ALTER SESSION SET QUERY_TAG = '{"app":"districtpilot_ai","module":"ajd_integration","version":"v2"}';


-- ============================================================
-- PATH A: AJD 실데이터 (Marketplace 구독 필요)
-- ============================================================

-- ---- 1. Discovery: Snowsight에서 아래를 실행하여 스키마 확인 ----
-- SHOW SCHEMAS IN DATABASE SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION;
-- SHOW VIEWS IN SCHEMA SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS;
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V01_MONTHLY_REGIONAL_CONTRACT_STATS LIMIT 5;
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V06_RENTAL_CATEGORY_STATS LIMIT 5;

-- ---- 2. 실데이터 스테이징 (Discovery 후 컬럼명 확인하여 사용) ----
-- CREATE OR REPLACE TABLE STG_TELECOM AS
-- SELECT ...  (Discovery 결과에 따라 작성)


-- ============================================================
-- PATH B: 합성 AJD 데이터 (해커톤 데모용)
-- ============================================================
-- AJD 실데이터 구조를 모사한 합성 데이터입니다.
-- Production 환경에서는 Path A로 교체합니다.
--
-- 합성 근거:
--   - 서울시 통신 가입 통계 (KOSIS) 참고 district별 비율 적용
--   - 렌탈 건수: 이사 시즌(3,9월) 피크 반영
--   - CS 콜센터: 렌탈 건수 * 0.15 비율 (업계 평균)
-- ============================================================

-- 4. 합성 STG_TELECOM 생성
CREATE OR REPLACE TABLE STG_TELECOM AS
WITH months AS (
    SELECT DISTINCT YM FROM FEATURE_MART_V2
),
districts AS (
    SELECT '서초구' AS DISTRICT, 0.044 AS POP_RATIO, 1.15 AS INCOME_MULT UNION ALL
    SELECT '영등포구',               0.042,            0.95             UNION ALL
    SELECT '중구',                   0.014,            0.85
),
base AS (
    SELECT
        m.YM,
        d.DISTRICT,
        d.POP_RATIO,
        d.INCOME_MULT,
        CAST(SUBSTRING(m.YM, 5, 2) AS INT) AS MM,
        -- 이사 시즌 가중치 (3월/9월 피크, 1월/8월 비수기)
        CASE CAST(SUBSTRING(m.YM, 5, 2) AS INT)
            WHEN 1  THEN 0.85  WHEN 2  THEN 0.95  WHEN 3  THEN 1.25
            WHEN 4  THEN 1.10  WHEN 5  THEN 1.05  WHEN 6  THEN 0.95
            WHEN 7  THEN 0.90  WHEN 8  THEN 0.85  WHEN 9  THEN 1.20
            WHEN 10 THEN 1.10  WHEN 11 THEN 1.00  WHEN 12 THEN 0.90
        END AS SEASON_MULT,
        -- 연도별 성장 트렌드
        1 + (CAST(SUBSTRING(m.YM, 1, 4) AS INT) - 2020) * 0.03 AS YEAR_GROWTH
    FROM months m
    CROSS JOIN districts d
)
SELECT
    YM,
    DISTRICT,
    -- 통신 계약 통계
    ROUND(12000 * POP_RATIO * SEASON_MULT * YEAR_GROWTH)          AS CONTRACT_COUNT,
    ROUND(1200 * POP_RATIO * SEASON_MULT * YEAR_GROWTH * 1.1)     AS NEW_CONTRACT_COUNT,
    ROUND(800 * POP_RATIO * SEASON_MULT * 0.9 * YEAR_GROWTH)      AS CANCEL_COUNT,
    -- 렌탈 통계 (홈서비스 핵심 지표)
    ROUND(3500 * POP_RATIO * SEASON_MULT * INCOME_MULT * YEAR_GROWTH) AS RENTAL_COUNT,
    ROUND(3500 * POP_RATIO * SEASON_MULT * INCOME_MULT * YEAR_GROWTH * 25000, 2) AS RENTAL_AMOUNT,
    -- 마케팅 (캠페인 반응률)
    ROUND(0.12 * INCOME_MULT * (1 + (SEASON_MULT - 1) * 0.3), 4)  AS MARKETING_SCORE,
    ROUND(450 * POP_RATIO * SEASON_MULT * YEAR_GROWTH)             AS CAMPAIGN_COUNT,
    -- CS 콜센터 인입 (렌탈 건수 * 15% 비율)
    ROUND(3500 * POP_RATIO * SEASON_MULT * INCOME_MULT * YEAR_GROWTH * 0.15) AS CS_CALLS
FROM base
ORDER BY YM, DISTRICT;


-- ============================================================
-- 5. FEATURE_MART_V2 확장: AJD 컬럼 추가
-- ============================================================
-- 기존 FEATURE_MART_V2에 AJD 렌탈/계약/CS 컬럼과
-- RENTAL_SIGNAL 복합 피처를 추가합니다.

CREATE OR REPLACE TABLE FEATURE_MART_V3 AS
SELECT
    f.*,
    -- AJD 통신/렌탈 컬럼
    t.CONTRACT_COUNT,
    t.NEW_CONTRACT_COUNT,
    t.CANCEL_COUNT,
    t.RENTAL_COUNT,
    t.RENTAL_AMOUNT,
    t.MARKETING_SCORE,
    t.CAMPAIGN_COUNT,
    t.CS_CALLS,
    -- RENTAL_SIGNAL: 렌탈건수 + 순이동 + 소비증감 복합 시그널
    -- 전입·이사 직후 홈서비스 수요를 대변하는 핵심 지표
    (
        COALESCE(t.RENTAL_COUNT, 0) * 0.5
        + COALESCE(f.NET_MOVE, 0) * 0.01
        + COALESCE(f.SALES_CHG_PCT, 0) * 0.1
    ) AS RENTAL_SIGNAL,
    -- 렌탈 전환율 (전입 세대 대비 렌탈 건수)
    CASE WHEN f.MOVE_IN > 0
         THEN ROUND(COALESCE(t.RENTAL_COUNT, 0) / f.MOVE_IN, 4)
         ELSE NULL
    END AS RENTAL_CONVERSION_RATE
FROM FEATURE_MART_V2 f
LEFT JOIN STG_TELECOM t ON f.YM = t.YM AND f.DISTRICT = t.DISTRICT
ORDER BY f.YM, f.DISTRICT;


-- ============================================================
-- 6. 검증 쿼리
-- ============================================================

-- 6a. STG_TELECOM 요약
SELECT DISTRICT,
       COUNT(*) AS MONTHS,
       MIN(YM) AS START_YM, MAX(YM) AS END_YM,
       ROUND(AVG(RENTAL_COUNT)) AS AVG_RENTAL,
       ROUND(AVG(CS_CALLS)) AS AVG_CS
FROM STG_TELECOM
GROUP BY DISTRICT
ORDER BY DISTRICT;

-- 6b. FEATURE_MART_V3 AJD 커버리지
SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT(RENTAL_COUNT) AS HAS_RENTAL,
    COUNT(RENTAL_SIGNAL) AS HAS_SIGNAL,
    ROUND(AVG(RENTAL_CONVERSION_RATE), 4) AS AVG_CONVERSION
FROM FEATURE_MART_V3;

-- 6c. 구별 RENTAL_SIGNAL 분포
SELECT DISTRICT,
       ROUND(AVG(RENTAL_SIGNAL), 2) AS AVG_SIGNAL,
       ROUND(AVG(RENTAL_CONVERSION_RATE), 4) AS AVG_CONV_RATE,
       ROUND(AVG(RENTAL_COUNT)) AS AVG_RENTAL_CNT
FROM FEATURE_MART_V3
WHERE YM >= '202401'
GROUP BY DISTRICT
ORDER BY AVG_SIGNAL DESC;

SELECT 'AJD integration complete — STG_TELECOM + FEATURE_MART_V3 ready' AS STATUS;
