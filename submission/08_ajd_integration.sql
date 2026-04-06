-- ============================================================
-- MoveSignal AI: AJD Telecom Subscription Analytics Integration
-- Source: Snowflake Marketplace - AJD 통신 가입/계약/마케팅/콜센터
-- DB: SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION
-- Views: V01 ~ V11
-- ============================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MOVESIGNAL_AI;
USE SCHEMA ANALYTICS;

-- ============================================================
-- 0. 짧은 별칭 (AJD DB 이름이 매우 길어서 변수로 관리)
-- ============================================================
-- Full DB reference:
--   SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION
-- Abbreviated as AJD_DB in comments below.

SET AJD_DB = 'SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION';

-- ============================================================
-- 1. Discovery Queries (주석 해제 후 실행하여 스키마/컬럼 확인)
-- ============================================================

-- ---- 1a. 스키마 내 뷰 목록 확인 (PUBLIC 또는 TELECOM_INSIGHTS) ----
-- SHOW VIEWS IN SCHEMA SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC;
-- SHOW VIEWS IN SCHEMA SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS;

-- ---- 1b. 각 뷰 미리보기 (PUBLIC 스키마 기준, 없으면 TELECOM_INSIGHTS로 교체) ----
-- TODO: 스키마가 PUBLIC인지 TELECOM_INSIGHTS인지 확인 후 아래 경로 수정

-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V01_MONTHLY_REGIONAL_CONTRACT_STATS LIMIT 5;
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V02 LIMIT 5;  -- TODO: 실제 뷰 이름 확인
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V03 LIMIT 5;  -- TODO: 실제 뷰 이름 확인
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V04 LIMIT 5;  -- TODO: 실제 뷰 이름 확인
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V05 LIMIT 5;  -- TODO: 실제 뷰 이름 확인
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V06_RENTAL_CATEGORY_STATS LIMIT 5;
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V07 LIMIT 5;  -- TODO: 실제 뷰 이름 확인
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V08 LIMIT 5;  -- TODO: 실제 뷰 이름 확인
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V09 LIMIT 5;  -- TODO: 실제 뷰 이름 확인
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V10 LIMIT 5;  -- TODO: 실제 뷰 이름 확인
-- SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V11 LIMIT 5;  -- TODO: 실제 뷰 이름 확인

-- ---- 1c. 컬럼 구조 확인 ----
-- DESCRIBE VIEW SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V01_MONTHLY_REGIONAL_CONTRACT_STATS;
-- DESCRIBE VIEW SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V06_RENTAL_CATEGORY_STATS;

-- ---- 1d. 지역 컬럼에 어떤 값이 들어있는지 확인 (시/군 수준) ----
-- TODO: REGION_NAME 컬럼명을 실제 컬럼으로 교체
-- SELECT DISTINCT REGION_NAME
-- FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.PUBLIC.V01_MONTHLY_REGIONAL_CONTRACT_STATS
-- WHERE REGION_NAME LIKE '%서울%' OR REGION_NAME LIKE '%서초%' OR REGION_NAME LIKE '%영등포%' OR REGION_NAME LIKE '%중%'
-- ORDER BY REGION_NAME;


-- ============================================================
-- 2. 지역 매핑 테이블 (시/군 -> 구 매핑)
-- ============================================================
-- AJD 데이터는 시/군(city/county) 수준이므로 서울 구(district)로 매핑 필요
-- 서초구, 영등포구, 중구 모두 서울특별시에 속함

CREATE OR REPLACE TEMPORARY TABLE DISTRICT_MAPPING (
    AJD_REGION_NAME  VARCHAR(100),   -- TODO: AJD 뷰에서 사용하는 지역 값 (예: '서울특별시' 또는 '서울')
    TARGET_DISTRICT  VARCHAR(50)
);

-- TODO: AJD 데이터의 지역 granularity 확인 후 매핑 수정
-- Case A: 데이터가 '서울특별시' 단위 -> 모든 구에 동일값 배분 (나누기 25 또는 인구비례)
-- Case B: 데이터가 구 단위 (예: '서초구') -> 직접 매핑
-- Case C: 데이터가 시/도 + 시/군/구 복합키 -> WHERE 조건으로 필터

INSERT INTO DISTRICT_MAPPING VALUES
    ('서울특별시', '서초구'),    -- TODO: AJD 원본의 실제 지역값으로 교체
    ('서울특별시', '영등포구'),
    ('서울특별시', '중구');

-- 구별 인구 비중 (서울시 전체에서 각 구의 비율, 대략적 근사치)
-- 서초구 ~4.1%, 영등포구 ~3.9%, 중구 ~1.3% of 서울 전체
CREATE OR REPLACE TEMPORARY TABLE DISTRICT_WEIGHT (
    DISTRICT  VARCHAR(50),
    POP_RATIO FLOAT
);
INSERT INTO DISTRICT_WEIGHT VALUES
    ('서초구',   0.044),
    ('영등포구', 0.042),
    ('중구',     0.014);


-- ============================================================
-- 3. STG_TELECOM: AJD 통신 데이터 스테이징 (구별 월별)
-- ============================================================
-- TODO: 아래 쿼리는 placeholder 컬럼명 사용. Discovery 실행 후 실제 컬럼으로 교체 필요.
--
-- 주요 가정:
--   V01: 월별 지역별 계약 통계 -> 계약수, 신규/해지 건수
--   V06: 렌탈 카테고리 통계   -> 렌탈 건수 (통신장비/기기 렌탈)
--   V0?_MARKETING_*           -> 마케팅 캠페인 반응률/스코어  (TODO: 해당 뷰 번호 확인)
--   V0?_CALL_CENTER_*         -> CS 콜센터 인입 건수           (TODO: 해당 뷰 번호 확인)
--
-- 지역 컬럼: REGION_NAME (placeholder)
-- 날짜 컬럼: YM 또는 YEAR_MONTH 또는 STATS_MONTH (placeholder)

CREATE OR REPLACE TABLE STG_TELECOM AS
WITH

-- ---- 3a. 계약 통계 (V01) ----
contract_base AS (
    SELECT
        -- TODO: 실제 날짜 컬럼으로 교체 (예: STATS_MONTH, YEAR_MONTH, YM 등)
        V01.YEAR_MONTH                          AS YM,          -- TODO: verify column name
        -- TODO: 실제 지역 컬럼으로 교체
        V01.REGION_NAME                         AS REGION,      -- TODO: verify column name
        -- TODO: 실제 계약 관련 컬럼으로 교체
        SUM(V01.CONTRACT_COUNT)                 AS CONTRACT_CNT,        -- TODO: verify column name
        SUM(V01.NEW_CONTRACT_COUNT)             AS NEW_CONTRACT_CNT,    -- TODO: verify column name
        SUM(V01.CANCEL_COUNT)                   AS CANCEL_CNT           -- TODO: verify column name
    FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION
         .PUBLIC.V01_MONTHLY_REGIONAL_CONTRACT_STATS V01
         -- TODO: 스키마가 TELECOM_INSIGHTS이면 위 .PUBLIC -> .TELECOM_INSIGHTS 로 교체
    GROUP BY V01.YEAR_MONTH, V01.REGION_NAME
),

-- ---- 3b. 렌탈 통계 (V06) ----
rental_base AS (
    SELECT
        V06.YEAR_MONTH                          AS YM,          -- TODO: verify column name
        V06.REGION_NAME                         AS REGION,      -- TODO: verify column name
        SUM(V06.RENTAL_COUNT)                   AS RENTAL_CNT,          -- TODO: verify column name
        SUM(V06.RENTAL_AMOUNT)                  AS RENTAL_AMT           -- TODO: verify column name
    FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION
         .PUBLIC.V06_RENTAL_CATEGORY_STATS V06
    GROUP BY V06.YEAR_MONTH, V06.REGION_NAME
),

-- ---- 3c. 마케팅 통계 (TODO: V0? 뷰 번호 확인) ----
marketing_base AS (
    SELECT
        MKT.YEAR_MONTH                          AS YM,          -- TODO: verify column name
        MKT.REGION_NAME                         AS REGION,      -- TODO: verify column name
        AVG(MKT.RESPONSE_RATE)                  AS AVG_RESPONSE_RATE,   -- TODO: verify column name
        SUM(MKT.CAMPAIGN_COUNT)                 AS CAMPAIGN_CNT         -- TODO: verify column name
    FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION
         .PUBLIC.V01_MONTHLY_REGIONAL_CONTRACT_STATS MKT  -- TODO: 실제 마케팅 뷰로 교체 (V03? V04? V05?)
    WHERE 1=0  -- TODO: 실제 뷰 확인 전 실행 방지용, 확인 후 제거
    GROUP BY MKT.YEAR_MONTH, MKT.REGION_NAME
),

-- ---- 3d. 콜센터/CS 통계 (TODO: V0? 뷰 번호 확인) ----
cs_base AS (
    SELECT
        CS.YEAR_MONTH                           AS YM,          -- TODO: verify column name
        CS.REGION_NAME                          AS REGION,      -- TODO: verify column name
        SUM(CS.CALL_COUNT)                      AS CS_CALL_CNT          -- TODO: verify column name
    FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION
         .PUBLIC.V01_MONTHLY_REGIONAL_CONTRACT_STATS CS  -- TODO: 실제 콜센터 뷰로 교체 (V08? V09? V10?)
    WHERE 1=0  -- TODO: 실제 뷰 확인 전 실행 방지용, 확인 후 제거
    GROUP BY CS.YEAR_MONTH, CS.REGION_NAME
),

-- ---- 3e. 구별 매핑 + 비중 배분 ----
combined AS (
    SELECT
        COALESCE(cb.YM, rb.YM)                  AS YM,
        DW.DISTRICT,
        -- 계약
        COALESCE(cb.CONTRACT_CNT, 0) * DW.POP_RATIO   AS CONTRACT_COUNT,
        COALESCE(cb.NEW_CONTRACT_CNT, 0) * DW.POP_RATIO AS NEW_CONTRACT_COUNT,
        COALESCE(cb.CANCEL_CNT, 0) * DW.POP_RATIO     AS CANCEL_COUNT,
        -- 렌탈
        COALESCE(rb.RENTAL_CNT, 0) * DW.POP_RATIO     AS RENTAL_COUNT,
        COALESCE(rb.RENTAL_AMT, 0) * DW.POP_RATIO     AS RENTAL_AMOUNT,
        -- 마케팅
        COALESCE(mb.AVG_RESPONSE_RATE, 0)              AS MARKETING_SCORE,  -- 비율은 배분 불필요
        COALESCE(mb.CAMPAIGN_CNT, 0) * DW.POP_RATIO   AS CAMPAIGN_COUNT,
        -- CS
        COALESCE(csb.CS_CALL_CNT, 0) * DW.POP_RATIO   AS CS_CALLS
    FROM contract_base cb
    -- 지역 매핑: 서울특별시 -> 서초구/영등포구/중구
    INNER JOIN DISTRICT_MAPPING DM ON cb.REGION = DM.AJD_REGION_NAME
    INNER JOIN DISTRICT_WEIGHT DW  ON DM.TARGET_DISTRICT = DW.DISTRICT
    LEFT JOIN rental_base rb       ON cb.YM = rb.YM AND cb.REGION = rb.REGION
    LEFT JOIN marketing_base mb    ON cb.YM = mb.YM AND cb.REGION = mb.REGION
    LEFT JOIN cs_base csb          ON cb.YM = csb.YM AND cb.REGION = csb.REGION
)

SELECT
    YM,
    DISTRICT,
    ROUND(CONTRACT_COUNT)       AS CONTRACT_COUNT,
    ROUND(NEW_CONTRACT_COUNT)   AS NEW_CONTRACT_COUNT,
    ROUND(CANCEL_COUNT)         AS CANCEL_COUNT,
    ROUND(RENTAL_COUNT)         AS RENTAL_COUNT,
    ROUND(RENTAL_AMOUNT, 2)    AS RENTAL_AMOUNT,
    ROUND(MARKETING_SCORE, 4)  AS MARKETING_SCORE,
    ROUND(CAMPAIGN_COUNT)       AS CAMPAIGN_COUNT,
    ROUND(CS_CALLS)             AS CS_CALLS
FROM combined
ORDER BY YM, DISTRICT;


-- ============================================================
-- 4. FEATURE_MART_FINAL 업데이트: AJD 통신 컬럼 추가
-- ============================================================
-- 기존 02_feature_mart_v4.sql 의 FEATURE_MART_FINAL 재생성
-- STG_TELECOM을 LEFT JOIN으로 추가

CREATE OR REPLACE TABLE FEATURE_MART_FINAL AS
SELECT
    p.YM,
    p.DISTRICT,

    -- ========== 유동인구 (SPH) ==========
    p.TOTAL_POP, p.RES_POP, p.WORK_POP, p.VISIT_POP,

    -- ========== 카드소비 (SPH) ==========
    c.TOTAL_SALES, c.FOOD, c.COFFEE, c.ENTERTAIN, c.CLOTHING, c.CULTURE,
    c.ACCOMMODATION, c.BEAUTY, c.MEDICAL, c.EDUCATION, c.TOTAL_TX_COUNT,

    -- ========== 자산소득 (SPH) ==========
    a.CUSTOMERS, a.AVG_INCOME, a.AVG_ASSET, a.AVG_CREDIT_SCORE, a.AVG_HH_INCOME,

    -- ========== 부동산 (Richgo) ==========
    r.AVG_MEME_PRICE, r.AVG_JEONSE_PRICE, r.AVG_PRICE_PER_PYEONG, r.TOTAL_HOUSEHOLDS,

    -- ========== 인구이동 (Richgo) ==========
    m.MOVE_IN, m.MOVE_OUT, m.NET_MOVE,

    -- ========== 통신/렌탈 (AJD) ==========
    t.CONTRACT_COUNT,
    t.NEW_CONTRACT_COUNT,
    t.CANCEL_COUNT,
    t.RENTAL_COUNT,
    t.RENTAL_AMOUNT,
    t.MARKETING_SCORE,
    t.CAMPAIGN_COUNT,
    t.CS_CALLS,

    -- ========== 기존 파생 피처 ==========
    CASE WHEN p.TOTAL_POP > 0 THEN c.TOTAL_SALES / p.TOTAL_POP ELSE 0 END AS SALES_PER_POP,
    CASE WHEN a.AVG_ASSET > 0 THEN r.AVG_MEME_PRICE / a.AVG_ASSET ELSE NULL END AS PRICE_TO_ASSET_RATIO,

    LAG(c.TOTAL_SALES) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) AS PREV_SALES,
    LAG(p.TOTAL_POP)   OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) AS PREV_POP,

    CASE WHEN LAG(c.TOTAL_SALES) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) > 0
         THEN (c.TOTAL_SALES - LAG(c.TOTAL_SALES) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM))
              / LAG(c.TOTAL_SALES) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) * 100
         ELSE 0 END AS SALES_CHG_PCT,

    CASE WHEN LAG(p.TOTAL_POP) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) > 0
         THEN (p.TOTAL_POP - LAG(p.TOTAL_POP) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM))
              / LAG(p.TOTAL_POP) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) * 100
         ELSE 0 END AS POP_CHG_PCT,

    CASE WHEN LAG(r.AVG_MEME_PRICE) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) > 0
         THEN (r.AVG_MEME_PRICE - LAG(r.AVG_MEME_PRICE) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM))
              / LAG(r.AVG_MEME_PRICE) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) * 100
         ELSE 0 END AS PRICE_CHG_PCT,

    -- ========== 신규 파생 피처: RENTAL_SIGNAL ==========
    -- 렌탈건수 + 순이동인구 + 소비증감을 결합한 복합 이사 시그널
    -- 렌탈 증가 + 전입 증가 + 소비 증가 = 강한 전입 시그널
    (
        -- 렌탈 건수 z-score 근사 (구별 평균 대비 편차)
        COALESCE(t.RENTAL_COUNT, 0)
        -- 순이동 (양수 = 전입 우세)
        + COALESCE(m.NET_MOVE, 0) * 0.01
        -- 매출 변화율 (양수 = 소비 증가)
        + CASE WHEN LAG(c.TOTAL_SALES) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) > 0
               THEN (c.TOTAL_SALES - LAG(c.TOTAL_SALES) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM))
                    / LAG(c.TOTAL_SALES) OVER (PARTITION BY p.DISTRICT ORDER BY p.YM) * 10
               ELSE 0 END
    ) AS RENTAL_SIGNAL

FROM STG_POP p
LEFT JOIN STG_CARD   c ON p.YM = c.YM AND p.DISTRICT = c.DISTRICT
LEFT JOIN STG_ASSET  a ON p.YM = a.YM AND p.DISTRICT = a.DISTRICT
LEFT JOIN STG_PRICE  r ON p.YM = r.YM AND p.DISTRICT = r.DISTRICT
LEFT JOIN STG_MOVE   m ON p.YM = m.YM AND p.DISTRICT = m.DISTRICT
LEFT JOIN STG_TELECOM t ON p.YM = t.YM AND p.DISTRICT = t.DISTRICT
ORDER BY p.YM, p.DISTRICT;


-- ============================================================
-- 5. 검증 쿼리
-- ============================================================

-- 5a. STG_TELECOM 건수 확인
SELECT DISTRICT, COUNT(*) AS ROWS, MIN(YM) AS START_YM, MAX(YM) AS END_YM
FROM STG_TELECOM
GROUP BY DISTRICT
ORDER BY DISTRICT;

-- 5b. FEATURE_MART_FINAL에 AJD 컬럼이 잘 붙었는지 확인
SELECT YM, DISTRICT,
       TOTAL_POP, TOTAL_SALES, NET_MOVE,
       CONTRACT_COUNT, RENTAL_COUNT, MARKETING_SCORE, CS_CALLS,
       RENTAL_SIGNAL
FROM FEATURE_MART_FINAL
WHERE YM >= '202401'
ORDER BY YM DESC, DISTRICT
LIMIT 15;

-- 5c. AJD 조인 커버리지 (NULL 비율)
SELECT
    COUNT(*)                                          AS TOTAL_ROWS,
    COUNT(CONTRACT_COUNT)                             AS HAS_CONTRACT,
    COUNT(RENTAL_COUNT)                               AS HAS_RENTAL,
    ROUND(COUNT(CONTRACT_COUNT) / COUNT(*) * 100, 1)  AS CONTRACT_COVER_PCT,
    ROUND(COUNT(RENTAL_COUNT) / COUNT(*) * 100, 1)    AS RENTAL_COVER_PCT
FROM FEATURE_MART_FINAL;

-- 5d. RENTAL_SIGNAL 분포 확인
SELECT DISTRICT,
       AVG(RENTAL_SIGNAL)    AS AVG_SIGNAL,
       MIN(RENTAL_SIGNAL)    AS MIN_SIGNAL,
       MAX(RENTAL_SIGNAL)    AS MAX_SIGNAL,
       STDDEV(RENTAL_SIGNAL) AS STDDEV_SIGNAL
FROM FEATURE_MART_FINAL
GROUP BY DISTRICT
ORDER BY AVG_SIGNAL DESC;
