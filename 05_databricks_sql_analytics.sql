-- ============================================================
-- MoveSignal AI: Databricks SQL Analytics Layer
-- Unity Catalog: movesignal_ai.analytics.*
-- Runs on Databricks SQL Warehouse (Serverless or Pro)
-- ============================================================

-- ============================================================
-- 1. Verify Delta tables from Python pipeline
-- ============================================================
DESCRIBE TABLE EXTENDED movesignal_ai.analytics.feature_mart_final;
DESCRIBE TABLE EXTENDED movesignal_ai.analytics.forecast_results_databricks;
DESCRIBE TABLE EXTENDED movesignal_ai.analytics.gold_district_kpi;

-- ============================================================
-- 2. Feature Mart summary — district-level monthly stats
-- ============================================================
SELECT
    DISTRICT,
    COUNT(*) AS months,
    MIN(YM) AS start_ym,
    MAX(YM) AS end_ym,
    ROUND(AVG(TOTAL_SALES), 0) AS avg_monthly_sales,
    ROUND(AVG(TOTAL_POP), 0) AS avg_monthly_pop,
    ROUND(AVG(SALES_PER_POP), 2) AS avg_sales_per_pop
FROM movesignal_ai.analytics.feature_mart_final
GROUP BY DISTRICT
ORDER BY avg_monthly_sales DESC;

-- ============================================================
-- 3. YoY growth by district (카드 소비 전년 대비 성장률)
-- ============================================================
WITH yearly AS (
    SELECT
        DISTRICT,
        LEFT(YM, 4) AS YEAR,
        SUM(TOTAL_SALES) AS ANNUAL_SALES
    FROM movesignal_ai.analytics.feature_mart_final
    GROUP BY DISTRICT, LEFT(YM, 4)
)
SELECT
    curr.DISTRICT,
    curr.YEAR,
    curr.ANNUAL_SALES,
    prev.ANNUAL_SALES AS PREV_YEAR_SALES,
    ROUND((curr.ANNUAL_SALES - prev.ANNUAL_SALES) / prev.ANNUAL_SALES * 100, 1) AS YOY_GROWTH_PCT
FROM yearly curr
LEFT JOIN yearly prev
    ON curr.DISTRICT = prev.DISTRICT AND curr.YEAR = CAST(CAST(prev.YEAR AS INT) + 1 AS STRING)
WHERE prev.ANNUAL_SALES IS NOT NULL
ORDER BY curr.DISTRICT, curr.YEAR;

-- ============================================================
-- 4. Forecast vs actual comparison (latest 3 months)
-- ============================================================
SELECT
    f.DISTRICT,
    f.DS AS FORECAST_DATE,
    f.FORECAST,
    f.RMSE,
    f.MAPE_PCT,
    f.MODEL
FROM movesignal_ai.analytics.forecast_results_databricks f
ORDER BY f.DISTRICT, f.DS;

-- ============================================================
-- 5. Budget allocation view (parametric)
-- ============================================================
CREATE OR REPLACE VIEW movesignal_ai.analytics.v_budget_allocation AS
SELECT
    DISTRICT,
    FORECAST,
    ROUND(FORECAST / SUM(FORECAST) OVER () * 100, 1) AS SHARE_PCT,
    ROUND(50000000 * FORECAST / SUM(FORECAST) OVER (), 0) AS BUDGET_50M,
    ROUND(100000000 * FORECAST / SUM(FORECAST) OVER (), 0) AS BUDGET_100M,
    MODEL,
    MAPE_PCT
FROM movesignal_ai.analytics.forecast_results_databricks;

SELECT * FROM movesignal_ai.analytics.v_budget_allocation;

-- ============================================================
-- 6. Consumption pattern pivot — category share by district
-- ============================================================
WITH latest AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY YM DESC) AS rn
    FROM movesignal_ai.analytics.feature_mart_final
)
SELECT
    DISTRICT,
    ROUND(FOOD / TOTAL_SALES * 100, 1) AS food_pct,
    ROUND(COFFEE / TOTAL_SALES * 100, 1) AS coffee_pct,
    ROUND(ENTERTAIN / TOTAL_SALES * 100, 1) AS entertain_pct,
    ROUND(CLOTHING / TOTAL_SALES * 100, 1) AS clothing_pct,
    ROUND(CULTURE / TOTAL_SALES * 100, 1) AS culture_pct,
    ROUND(ACCOMMODATION / TOTAL_SALES * 100, 1) AS accommodation_pct,
    ROUND(BEAUTY / TOTAL_SALES * 100, 1) AS beauty_pct,
    ROUND(MEDICAL / TOTAL_SALES * 100, 1) AS medical_pct
FROM latest
WHERE rn = 1
ORDER BY DISTRICT;

-- ============================================================
-- 7. Gold KPI — executive summary
-- ============================================================
SELECT
    DISTRICT,
    ROUND(TOTAL_REVENUE / 1e8, 1) AS total_revenue_억원,
    ROUND(AVG_MONTHLY_REVENUE / 1e8, 2) AS avg_monthly_억원,
    ROUND(TOTAL_FOOTFALL / 1e6, 1) AS total_footfall_백만,
    ROUND(AVG_REVENUE_PER_PERSON, 0) AS revenue_per_person,
    ROUND(AVG_PROPERTY_PRICE / 10000, 0) AS avg_price_만원,
    CUMULATIVE_NET_MIGRATION AS net_migration,
    MONTHS_COVERED,
    START_YM,
    END_YM
FROM movesignal_ai.analytics.gold_district_kpi
ORDER BY total_revenue_억원 DESC;

-- ============================================================
-- 8. Delta Lake time-travel — audit trail
-- ============================================================
DESCRIBE HISTORY movesignal_ai.analytics.feature_mart_final;
DESCRIBE HISTORY movesignal_ai.analytics.audit_log;

-- Query previous version (example: 1 hour ago)
-- SELECT * FROM movesignal_ai.analytics.feature_mart_final TIMESTAMP AS OF '2026-04-06T00:00:00';

-- ============================================================
-- 9. Audit log review
-- ============================================================
SELECT * FROM movesignal_ai.analytics.audit_log
ORDER BY event_ts DESC
LIMIT 20;
