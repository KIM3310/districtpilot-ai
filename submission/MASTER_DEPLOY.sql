-- ============================================================
-- DistrictPilot AI — MASTER DEPLOY SCRIPT
-- Marketplace 데이터 설치 후, 이 파일을 Snowsight에서 Run All
--
-- 사전 조건:
--   1. Marketplace에서 SPH 데이터 "Get" 완료
--      -> DB: SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS
--   2. Marketplace에서 Richgo 데이터 "Get" 완료
--      -> DB: KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA
--   3. Role: ACCOUNTADMIN
--   4. Warehouse: COMPUTE_WH (X-Small)
-- ============================================================

-- ============================================================
-- STEP 0: 환경 설정
-- ============================================================
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS DISTRICTPILOT_AI;
CREATE SCHEMA IF NOT EXISTS DISTRICTPILOT_AI.ANALYTICS;
USE DATABASE DISTRICTPILOT_AI;
USE SCHEMA ANALYTICS;

CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE DIRECTORY = (ENABLE = TRUE);

ALTER SESSION SET QUERY_TAG = '{"app":"districtpilot_ai","module":"master_deploy","version":"v8"}';

SELECT 'STEP 0 COMPLETE: Environment ready' AS STATUS;
