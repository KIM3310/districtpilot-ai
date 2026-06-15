-- ============================================================
-- DistrictPilot AI: Korean Public API Integration Readiness
-- public-apis-4Kr aligned registry for live external-data rollout
--
-- Purpose:
--   Keep the current synthetic/public-data demo stable while documenting
--   exactly which Korean public APIs can replace or enrich each Snowflake
--   source table when provider keys are available.
--
-- Catalog reference:
--   https://github.com/yybmion/public-apis-4Kr
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE DISTRICTPILOT_AI;
USE SCHEMA ANALYTICS;

-- ============================================================
-- 1. Provider-to-feature registry
-- ============================================================

CREATE OR REPLACE TABLE PUBLIC_API_SOURCE_REGISTRY AS
SELECT
  COLUMN1::VARCHAR AS SOURCE_ID,
  COLUMN2::VARCHAR AS SOURCE_NAME,
  COLUMN3::VARCHAR AS SIGNAL_GROUP,
  COLUMN4::VARCHAR AS PROVIDER,
  COLUMN5::VARCHAR AS SECRET_NAME,
  COLUMN6::VARCHAR AS TARGET_OBJECT,
  COLUMN7::VARCHAR AS FEATURE_USAGE,
  COLUMN8::VARCHAR AS ENABLEMENT_MODE,
  COLUMN9::VARCHAR AS CURRENT_STATUS,
  COLUMN10::VARCHAR AS REFERENCE_URL,
  COLUMN11::VARCHAR AS CATALOG_URL
FROM VALUES
  (
    'public-holiday-spcde',
    'Korean special day / holiday API',
    'calendar',
    'Korea Astronomy and Space Science Institute via data.go.kr',
    'DATA_GO_KR_SERVICE_KEY',
    'STG_HOLIDAY',
    'Replace synthetic holiday counts before FEATURE_MART_V2 and FORECAST_INPUT_E refresh.',
    'Snowpark task or external function -> STG_HOLIDAY -> FEATURE_MART_V2',
    'mapped-synthetic-now',
    'https://www.data.go.kr/data/15012690/openapi.do',
    'https://github.com/yybmion/public-apis-4Kr'
  ),
  (
    'mois-resident-population',
    'Resident registration population and household status',
    'demographics',
    'Ministry of the Interior and Safety via data.go.kr',
    'DATA_GO_KR_SERVICE_KEY',
    'STG_DEMOGRAPHICS',
    'Replace synthetic age and household signals used by FEATURE_MART_V2.',
    'Monthly Snowpark ingest -> district/month aggregation -> STG_DEMOGRAPHICS',
    'mapped-synthetic-now',
    'https://www.data.go.kr/data/15098929/openapi.do',
    'https://github.com/yybmion/public-apis-4Kr'
  ),
  (
    'kosis-migration-statistics',
    'KOSIS population movement statistics',
    'move-signal',
    'Statistics Korea KOSIS',
    'KOSIS_API_KEY',
    'FEATURE_MART_FINAL / FEATURE_MART_V2',
    'Cross-check move-in, move-out, and net migration features before retraining DISTRICTPILOT_FORECAST_V2.',
    'Monthly API pull -> staging validation -> FEATURE_MART_V2 migration features',
    'mapped-synthetic-now',
    'https://kosis.kr/',
    'https://github.com/yybmion/public-apis-4Kr'
  ),
  (
    'tourapi-visitkorea',
    'Korea Tourism Organization TourAPI / DataLab',
    'tourism-demand',
    'Korea Tourism Organization',
    'TOURAPI_SERVICE_KEY',
    'STG_TOURISM',
    'Replace synthetic domestic/foreign visitor and tourism-spend indexes.',
    'Monthly API pull -> STG_TOURISM -> FEATURE_MART_V2 tourism features',
    'mapped-synthetic-now',
    'https://datalab.visitkorea.or.kr/',
    'https://github.com/yybmion/public-apis-4Kr'
  ),
  (
    'seoul-commercial-district',
    'Seoul commercial district analysis service',
    'commercial-stability',
    'Seoul Open Data / 골목상권',
    'SEOUL_OPEN_DATA_API_KEY',
    'STG_COMMERCIAL',
    'Replace synthetic stability, open/close, and closure-risk features.',
    'Quarterly API pull -> forward-fill to month -> STG_COMMERCIAL',
    'mapped-synthetic-now',
    'https://golmok.seoul.go.kr/',
    'https://github.com/yybmion/public-apis-4Kr'
  ),
  (
    'molit-apt-transaction',
    'Apartment transaction and lease public data',
    'housing-market',
    'Ministry of Land, Infrastructure and Transport via data.go.kr',
    'MOLIT_API_KEY',
    'FEATURE_MART_FINAL / FEATURE_MART_V2',
    'Cross-check Richgo apartment and housing-price signals for public-sector explainability.',
    'Monthly API pull -> housing staging -> model feature QA before FORECAST refresh',
    'future-live-enrichment',
    'https://www.data.go.kr/',
    'https://github.com/yybmion/public-apis-4Kr'
  ),
  (
    'kma-weather-risk',
    'KMA API Hub weather alerts and forecasts',
    'weather-risk',
    'Korea Meteorological Administration',
    'KMA_API_KEY',
    'STG_WEATHER_RISK',
    'Add weather disruption context for move-in and field-service staffing action cards.',
    'Daily API pull -> district weather mapping -> optional FEATURE_MART_V4 enrichment',
    'future-live-enrichment',
    'https://apiportal.kma.go.kr/',
    'https://github.com/yybmion/public-apis-4Kr'
  ),
  (
    'airkorea-environment-risk',
    'AirKorea air-quality observations',
    'environment-risk',
    'Korea Environment Corporation',
    'AIRKOREA_API_KEY',
    'STG_ENVIRONMENT_RISK',
    'Add air-quality context for outdoor move-in, queue, and service-exception planning.',
    'Daily API pull -> station-to-district mapping -> optional FEATURE_MART_V4 enrichment',
    'future-live-enrichment',
    'https://www.airkorea.or.kr/',
    'https://github.com/yybmion/public-apis-4Kr'
  );

-- ============================================================
-- 2. Readiness view for operators and operators
-- ============================================================

CREATE OR REPLACE VIEW V_PUBLIC_API_INTEGRATION_READINESS AS
SELECT
  SOURCE_ID,
  SOURCE_NAME,
  SIGNAL_GROUP,
  PROVIDER,
  SECRET_NAME,
  TARGET_OBJECT,
  FEATURE_USAGE,
  ENABLEMENT_MODE,
  CURRENT_STATUS,
  CASE
    WHEN CURRENT_STATUS = 'mapped-synthetic-now' THEN 'ready-for-keyed-ingest'
    ELSE 'planned-enrichment'
  END AS READINESS_STATUS,
  CASE
    WHEN CURRENT_STATUS = 'mapped-synthetic-now'
      THEN 'Validate API fixtures, replace synthetic staging rows, then rerun FEATURE_MART_V2 and FORECAST_INPUT_E.'
    ELSE 'Create staging table and keep enrichment optional until forecast ablation proves value.'
  END AS NEXT_ACTION,
  REFERENCE_URL,
  CATALOG_URL
FROM PUBLIC_API_SOURCE_REGISTRY;

-- ============================================================
-- 3. Group coverage view
-- ============================================================

CREATE OR REPLACE VIEW V_PUBLIC_API_SIGNAL_GROUP_COVERAGE AS
SELECT
  SIGNAL_GROUP,
  COUNT(*) AS SOURCE_COUNT,
  COUNT_IF(CURRENT_STATUS = 'mapped-synthetic-now') AS DIRECT_REPLACEMENT_COUNT,
  COUNT_IF(CURRENT_STATUS = 'future-live-enrichment') AS OPTIONAL_ENRICHMENT_COUNT,
  LISTAGG(SOURCE_ID, ', ') WITHIN GROUP (ORDER BY SOURCE_ID) AS SOURCES,
  LISTAGG(DISTINCT SECRET_NAME, ', ') WITHIN GROUP (ORDER BY SECRET_NAME) AS REQUIRED_SECRETS
FROM PUBLIC_API_SOURCE_REGISTRY
GROUP BY SIGNAL_GROUP;

-- ============================================================
-- 4. Review queries
-- ============================================================

SELECT
  'PUBLIC_API_READINESS' AS CHECK_NAME,
  COUNT(*) AS SOURCE_COUNT,
  COUNT_IF(CURRENT_STATUS = 'mapped-synthetic-now') AS DIRECT_REPLACEMENT_COUNT,
  COUNT_IF(CURRENT_STATUS = 'future-live-enrichment') AS OPTIONAL_ENRICHMENT_COUNT
FROM PUBLIC_API_SOURCE_REGISTRY;

SELECT * FROM V_PUBLIC_API_SIGNAL_GROUP_COVERAGE ORDER BY SIGNAL_GROUP;
SELECT * FROM V_PUBLIC_API_INTEGRATION_READINESS ORDER BY CURRENT_STATUS, SIGNAL_GROUP, SOURCE_ID;
