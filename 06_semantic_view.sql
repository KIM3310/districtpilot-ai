/*=============================================================================
  MoveSignal AI - Semantic View for Cortex Analyst / AI SQL Generation
  06_semantic_view.sql

  Purpose: Expose FEATURE_MART_FINAL, FORECAST_RESULTS, ACTUAL_VS_FORECAST
           as a unified semantic layer for natural-language-to-SQL queries.

  Target Districts: 서초구, 영등포구, 중구
  Schema: MOVESIGNAL_AI.ANALYTICS
=============================================================================*/

-- ============================================================
-- 0. Session Setup
-- ============================================================
USE ROLE SYSADMIN;
USE WAREHOUSE MOVESIGNAL_WH;
USE DATABASE MOVESIGNAL_AI;
USE SCHEMA ANALYTICS;

-- ============================================================
-- 1. Create Semantic View
-- ============================================================
CREATE OR REPLACE SEMANTIC VIEW MOVESIGNAL_SV

  -- ---------------------------------------------------------
  -- LOGICAL TABLES
  -- ---------------------------------------------------------
  TABLES (

    -- -------------------------------------------------------
    -- Table 1: Feature Mart (monthly district-level features)
    -- -------------------------------------------------------
    FEATURE_MART AS (
      SELECT *
      FROM MOVESIGNAL_AI.ANALYTICS.FEATURE_MART_V2
    )
      PRIMARY KEY (YM, DISTRICT)
      WITH SYNONYMS = ('피쳐마트', '피처마트', 'feature mart', '마트')

      -- Dimensions
      COLUMNS (
        YM
          COMMENT 'Year-month key in YYYY-MM format (기준 연월)'
          WITH SYNONYMS = ('연월', '기준월', 'year_month', '월', '날짜'),

        DISTRICT
          COMMENT 'Seoul district name: 서초구, 영등포구, 중구 (서울시 자치구)'
          WITH SYNONYMS = ('자치구', '구', 'gu', '지역', '지구')
      )

      -- Population metrics
      COLUMNS (
        TOTAL_POP
          COMMENT 'Total population count for the district-month (총 인구수)'
          WITH SYNONYMS = ('총인구', '인구', '전체인구')
          AS METRIC,

        RES_POP
          COMMENT 'Resident population (거주 인구)'
          WITH SYNONYMS = ('거주인구', '상주인구')
          AS METRIC,

        WORK_POP
          COMMENT 'Working (commuting) population (직장 인구)'
          WITH SYNONYMS = ('직장인구', '근무인구', '통근인구')
          AS METRIC,

        VISIT_POP
          COMMENT 'Visiting (floating) population (방문 인구, 유동인구)'
          WITH SYNONYMS = ('방문인구', '유동인구')
          AS METRIC
      )

      -- Sales / consumption metrics
      COLUMNS (
        TOTAL_SALES
          COMMENT 'Total card-based sales amount in KRW (총 매출액, 카드매출 합계)'
          WITH SYNONYMS = ('총매출', '매출', '카드매출', '소비금액')
          AS METRIC,

        FOOD
          COMMENT 'Food & dining sales (음식/외식 매출)'
          WITH SYNONYMS = ('음식', '외식', '식비')
          AS METRIC,

        COFFEE
          COMMENT 'Coffee & beverage sales (커피/음료 매출)'
          WITH SYNONYMS = ('커피', '음료', '카페')
          AS METRIC,

        ENTERTAIN
          COMMENT 'Entertainment sales (유흥/오락 매출)'
          WITH SYNONYMS = ('유흥', '오락', '엔터테인먼트')
          AS METRIC,

        CLOTHING
          COMMENT 'Clothing & fashion sales (의류/패션 매출)'
          WITH SYNONYMS = ('의류', '패션', '옷')
          AS METRIC,

        CULTURE
          COMMENT 'Culture & leisure sales (문화/여가 매출)'
          WITH SYNONYMS = ('문화', '여가', '레저')
          AS METRIC,

        ACCOMMODATION
          COMMENT 'Accommodation sales (숙박 매출)'
          WITH SYNONYMS = ('숙박', '호텔', '펜션')
          AS METRIC,

        BEAUTY
          COMMENT 'Beauty & cosmetics sales (미용/뷰티 매출)'
          WITH SYNONYMS = ('미용', '뷰티', '화장품')
          AS METRIC,

        MEDICAL
          COMMENT 'Medical & healthcare sales (의료/건강 매출)'
          WITH SYNONYMS = ('의료', '건강', '병원')
          AS METRIC,

        EDUCATION
          COMMENT 'Education-related sales (교육 매출)'
          WITH SYNONYMS = ('교육', '학원', '학습')
          AS METRIC,

        TOTAL_TX_COUNT
          COMMENT 'Total transaction count (총 거래건수)'
          WITH SYNONYMS = ('거래건수', '거래수', '트랜잭션')
          AS METRIC,

        CUSTOMERS
          COMMENT 'Unique customer count (고객수, 이용자수)'
          WITH SYNONYMS = ('고객수', '이용자수', '소비자수')
          AS METRIC,

        SALES_PER_POP
          COMMENT 'Sales per capita = TOTAL_SALES / TOTAL_POP (인구당 매출)'
          WITH SYNONYMS = ('인구당매출', '1인당매출', '인당소비')
          AS METRIC
      )

      -- Wealth / economic indicators
      COLUMNS (
        AVG_INCOME
          COMMENT 'Average income of district residents in KRW (평균 소득)'
          WITH SYNONYMS = ('평균소득', '소득', '수입')
          AS METRIC,

        AVG_ASSET
          COMMENT 'Average asset value in KRW (평균 자산)'
          WITH SYNONYMS = ('평균자산', '자산', '재산')
          AS METRIC,

        AVG_CREDIT_SCORE
          COMMENT 'Average credit score (평균 신용점수)'
          WITH SYNONYMS = ('신용점수', '크레딧스코어', '신용등급')
          AS METRIC,

        AVG_HH_INCOME
          COMMENT 'Average household income (평균 가구소득)'
          WITH SYNONYMS = ('가구소득', '세대소득')
          AS METRIC
      )

      -- Real-estate price indicators
      COLUMNS (
        AVG_MEME_PRICE
          COMMENT 'Average Maemae (sale) price in KRW (평균 매매가)'
          WITH SYNONYMS = ('매매가', '매매가격', '집값')
          AS METRIC,

        AVG_JEONSE_PRICE
          COMMENT 'Average Jeonse (deposit lease) price in KRW (평균 전세가)'
          WITH SYNONYMS = ('전세가', '전세가격')
          AS METRIC,

        AVG_PRICE_PER_PYEONG
          COMMENT 'Average price per pyeong in KRW (평당가격)'
          WITH SYNONYMS = ('평당가격', '평당가', '평단가')
          AS METRIC,

        PRICE_TO_ASSET_RATIO
          COMMENT 'Ratio of real-estate price to average asset (매매가 대비 자산 비율, PIR)'
          WITH SYNONYMS = ('PIR', '가격자산비율', '매매가대비자산')
          AS METRIC
      )

      -- Household & migration metrics
      COLUMNS (
        TOTAL_HOUSEHOLDS
          COMMENT 'Total number of households (총 세대수)'
          WITH SYNONYMS = ('세대수', '가구수', '총가구')
          AS METRIC,

        MOVE_IN
          COMMENT 'Number of households moving into the district (전입 세대수)'
          WITH SYNONYMS = ('전입', '이주전입', '유입')
          AS METRIC,

        MOVE_OUT
          COMMENT 'Number of households moving out of the district (전출 세대수)'
          WITH SYNONYMS = ('전출', '이주전출', '유출')
          AS METRIC,

        NET_MOVE
          COMMENT 'Net migration = MOVE_IN - MOVE_OUT (순이동, 순전입)'
          WITH SYNONYMS = ('순이동', '순전입', '순유입', '넷무브')
          AS METRIC
      )

      -- Period-over-period change columns
      COLUMNS (
        PREV_SALES
          COMMENT 'Previous month total sales (전월 매출)'
          WITH SYNONYMS = ('전월매출', '이전매출')
          AS METRIC,

        PREV_POP
          COMMENT 'Previous month total population (전월 인구)'
          WITH SYNONYMS = ('전월인구', '이전인구')
          AS METRIC,

        SALES_CHG_PCT
          COMMENT 'Month-over-month sales change percentage (매출 변화율 %)'
          WITH SYNONYMS = ('매출변화율', '매출증감률', '매출성장률')
          AS METRIC,

        POP_CHG_PCT
          COMMENT 'Month-over-month population change percentage (인구 변화율 %)'
          WITH SYNONYMS = ('인구변화율', '인구증감률')
          AS METRIC,

        PRICE_CHG_PCT
          COMMENT 'Month-over-month real-estate price change percentage (가격 변화율 %)'
          WITH SYNONYMS = ('가격변화율', '가격증감률', '매매가변화율')
          AS METRIC
      )

      -- External data: Holiday calendar
      COLUMNS (
        HOLIDAY_DAYS
          COMMENT 'Number of public holidays in the month (월내 공휴일 일수)'
          WITH SYNONYMS = ('공휴일', '공휴일수', '휴일')
          AS METRIC,

        LONG_WEEKEND_CNT
          COMMENT 'Number of long weekends in the month (연휴 횟수)'
          WITH SYNONYMS = ('연휴', '연휴수', '징검다리')
          AS METRIC,

        BUSINESS_DAYS
          COMMENT 'Number of business days (영업일수)'
          WITH SYNONYMS = ('영업일', '근무일', '평일수')
          AS METRIC
      )

      -- External data: Demographics
      COLUMNS (
        AGE_20_39_SHARE
          COMMENT 'Share of population aged 20-39 (20-39세 비중)'
          WITH SYNONYMS = ('청년비중', '2030비중', '젊은층비중')
          AS METRIC,

        SENIOR_60P_SHARE
          COMMENT 'Share of population aged 60+ (60세이상 비중)'
          WITH SYNONYMS = ('시니어비중', '고령비중', '노인비중')
          AS METRIC,

        FAMILY_30_49_SHARE
          COMMENT 'Share of population aged 30-49, family demographic (30-49세 가족세대 비중)'
          WITH SYNONYMS = ('가족세대비중', '3040비중')
          AS METRIC
      )

      -- External data: Tourism
      COLUMNS (
        TOURISM_DEMAND_IDX
          COMMENT 'Tourism demand index, base 100 = 2021-01 (관광수요지수)'
          WITH SYNONYMS = ('관광수요', '관광지수', '관광수요지수')
          AS METRIC,

        FOREIGN_VISITOR_IDX
          COMMENT 'Foreign visitor index, base 100 = 2021-01 (외래객지수)'
          WITH SYNONYMS = ('외래객', '외국인관광', '외래객지수')
          AS METRIC
      )

      -- External data: Commercial health
      COLUMNS (
        STABILITY_SCORE
          COMMENT 'Commercial zone stability score 0-1 (상권 안정도)'
          WITH SYNONYMS = ('상권안정도', '상권건강도', '안정도')
          AS METRIC,

        NET_STORE_CHANGE
          COMMENT 'Net store openings minus closures (순 점포 변동)'
          WITH SYNONYMS = ('순점포변동', '개폐업차이', '점포변동')
          AS METRIC,

        RENTAL_SIGNAL_V2
          COMMENT 'Derived rental signal combining migration, stores, tourism (렌탈 신호)'
          WITH SYNONYMS = ('렌탈신호', '렌탈시그널', '임대신호')
          AS METRIC,

        MARKET_HEALTH_SCORE
          COMMENT 'Market health = stability * (1 - closure_risk * 0.3) (시장건강도)'
          WITH SYNONYMS = ('시장건강도', '마켓건강도')
          AS METRIC
      ),

    -- -------------------------------------------------------
    -- Table 2: Forecast Results (time-series predictions)
    -- -------------------------------------------------------
    FORECAST AS (
      SELECT
        SERIES   AS DISTRICT,
        TS       AS DS,
        FORECAST AS NEXT_MONTH_FORECAST,
        LOWER_BOUND,
        UPPER_BOUND
      FROM MOVESIGNAL_AI.ANALYTICS.FORECAST_RESULTS
    )
      PRIMARY KEY (DISTRICT, DS)
      WITH SYNONYMS = ('예측', '포캐스트', 'forecast', '예측결과')

      COLUMNS (
        DISTRICT
          COMMENT 'District name matching FEATURE_MART (자치구명)'
          WITH SYNONYMS = ('자치구', '구', '지역'),

        DS
          COMMENT 'Forecast timestamp / target date (예측 대상 일자)'
          WITH SYNONYMS = ('예측일자', '예측날짜', '타임스탬프', '날짜')
      )

      COLUMNS (
        NEXT_MONTH_FORECAST
          COMMENT 'Forecasted value for next period, used for budget allocation (다음달 예측값, 배분 기준)'
          WITH SYNONYMS = ('예측값', '다음달예측', '포캐스트값', 'forecast값')
          AS METRIC,

        LOWER_BOUND
          COMMENT '95% prediction interval lower bound (예측 하한)'
          WITH SYNONYMS = ('하한', '최소예측', '하한값')
          AS METRIC,

        UPPER_BOUND
          COMMENT '95% prediction interval upper bound (예측 상한)'
          WITH SYNONYMS = ('상한', '최대예측', '상한값')
          AS METRIC
      ),

    -- -------------------------------------------------------
    -- Table 3: Actual vs Forecast (back-test / accuracy)
    -- -------------------------------------------------------
    ACTUAL_VS_FORECAST AS (
      SELECT *
      FROM MOVESIGNAL_AI.ANALYTICS.ACTUAL_VS_FORECAST
    )
      PRIMARY KEY (DISTRICT, DS)
      WITH SYNONYMS = ('실적대비예측', '백테스트', 'actual_forecast', '예측정확도')

      COLUMNS (
        DISTRICT
          COMMENT 'District name (자치구명)'
          WITH SYNONYMS = ('자치구', '구', '지역'),

        DS
          COMMENT 'Date of the actual observation (실적 일자)'
          WITH SYNONYMS = ('날짜', '실적일자', '기준일')
      )

      COLUMNS (
        ACTUAL
          COMMENT 'Observed actual value (실측값, 실제값)'
          WITH SYNONYMS = ('실측값', '실제값', '실적')
          AS METRIC,

        FORECAST_VAL
          COMMENT 'Forecasted value at that date (예측값)'
          WITH SYNONYMS = ('예측값', '모델예측')
          AS METRIC,

        LOWER_BOUND
          COMMENT 'Prediction interval lower bound (예측 하한)'
          WITH SYNONYMS = ('하한', '하한값')
          AS METRIC,

        UPPER_BOUND
          COMMENT 'Prediction interval upper bound (예측 상한)'
          WITH SYNONYMS = ('상한', '상한값')
          AS METRIC
      )
  )

  -- ---------------------------------------------------------
  -- RELATIONSHIPS
  -- ---------------------------------------------------------
  RELATIONSHIPS (
    FEATURE_MART (DISTRICT) REFERENCES FORECAST (DISTRICT),
    FEATURE_MART (DISTRICT) REFERENCES ACTUAL_VS_FORECAST (DISTRICT)
  )

  -- ---------------------------------------------------------
  -- FACTS / COMPUTED METRICS
  -- ---------------------------------------------------------
  METRICS (
    -- Allocation percentage: district forecast / sum of all forecasts
    ALLOCATION_PCT AS (
      FORECAST.NEXT_MONTH_FORECAST
      / NULLIF(SUM(FORECAST.NEXT_MONTH_FORECAST) OVER (), 0)
      * 100
    )
      COMMENT '마케팅 예산 배분 비율 (%) = 해당 구 예측값 / 전체 예측값 합계 * 100'
      WITH SYNONYMS = ('배분비율', '배분율', '할당비율', '예산배분', 'allocation')
  )

  -- ---------------------------------------------------------
  -- AI SQL GENERATION INSTRUCTIONS
  -- ---------------------------------------------------------
  COMMENT = 'MoveSignal AI 시맨틱 뷰 - 서울시 서초구/영등포구/중구 대상 임대마케팅 예산배분 엔진'

  AI_SQL_GENERATION (
    INSTRUCTIONS = '
      1. 배분 질문은 다음 달 forecast 기준으로 ALLOCATION_PCT를 사용하여 답변한다.
         예: "서초구 배분 비율은?" → FORECAST 테이블의 NEXT_MONTH_FORECAST 기반 ALLOCATION_PCT 산출.
      2. 비교 질문은 district level aggregate로 답변한다.
         예: "영등포구와 중구 매출 비교" → FEATURE_MART에서 DISTRICT별 GROUP BY.
      3. 숫자는 반올림 규칙 통일: 비율은 ROUND(x, 1), 금액은 ROUND(x, 0).
      4. 금액은 억원 단위로 표시한다. 원화 금액 / 100000000 변환 후 ROUND(x, 1) || ''억원'' 형태.
      5. 예측 정확도 질문은 ACTUAL_VS_FORECAST 테이블을 사용하고,
         오차율 = ABS(ACTUAL - FORECAST_VAL) / NULLIF(ACTUAL, 0) * 100 으로 계산.
      6. 시계열 추세 질문은 YM 또는 DS 기준 ORDER BY ASC 사용.
      7. 순이동(NET_MOVE) 양수는 전입 우세, 음수는 전출 우세로 해석.
      8. DISTRICT 값은 항상 한글 구명 (서초구, 영등포구, 중구)으로 필터링.
      9. 최신 데이터 요청 시 MAX(YM) 또는 MAX(DS) 서브쿼리를 사용.
      10. 복합 질문(예: "자산 대비 매출이 가장 높은 구")은 파생 컬럼을 CTE로 구성.
    '
  )

  AI_QUESTION_CATEGORIZATION (
    INSTRUCTIONS = '
      질문을 아래 카테고리로 분류한다:

      [배분/할당] - 예산 배분, 마케팅 할당, 투자 비율 관련
        예: "다음 달 예산을 어떻게 배분할까?", "서초구 할당 비율은?"
        → FORECAST + ALLOCATION_PCT 사용

      [비교/분석] - 구별 지표 비교, 순위, 랭킹
        예: "매출이 가장 높은 구는?", "인구 대비 매출 비교"
        → FEATURE_MART 사용, GROUP BY DISTRICT

      [추세/변화] - 시간에 따른 변화, 트렌드, 증감
        예: "서초구 매출 추이", "최근 3개월 인구 변화"
        → FEATURE_MART 사용, ORDER BY YM

      [예측/전망] - 미래 예측값, 신뢰구간
        예: "다음 달 영등포구 예측값은?", "예측 상한/하한"
        → FORECAST 사용

      [정확도/검증] - 예측 모델 성능, 백테스트
        예: "예측 정확도는?", "실제값과 예측값 차이"
        → ACTUAL_VS_FORECAST 사용

      [부동산/자산] - 매매가, 전세가, 자산 관련
        예: "평당가격 비교", "매매가 추이"
        → FEATURE_MART 부동산 컬럼 사용

      [인구/이동] - 인구, 전입/전출, 유동인구
        예: "순이동이 가장 많은 구", "유동인구 변화"
        → FEATURE_MART 인구/이동 컬럼 사용
    '
  )
;

-- ============================================================
-- 2. Grant Usage (adjust roles as needed)
-- ============================================================
GRANT SELECT ON SEMANTIC VIEW MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_SV
  TO ROLE SYSADMIN;

-- Optional: grant to analyst / application roles
-- GRANT SELECT ON SEMANTIC VIEW MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_SV
--   TO ROLE MOVESIGNAL_ANALYST;

-- ============================================================
-- 3. Verification
-- ============================================================
DESCRIBE SEMANTIC VIEW MOVESIGNAL_SV;

SELECT SYSTEM$VALIDATE_SEMANTIC_VIEW('MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_SV');
