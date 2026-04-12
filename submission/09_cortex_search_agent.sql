/*=============================================================================
  DistrictPilot AI - Cortex Search Service & Cortex Agent
  09_cortex_search_agent.sql

  Purpose: Stand up a Cortex Search service over internal policy/rulebook
           documents, then wire it together with Cortex Analyst (Semantic View)
           and a custom tool via the Cortex Agent API for move-in/home-service
           orchestration.

  Target Districts: 서초구, 영등포구, 중구
  Schema: DISTRICTPILOT_AI.ANALYTICS
=============================================================================*/

-- ============================================================
-- 0. Session Setup
-- ============================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE DISTRICTPILOT_AI;
USE SCHEMA ANALYTICS;

-- ============================================================
-- 1. Policy Document Table
-- ============================================================
CREATE OR REPLACE TABLE POLICY_DOCUMENTS (
    DOC_ID        VARCHAR(20)   NOT NULL,
    TITLE         VARCHAR(200)  NOT NULL,
    CATEGORY      VARCHAR(30)   NOT NULL,   -- rental_policy | move_in_rule | installation_rule | product_guide | cs_policy
    CONTENT       VARCHAR(8000) NOT NULL,
    DISTRICT      VARCHAR(20),              -- NULL = company-wide
    UPDATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_POLICY_DOCUMENTS PRIMARY KEY (DOC_ID)
);

-- ============================================================
-- 2. Sample Policy Documents (Korean)
-- ============================================================
INSERT INTO POLICY_DOCUMENTS (DOC_ID, TITLE, CATEGORY, CONTENT, DISTRICT)
VALUES
-- 2-1. 렌탈 설치 지역 규정
('POL-001',
 '정수기/공기청정기 렌탈 설치 가능 지역 규정',
 'rental_policy',
 '1. 적용 범위: 서울시 전 자치구 및 수도권 지역.
2. 설치 가능 조건: 건물 내 상수도 직결 배관이 확인된 경우에 한하여 설치 가능.
3. 서초구 특이사항: 반포동, 잠원동 일부 노후 건물은 현장 점검 후 설치 여부 결정.
4. 영등포구 특이사항: 여의도 오피스 밀집 지역은 주말 설치만 허용 (빌딩 관리 규정).
5. 중구 특이사항: 을지로 3~4가 일대는 주차 제한으로 인해 설치 스케줄 사전 조율 필수.
6. 제외 지역: 재개발 구역 내 철거 예정 건물, 군사시설 보호구역.',
 NULL),

-- 2-2. 전입 수요 캡처 집행 원칙
('POL-002',
 '전입 수요 캡처 집행 기본 원칙',
 'move_in_rule',
 '1. 총 집행 예산은 전월 실적의 8~12% 범위 내에서 설정한다.
2. 자치구별 집행 강도는 다음 달 forecast + 순이동 + 카드소비 반응을 함께 반영한다.
3. 서초구: 프리미엄 가구/신혼부부 타깃 오퍼와 제휴 채널 비중을 높인다.
4. 영등포구: 오피스/주거 혼합 특성을 반영해 B2B 제안과 B2C 체험 오퍼를 병행한다.
5. 중구: 관광/단기 체류 변동성이 높으므로 팝업형 체험과 단기 프로모션 비중을 높인다.
6. 신규 집행안은 항상 5% 수준의 실험 예산을 별도 확보한다.',
 NULL),

-- 2-3. 서초구 전입 직후 오퍼 가이드
('POL-003',
 '서초구 전입 직후 오퍼 가이드',
 'move_in_rule',
 '1. 반포·잠원·서초동 권역은 고소득 가구와 가족 단위 전입 비중이 높다.
2. 초기 오퍼는 프리미엄 정수기, 공기청정기, 홈케어 번들을 우선 제안한다.
3. 첫 접점은 디지털 광고보다 소개/제휴/컨시어지 채널 효율이 높다.
4. 계약 전환 목표는 체험 신청 -> 설치 예약 -> 패키지 업셀 순서로 설계한다.
5. 설치 전 현장 점검이 필요한 고가 주거지는 예약 확정 전에 스케줄 슬롯을 선확보한다.',
 '서초구'),

-- 2-4. 영등포구 오피스/주거 혼합권 리드 라우팅 규칙
('POL-004',
 '영등포구 오피스/주거 혼합권 리드 라우팅 규칙',
 'move_in_rule',
 '1. 여의도/문래/당산 권역은 B2B와 B2C 리드가 혼재하므로 최초 문의 단계에서 용도 구분이 필요하다.
2. 영업일 주간에는 B2B 제안, 퇴근 시간대와 주말에는 B2C 체험 오퍼 효율이 높다.
3. 오피스 밀집 지역은 설치 가능 시간대 제약이 크므로 예약 전 건물 운영 규정을 확인한다.
4. 신규 리드 라우팅은 법인/가정/소형사업장 세 그룹으로 우선 분기한다.
5. 설치 일정이 길어질 가능성이 높은 리드는 보상 쿠폰보다 빠른 일정 확보를 우선 제안한다.',
 '영등포구'),

-- 2-5. 중구 관광·단기체류 권역 설치 운영 가이드
('POL-005',
 '중구 관광·단기체류 권역 설치 운영 가이드',
 'installation_rule',
 '1. 명동·남대문·을지로 권역은 단기 체류와 관광 유입 변동이 커서 장기 계약 전환율만으로 판단하면 왜곡된다.
2. 체험형 오퍼와 단기 프로모션은 허용되지만, 현장 운영 시간과 보관 공간 제약을 먼저 확인해야 한다.
3. 을지로 3~4가 일대는 주차 제한과 하역 제약으로 설치 스케줄 사전 조율이 필수다.
4. 피크 관광 시즌에는 설치 리드타임보다 현장 대기시간과 회수 동선을 먼저 점검한다.
5. 중구 권역은 체험 -> 즉시 상담 -> 빠른 설치 예약 전환 흐름으로 운영하는 것이 유리하다.',
 '중구'),

-- 2-6. 렌탈 상품 마진율 기준표
('POL-006',
 '렌탈 상품 마진율 기준표',
 'product_guide',
 '1. 정수기 렌탈: 월 요금 대비 마진율 목표 35~42%.
2. 공기청정기 렌탈: 월 요금 대비 마진율 목표 30~38%.
3. 복합기(정수기+공기청정기 패키지): 마진율 목표 28~35%, 교차 판매 인센티브 별도.
4. 법인 계약(B2B): 마진율 하한선 25%, 3년 이상 장기 계약 시 22%까지 허용.
5. 프로모션 기간 마진율 하한: 개인 20%, 법인 18% — 이하로 책정 시 본부장 승인 필요.
6. 마진율 산정 기준: (월 렌탈료 - 감가상각비 - 유지보수비 - 물류비) / 월 렌탈료 × 100.',
 NULL),

-- 2-7. 고객 상담 에스컬레이션 정책
('POL-007',
 '고객 상담 에스컬레이션 정책',
 'cs_policy',
 '1. 1차 상담사: 일반 문의, 설치 일정 조회, 요금 안내 — 목표 해결율 85%.
2. 2차 상담사(시니어): 불만 접수, 계약 변경, 위약금 관련 — 1차 미해결 건 자동 전환.
3. 3차 에스컬레이션(팀장): 법적 분쟁 가능성, SNS/언론 노출 위험, VIP 고객 불만.
4. 응대 시간 기준: 1차 3분 이내 응답, 2차 10분 이내 콜백, 3차 1시간 이내 직접 연락.
5. 보상 권한: 1차 — 월 렌탈료 1회 면제, 2차 — 3개월 할인, 3차 — 계약 조건 재협상 가능.
6. 에스컬레이션 기록은 CRM에 48시간 이내 반드시 등록.',
 NULL);

-- 확인
SELECT DOC_ID, TITLE, CATEGORY, DISTRICT, UPDATED_AT
FROM POLICY_DOCUMENTS
ORDER BY DOC_ID;

-- ============================================================
-- 3. Cortex Search Service
-- ============================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE DISTRICTPILOT_SEARCH_SVC
  ON CONTENT
  ATTRIBUTES TITLE, CATEGORY, DISTRICT
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 hour'
  AS (
    SELECT
        CONTENT,
        TITLE,
        CATEGORY,
        DISTRICT
    FROM POLICY_DOCUMENTS
  );

-- Verify the service was created
SHOW CORTEX SEARCH SERVICES;

-- ============================================================
-- 4. Custom Tool: RECOMMEND_ALLOCATION Stored Procedure
--    Returns a JSON recommendation for move-in capture orchestration
--    based on district performance metrics.
-- ============================================================
CREATE OR REPLACE PROCEDURE RECOMMEND_ALLOCATION(
    P_DISTRICT   VARCHAR,
    P_BUDGET_KRW NUMBER
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_result VARIANT;
BEGIN
    -- Pull the latest feature mart data for the district
    SELECT OBJECT_CONSTRUCT(
        'district',        :P_DISTRICT,
        'total_budget_krw', :P_BUDGET_KRW,
        'recommended_split', OBJECT_CONSTRUCT(
            'digital_capture',    ROUND(:P_BUDGET_KRW * 0.25),
            'partner_referrals',  ROUND(:P_BUDGET_KRW * 0.20),
            'b2b_outreach',       ROUND(:P_BUDGET_KRW * 0.15),
            'experiential_offer', ROUND(:P_BUDGET_KRW * 0.15),
            'installation_buffer',ROUND(:P_BUDGET_KRW * 0.15),
            'test_reserve',       ROUND(:P_BUDGET_KRW * 0.10)
        ),
        'basis', OBJECT_CONSTRUCT(
            'move_in',       f.MOVE_IN,
            'total_sales',   f.TOTAL_SALES,
            'net_move',      f.NET_MOVE,
            'sales_per_pop', f.SALES_PER_POP,
            'avg_asset',     f.AVG_ASSET
        ),
        'generated_at', CURRENT_TIMESTAMP()
    ) INTO :v_result
    FROM FEATURE_MART_V2 f
    WHERE f.DISTRICT = :P_DISTRICT
    ORDER BY f.YM DESC
    LIMIT 1;

    RETURN v_result;
END;
$$;

-- Quick test
CALL RECOMMEND_ALLOCATION('서초구', 50000000);

-- ============================================================
-- 5. Cortex Agent - Complete API Call
--    Orchestrates: Cortex Analyst + Cortex Search + Custom Tool
-- ============================================================

/*
   Snowflake Cortex Agent is invoked via the SNOWFLAKE.CORTEX.COMPLETE()
   function (model = 'llama3.1-70b' or 'mistral-large2') with a tools array
   that references:
     1. A Cortex Analyst tool  -> Semantic View for structured SQL analytics
     2. A Cortex Search tool   -> Policy document retrieval
     3. A function tool        -> RECOMMEND_ALLOCATION stored procedure
*/

-- 5-1. Agent call via SNOWFLAKE.CORTEX.COMPLETE() with tool definitions
-- This pattern can be wrapped in a stored procedure or called directly.

CREATE OR REPLACE PROCEDURE DISTRICTPILOT_AGENT(
    P_USER_QUESTION VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_response   VARIANT;
    v_messages   VARCHAR;
    v_tools      VARCHAR;
BEGIN
    -- Build the messages payload
    v_messages := '[
        {
            "role": "system",
            "content": "You are DistrictPilot AI Agent, an expert assistant for move-in driven home-service and rental orchestration in Seoul districts (서초구, 영등포구, 중구). You can: (1) query structured data via Cortex Analyst on the DISTRICTPILOT_SV semantic view, (2) look up internal policies and rulebooks via Cortex Search, and (3) generate district-level capture plan recommendations. Always answer in Korean unless asked otherwise."
        },
        {
            "role": "user",
            "content": "' || REPLACE(:P_USER_QUESTION, '"', '\\"') || '"
        }
    ]';

    -- Define the three tools available to the agent
    v_tools := '[
        {
            "type": "cortex_analyst_text_to_sql",
            "tool_definition": {
                "name": "analyst",
                "description": "Translate natural language questions into SQL queries against the DistrictPilot semantic view. Use for any question about sales, population, migration, forecasts, or district-level KPIs.",
                "semantic_view": "DISTRICTPILOT_AI.ANALYTICS.DISTRICTPILOT_SV"
            }
        },
        {
            "type": "cortex_search",
            "tool_definition": {
                "name": "policy_search",
                "description": "Search internal policy documents, rulebooks, and guidelines. Use for questions about rental policies, move-in capture rules, installation constraints, product pricing, or customer service escalation procedures.",
                "cortex_search_service": "DISTRICTPILOT_AI.ANALYTICS.DISTRICTPILOT_SEARCH_SVC",
                "max_results": 3,
                "title_column": "TITLE",
                "id_column": "DOC_ID"
            }
        },
        {
            "type": "function",
            "tool_definition": {
                "name": "recommend_allocation",
                "description": "Generate a recommended move-in capture plan for a given district and total budget (KRW). Returns a JSON object with channel-level splits and the underlying data basis.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "district": {
                            "type": "string",
                            "description": "Target district name (서초구, 영등포구, or 중구)"
                        },
                        "budget_krw": {
                            "type": "number",
                            "description": "Total execution budget in KRW"
                        }
                    },
                    "required": ["district", "budget_krw"]
                }
            }
        }
    ]';

    -- Call Cortex Complete with agent-mode tools
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        PARSE_JSON(:v_messages),
        PARSE_JSON(:v_tools)
    ) INTO :v_response;

    RETURN v_response;
END;
$$;


-- ============================================================
-- 6. Alternative: Direct COMPLETE() Calls (no wrapper proc)
-- ============================================================

-- 6-1. Analyst + Search combined agent call (single-shot)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'llama3.1-70b',
    [
        {
            'role': 'system',
            'content': 'You are DistrictPilot AI Agent. Use the provided tools to answer questions about move-in driven home-service demand and internal operating policies for Seoul districts.'
        },
        {
            'role': 'user',
            'content': '영등포구의 최근 3개월 매출 추이를 알려주고, 설치 제약과 리드 라우팅 규칙도 알려줘.'
        }
    ],
    {
        'tools': [
            {
                'type': 'cortex_analyst_text_to_sql',
                'tool_definition': {
                    'name': 'analyst',
                    'semantic_view': 'DISTRICTPILOT_AI.ANALYTICS.DISTRICTPILOT_SV'
                }
            },
            {
                'type': 'cortex_search',
                'tool_definition': {
                    'name': 'policy_search',
                    'cortex_search_service': 'DISTRICTPILOT_AI.ANALYTICS.DISTRICTPILOT_SEARCH_SVC',
                    'max_results': 3
                }
            }
        ]
    }
);


-- ============================================================
-- 7. Test Queries
-- ============================================================

-- Test 1: Policy search - 렌탈 설치 관련 규정 조회
CALL DISTRICTPILOT_AGENT('정수기 렌탈 설치가 안 되는 지역이 있나요?');

-- Test 2: Structured data via Analyst - 매출 분석
CALL DISTRICTPILOT_AGENT('서초구, 영등포구, 중구의 최근 6개월 매출 비교를 보여줘.');

-- Test 3: Policy search - 전입 수요 집행 규정
CALL DISTRICTPILOT_AGENT('전입 수요를 잡기 위한 집행 강도는 어떤 기준으로 정해야 하나요?');

-- Test 4: Custom tool - 캡처 플랜 추천
CALL DISTRICTPILOT_AGENT('영등포구에 5천만 원 집행 예산을 투입한다면 어떤 캡처 플랜이 적절할까요?');

-- Test 5: Multi-tool - Analyst + Search 복합 질의
CALL DISTRICTPILOT_AGENT('중구 관광 권역 설치 운영 가이드를 알려주고, 최근 중구 수요 예측 결과도 보여줘.');

-- Test 6: CS policy lookup
CALL DISTRICTPILOT_AGENT('고객이 강하게 불만을 제기하면 어떤 보상을 할 수 있나요?');

-- Test 7: Product margin question
CALL DISTRICTPILOT_AGENT('법인 고객 대상 렌탈 마진율 하한선이 얼마인가요?');

-- Test 8: Direct Cortex Search service call (without agent)
SELECT SNOWFLAKE.CORTEX.SEARCH(
    'DISTRICTPILOT_AI.ANALYTICS.DISTRICTPILOT_SEARCH_SVC',
    '렌탈 마진율',
    3
);

-- ============================================================
-- 8. Grants (run as ACCOUNTADMIN if needed)
-- ============================================================
/*
USE ROLE ACCOUNTADMIN;

GRANT USAGE ON CORTEX SEARCH SERVICE DISTRICTPILOT_AI.ANALYTICS.DISTRICTPILOT_SEARCH_SVC
  TO ROLE SYSADMIN;

GRANT USAGE ON PROCEDURE DISTRICTPILOT_AI.ANALYTICS.DISTRICTPILOT_AGENT(VARCHAR)
  TO ROLE SYSADMIN;

GRANT USAGE ON PROCEDURE DISTRICTPILOT_AI.ANALYTICS.RECOMMEND_ALLOCATION(VARCHAR, NUMBER)
  TO ROLE SYSADMIN;
*/
