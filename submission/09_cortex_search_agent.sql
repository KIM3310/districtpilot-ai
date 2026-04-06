/*=============================================================================
  MoveSignal AI - Cortex Search Service & Cortex Agent
  09_cortex_search_agent.sql

  Purpose: Stand up a Cortex Search service over internal policy/rulebook
           documents, then wire it together with Cortex Analyst (Semantic View)
           and a custom tool via the Cortex Agent API.

  Target Districts: 서초구, 영등포구, 중구
  Schema: MOVESIGNAL_AI.ANALYTICS
=============================================================================*/

-- ============================================================
-- 0. Session Setup
-- ============================================================
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MOVESIGNAL_AI;
USE SCHEMA ANALYTICS;

-- ============================================================
-- 1. Policy Document Table
-- ============================================================
CREATE OR REPLACE TABLE POLICY_DOCUMENTS (
    DOC_ID        VARCHAR(20)   NOT NULL,
    TITLE         VARCHAR(200)  NOT NULL,
    CATEGORY      VARCHAR(30)   NOT NULL,   -- rental_policy | marketing_rule | public_admin | product_guide | cs_policy
    CONTENT       VARCHAR(4000) NOT NULL,
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

-- 2-2. 마케팅 예산 배분 원칙
('POL-002',
 '마케팅 예산 배분 기본 원칙',
 'marketing_rule',
 '1. 총 마케팅 예산은 전월 매출액의 8~12% 범위 내에서 집행한다.
2. 자치구별 배분 비율은 해당 구의 전월 매출 비중 + 순이동 인구 성장률을 가중 반영한다.
3. 서초구: 고소득 타겟 프리미엄 채널(잡지, 프리미엄 옥외광고) 비중 40% 이상 권장.
4. 영등포구: 오피스 밀집 특성상 B2B 제안서 및 기업 대상 프로모션에 예산 30% 이상 배정.
5. 중구: 관광객 유동 인구 활용 체험 부스, 팝업 스토어 중심으로 예산 25% 이상 배정.
6. 신규 캠페인 론칭 시 A/B 테스트 비용으로 전체 예산의 5%를 별도 확보해야 한다.',
 NULL),

-- 2-3. 서초구 상권 활성화 지원 정책
('POL-003',
 '서초구 상권 활성화 지원 정책',
 'public_admin',
 '1. 서초구청은 연 2회(상반기/하반기) 소상공인 상권 활성화 보조금을 지급한다.
2. 대상: 서초구 소재 사업장 중 연매출 3억 원 이하 사업체.
3. 지원 금액: 업체당 최대 500만 원 (마케팅비, 인테리어비, 컨설팅비 포함).
4. 신청 방법: 서초구청 경제과 온라인 접수 → 현장 실사 → 심사위원회 → 확정 통보.
5. MoveSignal 활용: 상권 분석 데이터 기반으로 보조금 신청서 내 시장 분석 항목을 자동 생성.',
 '서초구'),

-- 2-4. 영등포구 소상공인 지원금 우선순위 기준
('POL-004',
 '영등포구 소상공인 지원금 우선순위 기준',
 'public_admin',
 '1. 영등포구 소상공인 지원금은 분기별 선착순 + 가점제 혼합 방식으로 운영한다.
2. 가점 항목: (a) 업력 5년 이상 +10점, (b) 고용 인원 3인 이상 +5점,
   (c) 디지털 전환 계획 보유 +8점, (d) 친환경 인증 보유 +3점.
3. 우선 지원 업종: 생활 서비스(세탁, 렌탈, 수리), 외식업, 소매업.
4. 지원 한도: 업체당 연간 최대 800만 원.
5. 렌탈 사업자 특이사항: 렌탈 계약 건수 증빙 시 디지털 전환 가점 자동 부여.',
 '영등포구'),

-- 2-5. 중구 관광 인프라 배치 가이드
('POL-005',
 '중구 관광 인프라 배치 가이드',
 'public_admin',
 '1. 중구 관광특구(명동, 남대문, 을지로) 내 인프라 배치는 관광과 협의 필수.
2. 체험형 장비(정수기 시음대, 공기질 측정 부스 등) 설치 시 관광특구 조례에 따른 허가 필요.
3. 배치 우선 순위: 명동역 반경 300m > 을지로입구역 > 충무로역 > 남대문시장 입구.
4. 운영 시간: 관광특구 내 옥외 장비는 09:00~21:00, 실내 장비는 건물 운영 시간에 준함.
5. 계절별 가이드: 하절기(6~8월) 냉음수기 프로모션, 동절기(12~2월) 공기청정기 체험 강화.',
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
CREATE OR REPLACE CORTEX SEARCH SERVICE MOVESIGNAL_SEARCH_SVC
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
--    Returns a JSON recommendation for marketing budget allocation
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
            'digital_ads',        ROUND(:P_BUDGET_KRW * 0.30),
            'offline_promo',      ROUND(:P_BUDGET_KRW * 0.25),
            'b2b_outreach',       ROUND(:P_BUDGET_KRW * 0.20),
            'experiential_booth', ROUND(:P_BUDGET_KRW * 0.15),
            'ab_test_reserve',    ROUND(:P_BUDGET_KRW * 0.05),
            'contingency',        ROUND(:P_BUDGET_KRW * 0.05)
        ),
        'basis', OBJECT_CONSTRUCT(
            'total_sales',   f.TOTAL_SALES,
            'net_move',      f.NET_MOVE,
            'sales_per_pop', f.SALES_PER_POP,
            'avg_asset',     f.AVG_ASSET
        ),
        'generated_at', CURRENT_TIMESTAMP()
    ) INTO :v_result
    FROM FEATURE_MART_FINAL f
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

CREATE OR REPLACE PROCEDURE MOVESIGNAL_AGENT(
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
            "content": "You are MoveSignal AI Agent, an expert assistant for rental business analytics in Seoul districts (서초구, 영등포구, 중구). You can: (1) query structured data via Cortex Analyst on the MOVESIGNAL_SV semantic view, (2) look up internal policies and rulebooks via Cortex Search, and (3) generate budget allocation recommendations. Always answer in Korean unless asked otherwise."
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
                "description": "Translate natural language questions into SQL queries against the MoveSignal semantic view. Use for any question about sales, population, migration, forecasts, or district-level KPIs.",
                "semantic_view": "MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_SV"
            }
        },
        {
            "type": "cortex_search",
            "tool_definition": {
                "name": "policy_search",
                "description": "Search internal policy documents, rulebooks, and guidelines. Use for questions about rental policies, marketing rules, public administration support, product pricing, or customer service escalation procedures.",
                "cortex_search_service": "MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_SEARCH_SVC",
                "max_results": 3,
                "title_column": "TITLE",
                "id_column": "DOC_ID"
            }
        },
        {
            "type": "function",
            "tool_definition": {
                "name": "recommend_allocation",
                "description": "Generate a recommended marketing budget allocation for a given district and total budget (KRW). Returns a JSON object with channel-level splits and the underlying data basis.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "district": {
                            "type": "string",
                            "description": "Target district name (서초구, 영등포구, or 중구)"
                        },
                        "budget_krw": {
                            "type": "number",
                            "description": "Total marketing budget in KRW"
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
            'content': 'You are MoveSignal AI Agent. Use the provided tools to answer questions about rental business analytics and internal policies for Seoul districts.'
        },
        {
            'role': 'user',
            'content': '영등포구의 최근 3개월 매출 추이를 알려주고, 소상공인 지원금 신청 시 가점 항목도 알려줘.'
        }
    ],
    {
        'tools': [
            {
                'type': 'cortex_analyst_text_to_sql',
                'tool_definition': {
                    'name': 'analyst',
                    'semantic_view': 'MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_SV'
                }
            },
            {
                'type': 'cortex_search',
                'tool_definition': {
                    'name': 'policy_search',
                    'cortex_search_service': 'MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_SEARCH_SVC',
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
CALL MOVESIGNAL_AGENT('정수기 렌탈 설치가 안 되는 지역이 있나요?');

-- Test 2: Structured data via Analyst - 매출 분석
CALL MOVESIGNAL_AGENT('서초구, 영등포구, 중구의 최근 6개월 매출 비교를 보여줘.');

-- Test 3: Policy search - 마케팅 규정
CALL MOVESIGNAL_AGENT('마케팅 예산을 어떤 기준으로 배분해야 하나요?');

-- Test 4: Custom tool - 예산 배분 추천
CALL MOVESIGNAL_AGENT('영등포구에 5천만 원 마케팅 예산을 배분하려면 어떻게 해야 할까요?');

-- Test 5: Multi-tool - Analyst + Search 복합 질의
CALL MOVESIGNAL_AGENT('중구 관광특구 내 렌탈 장비 배치 규정을 알려주고, 최근 중구 매출 예측 결과도 보여줘.');

-- Test 6: CS policy lookup
CALL MOVESIGNAL_AGENT('고객이 강하게 불만을 제기하면 어떤 보상을 할 수 있나요?');

-- Test 7: Product margin question
CALL MOVESIGNAL_AGENT('법인 고객 대상 렌탈 마진율 하한선이 얼마인가요?');

-- Test 8: Direct Cortex Search service call (without agent)
SELECT SNOWFLAKE.CORTEX.SEARCH(
    'MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_SEARCH_SVC',
    '렌탈 마진율',
    3
);

-- ============================================================
-- 8. Grants (run as ACCOUNTADMIN if needed)
-- ============================================================
/*
USE ROLE ACCOUNTADMIN;

GRANT USAGE ON CORTEX SEARCH SERVICE MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_SEARCH_SVC
  TO ROLE SYSADMIN;

GRANT USAGE ON PROCEDURE MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_AGENT(VARCHAR)
  TO ROLE SYSADMIN;

GRANT USAGE ON PROCEDURE MOVESIGNAL_AI.ANALYTICS.RECOMMEND_ALLOCATION(VARCHAR, NUMBER)
  TO ROLE SYSADMIN;
*/
