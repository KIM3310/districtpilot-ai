# MoveSignal AI — 10분 데모 스크립트 (최종)

> Snowflake Korea Hackathon 2026 Tech Track
> 제출 마감: 2026-04-12 23:59 KST | 결선: 2026-04-29 서울

---

## 발표 흐름 (10분)

### [0:00-0:40] 문제 정의 (슬라이드 1-2)

**화면**: 타이틀 슬라이드

> "다음 달 서초·영등포·중구, 어디에 어떤 렌탈 상품과 마케팅 예산을 얼마나 넣어야 하는가?"
>
> 기존 방식은 경험과 감에 의존합니다.
> MoveSignal AI는 이 질문을 데이터와 AI로 답합니다.

---

### [0:40-1:30] 왜 이 데이터 조합인가 (슬라이드 3)

**화면**: 데이터 소스 슬라이드

> 스폰서 데이터 3개: SPH 유동인구/카드소비, Richgo 부동산/인구이동, AJD 통신/렌탈
>
> 여기에 외부 공개 데이터 4개를 추가했습니다:
> - 공휴일 캘린더: 미래에 이미 아는 값 → Forecast 외생변수
> - 연령구조: 어떤 상품이 먹히는지
> - 관광수요: 중구/명동 설명력 강화
> - 상권변화지표: 투자 리스크 판단
>
> 핵심은 중복을 피하면서 의사결정 해석력을 높이는 조합입니다.

---

### [1:30-3:00] Forecast 검증 — 숫자로 증명 (슬라이드 4-5)

**화면**: Ablation 결과 슬라이드 → Streamlit Allocation 탭

> Snowflake ML FORECAST로 5개 모델을 ablation 비교했습니다.
>
> Model A: 스폰서 데이터만 (Y-only baseline)
> Model B: + 공휴일 → MAPE 개선
> Model C: + 연령구조
> Model D: + 관광수요
> Model E: 전체 (production)
>
> MAPE가 단계적으로 떨어지는 걸 보여드립니다.
> [Streamlit Allocation 탭 → Ablation 차트 클릭]
>
> EXPLAIN_FEATURE_IMPORTANCE()로 왜 이 구가 높은지,
> SHOW_EVALUATION_METRICS()로 MAPE/SMAPE/MAE를 구별로 보여줍니다.
> 이건 "예측을 진짜 검증했다"의 증거입니다.

---

### [3:00-6:00] 라이브 데모 (Streamlit 5탭)

**[3:00-3:40] Allocation 탭**

> 다음 달 배분 추천: 서초구 X%, 영등포구 Y%, 중구 Z%
> Actual vs Forecast 오버레이 차트
> Ablation MAPE 개선 차트

**[3:40-4:20] Analysis 탭**

> [서초구 선택]
> 유동인구, 카드소비, 순이동, 평균자산
> 연령구조: 20-39세 비중, 시니어 비중
> 관광수요 지수, 상권 안정도
> Feature importance 차트

**[4:20-5:00] AI Agent 탭**

> [질문: "다음 달 어디에 렌탈 예산을 더 배분해야 해?"]
>
> AI_COMPLETE structured output:
> - 추천 지역 + 배분 비율 + 근거 + 리스크 + 실행 액션
> - Feature Mart JSON 컨텍스트 주입 → grounded recommendation

**[5:00-5:40] Simulation 탭**

> 예산 5,000만원, 슬라이더로 배분 조정
> AI 추천 vs 사용자 시뮬레이션 비교
> AI 코멘트 요청

**[5:40-6:00] Ops/Trust 탭**

> DT 상태, Task 이력, target lag, model version
> 보안 모델, 데이터 거버넌스, 라이선스

---

### [6:00-7:10] 왜 Snowflake인가 (슬라이드 6-7)

> 1. Semantic View + Cortex Analyst → SQL 생성
> 2. Cortex Search → 정책 문서 검색
> 3. AI_COMPLETE → Structured output 액션 카드
> 4. ML FORECAST → 외생변수 + evaluation + feature importance
> 5. Dynamic Tables + Tasks → 자동 갱신/재학습
> 6. Streamlit in Snowflake → 데이터 외부 미유출
>
> 이 6개가 하나의 플랫폼에서 동작합니다.

---

### [7:10-8:10] 운영 가능성 (슬라이드 8)

> 월 ~$80. Task 자동 재시도. V_APP_HEALTH 모니터링.
> 스폰서 데이터 공유/배포 안 함. 외부 데이터 공공누리 1유형.

---

### [8:10-9:00] One Engine, Two Impacts (슬라이드 9-10)

> 민간: 렌탈/마케팅 예산 배분
> 공공: 상권 활성화, 생활SOC, 현장 점검 우선순위
> 같은 Feature Mart, 같은 엔진의 재사용.

---

### [9:00-10:00] 다음 단계 (슬라이드 11-13)

> 1. AJD 완전 통합 → 상품/채널 수준 추천
> 2. Cortex Agent 오케스트레이션
> 3. Managed MCP Server
> 4. Native App → Marketplace 배포

---

## Q&A 대비 (5문항)

### Q1: "왜 서초·영등포·중구?"
> SPH 스폰서 데이터가 이 3구를 제공합니다. 엔진 구조는 25개 구 확장 가능합니다.

### Q2: "중구 67% 치우침?"
> 중구는 거주인구 13만이지만 유동인구와 관광 소비가 높아 카드매출 예측값이 높습니다.
> Model D에서 관광수요 추가 시 중구 MAPE가 유의미하게 개선됩니다.

### Q3: "Hallucination 방지?"
> 1. Feature Mart JSON 컨텍스트 주입 + "CONTEXT 밖 사실 금지" 명시
> 2. AI_COMPLETE structured output으로 필드 강제
> 3. Cortex Search 정책 문서 grounding

### Q4: "Dynamic Tables SUSPEND 상태?"
> 해커톤 환경 크레딧 절약. ALTER TASK RESUME 한 줄로 전환.

### Q5: "Snowflake 고객에게 제안한다면?"
> 민간: Marketplace + ML + AI 배분 → ROI 측정
> 공공: 같은 구조로 행정 배분
> "하나의 Snowflake 계정에서 양쪽 다 가능" = Data Cloud 가치 제안
