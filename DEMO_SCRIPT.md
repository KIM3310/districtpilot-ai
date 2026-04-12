# DistrictPilot AI — 10분 데모 스크립트 (최종)

> Snowflake Korea Hackathon 2026 Tech Track
> 제출 마감: 2026-04-12 23:59 KST | 결선: 2026-04-29 서울

---

## 발표 흐름 (10분)

### [0:00-0:40] 문제 정의 (슬라이드 1-2)

**화면**: 타이틀 슬라이드

> "한국에서 연간 700만 건의 이사가 일어납니다.
> 이사 직후 72시간이 홈서비스 수요의 골든타임입니다.
> 정수기, 공기청정기, 인터넷 — 이사 당일 결정합니다.
>
> 문제는 '어디에, 언제, 얼마 강도로' 설치 인력을 배치할 것인가.
> DistrictPilot AI는 이 질문을 Snowflake 안의 데이터와 AI로 답합니다."

---

### [0:40-1:30] 왜 이 데이터 조합인가 (슬라이드 3)

**화면**: 데이터 소스 슬라이드

> 스폰서 Marketplace 데이터: SPH 유동인구/카드소비, Richgo 부동산/인구이동
> 이 두 데이터를 함께 보면 "사람이 움직이는 곳"과 "실제 소비가 일어나는 곳"이 겹치는 지점을 찾을 수 있습니다.
>
> 여기에 외부 공개 데이터 4개를 추가했습니다:
> - 공휴일 캘린더: 미래에 이미 아는 값 → Forecast 외생변수
> - 연령구조: 어떤 가구/상품 조합이 맞는지
> - 관광수요: 중구/명동권 단기 체류 수요 설명 강화
> - 상권변화지표: 설치·운영 리스크 판단
>
> 핵심은 이 도메인이 SPH와 Richgo, 그리고 optional AJD가 가장 자연스럽게 만나는 문제라는 점입니다.

---

### [1:30-3:00] Forecast 검증 — 숫자로 증명 (슬라이드 4-5) ⭐ 배점 30점

**화면**: Ablation 결과 슬라이드 → Streamlit Capture Plan 탭

> Snowflake ML FORECAST로 5개 모델을 ablation 비교했습니다.
>
> Model A: 스폰서 데이터만 (Y-only baseline)
> Model B: + 공휴일 → MAPE 개선
> Model C: + 연령구조
> Model D: + 관광수요
> Model E: 전체 (production)
>
> [Streamlit Capture Plan 탭 → Ablation 차트 + MAPE 개선 delta metric 강조]
>
> **핵심 멘트**: "각 구별 MAPE 개선율을 delta metric으로 보여드립니다.
> 외부 데이터 하나를 추가할 때마다 예측이 얼마나 정확해지는지 정량화했습니다."
>
> [95% 신뢰구간 표시]
> "예측값뿐 아니라 95% 신뢰구간도 표시해서, 의사결정자가 불확실성을 감안할 수 있습니다."
>
> EXPLAIN_FEATURE_IMPORTANCE()로 왜 이 구가 높은지,
> SHOW_EVALUATION_METRICS()로 MAPE/SMAPE/MAE를 구별로 보여줍니다.
> 이건 "예측을 진짜 검증했다"의 증거입니다.
>
> **결과 품질 포인트**: ablation + evaluation metrics + feature importance + 신뢰구간
> = "데이터 과학적 엄밀성"

---

### [3:00-6:00] 라이브 데모 (Streamlit 5탭)

**[3:00-3:40] Capture Plan 탭**

> 다음 달 집행 강도 추천: 서초구 X%, 영등포구 Y%, 중구 Z%
> Actual vs Forecast 오버레이 차트
> Ablation MAPE 개선 차트

**[3:40-4:20] Move-in Signals 탭**

> [서초구 선택]
> 핵심 인사이트 콜아웃 → 전입 세대, 순이동, 카드소비, 평균자산
> 연령구조: 20-39세 비중, 시니어 비중
> **AJD 렌탈 신호**: 렌탈 건수, 렌탈 전환율, CS 인입, Rental Signal
> Feature importance 차트

**[4:20-5:00] AI Playbook 탭** ⭐ 기술 구현 + 결과 품질

> [질문: "다음 달 어느 구의 전입 수요를 먼저 잡아야 해?"]
>
> AI_COMPLETE structured output:
> - 추천 지역 + 집행 강도 + 근거 + 리스크 + 실행 액션
> - Feature Mart JSON 컨텍스트 주입 → grounded recommendation
>
> **핵심 멘트**: "Cortex Search 근거 문서 섹션을 펼쳐보겠습니다.
> AI가 답변할 때 어떤 정책/룰북 문서를 참조했는지 투명하게 보여줍니다.
> 이것이 hallucination을 방지하는 grounding입니다."

**[5:00-5:40] Scenario Lab 탭**

> 예산 5,000만원, 슬라이더로 집행 비중 조정
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
>
> **투명성 포인트**: "외부 공개 데이터와 AJD 렌탈 데이터는 공개 통계 기반 합성 데이터입니다.
> Production에서는 API/Marketplace 실데이터로 자동 교체됩니다.
> 스폰서 데이터(SPH, Richgo)는 Marketplace 실데이터입니다."

---

### [8:10-9:00] 비즈니스 임팩트 (슬라이드 9-10)

> 이 프로젝트가 독특한 이유는 **이사 직후 72시간이라는 구체적 골든타임**을 잡았다는 점입니다.
>
> 정수기, 공기청정기, 인터넷 — 이사 당일 결정됩니다.
> 연 700만 이사에서 이 골든타임을 예측하면 수조 원 시장이 열립니다.
>
> 그리고 같은 예측 엔진은 자치구 전입 행정으로도 확장 가능합니다.
> 비용 추가 없이 공공 시장까지 커버할 수 있는 플랫폼 구조입니다.

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

### Q5: "외부 데이터가 합성인데, 신뢰할 수 있나?"
> 스폰서 데이터(SPH, Richgo)는 Marketplace 실데이터이고, 이것이 Forecast의 핵심 입력입니다.
> 외부 데이터는 공개 통계 기반으로 현실적인 패턴을 재현했고,
> ablation study로 각 데이터셋의 기여도를 정량 검증했습니다.
> Production에서는 API 자동 적재로 교체하는 파이프라인이 이미 설계되어 있습니다.

### Q6: "AJD 렌탈 데이터는 왜 합성인가?"
> AJD Marketplace 데이터는 시/군 수준이라 구별 매핑에 가정이 필요합니다.
> 인구비례 배분을 적용했고, Production에서는 실데이터로 검증 후 교체합니다.
> 핵심은 렌탈 건수와 전입 세대의 상관관계 구조를 보여주는 것입니다.

### Q7: "왜 72시간 골든타임에 집중하나?"
> 이사 직후 72시간 안에 정수기, 인터넷, 가전렌탈의 80%가 결정됩니다.
> 이 타이밍을 예측하는 것이 홈서비스 사업자에게 가장 큰 가치입니다.
> 예측을 못 하면 경쟁사에 골든타임을 빼앗깁니다.

### Q8: "자치구가 실제로 쓸 수 있나?"
> 전입신고 데이터는 행정안전부에서 이미 관리합니다.
> DistrictPilot는 전입 "예측"을 추가하는 것이 핵심입니다.
> "이미 이사 온 사람"이 아니라 "다음 달 이사 올 사람"을 미리 알려줍니다.

### Q9: "Snowflake 고객에게 제안한다면?"
> 민간 사업자와 공공기관이 같은 Snowflake 계정에서
> 전입 예측 → 민간 집행 + 행정 배치를 동시에 할 수 있다는 것이 핵심입니다.
> 이건 Snowflake의 데이터 공유(Data Sharing) 기능으로 자연스럽게 확장됩니다.
