# MoveSignal AI - Snowflake Korea Hackathon 2026 Tech Track

## 제출 체크리스트 (2026-04-12 23:59 KST)

### 필수 제출물
- [ ] 소스코드 ZIP (MoveSignal_AI_Submission.zip)
- [ ] 프로젝트/기술 접근 슬라이드 (MoveSignal_AI_Hackathon.pptx, 15슬라이드)
- [ ] 10분 이하 데모 영상 (한국어, MP4)
- [ ] 모든 제출물 및 구두 발표: 한국어

### 코드 ZIP 내용 확인
- [ ] SQL 파일 8개 (02~11)
- [ ] streamlit_app_v7.py
- [ ] README.md (실행 순서, 아키텍처, 데이터 소스)
- [ ] DEMO_SCRIPT.md
- [ ] PPT 파일
- [ ] 스폰서 원본 데이터 미포함 확인

### 데이터셋 명시 (규칙 필수)

| # | 데이터셋 | 유형 | 출처/라이선스 |
|---|---------|------|-------------|
| 1 | SPH | Snowflake Marketplace - 스폰서 제공 | Snowflake Marketplace |
| 2 | Richgo | Snowflake Marketplace - 스폰서 제공 | Snowflake Marketplace |
| 3 | AJD *(Optional — 별도 통합 필요)* | Snowflake Marketplace - 스폰서 제공 | Snowflake Marketplace |
| 4 | 한국천문연구원 특일 정보 API | 공공누리 1유형 | https://www.data.go.kr |
| 5 | 행정안전부 주민등록 인구통계 | 공공누리 1유형 | https://jumin.mois.go.kr |

### 규칙 준수 확인
- [ ] 팀당 1개 엔트리
- [ ] 스폰서 데이터: 배포/공유/게시 안 함
- [ ] 스폰서 데이터: 해커톤 종료 후 삭제 예정
- [ ] 비스폰서 데이터: 라이선스 링크 코드 내 명시
- [ ] GitHub: 스폰서 원데이터 미포함
- [ ] 모든 외부 라이브러리/오픈소스 명시

### Snowflake 환경 확인
- [ ] 02_feature_mart_v4.sql 실행 완료
- [ ] 03_ml_and_cortex_v2.sql 실행 완료
- [ ] 10_external_data.sql 실행 완료 (공휴일 + 연령구조)
- [ ] 11_ablation_study.sql 실행 완료
- [ ] 06_semantic_view.sql 실행 완료
- [ ] 09_cortex_search_agent.sql 실행 완료
- [ ] 07_dynamic_tables_tasks.sql 실행 완료
- [ ] streamlit_app_v7.py 배포 완료

### 증거 화면 확인 (앱에서)
- [ ] Allocation 탭: 배분 추천 %, Actual vs Forecast overlay
- [ ] Allocation 탭: Ablation MAPE 차트 (A→E)
- [ ] Allocation 탭: 평가 지표 (MAPE/SMAPE/MAE)
- [ ] Analysis 탭: KPI + 연령구조 + Feature Importance
- [ ] AI Agent 탭: AI_COMPLETE structured output 동작
- [ ] Simulation 탭: 예산 슬라이더 + AI 코멘트
- [ ] Ops/Trust 탭: Health panel + 보안 모델 + 데이터 거버넌스

### 데모 영상 녹화 (10분)
- [ ] 0:00-0:40 문제 정의
- [ ] 0:40-1:30 데이터 소스 설명
- [ ] 1:30-3:00 Forecast 검증 (Ablation + 평가 지표)
- [ ] 3:00-6:00 라이브 데모 (5탭)
- [ ] 6:00-7:10 Snowflake 아키텍처
- [ ] 7:10-8:10 운영 가능성
- [ ] 8:10-9:00 민간 + 공공 듀얼
- [ ] 9:00-10:00 다음 단계 + 마무리
- [ ] 한국어 전체, 자막 불필요

### GitHub 정리
- [ ] README: 문제 정의 / 아키텍처 / 데이터 소스 / 실행 순서 / 라이선스
- [ ] .gitignore: 스폰서 데이터 경로 제외
- [ ] 데모 영상 링크 README에 추가
- [ ] 스폰서 원본 CSV/파케이 파일 미포함 확인

### 결선 대비 (4/29 서울)
- [ ] 10분 발표 리허설 2회 이상
- [ ] Q&A 5문항 답변 준비 (DEMO_SCRIPT.md)
- [ ] 노트북 + 충전기 + HDMI 어댑터

## 일정

| 날짜 | 할 일 |
|------|------|
| 4/6-7 | Snowflake SQL 실행 + 앱 배포 + 디버깅 |
| 4/8-9 | 데모 영상 녹화 + 편집 |
| 4/10 | 안전 제출본 업로드 (1차) |
| 4/11 | 최종 다듬기 + 2차 제출 |
| 4/12 | 최종 확인 + 마감 전 제출 |
| 4/20 | Top 3 발표 확인 |
| 4/29 | 결선 발표 (서울) |
