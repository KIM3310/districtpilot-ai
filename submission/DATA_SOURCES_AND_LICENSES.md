# DistrictPilot AI — 데이터 출처 및 라이선스

## 스폰서 데이터 (Snowflake Marketplace)

| Dataset | Provider | Grain | Period | Purpose | License | Redistribution |
|---------|----------|-------|--------|---------|---------|----------------|
| 서울 유동인구·카드소비·자산 | SPH (Grandata) | 법정동 → 구 집계 | 2019-2024 (월) | 수요 예측 Feature | Snowflake Marketplace | 배포·공유·게시 금지, 해커톤 종료 후 삭제 |
| 부동산 시세·인구이동 | Richgo | 시군구 (SGG) | 2019-2024 (월) | 시세·이동 Feature | Snowflake Marketplace | 배포·공유·게시 금지, 해커톤 종료 후 삭제 |

> **주의**: 스폰서 사전선정 데이터는 공식 규칙에 따라 배포·공유·게시가 금지되며, 해커톤 종료 후 삭제합니다.
> 본 제출물(GitHub, ZIP)에는 스폰서 원본 데이터가 포함되어 있지 않습니다.

## 외부 공개 데이터

| Dataset | Provider | Grain | Period | Purpose | License | URL |
|---------|----------|-------|--------|---------|---------|-----|
| 공휴일/특일 캘린더 | 한국천문연구원 | 일별 | 2019-2025 | Forecast 외생변수 (IS_HOLIDAY) | 공공누리 1유형 (출처 표시) | https://www.data.go.kr |
| 연령·성별 주민등록 인구 | 행정안전부 | 시군구/월별 | 2019-2024 | 연령구조 Feature (SENIOR_RATIO 등) | 공공누리 1유형 (출처 표시) | https://jumin.mois.go.kr |
| 관광수요/외래객 지수 | 한국관광 데이터랩 | 시도/월별 | 2019-2024 | 관광 수요 Feature | 공공누리 1유형 (출처 표시) | https://datalab.visitkorea.or.kr |
| 상권변화지표 | 서울시 상권분석서비스 | 구/분기별 | 2019-2024 | 상권 안정성 Feature | 공공누리 1유형 (출처 표시) | https://golmok.seoul.go.kr |

## AJD 통신/렌탈 (통합 완료)

| Dataset | Provider | Grain | Period | Purpose | License |
|---------|----------|-------|--------|---------|---------|
| 통신 가입/계약/렌탈/CS | AJD | 시군구 → 구 배분 | 2019-2024 (월) | 렌탈 전환율, CS 인입, RENTAL_SIGNAL | Snowflake Marketplace |

> **투명성 노트**: 현재 AJD 데이터는 업계 통계 기반 합성 데이터입니다 (인구비례 배분 적용).
> Production에서는 AJD Marketplace 실데이터로 교체합니다.

## 라이선스 준수 확인

- [x] 스폰서 원본 데이터: GitHub/ZIP에 미포함
- [x] 스폰서 데이터: 해커톤 종료 후 삭제 예정
- [x] 외부 공개 데이터: 모두 공공누리 1유형 (상업적 이용 가능, 출처 표시 의무)
- [x] 외부 데이터 출처: SQL 코드 내 주석 및 본 문서에 명시
- [x] 외부 라이브러리: Streamlit, Pandas (Snowflake 내장)
- [x] 합성 데이터: 외부 공개 + AJD 데이터는 합성임을 README/앱에서 명시
- [x] Production 교체 경로: API/Marketplace 실데이터 파이프라인 설계 완료
