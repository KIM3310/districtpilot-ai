# Evidence Chain — DistrictPilot AI

## 슬라이드 구성 (1장, 좌→우 플로우)

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Forecast │───>│  Feature     │───>│ Semantic View│───>│  Cortex      │───>│ AI_COMPLETE  │───>│  DT Refresh  │
│  Metric  │    │ Importance   │    │  + Analyst   │    │   Search     │    │ Structured   │    │   State      │
│          │    │              │    │              │    │  Grounding   │    │   Output     │    │              │
│ ML       │    │ SHAP-style   │    │ VQR 10개 +   │    │ cortex.      │    │ JSON action  │    │ INFORMATION_ │
│ FORECAST │    │ 기여도 순위   │    │ 13개 SQL규칙 │    │ search()     │    │ cards +      │    │ SCHEMA +     │
│ 3개월    │    │ + ablation   │    │ + synonyms   │    │ 외부 근거    │    │ confidence   │    │ REFRESH      │
│ MAPE<10% │    │ 개선 증명    │    │ 7개 카테고리 │    │ 첨부         │    │ score        │    │ _HISTORY     │
└──────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
     ①                ②                   ③                  ④                   ⑤                   ⑥
```

## 각 단계 핵심 증거

| # | 단계 | Snowflake Object | 증거 |
|---|------|-------------------|------|
| ① | Forecast Metric | `DISTRICTPILOT_FORECAST` (ML FORECAST) | 3구 × 3개월 수요 예측, MAPE < 10%, 95% CI |
| ② | Feature Importance | `FEATURE_IMPORTANCE` + `ABLATION_RESULTS` | 전입·이사/소비/관광 신호 Top-5 기여도, ablation 전후 MAPE 개선 |
| ③ | Semantic View / Analyst | `DISTRICTPILOT_SV` (Semantic View) | VQR 10개, SQL규칙 13개, synonym 100+, 카테고리 7개 |
| ④ | Search Grounding | `CORTEX.SEARCH()` in AI_COMPLETE | 설치/운영 룰북 근거로 hallucination 방지 |
| ⑤ | AI_COMPLETE Structured Output | `SNOWFLAKE.CORTEX.COMPLETE()` | JSON action card (recommended_district / allocation_pct / next_action), 시뮬레이션 코멘트 |
| ⑥ | AJD Rental Signal | `STG_TELECOM` + `FEATURE_MART_V3` | 렌탈 건수, 전환율(전입 대비), CS 인입, RENTAL_SIGNAL 복합 지표 |
| ⑦ | Refresh State | `INFORMATION_SCHEMA.DYNAMIC_TABLES` + `DYNAMIC_TABLE_REFRESH_HISTORY` | 실시간 LAG_SEC, 갱신 성공/실패 이력, query_tag 감사 |

## 30초 멘트 (한국어)

> "DistrictPilot의 의사결정은 일곱 단계 증거 체인으로 구성됩니다.
> 첫째, ML Forecast가 3개 구의 다음 달 전입·이사 기반 수요를 예측하고,
> 둘째, Feature Importance가 어떤 신호가 예측을 움직였는지 보여줍니다.
> 셋째, Semantic View와 Cortex Analyst가 자연어 질문을 정확한 SQL로 변환하고,
> 넷째, Search 기반 grounding이 설치와 운영 룰북 근거를 첨부합니다.
> 다섯째, AI_COMPLETE가 구조화된 액션 카드를 JSON으로 생성하고,
> 여섯째, AJD 렌탈 시그널이 전입 세대의 실제 홈서비스 전환율을 검증하며,
> 마지막으로 Dynamic Table 갱신 이력이 데이터가 최신임을 증명합니다.
> 모든 단계가 Snowflake-native이고, 앱 밖으로 데이터가 나가지 않습니다."

## Q&A 방어 포인트

- **"왜 이 구가 1순위인가요?"** → ② Feature Importance 탭 + ③ VQR `vq_why_higher`
- **"왜 전입 수요라고 볼 수 있나요?"** → ② MOVE_IN / NET_MOVE + 카드소비 + 관광수요 신호를 같이 제시
- **"예측을 얼마나 믿을 수 있나요?"** → ① MAPE + ⑤ confidence score + ② ablation 개선
- **"데이터가 최신인가요?"** → ⑥ DT Refresh History의 LATEST_DATA_TIMESTAMP + LAG_SEC
- **"Hallucination은 없나요?"** → ④ Search grounding + ③ VQR 고정 SQL
- **"보안은?"** → Owner's Rights, query_tag, Marketplace 데이터 내부 복제
