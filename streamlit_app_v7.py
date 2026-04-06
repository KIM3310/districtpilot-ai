"""
MoveSignal AI v7 — Competition-Winning Snowflake-native Decision Agent
서초/영등포/중구 렌탈/마케팅 배분 의사결정 엔진

Architecture:
  FEATURE_MART_V2 (holiday + demographics + tourism + commercial)
  Snowflake ML FORECAST -> evaluation metrics + feature importance
  ABLATION_RESULTS -> model improvement evidence
  AI_COMPLETE structured output -> action cards with fallback
  Dynamic Tables + Tasks -> operational freshness
  V_APP_HEALTH -> real-time ops monitoring

Tabs: Allocation | Analysis | AI Agent | Simulation | Ops/Trust
"""
import json
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# ---------------------------------------------------------------------------
# Config constants
# ---------------------------------------------------------------------------
# V2 includes external data; falls back to FINAL if V2 doesn't exist yet
FEATURE_MART_FQN = "MOVESIGNAL_AI.ANALYTICS.FEATURE_MART_V2"
FEATURE_MART_FALLBACK = "MOVESIGNAL_AI.ANALYTICS.FEATURE_MART_FINAL"
FORECAST_RESULTS_FQN = "MOVESIGNAL_AI.ANALYTICS.FORECAST_RESULTS"
AVF_FQN = "MOVESIGNAL_AI.ANALYTICS.ACTUAL_VS_FORECAST"
FORECAST_MODEL_FQN = "MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_FORECAST"
SEMANTIC_VIEW_FQN = "MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_SV"
HEALTH_VIEW_FQN = "MOVESIGNAL_AI.ANALYTICS.V_APP_HEALTH"
ABLATION_FQN = "MOVESIGNAL_AI.ANALYTICS.ABLATION_RESULTS"
LLM_MODEL = "mistral-large2"

DISTRICTS_KR = {"서초구": "서초구", "영등포구": "영등포구", "중구": "중구"}

session = get_active_session()

st.set_page_config(page_title="MoveSignal AI v7", layout="wide")
st.title("MoveSignal AI")
st.caption(
    "서초/영등포/중구 렌탈/마케팅 배분 의사결정 엔진  |  "
    "100% Snowflake Native  |  v7"
)

# ---------------------------------------------------------------------------
# Utility / helper functions
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)
def load_df(_session, sql: str) -> pd.DataFrame:
    """Cached SQL execution returning a pandas DataFrame."""
    return _session.sql(sql).to_pandas()


def safe_read(sql: str) -> pd.DataFrame:
    """Execute SQL with graceful error handling. Returns empty DataFrame on failure."""
    try:
        return load_df(session, sql)
    except Exception:  # noqa: BLE001 – intentional catch-all for resilient UI
        return pd.DataFrame()


def clean_variant(value) -> str:
    """Normalize VARIANT column values that may contain quotes or NULL strings."""
    if pd.isna(value):
        return "ALL"
    text = str(value).strip().strip('"')
    return "ALL" if text.upper() in {"NULL", "NONE", ""} else text


def fmt_int(value) -> str:
    """Format numeric value as integer with comma separators."""
    return "-" if pd.isna(value) else f"{float(value):,.0f}"


def fmt_eok(value) -> str:
    """Format value in 억원 (100M KRW) unit."""
    return "-" if pd.isna(value) else f"{float(value) / 1e8:,.1f}"


def fmt_pct(value) -> str:
    """Format as signed percentage string."""
    return "-" if pd.isna(value) else f"{float(value):+.1f}%"


def safe_float(value, default=0.0) -> float:
    """Safely convert value to float."""
    try:
        result = float(value)
        return result if not pd.isna(result) else default
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

# Try FEATURE_MART_V2 first; if it doesn't exist, fall back to FEATURE_MART_FINAL
feature_mart = safe_read(f"""
    SELECT * FROM {FEATURE_MART_FQN} ORDER BY YM, DISTRICT
""")
if feature_mart.empty:
    feature_mart = safe_read(f"""
        SELECT * FROM {FEATURE_MART_FALLBACK} ORDER BY YM, DISTRICT
    """)

forecast_raw = safe_read(f"""
    SELECT SERIES AS DISTRICT, TS, FORECAST, LOWER_BOUND, UPPER_BOUND
    FROM {FORECAST_RESULTS_FQN}
    ORDER BY TS, DISTRICT
""")

avf_raw = safe_read(f"""
    SELECT DISTRICT, DS, ACTUAL, FORECAST_VAL
    FROM {AVF_FQN}
    ORDER BY DS, DISTRICT
""")

eval_raw = safe_read(f"""
    SELECT * FROM TABLE({FORECAST_MODEL_FQN}!SHOW_EVALUATION_METRICS())
""")

fi_raw = safe_read(f"""
    SELECT * FROM TABLE({FORECAST_MODEL_FQN}!EXPLAIN_FEATURE_IMPORTANCE())
""")

health_raw = safe_read(f"SELECT * FROM {HEALTH_VIEW_FQN}")

ablation_raw = safe_read(f"SELECT * FROM {ABLATION_FQN}")

if feature_mart.empty and forecast_raw.empty:
    st.error("데이터 로딩 실패. 테이블명/권한을 확인하세요.")
    st.stop()

# ---------------------------------------------------------------------------
# Derived dataframes
# ---------------------------------------------------------------------------

def latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """Return the most recent row per district."""
    if df.empty:
        return df
    out = df.copy()
    out["_ts"] = pd.to_datetime(
        out["YM"].astype(str).str.strip(), format="%Y%m", errors="coerce"
    )
    return (
        out.sort_values("_ts")
        .groupby("DISTRICT", as_index=False)
        .tail(1)
        .sort_values("DISTRICT")
    )


latest = latest_snapshot(feature_mart)
districts = (
    sorted(latest["DISTRICT"].dropna().unique().tolist())
    if not latest.empty
    else []
)


def build_allocation(df: pd.DataFrame):
    """Compute allocation percentages from first forecast timestamp."""
    if df.empty:
        return pd.DataFrame(), None
    out = df.copy()
    out["_ts"] = pd.to_datetime(out["TS"], errors="coerce")
    first_ts = out["_ts"].dropna().min()
    if pd.isna(first_ts):
        return pd.DataFrame(), None
    sub = out[out["_ts"] == first_ts].copy()
    total = sub["FORECAST"].sum()
    sub["ALLOC_PCT"] = (sub["FORECAST"] / total * 100).round(1) if total > 0 else 0.0
    return sub.sort_values("ALLOC_PCT", ascending=False), first_ts


alloc_df, next_ts = build_allocation(forecast_raw)


def build_overlay(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot actual vs forecast for overlay chart."""
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["_ds"] = pd.to_datetime(out["DS"], errors="coerce")
    actual = (
        out.pivot_table(index="_ds", columns="DISTRICT", values="ACTUAL", aggfunc="max")
        .add_suffix(" Actual")
    )
    fcast = (
        out.pivot_table(index="_ds", columns="DISTRICT", values="FORECAST_VAL", aggfunc="max")
        .add_suffix(" Forecast")
    )
    return actual.join(fcast, how="outer").sort_index()


overlay = build_overlay(avf_raw)


def build_eval_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot evaluation metrics to one row per series."""
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "SERIES" in out.columns:
        out["SERIES"] = out["SERIES"].apply(clean_variant)
    if "ERROR_METRIC" in out.columns:
        out["ERROR_METRIC"] = (
            out["ERROR_METRIC"].astype(str).str.replace('"', "", regex=False)
        )
    return (
        out.pivot_table(
            index="SERIES",
            columns="ERROR_METRIC",
            values="METRIC_VALUE",
            aggfunc="first",
        )
        .reset_index()
    )


eval_pivot = build_eval_pivot(eval_raw)


def normalize_fi(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize feature importance column names."""
    if df.empty:
        return df
    out = df.copy()
    if "SCORE" in out.columns and "IMPORTANCE_SCORE" not in out.columns:
        out = out.rename(columns={"SCORE": "IMPORTANCE_SCORE"})
    if "SERIES" in out.columns:
        out["SERIES"] = out["SERIES"].apply(clean_variant)
    return out


fi_df = normalize_fi(fi_raw)


# ---------------------------------------------------------------------------
# AI functions
# ---------------------------------------------------------------------------

def build_context_json(
    scope: str,
    alloc: pd.DataFrame,
    lat: pd.DataFrame,
    fi: pd.DataFrame,
) -> str:
    """Build grounded context JSON for LLM calls with extended data."""
    # Allocation payload
    alloc_cols = ["DISTRICT", "FORECAST", "ALLOC_PCT"]
    alloc_payload = (
        alloc[[c for c in alloc_cols if c in alloc.columns]].to_dict("records")
        if not alloc.empty
        else []
    )

    # Snapshot payload — extended columns
    snap_cols = [
        "DISTRICT", "TOTAL_POP", "TOTAL_SALES", "NET_MOVE",
        "AVG_ASSET", "AVG_MEME_PRICE",
        "AGE_20_39_SHARE", "SENIOR_60P_SHARE",
        "TOURISM_DEMAND_IDX", "FOREIGN_VISITOR_IDX",
        "STABILITY_SCORE", "NET_STORE_CHANGE",
        "HOLIDAY_COUNT",
    ]
    if scope != "전체" and not lat.empty:
        snap = lat[lat["DISTRICT"] == scope]
    else:
        snap = lat
    snap_payload = (
        snap[[c for c in snap_cols if c in snap.columns]].to_dict("records")
        if not snap.empty
        else []
    )

    # Feature importance payload
    fi_payload = []
    if not fi.empty and "IMPORTANCE_SCORE" in fi.columns:
        fi_sub = fi[fi["SERIES"] == scope] if scope != "전체" else fi
        if fi_sub.empty:
            fi_sub = fi[fi["SERIES"] == "ALL"]
        fi_cols = [
            c
            for c in ["SERIES", "FEATURE_NAME", "IMPORTANCE_SCORE"]
            if c in fi_sub.columns
        ]
        if "RANK" in fi_sub.columns:
            fi_payload = (
                fi_sub.sort_values("RANK").head(5)[fi_cols].to_dict("records")
            )
        else:
            fi_payload = fi_sub.head(5)[fi_cols].to_dict("records")

    # Holiday info
    holiday_info = {}
    if not lat.empty and "HOLIDAY_COUNT" in lat.columns:
        if scope != "전체":
            hrow = lat[lat["DISTRICT"] == scope]
        else:
            hrow = lat
        if not hrow.empty:
            holiday_info = {
                "holiday_count": safe_float(hrow.iloc[0].get("HOLIDAY_COUNT", 0)),
                "is_peak": bool(hrow.iloc[0].get("IS_PEAK_HOLIDAY", False))
                if "IS_PEAK_HOLIDAY" in hrow.columns
                else False,
            }

    # Demographics
    demo_info = {}
    if not lat.empty and "AGE_20_39_SHARE" in lat.columns:
        if scope != "전체":
            drow = lat[lat["DISTRICT"] == scope]
        else:
            drow = lat
        if not drow.empty:
            demo_info = {
                "young_adult_share": safe_float(
                    drow.iloc[0].get("AGE_20_39_SHARE", 0)
                ),
                "senior_share": safe_float(
                    drow.iloc[0].get("SENIOR_60P_SHARE", 0)
                ),
            }

    # Tourism
    tourism_info = {}
    if not lat.empty and "TOURISM_DEMAND_IDX" in lat.columns:
        if scope != "전체":
            trow = lat[lat["DISTRICT"] == scope]
        else:
            trow = lat
        if not trow.empty:
            tourism_info = {
                "tourism_demand": safe_float(
                    trow.iloc[0].get("TOURISM_DEMAND_IDX", 0)
                ),
                "foreign_visitor": safe_float(
                    trow.iloc[0].get("FOREIGN_VISITOR_IDX", 0)
                ),
            }

    # Commercial stability
    commercial_info = {}
    if not lat.empty and "STABILITY_SCORE" in lat.columns:
        if scope != "전체":
            crow = lat[lat["DISTRICT"] == scope]
        else:
            crow = lat
        if not crow.empty:
            commercial_info = {
                "stability_score": safe_float(
                    crow.iloc[0].get("STABILITY_SCORE", 0)
                ),
                "net_store_change": safe_float(
                    crow.iloc[0].get("NET_STORE_CHANGE", 0)
                ),
            }

    return json.dumps(
        {
            "scope": scope,
            "next_month_allocation": alloc_payload,
            "latest_snapshot": snap_payload,
            "top_features": fi_payload,
            "holiday": holiday_info,
            "demographics": demo_info,
            "tourism": tourism_info,
            "commercial_stability": commercial_info,
        },
        ensure_ascii=False,
        indent=2,
    )


def call_ai_complete(question: str, context_json: str) -> dict:
    """AI_COMPLETE with structured output. Falls back to CORTEX.COMPLETE."""
    prompt = f"""당신은 MoveSignal AI의 한국어 의사결정 보조 모델이다.

반드시 지킬 규칙:
1) 아래 CONTEXT 밖의 사실은 만들지 말 것.
2) 숫자는 CONTEXT에 있는 경우에만 사용할 것.
3) 확실하지 않으면 '현재 데이터만으로는 확정할 수 없습니다.'라고 말 것.
4) 답변은 짧고 실행 중심으로 작성할 것.
5) 한국어로 작성할 것.

CONTEXT:
{context_json}

사용자 질문: {question}""".strip()

    try:
        rows = session.sql(
            """
            SELECT AI_COMPLETE(
                model => ?,
                prompt => ?,
                model_parameters => {'temperature': 0, 'max_tokens': 600, 'guardrails': TRUE},
                response_format => TYPE OBJECT(
                    answer STRING,
                    recommended_district STRING,
                    allocation_pct FLOAT,
                    drivers ARRAY(STRING),
                    risk STRING,
                    next_action STRING
                ),
                show_details => TRUE
            ) AS RESPONSE
            """,
            params=[LLM_MODEL, prompt],
        ).collect()
        if not rows:
            return _fallback_complete(prompt)
        raw = rows[0]["RESPONSE"]
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return {"structured_output": {"answer": raw}}
        return raw
    except Exception:
        return _fallback_complete(prompt)


def _fallback_complete(prompt: str) -> dict:
    """Fallback to SNOWFLAKE.CORTEX.COMPLETE when AI_COMPLETE is unavailable."""
    try:
        safe_prompt = prompt.replace("'", "''").replace("\\", "\\\\")
        rows = session.sql(
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{LLM_MODEL}', '{safe_prompt}') AS R"
        ).collect()
        answer = rows[0]["R"] if rows else "응답을 생성하지 못했습니다."
        return {"structured_output": {"answer": answer}}
    except Exception:  # noqa: BLE001
        return {"structured_output": {"answer": "AI 호출 실패: 일시적 오류가 발생했습니다. 잠시 후 다시 시도해주세요."}}


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
if "ai_payload" not in st.session_state:
    st.session_state["ai_payload"] = None
if "ai_context" not in st.session_state:
    st.session_state["ai_context"] = ""
if "sim_ai_comment" not in st.session_state:
    st.session_state["sim_ai_comment"] = ""

# ---------------------------------------------------------------------------
# Header KPIs
# ---------------------------------------------------------------------------
header_col1, header_col2, header_col3, header_col4 = st.columns(4)
header_col1.metric("Districts", str(len(districts)))
header_col2.metric(
    "Feature months",
    str(feature_mart["YM"].nunique()) if not feature_mart.empty else "-",
)
header_col3.metric(
    "Forecast horizon",
    f"{forecast_raw['TS'].nunique()}개월" if not forecast_raw.empty else "-",
)
# Count external columns that actually exist
_ext_cols = ["HOLIDAY_COUNT", "AGE_20_39_SHARE", "TOURISM_DEMAND_IDX", "STABILITY_SCORE"]
_ext_count = sum(1 for c in _ext_cols if c in feature_mart.columns)
header_col4.metric(
    "외부 데이터",
    f"스폰서 3 + 공개 {_ext_count}",
)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tabs = st.tabs(["Allocation", "Analysis", "AI Agent", "Simulation", "Ops / Trust"])

# ===========================================================================
# Tab 1: Allocation
# ===========================================================================
with tabs[0]:
    if next_ts is not None:
        st.subheader(f"배분 추천 ({next_ts.strftime('%Y-%m')})")
    else:
        st.subheader("배분 추천")

    # --- Allocation metrics ---
    if not alloc_df.empty:
        alloc_cols = st.columns(len(alloc_df))
        for idx, row in alloc_df.reset_index(drop=True).iterrows():
            with alloc_cols[idx]:
                st.metric(row["DISTRICT"], f'{row["ALLOC_PCT"]:.1f}%')
                st.progress(row["ALLOC_PCT"] / 100)

        st.bar_chart(
            alloc_df.set_index("DISTRICT")[["ALLOC_PCT"]],
            height=260,
        )
    else:
        st.info("예측 데이터가 없습니다.")

    # --- Actual vs Forecast overlay ---
    st.subheader("Actual vs Forecast")
    if not overlay.empty:
        st.line_chart(overlay, height=360)
    else:
        st.info("비교 데이터가 없습니다.")

    # --- Ablation study ---
    st.subheader("Ablation Study: 외부 데이터 효과")
    if not ablation_raw.empty:
        st.caption(
            "모델 A(기본) -> E(전체 외부 데이터 포함) 순서로 MAPE 개선을 보여줍니다."
        )

        # Bar chart of MAPE per model per district
        if "MODEL" in ablation_raw.columns and "MAPE" in ablation_raw.columns:
            ablation_pivot = ablation_raw.pivot_table(
                index="MODEL",
                columns="DISTRICT",
                values="MAPE",
                aggfunc="first",
            )
            st.bar_chart(ablation_pivot, height=300)

            # Highlight improvement
            if "DISTRICT" in ablation_raw.columns:
                for district_name in districts:
                    district_abl = ablation_raw[
                        ablation_raw["DISTRICT"] == district_name
                    ].sort_values("MODEL")
                    if len(district_abl) >= 2:
                        first_mape = safe_float(district_abl.iloc[0]["MAPE"])
                        last_mape = safe_float(district_abl.iloc[-1]["MAPE"])
                        if first_mape > 0:
                            st.info(
                                f"{district_name}: 외부 데이터 추가로 "
                                f"MAPE {first_mape:.2f}% -> {last_mape:.2f}% 개선"
                            )

            with st.expander("Ablation 상세 데이터"):
                st.dataframe(ablation_raw, use_container_width=True, hide_index=True)
    else:
        st.info(
            "ABLATION_RESULTS 테이블이 없습니다. "
            "모델 A~E 비교 결과를 로딩하면 여기에 표시됩니다."
        )

    # --- Evaluation metrics ---
    if not eval_pivot.empty:
        st.subheader("모델 평가 지표")
        for district_name in districts:
            row = eval_pivot[eval_pivot["SERIES"] == district_name]
            if row.empty:
                row = eval_pivot[eval_pivot["SERIES"] == "ALL"]
            if not row.empty:
                record = row.iloc[0]
                metric_cols = st.columns(4)
                metric_cols[0].write(f"**{district_name}**")
                if "MAPE" in row.columns:
                    metric_cols[1].metric(
                        "MAPE", f'{safe_float(record.get("MAPE")):.3f}'
                    )
                if "SMAPE" in row.columns:
                    metric_cols[2].metric(
                        "SMAPE", f'{safe_float(record.get("SMAPE")):.3f}'
                    )
                if "MAE" in row.columns:
                    metric_cols[3].metric(
                        "MAE", f'{safe_float(record.get("MAE")):,.0f}'
                    )

# ===========================================================================
# Tab 2: Analysis
# ===========================================================================
with tabs[1]:
    if not districts:
        st.warning("Feature Mart 데이터가 없습니다.")
    else:
        analysis_district = st.selectbox(
            "District", districts, key="analysis_district"
        )
        district_latest = latest[latest["DISTRICT"] == analysis_district]
        if district_latest.empty:
            st.warning("선택한 지역의 데이터가 없습니다.")
        else:
            snap = district_latest.iloc[0]

            # --- Extended KPIs ---
            kpi_row1 = st.columns(4)
            kpi_row1[0].metric(
                "유동인구",
                fmt_int(snap.get("TOTAL_POP", 0)),
                fmt_pct(snap.get("POP_CHG_PCT", 0)),
            )
            kpi_row1[1].metric(
                "카드소비 (억원)",
                fmt_eok(snap.get("TOTAL_SALES", 0)),
                fmt_pct(snap.get("SALES_CHG_PCT", 0)),
            )
            kpi_row1[2].metric("순이동", fmt_int(snap.get("NET_MOVE", 0)))
            kpi_row1[3].metric("평균자산", fmt_int(snap.get("AVG_ASSET", 0)))

            kpi_row2 = st.columns(4)
            kpi_row2[0].metric(
                "연령구조 (20~39)",
                f'{safe_float(snap.get("AGE_20_39_SHARE", 0)):.1f}%',
            )
            kpi_row2[1].metric(
                "관광수요",
                f'{safe_float(snap.get("TOURISM_DEMAND_IDX", 0)):.1f}',
            )
            kpi_row2[2].metric(
                "상권건강도",
                f'{safe_float(snap.get("STABILITY_SCORE", 0)):.2f}',
            )
            kpi_row2[3].metric(
                "공휴일 수",
                fmt_int(snap.get("HOLIDAY_COUNT", 0)),
            )

            # --- Time series: Population & Spending ---
            st.divider()
            ts_col1, ts_col2 = st.columns(2)
            district_ts = feature_mart[
                feature_mart["DISTRICT"] == analysis_district
            ].copy()

            with ts_col1:
                st.subheader("유동인구 추이")
                pop_columns = [
                    c
                    for c in ["RES_POP", "WORK_POP", "VISIT_POP"]
                    if c in district_ts.columns
                ]
                if pop_columns:
                    st.area_chart(
                        district_ts[["YM"] + pop_columns].set_index("YM"),
                    )

            with ts_col2:
                st.subheader("소비 카테고리 추이")
                spend_columns = [
                    c
                    for c in ["FOOD", "COFFEE", "ENTERTAIN", "CLOTHING", "CULTURE"]
                    if c in district_ts.columns
                ]
                if spend_columns:
                    st.line_chart(
                        district_ts[["YM"] + spend_columns].set_index("YM"),
                    )

            # --- Demographics: Age structure ---
            st.divider()
            st.subheader("연령 구조")
            age_columns = [
                c
                for c in ["AGE_20_39_SHARE", "SENIOR_60P_SHARE"]
                if c in latest.columns
            ]
            if age_columns:
                age_chart = latest[["DISTRICT"] + age_columns].copy()
                age_chart = age_chart.set_index("DISTRICT")
                age_chart.columns = ["청년층 (20~39)", "고령층 (60+)"]
                st.bar_chart(age_chart, height=280)
            else:
                st.info("연령 구조 데이터가 없습니다.")

            # --- Tourism index ---
            st.subheader("관광 지수 추이")
            tourism_columns = [
                c
                for c in ["TOURISM_DEMAND_IDX", "FOREIGN_VISITOR_IDX"]
                if c in district_ts.columns
            ]
            if tourism_columns:
                tourism_chart = district_ts[["YM"] + tourism_columns].set_index("YM")
                tourism_chart.columns = ["관광수요지수", "외국인방문지수"]
                st.line_chart(tourism_chart, height=280)
            else:
                st.info("관광 지수 데이터가 없습니다.")

            # --- Commercial health ---
            st.subheader("상권 건강")
            commercial_columns = [
                c
                for c in ["STABILITY_SCORE", "NET_STORE_CHANGE"]
                if c in district_ts.columns
            ]
            if commercial_columns:
                comm_col1, comm_col2 = st.columns(2)
                with comm_col1:
                    if "STABILITY_SCORE" in district_ts.columns:
                        st.line_chart(
                            district_ts[["YM", "STABILITY_SCORE"]].set_index("YM"),
                            height=250,
                        )
                        st.caption("안정성 점수 (높을수록 안정)")
                with comm_col2:
                    if "NET_STORE_CHANGE" in district_ts.columns:
                        st.bar_chart(
                            district_ts[["YM", "NET_STORE_CHANGE"]].set_index("YM"),
                            height=250,
                        )
                        st.caption("순 점포 증감 (양수 = 신규 > 폐업)")
            else:
                st.info("상권 건강 데이터가 없습니다.")

            # --- Feature importance ---
            st.divider()
            st.subheader("Feature Importance")
            if not fi_df.empty and "IMPORTANCE_SCORE" in fi_df.columns:
                fi_district = fi_df[fi_df["SERIES"] == analysis_district]
                if fi_district.empty:
                    fi_district = fi_df[fi_df["SERIES"] == "ALL"]
                if not fi_district.empty:
                    if "RANK" in fi_district.columns:
                        fi_district = fi_district.sort_values("RANK")
                    fi_chart_data = fi_district.head(10)
                    if "FEATURE_NAME" in fi_chart_data.columns:
                        st.bar_chart(
                            fi_chart_data.set_index("FEATURE_NAME")[
                                ["IMPORTANCE_SCORE"]
                            ],
                            height=300,
                        )
                        with st.expander("상세"):
                            st.dataframe(
                                fi_chart_data,
                                use_container_width=True,
                                hide_index=True,
                            )
            else:
                st.info("Feature importance 데이터가 없습니다.")

            # --- 3구 비교 (extended) ---
            st.divider()
            st.subheader("3구 비교")
            compare_columns = [
                c
                for c in [
                    "DISTRICT",
                    "TOTAL_POP",
                    "TOTAL_SALES",
                    "AVG_MEME_PRICE",
                    "NET_MOVE",
                    "SALES_PER_POP",
                    "AGE_20_39_SHARE",
                    "SENIOR_60P_SHARE",
                    "TOURISM_DEMAND_IDX",
                    "FOREIGN_VISITOR_IDX",
                    "STABILITY_SCORE",
                    "NET_STORE_CHANGE",
                    "HOLIDAY_COUNT",
                ]
                if c in latest.columns
            ]
            st.dataframe(
                latest[compare_columns],
                use_container_width=True,
                hide_index=True,
            )

# ===========================================================================
# Tab 3: AI Agent
# ===========================================================================
with tabs[2]:
    st.subheader("MoveSignal AI Agent")
    st.caption(
        "Cortex Analyst(숫자) + AI_COMPLETE(액션) -- "
        "인구/소비/부동산/관광/상권/인구구조 데이터 기반 의사결정"
    )

    with st.form("ai_form", clear_on_submit=False):
        ai_scope = st.selectbox("Scope", ["전체"] + districts, key="ai_scope")
        question = st.text_area(
            "질문 입력 (한국어 OK)",
            placeholder="예: 다음 달 어디에 렌탈 예산을 더 배분해야 해?",
        )
        submitted = st.form_submit_button("Ask MoveSignal AI")

    # Quick questions (updated for extended data)
    st.caption("추천 질문:")
    quick_cols = st.columns(3)
    quick_questions = [
        "관광수요와 상권건강도를 고려한 예산 배분은?",
        "고령층 비율이 높은 구에 어떤 전략이 필요할까?",
        "외부 데이터 기반 3구 종합 평가해줘",
    ]
    for idx, qq_text in enumerate(quick_questions):
        if quick_cols[idx].button(qq_text, key=f"qq_{idx}"):
            question = qq_text
            submitted = True

    if submitted and question and question.strip():
        ctx = build_context_json(ai_scope, alloc_df, latest, fi_df)
        st.session_state["ai_context"] = ctx
        with st.spinner("Generating grounded recommendation..."):
            st.session_state["ai_payload"] = call_ai_complete(
                question.strip(), ctx
            )

    payload = st.session_state.get("ai_payload")
    if payload:
        structured = payload.get("structured_output", payload)
        usage = payload.get("usage", {})

        st.subheader("AI Recommendation")
        st.write(structured.get("answer", "응답이 없습니다."))

        rec_col1, rec_col2 = st.columns(2)
        rec_col1.metric(
            "추천 지역", structured.get("recommended_district", "-")
        )
        alloc_pct_val = structured.get("allocation_pct")
        rec_col2.metric(
            "추천 배분",
            f"{float(alloc_pct_val):.1f}%"
            if alloc_pct_val is not None
            else "-",
        )

        drivers = structured.get("drivers", [])
        if drivers:
            st.markdown("**근거 (Drivers)**")
            for driver_text in drivers:
                st.write(f"- {driver_text}")

        if structured.get("risk"):
            st.warning(f"**리스크**: {structured['risk']}")
        if structured.get("next_action"):
            st.success(f"**실행 액션**: {structured['next_action']}")

        with st.expander("AI 상세 (컨텍스트 + 토큰)"):
            if usage:
                st.write("Token usage:", usage)
            st.code(st.session_state["ai_context"], language="json")

# ===========================================================================
# Tab 4: Simulation
# ===========================================================================
with tabs[3]:
    st.subheader("예산 배분 시뮬레이션")
    st.caption("AI 추천과 사용자의 예산 배분을 비교합니다.")

    # Budget input
    total_budget = st.slider(
        "총 예산 (만원)",
        min_value=1000,
        max_value=100000,
        value=30000,
        step=1000,
        key="sim_budget",
    )

    st.divider()

    # District allocation sliders
    sim_col_input, sim_col_result = st.columns([1, 1])

    with sim_col_input:
        st.markdown("**사용자 배분 설정**")

        seocho_pct = st.slider(
            "서초구 (%)",
            min_value=0,
            max_value=100,
            value=40,
            step=1,
            key="sim_seocho",
        )
        yeongdeungpo_pct = st.slider(
            "영등포구 (%)",
            min_value=0,
            max_value=100 - seocho_pct,
            value=min(35, 100 - seocho_pct),
            step=1,
            key="sim_yeongdeungpo",
        )
        junggu_pct = 100 - seocho_pct - yeongdeungpo_pct

        st.metric("중구 (%)", f"{junggu_pct}%")

        # Validation
        total_pct = seocho_pct + yeongdeungpo_pct + junggu_pct
        if total_pct != 100:
            st.error(f"배분 합계가 {total_pct}%입니다. 100%가 되어야 합니다.")
        else:
            st.success("배분 합계: 100%")

    with sim_col_result:
        # AI recommended allocation
        ai_alloc = {}
        if not alloc_df.empty:
            for _, arow in alloc_df.iterrows():
                ai_alloc[arow["DISTRICT"]] = safe_float(arow["ALLOC_PCT"])

        st.markdown("**AI 추천 vs 사용자 시뮬레이션**")

        # Build comparison table
        user_alloc = {
            "서초구": seocho_pct,
            "영등포구": yeongdeungpo_pct,
            "중구": junggu_pct,
        }

        comparison_rows = []
        for district_name in ["서초구", "영등포구", "중구"]:
            ai_pct = ai_alloc.get(district_name, 33.3)
            user_pct = user_alloc.get(district_name, 0)
            ai_budget = total_budget * ai_pct / 100
            user_budget = total_budget * user_pct / 100
            comparison_rows.append(
                {
                    "구": district_name,
                    "AI 추천 (%)": f"{ai_pct:.1f}",
                    "사용자 (%)": f"{user_pct}",
                    "AI 예산 (만원)": f"{ai_budget:,.0f}",
                    "사용자 예산 (만원)": f"{user_budget:,.0f}",
                    "차이 (만원)": f"{user_budget - ai_budget:+,.0f}",
                }
            )

        comparison_df = pd.DataFrame(comparison_rows)
        st.dataframe(comparison_df, use_container_width=True, hide_index=True)

        # Visual comparison
        chart_data = pd.DataFrame(
            {
                "AI 추천": [
                    ai_alloc.get("서초구", 33.3),
                    ai_alloc.get("영등포구", 33.3),
                    ai_alloc.get("중구", 33.3),
                ],
                "사용자": [seocho_pct, yeongdeungpo_pct, junggu_pct],
            },
            index=["서초구", "영등포구", "중구"],
        )
        st.bar_chart(chart_data, height=280)

    # AI comment on user allocation
    st.divider()
    if st.button("AI 코멘트 받기", key="sim_ai_comment_btn"):
        sim_context = json.dumps(
            {
                "total_budget_manwon": total_budget,
                "ai_allocation": ai_alloc,
                "user_allocation": user_alloc,
                "latest_snapshot": latest[
                    [
                        c
                        for c in [
                            "DISTRICT",
                            "TOTAL_POP",
                            "TOTAL_SALES",
                            "NET_MOVE",
                            "TOURISM_DEMAND_IDX",
                            "STABILITY_SCORE",
                        ]
                        if c in latest.columns
                    ]
                ].to_dict("records")
                if not latest.empty
                else [],
            },
            ensure_ascii=False,
            indent=2,
        )
        sim_question = (
            "사용자가 위와 같이 예산을 배분하려 합니다. "
            "AI 추천 대비 어떤 리스크나 기회가 있는지 짧게 분석해주세요."
        )
        with st.spinner("AI 분석 중..."):
            sim_result = call_ai_complete(sim_question, sim_context)
            sim_structured = sim_result.get("structured_output", sim_result)
            st.session_state["sim_ai_comment"] = sim_structured.get(
                "answer", "코멘트를 생성하지 못했습니다."
            )

    if st.session_state.get("sim_ai_comment"):
        st.markdown("**AI 코멘트**")
        st.write(st.session_state["sim_ai_comment"])

# ===========================================================================
# Tab 5: Ops / Trust
# ===========================================================================
with tabs[4]:
    st.subheader("운영 / 신뢰성 패널")

    # --- Health panel ---
    if not health_raw.empty:
        st.dataframe(health_raw, use_container_width=True, hide_index=True)
    else:
        st.info(
            "V_APP_HEALTH 뷰를 생성하면 Dynamic Table/Task 상태가 여기에 표시됩니다."
        )
        health_cols = st.columns(5)
        health_cols[0].metric("데이터 갱신", "매일 06:00")
        health_cols[1].metric("모델 재학습", "주 1회")
        health_cols[2].metric("Target lag", "1시간")
        health_cols[3].metric("응답 시간", "~6초")
        health_cols[4].metric("월 비용", "~$80")

    # --- Architecture ---
    st.divider()
    st.subheader("아키텍처")
    arch_data = pd.DataFrame(
        [
            {
                "Layer": "Data",
                "Snowflake Object": "Marketplace (SPH + Richgo + AJD + Holiday + Demographics + Tourism)",
                "역할": "원천 데이터 (6종 외부 데이터 포함)",
            },
            {
                "Layer": "Feature Store",
                "Snowflake Object": "DT_FEATURE_MART_V2 (Dynamic Table)",
                "역할": "자동 갱신 Feature Mart (확장 컬럼 포함)",
            },
            {
                "Layer": "ML",
                "Snowflake Object": "MOVESIGNAL_FORECAST (ML FORECAST)",
                "역할": "3개월 수요 예측 + Ablation 검증",
            },
            {
                "Layer": "Semantic",
                "Snowflake Object": "MOVESIGNAL_SV (Semantic View)",
                "역할": "비즈니스 메트릭 정의",
            },
            {
                "Layer": "AI",
                "Snowflake Object": "AI_COMPLETE (Structured Output)",
                "역할": "액션 카드 생성 + 시뮬레이션 코멘트",
            },
            {
                "Layer": "Ops",
                "Snowflake Object": "Tasks + Dynamic Tables",
                "역할": "파이프라인 자동화 + 모니터링",
            },
            {
                "Layer": "App",
                "Snowflake Object": "Streamlit in Snowflake",
                "역할": "대시보드 + 에이전트 + 시뮬레이션",
            },
        ]
    )
    st.dataframe(arch_data, use_container_width=True, hide_index=True)

    # --- Security model ---
    st.divider()
    st.subheader("보안 모델")
    st.markdown(
        """
- **Streamlit 실행**: Owner's rights (소유자 권한 + 소유자 Warehouse)
- **Cortex Analyst**: RBAC 연동 -- Semantic View 접근 권한 기반
- **Query Tag**: `{"app":"movesignal_ai","version":"v7"}`
- **데이터 격리**: Marketplace 데이터 -> 내부 STG 테이블 복제 (원본 비노출)
"""
    )

    # --- Data governance ---
    st.divider()
    st.subheader("데이터 거버넌스")
    st.markdown(
        """
- **Object Tags**: 모든 테이블에 `DATA_CLASSIFICATION`, `PII_FLAG` 태그 적용
- **스폰서 데이터 규칙**: Marketplace 데이터는 재배포 불가, 내부 분석 전용
- **외부 데이터 라이선스**: 각 소스별 이용 약관 준수 (아래 표 참조)
"""
    )

    # --- External data sources ---
    st.divider()
    st.subheader("외부 데이터 소스")
    ext_data = pd.DataFrame(
        [
            {
                "데이터 소스": "서울시 유동인구 (SPH)",
                "라이선스": "Snowflake Marketplace",
                "URL": "marketplace.snowflake.com",
                "갱신 주기": "월 1회",
            },
            {
                "데이터 소스": "카드 소비 데이터 (Richgo)",
                "라이선스": "Snowflake Marketplace",
                "URL": "marketplace.snowflake.com",
                "갱신 주기": "월 1회",
            },
            {
                "데이터 소스": "부동산 데이터 (AJD)",
                "라이선스": "Snowflake Marketplace",
                "URL": "marketplace.snowflake.com",
                "갱신 주기": "월 1회",
            },
            {
                "데이터 소스": "공휴일/달력 데이터",
                "라이선스": "공공데이터포털 (Open API)",
                "URL": "data.go.kr",
                "갱신 주기": "연 1회",
            },
            {
                "데이터 소스": "인구 구조/연령 데이터",
                "라이선스": "통계청 KOSIS",
                "URL": "kosis.kr",
                "갱신 주기": "월 1회",
            },
            {
                "데이터 소스": "관광/외국인 방문 데이터",
                "라이선스": "한국관광공사",
                "URL": "datalab.visitkorea.or.kr",
                "갱신 주기": "월 1회",
            },
        ]
    )
    st.dataframe(ext_data, use_container_width=True, hide_index=True)

    # --- Dual use case ---
    st.divider()
    use_col_private, use_col_public = st.columns(2)
    with use_col_private:
        st.subheader("민간 활용")
        st.markdown(
            """
- 렌탈/마케팅 예산 최적 배분
- 신규 매장 출점 우선순위
- 재고/설치 인력 배치
- 채널별 ROI 시뮬레이션
- 관광 시즌 대비 재고 사전 배치
- 연령별 타겟 마케팅 전략 수립
"""
        )
    with use_col_public:
        st.subheader("공공 활용")
        st.markdown(
            """
- 구청 상권 활성화 예산 배분
- 소상공인 지원금 우선순위
- 관광 인프라 배치 근거
- 현장 점검 우선순위 산정
- 고령 인구 복지 서비스 배치
- 외국인 관광객 유입 정책 수립
"""
        )
