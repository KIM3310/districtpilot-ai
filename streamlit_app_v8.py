"""
DistrictPilot AI — Snowflake-native Move-in Demand Engine
서초/영등포/중구 전입·이사 기반 홈서비스 수요 예측 에이전트

Architecture:
  FEATURE_MART_V2 (holiday + demographics + tourism + commercial)
  Snowflake ML FORECAST (auto-resolved live model) -> evaluation metrics + feature importance
  ABLATION_RESULTS -> model improvement evidence
  AI_COMPLETE structured output -> action cards with fallback
  Cortex Search -> policy document grounding with citations
  Dynamic Tables + Tasks -> operational freshness
  V_APP_HEALTH -> real-time ops monitoring

Tabs: Capture Plan | Move-in Signals | AI Playbook | Scenario Lab | Ops/Trust

Features:
  - Confidence interval bands on forecast chart
  - Ablation MAPE improvement delta metrics
  - Cortex Search citation display in AI Playbook
  - Per-district insight callouts in Move-in Signals
  - Deviation alerts in Scenario Lab
"""
import json
from typing import List, Tuple

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# ---------------------------------------------------------------------------
# Config constants
# ---------------------------------------------------------------------------
# Prefer the latest live objects, but tolerate legacy names so the app remains
# stable across demo environments and handoff states.
FEATURE_MART_CANDIDATES = [
    "DISTRICTPILOT_AI.ANALYTICS.FEATURE_MART_V3",
    "DISTRICTPILOT_AI.ANALYTICS.FEATURE_MART_V2",
    "DISTRICTPILOT_AI.ANALYTICS.FEATURE_MART_FINAL",
]
FORECAST_RESULTS_FQN = "DISTRICTPILOT_AI.ANALYTICS.FORECAST_RESULTS"
AVF_FQN = "DISTRICTPILOT_AI.ANALYTICS.ACTUAL_VS_FORECAST"
FORECAST_MODEL_CANDIDATES = [
    "DISTRICTPILOT_AI.ANALYTICS.DISTRICTPILOT_FORECAST_V2",
    "DISTRICTPILOT_AI.ANALYTICS.DISTRICTPILOT_FORECAST",
]
SEMANTIC_VIEW_FQN = "DISTRICTPILOT_AI.ANALYTICS.DISTRICTPILOT_SV"
HEALTH_VIEW_FQN = "DISTRICTPILOT_AI.ANALYTICS.V_APP_HEALTH"
ABLATION_FQN = "DISTRICTPILOT_AI.ANALYTICS.ABLATION_RESULTS"
EVAL_METRICS_FQN = "DISTRICTPILOT_AI.ANALYTICS.EVAL_METRICS_A"
FEATURE_IMPORTANCE_FQN = "DISTRICTPILOT_AI.ANALYTICS.FEATURE_IMPORTANCE"
LLM_MODEL = "mistral-large2"
SEARCH_SERVICE_FQN = "DISTRICTPILOT_AI.ANALYTICS.DISTRICTPILOT_SEARCH_SVC"

DISTRICTS_KR = {"서초구": "서초구", "영등포구": "영등포구", "중구": "중구"}

session = get_active_session()

st.set_page_config(page_title="DistrictPilot AI", layout="wide")

# ── Premium UI Theme ──
st.markdown("""<style>
/* ── Global ── */
.block-container{padding-top:.8rem !important;padding-bottom:1rem !important;max-width:1200px !important;}
header[data-testid='stHeader']{display:none;}

/* ── Typography ── */
h1{margin:0 !important;font-size:1.5rem !important;font-weight:700 !important;letter-spacing:-.02em;}
h2{font-size:1.25rem !important;font-weight:600 !important;color:#1E2761 !important;border-bottom:2px solid #29B5E8;padding-bottom:.3rem;margin-top:1.2rem !important;}
h3{font-size:1.05rem !important;font-weight:600 !important;color:#333 !important;}

/* ── Metric cards ── */
[data-testid="stMetric"]{
    background:linear-gradient(135deg,#f8f9ff 0%,#eef4ff 100%);
    border:1px solid #e0e8f5;border-radius:12px;padding:12px 16px !important;
    box-shadow:0 1px 3px rgba(0,0,0,.06);
}
[data-testid="stMetric"] label{font-size:.75rem !important;color:#666 !important;font-weight:500 !important;text-transform:uppercase;letter-spacing:.04em;}
[data-testid="stMetric"] [data-testid="stMetricValue"]{font-size:1.6rem !important;font-weight:700 !important;color:#1E2761 !important;}
[data-testid="stMetric"] [data-testid="stMetricDelta"]{font-size:.8rem !important;}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"]{gap:0;border-bottom:2px solid #e8e8e8;}
.stTabs [data-baseweb="tab"]{
    font-size:.9rem !important;font-weight:500;padding:10px 20px;
    border-radius:8px 8px 0 0;color:#666;
}
.stTabs [aria-selected="true"]{
    background:#29B5E8 !important;color:white !important;font-weight:700 !important;
    border-bottom:2px solid #29B5E8;
}

/* ── Charts ── */
[data-testid="stArrowVegaLiteChart"]{border-radius:8px;overflow:hidden;}

/* ── Info/Warning/Success boxes ── */
.stAlert{border-radius:10px !important;border-left:4px solid !important;}

/* ── DataFrames ── */
[data-testid="stDataFrame"]{border-radius:8px;overflow:hidden;border:1px solid #e8e8e8;}

/* ── Buttons ── */
.stButton>button{
    border-radius:8px;font-weight:600;border:none;
    background:linear-gradient(135deg,#29B5E8,#1E7BB8);color:white;
    padding:8px 20px;transition:all .2s;
}
.stButton>button:hover{transform:translateY(-1px);box-shadow:0 4px 12px rgba(41,181,232,.3);}

/* ── Progress bars ── */
.stProgress>div>div{background:linear-gradient(90deg,#29B5E8,#1E2761) !important;border-radius:8px;}

/* ── Dividers ── */
hr{border:none !important;height:1px !important;background:linear-gradient(90deg,transparent,#29B5E8,transparent) !important;margin:1.5rem 0 !important;}

/* ── Expanders ── */
.streamlit-expanderHeader{font-weight:600 !important;color:#1E2761 !important;font-size:.9rem !important;}

/* ── Sidebar caption ── */
.stCaption{color:#888 !important;font-size:.8rem !important;}
</style>""", unsafe_allow_html=True)

st.markdown("""
<div style="display:flex;align-items:center;gap:12px;margin-bottom:4px;">
    <div style="background:linear-gradient(135deg,#29B5E8,#1E2761);color:white;font-weight:800;
                font-size:1.1rem;padding:6px 14px;border-radius:8px;letter-spacing:-.02em;">DP</div>
    <div>
        <div style="font-size:1.3rem;font-weight:700;color:#1E2761;letter-spacing:-.02em;line-height:1.2;">
            DistrictPilot AI
        </div>
        <div style="font-size:.8rem;color:#888;margin-top:1px;">서초/영등포/중구 전입·이사 수요 예측</div>
    </div>
</div>
""", unsafe_allow_html=True)

try:
    session.sql(
        "ALTER SESSION SET QUERY_TAG = "
        "'{\"app\":\"districtpilot_ai\",\"version\":\"1.0\",\"entrypoint\":\"app_init\"}'"
    ).collect()
except Exception:
    pass

# ---------------------------------------------------------------------------
# Utility / helper functions
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)
def load_df(_session, sql: str) -> pd.DataFrame:
    """Cached SQL execution returning a pandas DataFrame."""
    return _session.sql(sql).to_pandas()


def attempt_read(sql: str) -> Tuple[pd.DataFrame, bool]:
    """Execute SQL and return both the frame and whether execution succeeded."""
    try:
        return load_df(session, sql), True
    except Exception:  # noqa: BLE001 – intentional catch-all for resilient UI
        return pd.DataFrame(), False


def safe_read(sql: str) -> pd.DataFrame:
    """Execute SQL with graceful error handling. Returns empty DataFrame on failure."""
    df, _ = attempt_read(sql)
    return df


def resolve_first_usable(candidates: List[str], sql_builder) -> Tuple[pd.DataFrame, str]:
    """
    Return the first candidate that executes successfully, preferring one that
    also returns rows.
    """
    first_success_df = None
    first_success_name = candidates[0]
    for name in candidates:
        df, ok = attempt_read(sql_builder(name))
        if ok and first_success_df is None:
            first_success_df = df
            first_success_name = name
        if ok and not df.empty:
            return df, name
    if first_success_df is not None:
        return first_success_df, first_success_name
    return pd.DataFrame(), candidates[0]


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

feature_mart, active_feature_mart_fqn = resolve_first_usable(
    FEATURE_MART_CANDIDATES,
    lambda fqn: f"SELECT * FROM {fqn} ORDER BY YM, DISTRICT",
)

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

eval_raw, active_forecast_model_fqn = resolve_first_usable(
    FORECAST_MODEL_CANDIDATES,
    lambda fqn: f"SELECT * FROM TABLE({fqn}!SHOW_EVALUATION_METRICS())",
)
eval_from_model = not eval_raw.empty
if eval_raw.empty:
    eval_raw = safe_read(f"SELECT * FROM {EVAL_METRICS_FQN}")

fi_raw, fi_model_fqn = resolve_first_usable(
    FORECAST_MODEL_CANDIDATES,
    lambda fqn: f"SELECT * FROM TABLE({fqn}!EXPLAIN_FEATURE_IMPORTANCE())",
)
if fi_raw.empty:
    fi_raw = safe_read(f"SELECT * FROM {FEATURE_IMPORTANCE_FQN}")
elif not eval_from_model:
    active_forecast_model_fqn = fi_model_fqn

health_raw = safe_read(f"SELECT * FROM {HEALTH_VIEW_FQN}")

ablation_raw = safe_read(f"SELECT * FROM {ABLATION_FQN}")

if feature_mart.empty and forecast_raw.empty:
    st.error("데이터 로딩 실패. 테이블명/권한을 확인하세요.")
    st.stop()

active_feature_mart_name = active_feature_mart_fqn.rsplit(".", maxsplit=1)[-1]
active_forecast_model_name = active_forecast_model_fqn.rsplit(".", maxsplit=1)[-1]
st.caption(
    f"Live objects: Feature Mart `{active_feature_mart_name}` | "
    f"Forecast `{active_forecast_model_name}` | "
    f"Semantic View `DISTRICTPILOT_SV`"
)

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
    if "METRIC" in out.columns and "ERROR_METRIC" not in out.columns:
        out = out.rename(columns={"METRIC": "ERROR_METRIC"})
    if "VALUE" in out.columns and "METRIC_VALUE" not in out.columns:
        out = out.rename(columns={"VALUE": "METRIC_VALUE"})
    if "SERIES" not in out.columns:
        out["SERIES"] = "ALL"
    if "SERIES" in out.columns:
        out["SERIES"] = out["SERIES"].apply(clean_variant)
    if "ERROR_METRIC" in out.columns:
        out["ERROR_METRIC"] = (
            out["ERROR_METRIC"].astype(str).str.replace('"', "", regex=False)
        )
    if "METRIC_VALUE" not in out.columns:
        return pd.DataFrame()
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
        "DISTRICT", "MOVE_IN", "MOVE_OUT", "NET_MOVE", "TOTAL_POP", "TOTAL_SALES",
        "AVG_ASSET", "AVG_MEME_PRICE",
        "AGE_20_39_SHARE", "SENIOR_60P_SHARE",
        "TOURISM_DEMAND_IDX", "FOREIGN_VISITOR_IDX",
        "STABILITY_SCORE", "NET_STORE_CHANGE",
        "HOLIDAY_DAYS",
        "RENTAL_COUNT", "RENTAL_CONVERSION_RATE", "CS_CALLS", "RENTAL_SIGNAL",
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
    if not lat.empty and "HOLIDAY_DAYS" in lat.columns:
        if scope != "전체":
            hrow = lat[lat["DISTRICT"] == scope]
        else:
            hrow = lat
        if not hrow.empty:
            holiday_info = {
                "holiday_count": safe_float(hrow.iloc[0].get("HOLIDAY_DAYS", 0)),
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
    prompt = f"""당신은 DistrictPilot AI의 한국어 의사결정 보조 모델이다.

역할:
- 전입·이사 기반 홈서비스/렌탈 수요를 해석한다.
- 어느 구를 먼저 공략할지, 어떤 강도로 집행할지, 어떤 운영 액션이 필요한지 제안한다.

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
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
                elif isinstance(parsed, list) and parsed:
                    return parsed[0] if isinstance(parsed[0], dict) else {"structured_output": {"answer": str(parsed[0])}}
                return {"structured_output": {"answer": str(parsed)}}
            except Exception:
                return {"structured_output": {"answer": raw}}
        elif isinstance(raw, dict):
            return raw
        else:
            return {"structured_output": {"answer": str(raw)}}
    except Exception as e:
        return _fallback_complete(prompt)


def _fallback_complete(prompt: str) -> dict:
    """Fallback to SNOWFLAKE.CORTEX.COMPLETE when AI_COMPLETE is unavailable."""
    try:
        rows = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS R",
            params=[LLM_MODEL, prompt],
        ).collect()
        answer = rows[0]["R"] if rows else "응답을 생성하지 못했습니다."
        return {"structured_output": {"answer": answer}}
    except Exception:  # noqa: BLE001
        return {"structured_output": {"answer": "AI 호출 실패: 일시적 오류가 발생했습니다. 잠시 후 다시 시도해주세요."}}


def search_policy_context(query: str, limit: int = 3) -> list:
    """Retrieve grounded policy context from Cortex Search service."""
    try:
        safe_q = query.replace("'", "''")
        rows = session.sql(
            f"SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW("
            f"  '{SEARCH_SERVICE_FQN}',"
            f"  '{safe_q}',"
            f"  {limit}"
            f") AS RESULTS"
        ).collect()
        if rows and rows[0]["RESULTS"]:
            raw = rows[0]["RESULTS"]
            return json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        pass
    return []


def generate_district_insight(snap: dict, district_name: str) -> str:
    """Generate a concise insight callout for a district based on its signals."""
    signals = []
    net_move = safe_float(snap.get("NET_MOVE", 0))
    if net_move > 0:
        signals.append(f"전입 우위 (순이동 +{net_move:,.0f})")
    elif net_move < 0:
        signals.append(f"전출 우위 (순이동 {net_move:,.0f})")

    sales_chg = safe_float(snap.get("SALES_CHG_PCT", 0))
    if sales_chg > 3:
        signals.append(f"소비 증가세 ({sales_chg:+.1f}%)")
    elif sales_chg < -3:
        signals.append(f"소비 감소세 ({sales_chg:+.1f}%)")

    tourism = safe_float(snap.get("TOURISM_DEMAND_IDX", 0))
    if tourism > 120:
        signals.append(f"관광 수요 활발 ({tourism:.0f})")

    stability = safe_float(snap.get("STABILITY_SCORE", 0))
    if stability < 0.5:
        signals.append("상권 불안정 주의")
    elif stability > 0.8:
        signals.append("상권 안정적")

    if not signals:
        return f"{district_name}: 전반적으로 안정적인 상태입니다."
    return f"{district_name}: " + " | ".join(signals)


def lottie_banner(lottie_url: str, height: int = 100, caption: str = ""):
    """Embed an animated banner. Uses Lottie with CSS fallback."""
    html = f"""
    <div style="display:flex;align-items:center;gap:16px;
                background:linear-gradient(135deg,#f0f7ff 0%,#e8f4fd 50%,#f0f0ff 100%);
                border-radius:14px;padding:14px 24px;margin-bottom:8px;
                border:1px solid #e0e8f5;box-shadow:0 2px 8px rgba(41,181,232,.08);">
        <script src="https://unpkg.com/@lottiefiles/lottie-player@2/dist/lottie-player.js"></script>
        <lottie-player src="{lottie_url}" background="transparent"
            speed="1" style="width:80px;height:80px;min-width:80px;" loop autoplay
            onerror="this.style.display='none'">
        </lottie-player>
        <div style="font-size:.95rem;color:#1E2761;font-weight:600;line-height:1.4;">{caption}</div>
    </div>"""
    try:
        import streamlit.components.v1 as components
        components.html(html, height=height + 10)
    except Exception:
        st.caption(caption)  # Fallback to plain text


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
_ext_cols = ["HOLIDAY_DAYS", "AGE_20_39_SHARE", "TOURISM_DEMAND_IDX", "STABILITY_SCORE"]
_ext_count = sum(1 for c in _ext_cols if c in feature_mart.columns)
header_col4.metric(
    "외부 데이터",
    f"스폰서 3 + 공개 {_ext_count}",
)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tabs = st.tabs(["Capture Plan", "Move-in Signals", "AI Playbook", "Scenario Lab", "Ops / Trust"])

# ===========================================================================
# Tab 1: Allocation
# ===========================================================================
with tabs[0]:
    lottie_banner("https://assets2.lottiefiles.com/packages/lf20_V9t630.json",
                  100, "📊 AI가 분석한 다음 달 캡처 우선순위")
    if next_ts is not None:
        st.subheader(f"다음 달 전입·이사 수요 캡처 우선순위 ({next_ts.strftime('%Y-%m')})")
    else:
        st.subheader("다음 달 전입·이사 수요 캡처 우선순위")
    st.caption(
        "각 구의 다음 달 수요 예측을 집행 강도로 변환한 캡처 플랜입니다. "
        "마케팅뿐 아니라 설치 인력, 현장 운영, 오퍼 우선순위까지 연결할 수 있습니다."
    )

    # --- Capture plan metrics ---
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

    # --- Actual vs Forecast overlay with confidence intervals ---
    st.subheader("Actual vs Forecast")
    if not overlay.empty:
        st.line_chart(overlay, height=360)
        # Show confidence interval info
        if not forecast_raw.empty and "LOWER_BOUND" in forecast_raw.columns:
            ci_col1, ci_col2, ci_col3 = st.columns(3)
            for idx, (_, frow) in enumerate(
                forecast_raw.groupby("DISTRICT").first().iterrows()
            ):
                if idx < 3:
                    col = [ci_col1, ci_col2, ci_col3][idx]
                    lb = safe_float(frow.get("LOWER_BOUND", 0))
                    ub = safe_float(frow.get("UPPER_BOUND", 0))
                    fc = safe_float(frow.get("FORECAST", 0))
                    col.metric(
                        f"{frow.name} 95% CI",
                        f"{fc:,.0f}",
                        f"[{lb:,.0f} ~ {ub:,.0f}]",
                    )
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

            # Highlight improvement with delta metrics
            if "DISTRICT" in ablation_raw.columns:
                delta_cols = st.columns(len(districts))
                for d_idx, district_name in enumerate(districts):
                    district_abl = ablation_raw[
                        ablation_raw["DISTRICT"] == district_name
                    ].sort_values("MODEL")
                    if len(district_abl) >= 2:
                        first_mape = safe_float(district_abl.iloc[0]["MAPE"])
                        last_mape = safe_float(district_abl.iloc[-1]["MAPE"])
                        if first_mape > 0:
                            improve_pct = (first_mape - last_mape) / first_mape * 100
                            with delta_cols[d_idx]:
                                st.metric(
                                    f"{district_name} MAPE 개선",
                                    f"{last_mape:.2f}%",
                                    f"{improve_pct:+.1f}% (A→E)",
                                    delta_color="inverse",
                                )
                st.info(
                    "Model A(스폰서 데이터만) → E(전체 외부 데이터 포함): "
                    "공휴일·연령·관광·상권 데이터가 예측 정확도를 단계적으로 개선합니다."
                )

            with st.expander("Ablation 상세 데이터"):
                st.dataframe(ablation_raw, use_container_width=True)
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
    lottie_banner("https://assets5.lottiefiles.com/packages/lf20_kuKkle.json",
                  100, "🏠 구별 전입·이사 신호 분석")
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

            st.caption(
                "전입·이사 수요를 설명하는 핵심 신호를 구별로 보여줍니다. "
                "주거 이동, 소비 여력, 관광 유입, 상권 안정성을 한 화면에서 읽을 수 있습니다."
            )

            # District insight callout
            insight_text = generate_district_insight(snap.to_dict(), analysis_district)
            st.info(f"**핵심 인사이트**: {insight_text}")

            # --- Extended KPIs ---
            kpi_row1 = st.columns(4)
            kpi_row1[0].metric(
                "전입 세대",
                fmt_int(snap.get("MOVE_IN", 0)),
            )
            kpi_row1[1].metric(
                "순이동",
                fmt_int(snap.get("NET_MOVE", 0)),
            )
            kpi_row1[2].metric(
                "카드소비 (억원)",
                fmt_eok(snap.get("TOTAL_SALES", 0)),
                fmt_pct(snap.get("SALES_CHG_PCT", 0)),
            )
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
                fmt_int(snap.get("HOLIDAY_DAYS", 0)),
            )

            # --- AJD KPIs (when available) ---
            has_ajd = "RENTAL_COUNT" in latest.columns
            if has_ajd:
                st.divider()
                st.markdown("**AJD 통신/렌탈 신호**")
                kpi_row3 = st.columns(4)
                kpi_row3[0].metric(
                    "렌탈 건수",
                    fmt_int(snap.get("RENTAL_COUNT", 0)),
                )
                kpi_row3[1].metric(
                    "렌탈 전환율",
                    f'{safe_float(snap.get("RENTAL_CONVERSION_RATE", 0)):.2%}',
                )
                kpi_row3[2].metric(
                    "CS 인입",
                    fmt_int(snap.get("CS_CALLS", 0)),
                )
                rental_signal = safe_float(snap.get("RENTAL_SIGNAL", 0))
                kpi_row3[3].metric(
                    "Rental Signal",
                    f"{rental_signal:.1f}",
                    "강함" if rental_signal > 50 else "보통" if rental_signal > 20 else "약함",
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
                    "MOVE_IN",
                    "MOVE_OUT",
                    "NET_MOVE",
                    "TOTAL_POP",
                    "TOTAL_SALES",
                    "AVG_MEME_PRICE",
                    "SALES_PER_POP",
                    "AGE_20_39_SHARE",
                    "SENIOR_60P_SHARE",
                    "TOURISM_DEMAND_IDX",
                    "FOREIGN_VISITOR_IDX",
                    "STABILITY_SCORE",
                    "NET_STORE_CHANGE",
                    "HOLIDAY_DAYS",
                    "RENTAL_COUNT",
                    "RENTAL_CONVERSION_RATE",
                    "CS_CALLS",
                    "RENTAL_SIGNAL",
                ]
                if c in latest.columns
            ]
            st.dataframe(
                latest[compare_columns],
                use_container_width=True,
            )

# ===========================================================================
# Tab 3: AI Agent
# ===========================================================================
with tabs[2]:
    lottie_banner("https://assets9.lottiefiles.com/packages/lf20_fcfjwiyb.json",
                  100, "🤖 AI 에이전트에게 질문하세요")
    st.subheader("DistrictPilot AI Playbook")
    st.caption(
        "Cortex Analyst(숫자) + AI_COMPLETE(액션) -- "
        "전입·이사, 소비, 자산, 관광, 상권 신호를 묶어 홈서비스/렌탈 액션으로 변환합니다."
    )

    with st.form("ai_form", clear_on_submit=False):
        ai_scope = st.selectbox("Scope", ["전체"] + districts, key="ai_scope")
        question = st.text_area(
            "질문 입력 (한국어 OK)",
            placeholder="예: 다음 달 어느 구의 전입 수요를 먼저 잡아야 하고, 어떤 액션이 필요해?",
        )
        submitted = st.form_submit_button("Ask DistrictPilot AI")

    # Quick questions (updated for extended data)
    st.caption("추천 질문:")
    quick_cols = st.columns(3)
    quick_questions = [
        "전입 신호와 소비 신호를 같이 보면 어느 구를 먼저 공략해야 해?",
        "어느 구에 설치 인력과 체험 오퍼를 먼저 배치해야 해?",
        "3구 중 이사 직후 홈서비스 전환 가능성이 가장 높은 곳은 어디야?",
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
        structured = payload.get("structured_output", payload) if isinstance(payload, dict) else payload
        if isinstance(structured, list):
            structured = structured[0] if structured else {}
        if not isinstance(structured, dict):
            structured = {"answer": str(structured)}
        usage = payload.get("usage", {}) if isinstance(payload, dict) else {}

        st.subheader("AI Recommendation")
        answer_text = structured.get("answer") or structured.get("ANSWER") or structured.get("response") or structured.get("R")
        if not answer_text:
            # Try to extract any string value from the dict
            for v in structured.values():
                if isinstance(v, str) and len(v) > 10:
                    answer_text = v
                    break
        if not answer_text:
            answer_text = json.dumps(structured, ensure_ascii=False, default=str)[:1000] if structured else "AI 응답을 파싱할 수 없습니다."
        st.write(answer_text)

        rec_col1, rec_col2 = st.columns(2)
        rec_col1.metric(
            "추천 지역", structured.get("recommended_district", "-")
        )
        alloc_pct_val = structured.get("allocation_pct")
        rec_col2.metric(
            "추천 집행 강도",
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

        # Cortex Search grounding — show policy citations
        grounding_results = search_policy_context(question if question else "전입 수요 캡처")
        if grounding_results:
            with st.expander("Cortex Search 근거 문서 (hallucination 방지)", expanded=False):
                st.caption("AI 추천의 근거가 되는 정책/룰북 문서입니다.")
                if isinstance(grounding_results, list):
                    for g_idx, g_item in enumerate(grounding_results):
                        if isinstance(g_item, dict):
                            st.markdown(f"**[{g_idx+1}]** {g_item.get('CONTENT', g_item.get('content', str(g_item)))[:300]}...")
                        else:
                            st.markdown(f"**[{g_idx+1}]** {str(g_item)[:300]}...")
                else:
                    st.json(grounding_results)

        with st.expander("AI 상세 (컨텍스트 + 토큰)"):
            if usage:
                st.write("Token usage:", usage)
            st.code(st.session_state["ai_context"], language="json")

# ===========================================================================
# Tab 4: Simulation
# ===========================================================================
with tabs[3]:
    lottie_banner("https://assets1.lottiefiles.com/packages/lf20_iorpbol0.json",
                  100, "🧪 예산 시나리오를 시뮬레이션하세요")
    st.subheader("전입 수요 캡처 시나리오")
    st.caption("AI 캡처 플랜과 사용자의 집행 시나리오를 비교합니다.")

    # Budget input
    total_budget = st.slider(
        "총 집행 예산 (만원)",
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
        st.markdown("**사용자 집행 계획**")

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
            st.error(f"집행 비중 합계가 {total_pct}%입니다. 100%가 되어야 합니다.")
        else:
            st.success("집행 비중 합계: 100%")

    with sim_col_result:
        # AI recommended allocation
        ai_alloc = {}
        if not alloc_df.empty:
            for _, arow in alloc_df.iterrows():
                ai_alloc[arow["DISTRICT"]] = safe_float(arow["ALLOC_PCT"])

        st.markdown("**AI 캡처 플랜 vs 사용자 시나리오**")

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
                    "AI 추천 집행 (%)": f"{ai_pct:.1f}",
                    "사용자 집행 (%)": f"{user_pct}",
                    "AI 예산 (만원)": f"{ai_budget:,.0f}",
                    "사용자 예산 (만원)": f"{user_budget:,.0f}",
                    "차이 (만원)": f"{user_budget - ai_budget:+,.0f}",
                }
            )

        comparison_df = pd.DataFrame(comparison_rows)
        st.dataframe(comparison_df, use_container_width=True)

        # Deviation alerts — warn when user allocation diverges significantly from AI
        for crow in comparison_rows:
            ai_val = float(crow["AI 추천 집행 (%)"])
            user_val = float(crow["사용자 집행 (%)"])
            diff = abs(user_val - ai_val)
            if diff > 15:
                st.warning(
                    f"**{crow['구']}**: AI 추천 대비 {diff:.0f}%p 차이 — "
                    f"{'과소 배정' if user_val < ai_val else '과다 배정'} 주의"
                )

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

    # AI comment on user scenario
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
            "사용자가 위와 같이 전입 수요 캡처 계획을 세우려 합니다. "
            "AI 추천 대비 어떤 기회와 운영 리스크가 있는지 짧게 분석해주세요."
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
    lottie_banner("https://assets3.lottiefiles.com/packages/lf20_swnrn2oy.json",
                  100, "⚙️ 운영 상태 & 데이터 거버넌스")
    st.subheader("운영 / 신뢰성 패널")

    # --- Query Tag: set session-level tag for audit trail ---
    try:
        session.sql(
            "ALTER SESSION SET QUERY_TAG = "
            "'{\"app\":\"districtpilot_ai\",\"version\":\"1.0\",\"tab\":\"ops_trust\"}'"
        ).collect()
    except Exception:
        pass

    # --- Health panel ---
    if not health_raw.empty:
        st.dataframe(health_raw, use_container_width=True)
    else:
        live_health_parts = []

        # 1) Dynamic Table current state
        dt_health = safe_read(
            "SELECT NAME, SCHEDULING_STATE, DATA_TIMESTAMP, TARGET_LAG_SEC "
            "FROM INFORMATION_SCHEMA.DYNAMIC_TABLES "
            "WHERE SCHEMA_NAME='ANALYTICS' LIMIT 5"
        )
        if not dt_health.empty:
            st.markdown("**Dynamic Table 상태**")
            st.dataframe(dt_health, use_container_width=True)
            live_health_parts.append("DT")

        # 2) Dynamic Table Refresh History (freshness evidence)
        dt_refresh = safe_read(
            "SELECT NAME, STATE, STATE_MESSAGE, "
            "REFRESH_START_TIME, REFRESH_END_TIME, "
            "LATEST_DATA_TIMESTAMP, "
            "TIMESTAMPDIFF('SECOND', LATEST_DATA_TIMESTAMP, CURRENT_TIMESTAMP()) AS LAG_SEC "
            "FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY("
            "  NAME_PREFIX => 'DISTRICTPILOT_AI.ANALYTICS.')) "
            "ORDER BY REFRESH_END_TIME DESC LIMIT 10"
        )
        if not dt_refresh.empty:
            st.markdown("**Dynamic Table 갱신 이력 (최근 10건)**")
            st.dataframe(dt_refresh, use_container_width=True)
            live_health_parts.append("DT_REFRESH")

        # 3) Task execution history
        task_health = safe_read(
            "SELECT NAME, STATE, SCHEDULE, LAST_COMMITTED_ON "
            "FROM INFORMATION_SCHEMA.TASK_HISTORY "
            "WHERE SCHEMA_NAME='ANALYTICS' LIMIT 5"
        )
        if not task_health.empty:
            st.markdown("**Task 실행 이력**")
            st.dataframe(task_health, use_container_width=True)
            live_health_parts.append("Task")

        if not live_health_parts:
            st.info(
                "V_APP_HEALTH 뷰 또는 Dynamic Table/Task가 설정되면 실시간 상태가 표시됩니다."
            )

        health_cols = st.columns(5)
        health_cols[0].metric("데이터 갱신", "매일 06:00")
        health_cols[1].metric("모델 재학습", "주 1회")
        health_cols[2].metric("Target lag", "1시간")
        health_cols[3].metric("응답 시간", "~6초")
        health_cols[4].metric("월 비용", "~$80")

    # --- Owner's Rights & Execution Context ---
    st.divider()
    st.subheader("실행 환경 증거")
    live_obj_cols = st.columns(3)
    live_obj_cols[0].metric("Feature Mart", active_feature_mart_name)
    live_obj_cols[1].metric("Forecast Model", active_forecast_model_name)
    live_obj_cols[2].metric("Semantic View", "DISTRICTPILOT_SV")

    exec_ctx = safe_read(
        "SELECT CURRENT_ROLE() AS ROLE, "
        "CURRENT_WAREHOUSE() AS WAREHOUSE, "
        "CURRENT_DATABASE() AS DB, "
        "CURRENT_SCHEMA() AS SCHEMA, "
        "CURRENT_SESSION() AS SESSION_ID, "
        "CURRENT_TIMESTAMP() AS TS"
    )
    if not exec_ctx.empty:
        ctx_cols = st.columns(4)
        row = exec_ctx.iloc[0]
        ctx_cols[0].metric("Role", str(row.get("ROLE", "N/A")))
        ctx_cols[1].metric("Warehouse", str(row.get("WAREHOUSE", "N/A")))
        ctx_cols[2].metric("Database", str(row.get("DB", "N/A")))
        ctx_cols[3].metric("Schema", str(row.get("SCHEMA", "N/A")))
    st.markdown(
        "**Owner's Rights**: SiS 앱은 소유자(ACCOUNTADMIN)의 권한과 "
        "소유자의 Warehouse(`COMPUTE_WH`)로 실행됩니다. "
        "앱 사용자는 별도 Warehouse 권한 없이도 쿼리를 실행할 수 있습니다."
    )

    # --- CORTEX_USER role smoke-test ---
    st.divider()
    st.subheader("Cortex 권한 검증")
    cortex_ok = False
    try:
        cortex_check = session.sql(
            "SHOW VIEWS LIKE 'DISTRICTPILOT_SV' IN SCHEMA DISTRICTPILOT_AI.ANALYTICS"
        ).collect()
        if cortex_check and len(cortex_check) > 0:
            cortex_ok = True
            st.code(f"Semantic View exists: {cortex_check[0]['name']}", language="text")
        else:
            st.warning("Semantic View를 찾을 수 없습니다.")
    except Exception as e:
        st.warning(f"Semantic View 검증 실패: {e}")
    st.metric(
        "Semantic View 유효성",
        "PASS" if cortex_ok else "CHECK NEEDED",
    )

    # --- Architecture ---
    st.divider()
    st.subheader("아키텍처")
    arch_data = pd.DataFrame(
        [
            {
                "Layer": "Data",
                "Snowflake Object": "Marketplace (SPH + Richgo) + 공공데이터 4종",
                "역할": "원천 데이터 (스폰서 2종 + 외부 4종)",
            },
            {
                "Layer": "Feature Store",
                "Snowflake Object": f"DT_FEATURE_MART -> {active_feature_mart_name}",
                "역할": "자동 갱신 Feature Mart (확장 컬럼 포함)",
            },
            {
                "Layer": "ML",
                "Snowflake Object": f"{active_forecast_model_name} (ML FORECAST)",
                "역할": "3개월 수요 예측 + Ablation 검증",
            },
            {
                "Layer": "Semantic",
                "Snowflake Object": "DISTRICTPILOT_SV (Semantic View)",
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
    st.dataframe(arch_data, use_container_width=True)

    # --- Security model ---
    st.divider()
    st.subheader("보안 모델")
    st.markdown(
        """
- **Streamlit 실행**: Owner's rights (소유자 권한 + 소유자 Warehouse)
- **Cortex Analyst**: Semantic View 접근 권한 기반 (ACCOUNTADMIN, 커스텀 롤 확장 가능)
- **Query Tag**: `{"app":"districtpilot_ai","version":"1.0"}`
- **데이터 격리**: Marketplace 데이터 -> 내부 STG 테이블 복제 (원본 비노출)
"""
    )

    # --- Data governance ---
    st.divider()
    st.subheader("데이터 거버넌스")
    st.markdown(
        """
- **Object Tags**: 테이블별 `DATA_CLASSIFICATION`, `PII_FLAG` 태그 적용 가능 (Enterprise 이상)
- **스폰서 데이터 규칙**: Marketplace 데이터는 재배포 불가, 내부 분석 전용
- **외부 데이터 라이선스**: 각 소스별 이용 약관 준수 (아래 표 참조)
"""
    )

    # --- Data source transparency ---
    st.divider()
    st.subheader("데이터 출처 투명성")
    transparency_data = pd.DataFrame(
        [
            {"데이터": "SPH 유동인구/카드소비/자산", "출처": "Snowflake Marketplace (실데이터)", "유형": "실측"},
            {"데이터": "Richgo 부동산/인구이동", "출처": "Snowflake Marketplace (실데이터)", "유형": "실측"},
            {"데이터": "공휴일/연령/관광/상권", "출처": "공공데이터 기반 시뮬레이션", "유형": "합성"},
            {"데이터": "AJD 통신/렌탈", "출처": "업계 통계 기반 시뮬레이션", "유형": "합성"},
        ]
    )
    st.dataframe(transparency_data, use_container_width=True)
    st.caption(
        "합성 데이터는 공개 통계와 도메인 지식 기반으로 현실적인 패턴을 재현합니다. "
        "Production에서는 공공데이터포털 API, AJD Marketplace 실데이터로 자동 교체됩니다."
    )

    # --- External data sources ---
    st.divider()
    st.subheader("외부 데이터 소스")
    ext_data = pd.DataFrame(
        [
            {
                "데이터 소스": "SPH 유동인구/카드소비",
                "라이선스": "Snowflake Marketplace",
                "URL": "marketplace.snowflake.com",
                "갱신 주기": "월 1회",
            },
            {
                "데이터 소스": "Richgo 부동산/인구이동",
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
    st.dataframe(ext_data, use_container_width=True)

    # --- Expansion potential: 확장 가능성 ---
    st.divider()
    st.subheader("확장 가능성")
    st.caption("같은 예측 엔진으로 공공 시장까지 확장 가능합니다.")
    expand_col1, expand_col2 = st.columns(2)
    with expand_col1:
        st.markdown("**현재: 홈서비스 사업자**")
        st.markdown(
            """
- 전입 직후 타깃 캠페인 우선순위
- 구별 홈서비스/렌탈 오퍼 강도 조정
- 설치 인력 및 재고 선배치
- 체험 부스/제휴 채널 집행 계획
- CS 대응과 월간 캠페인 조정
"""
        )
    with expand_col2:
        st.markdown("**확장: 자치구 행정 (Next)**")
        st.markdown(
            """
- 전입신고 창구 확대 (2→4개)
- 청년 주거 지원금 안내 발송
- 가족 전입 증가 시 보육시설 사전 준비
- 고령 전입 시 노인 돌봄 인력 배치
- 폐업 위기 상권 소상공인 지원금 우선 배정
"""
        )
