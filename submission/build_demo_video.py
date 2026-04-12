"""Build a narrated demo video for DistrictPilot AI."""

from __future__ import annotations

import math
import os
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

from PIL import Image, ImageDraw, ImageFont, ImageFilter
from moviepy import AudioFileClip, ImageClip, concatenate_videoclips, vfx


WIDTH = 1920
HEIGHT = 1080
FPS = 24

BG_TOP = (12, 21, 49)
BG_BOTTOM = (24, 38, 88)
ACCENT = (49, 190, 240)
ACCENT_ALT = (255, 167, 38)
WHITE = (246, 248, 255)
MUTED = (175, 187, 220)
CARD = (25, 36, 78)
CARD_2 = (30, 44, 94)
GREEN = (94, 214, 132)
RED = (255, 99, 132)
GRID = (61, 79, 138)

ROOT = Path("/Users/pizza/districtpilot-ai")
ASSET_DIR = ROOT / "video_assets"
DELIVERABLE_DIR = ROOT / "deliverables"


@dataclass
class Scene:
    key: str
    title: str
    subtitle: str
    narration: str
    bullets: Sequence[str]
    visual: str


SCENES: List[Scene] = [
    Scene(
        key="01_title",
        title="DistrictPilot AI",
        subtitle="Snowflake-Native Move-in Demand Orchestration Engine",
        narration=(
            "DistrictPilot AI는 서초, 영등포, 중구를 대상으로 다음 달 어디에서 전입 직후 홈서비스 수요를 먼저 잡아야 하는지 "
            "답하는 스노우플레이크 네이티브 오케스트레이션 엔진입니다. 핵심은 단순 예측이 아니라, 예측 결과를 실제 집행과 운영 액션으로 연결하는 것입니다."
        ),
        bullets=[
            "문제: 다음 달 어떤 구의 전입 수요를 먼저 잡아야 하는가",
            "대상: 서초구 · 영등포구 · 중구",
            "결과: 예측 + 근거 + 리스크 + 실행 액션",
        ],
        visual="title",
    ),
    Scene(
        key="02_data",
        title="Data Sources",
        subtitle="Marketplace 2종 + 공개데이터 + Optional AJD",
        narration=(
            "활성 핵심 데이터는 두 개의 스폰서 Marketplace 데이터와 네 개의 공개데이터입니다. "
            "SPH로 유동인구와 카드소비를 보고, Richgo로 부동산과 인구이동을 봅니다. "
            "여기에 공휴일, 연령구조, 관광수요, 상권변화 지표를 붙여서 수요 설명력과 해석력을 높였습니다. "
            "AJD는 옵션 확장 레이어로 두었습니다. 이 도메인은 세 파트너 데이터가 가장 자연스럽게 만나는 문제입니다."
        ),
        bullets=[
            "SPH: 유동인구 · 카드소비 · 자산소득",
            "Richgo: 매매/전세 시세 · 인구이동",
            "공개데이터: 공휴일 · 연령구조 · 관광수요 · 상권지표",
            "AJD: 선택 통합 옵션",
        ],
        visual="data",
    ),
    Scene(
        key="03_architecture",
        title="Architecture",
        subtitle="한 계정 안에서 끝나는 Snowflake-native 파이프라인",
        narration=(
            "아키텍처는 데이터 수집에서 피처 마트, 예측 모델, 검색과 생성형 AI, 그리고 Streamlit 앱과 운영 모니터링까지 모두 스노우플레이크 안에서 닫힙니다. "
            "DT_FEATURE_MART에서 자동 갱신하고 FEATURE_MART_V2로 서빙하며, DISTRICTPILOT_FORECAST_V2가 최종 production 모델입니다. "
            "이 흐름은 전입 신호 감지에서 실제 운영 액션까지 하나의 루프로 연결됩니다."
        ),
        bullets=[
            "DT_FEATURE_MART -> FEATURE_MART_V2",
            "DISTRICTPILOT_FORECAST_V2",
            "Semantic View + Cortex Search + AI_COMPLETE",
            "Tasks + V_APP_HEALTH + Query Tags",
        ],
        visual="architecture",
    ),
    Scene(
        key="04_validation",
        title="Forecast Validation",
        subtitle="Ablation으로 MAPE 개선을 숫자로 증명",
        narration=(
            "DistrictPilot AI는 감으로 추천하지 않습니다. "
            "스폰서 데이터만 쓴 베이스라인에서 시작해 공휴일, 연령구조, 관광수요, 상권지표를 단계적으로 추가하는 ablation 실험을 수행했습니다. "
            "Model A에서 Model E로 갈수록 MAPE가 개선되고, SHOW_EVALUATION_METRICS와 EXPLAIN_FEATURE_IMPORTANCE로 결과를 설명합니다."
        ),
        bullets=[
            "Model A: Y-only baseline",
            "Model B: + Holiday",
            "Model C: + Demographics",
            "Model D: + Tourism",
            "Model E: Full production",
        ],
        visual="validation",
    ),
    Scene(
        key="05_allocation",
        title="Capture Plan Tab",
        subtitle="다음 달 전입·이사 수요 캡처 우선순위",
        narration=(
            "Capture Plan 탭에서는 다음 달 집행 강도를 바로 보여줍니다. "
            "예를 들어 영등포구를 35점 8퍼센트, 중구를 33점 0퍼센트, 서초구를 31점 2퍼센트처럼 추천할 수 있습니다. "
            "같은 화면에서 Actual 대 Forecast 오버레이와 ablation 차트를 함께 보여줘서 추천의 신뢰성을 높입니다."
        ),
        bullets=[
            "서초구 31.2%",
            "영등포구 35.8%",
            "중구 33.0%",
            "Actual vs Forecast",
            "Ablation MAPE 차트",
        ],
        visual="allocation",
    ),
    Scene(
        key="06_analysis",
        title="Move-in Signals Tab",
        subtitle="왜 이 구가 높은지, 어떤 신호가 작동하는지",
        narration=(
            "Analysis 탭은 단순한 결과 화면이 아니라 설명 화면입니다. "
            "전입 세대와 순이동, 카드소비, 자산지표를 보고, 20대에서 39세 비중과 시니어 비중, 관광수요 지수와 상권 안정도를 함께 보여줍니다. "
            "그리고 feature importance를 통해 각 구의 핵심 드라이버를 시각적으로 드러냅니다."
        ),
        bullets=[
            "KPI: 전입 세대 · 카드소비 · 순이동 · 자산",
            "연령구조와 관광수요",
            "상권 안정도와 점포변화",
            "Feature importance",
        ],
        visual="analysis",
    ),
    Scene(
        key="07_agent",
        title="AI Playbook Tab",
        subtitle="Grounded Recommendation with Structured Output",
        narration=(
            "AI Playbook 탭에서는 사용자가 한국어로 질문하면 추천 지역, 집행 강도, 근거, 리스크, 다음 액션을 구조화된 형태로 반환합니다. "
            "Feature Mart 컨텍스트와 정책 문서를 함께 grounding해서 환각을 줄이고, 앱에 바로 쓸 수 있는 액션 카드 형태로 응답합니다."
        ),
        bullets=[
            "추천 지역",
            "배분 비율",
            "주요 드라이버",
            "리스크",
            "다음 액션",
        ],
        visual="agent",
    ),
    Scene(
        key="08_simulation",
        title="Scenario Lab Tab",
        subtitle="AI 추천안과 사용자 조정안을 비교",
        narration=(
            "Scenario Lab 탭에서는 예산 총액과 지역별 집행 비중을 사용자가 직접 조정할 수 있습니다. "
            "그래서 AI 추천안과 사용자의 가정이 어떻게 다른지 비교하고, 그 차이에 대한 코멘트를 즉시 받을 수 있습니다. "
            "이 기능은 실제 의사결정 회의에서 가장 실용적인 장치입니다."
        ),
        bullets=[
            "예산 총액 입력",
            "지역별 슬라이더 조정",
            "AI 추천안 비교",
            "즉시 코멘트 반환",
        ],
        visual="simulation",
    ),
    Scene(
        key="09_ops",
        title="Ops / Trust Tab",
        subtitle="데모가 아니라 운영 가능한 앱이라는 증거",
        narration=(
            "Ops와 Trust 탭은 이 프로젝트가 단순한 해커톤 데모가 아니라 운영 가능한 앱이라는 증거입니다. "
            "Dynamic Table의 freshness, Task 이력, 모델 버전, semantic validation, 그리고 V_APP_HEALTH 상태를 한 화면에 보여줍니다. "
            "또한 스폰서 데이터 미배포 원칙과 라이선스 준수까지 같이 설명합니다."
        ),
        bullets=[
            "Freshness와 target lag",
            "Task history와 model version",
            "Semantic validation",
            "V_APP_HEALTH",
            "Governance와 license",
        ],
        visual="ops",
    ),
    Scene(
        key="10_depth",
        title="Why Snowflake",
        subtitle="플랫폼 활용 깊이가 점수로 이어지는 구간",
        narration=(
            "이 프로젝트의 경쟁력은 스노우플레이크 기능을 여러 개 나열한 데 있지 않습니다. "
            "Marketplace, ML Forecast, Semantic View, Cortex Search, AI_COMPLETE, Dynamic Tables, Tasks, Streamlit이 하나의 사용자 흐름 안에서 유기적으로 연결되어 있다는 점이 핵심입니다."
        ),
        bullets=[
            "Marketplace",
            "ML Forecast",
            "Semantic View",
            "Cortex Search",
            "AI_COMPLETE",
            "Dynamic Tables / Tasks / Streamlit",
        ],
        visual="depth",
    ),
    Scene(
        key="11_impacts",
        title="One Engine, One Operating Loop",
        subtitle="획득 전략과 운영 실행을 하나로 연결",
        narration=(
            "같은 엔진은 획득 전략과 운영 실행을 한 루프로 연결합니다. "
            "먼저 어느 구를 공략할지와 어느 정도 강도로 집행할지를 정하고, "
            "그다음 설치 가능 지역, 인력, 재고, 고객 응대 같은 운영 액션으로 이어집니다."
        ),
        bullets=[
            "획득: 집행 강도 · 채널 믹스 · 오퍼 전략",
            "운영: 설치 인력 · 재고 · CS 대응",
            "같은 Feature Mart, 같은 엔진",
        ],
        visual="impacts",
    ),
    Scene(
        key="12_closing",
        title="Submission Ready",
        subtitle="검증, 운영, 설명 가능성까지 갖춘 제출물",
        narration=(
            "DistrictPilot AI는 예측 정확도, 설명 가능성, 운영 가능성, 그리고 Snowflake 플랫폼 활용 깊이를 모두 갖춘 제출물입니다. "
            "최종 제출에서는 사전 점검 SQL, judge fast path SQL, 그리고 라이브 호환 패치까지 포함해 데모 리스크를 낮췄습니다. "
            "이상으로 DistrictPilot AI 발표를 마치겠습니다."
        ),
        bullets=[
            "Precheck SQL included",
            "Judge fast-path SQL included",
            "Live compatibility patch included",
            "Korean README / PPT / demo video ready",
        ],
        visual="closing",
    ),
]


def ensure_dirs() -> None:
    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    DELIVERABLE_DIR.mkdir(parents=True, exist_ok=True)


def font_candidates() -> Sequence[str]:
    return [
        "/System/Library/Fonts/AppleSDGothicNeo.ttc",
        "/System/Library/Fonts/Supplemental/AppleGothic.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ]


def get_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    for candidate in font_candidates():
        if os.path.exists(candidate):
            try:
                return ImageFont.truetype(candidate, size=size)
            except OSError:
                continue
    return ImageFont.load_default()


TITLE_FONT = get_font(74, bold=True)
SUBTITLE_FONT = get_font(34)
BODY_FONT = get_font(30)
SMALL_FONT = get_font(22)
CARD_TITLE_FONT = get_font(28, bold=True)
BIG_NUMBER_FONT = get_font(64, bold=True)


def gradient_background() -> Image.Image:
    image = Image.new("RGB", (WIDTH, HEIGHT), BG_TOP)
    draw = ImageDraw.Draw(image)
    for y in range(HEIGHT):
        ratio = y / float(HEIGHT - 1)
        color = tuple(
            int(BG_TOP[i] * (1 - ratio) + BG_BOTTOM[i] * ratio) for i in range(3)
        )
        draw.line((0, y, WIDTH, y), fill=color)
    glow = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow)
    glow_draw.ellipse((1200, -150, 2100, 700), fill=(49, 190, 240, 46))
    glow_draw.ellipse((-200, 500, 900, 1350), fill=(255, 167, 38, 24))
    glow = glow.filter(ImageFilter.GaussianBlur(80))
    return Image.alpha_composite(image.convert("RGBA"), glow).convert("RGB")


def draw_header(draw: ImageDraw.ImageDraw, scene: Scene) -> None:
    draw.rounded_rectangle((72, 64, 510, 112), radius=24, fill=(30, 44, 94))
    draw.text((100, 77), "Snowflake Korea Hackathon 2026", font=SMALL_FONT, fill=ACCENT)
    draw.text((88, 150), scene.title, font=TITLE_FONT, fill=WHITE)
    draw.text((92, 250), scene.subtitle, font=SUBTITLE_FONT, fill=MUTED)


def wrap_lines(text: str, width: int) -> List[str]:
    lines: List[str] = []
    for paragraph in text.split("\n"):
        lines.extend(textwrap.wrap(paragraph, width=width) or [""])
    return lines


def draw_bullets(draw: ImageDraw.ImageDraw, bullets: Sequence[str]) -> None:
    x = 96
    y = 360
    for bullet in bullets:
        draw.ellipse((x, y + 12, x + 14, y + 26), fill=ACCENT)
        for line in wrap_lines(bullet, 34):
            draw.text((x + 30, y), line, font=BODY_FONT, fill=WHITE)
            y += 42
        y += 16


def draw_footer(draw: ImageDraw.ImageDraw, scene: Scene) -> None:
    footer = f"{scene.key.replace('_', ' ').upper()}  |  DistrictPilot AI  |  github.com/KIM3310"
    draw.text((92, 1000), footer, font=SMALL_FONT, fill=(145, 160, 204))


def draw_card(draw: ImageDraw.ImageDraw, xy: Tuple[int, int, int, int], title: str) -> None:
    draw.rounded_rectangle(xy, radius=28, fill=CARD)
    draw.rounded_rectangle((xy[0], xy[1], xy[0] + 8, xy[3]), radius=4, fill=ACCENT)
    draw.text((xy[0] + 28, xy[1] + 22), title, font=CARD_TITLE_FONT, fill=WHITE)


def draw_line_chart(draw: ImageDraw.ImageDraw, box: Tuple[int, int, int, int]) -> None:
    x0, y0, x1, y1 = box
    draw.rounded_rectangle(box, radius=28, fill=CARD_2)
    for step in range(5):
        y = y0 + 60 + step * ((y1 - y0 - 120) / 4)
        draw.line((x0 + 40, y, x1 - 40, y), fill=GRID, width=2)
    points_actual = [(x0 + 80, y1 - 120), (x0 + 180, y1 - 180), (x0 + 280, y1 - 145), (x0 + 380, y1 - 220), (x0 + 480, y1 - 180), (x0 + 580, y1 - 260)]
    points_forecast = [(x0 + 580, y1 - 260), (x0 + 680, y1 - 235), (x0 + 780, y1 - 285), (x0 + 880, y1 - 250)]
    draw.line(points_actual, fill=WHITE, width=6, joint="curve")
    draw.line(points_forecast, fill=ACCENT, width=6, joint="curve")
    for px, py in points_actual + points_forecast:
        draw.ellipse((px - 6, py - 6, px + 6, py + 6), fill=ACCENT_ALT if (px, py) in points_forecast else WHITE)
    draw.text((x0 + 40, y0 + 22), "Actual vs Forecast", font=CARD_TITLE_FONT, fill=WHITE)


def draw_bar_chart(draw: ImageDraw.ImageDraw, box: Tuple[int, int, int, int], labels: Sequence[str], values: Sequence[float]) -> None:
    x0, y0, x1, y1 = box
    draw.rounded_rectangle(box, radius=28, fill=CARD_2)
    draw.text((x0 + 40, y0 + 22), "MAPE by Model", font=CARD_TITLE_FONT, fill=WHITE)
    max_value = max(values)
    chart_height = y1 - y0 - 150
    bar_width = 70
    gap = 40
    start_x = x0 + 70
    base_y = y1 - 70
    for i, (label, value) in enumerate(zip(labels, values)):
        bar_h = chart_height * (value / max_value)
        bx = start_x + i * (bar_width + gap)
        by = base_y - bar_h
        color = ACCENT if i < len(values) - 1 else GREEN
        draw.rounded_rectangle((bx, by, bx + bar_width, base_y), radius=18, fill=color)
        draw.text((bx + 6, base_y + 12), label, font=SMALL_FONT, fill=MUTED)
        draw.text((bx - 2, by - 34), f"{value:.1f}", font=SMALL_FONT, fill=WHITE)


def draw_kpi_cards(draw: ImageDraw.ImageDraw, box: Tuple[int, int, int, int]) -> None:
    x0, y0, x1, y1 = box
    card_w = (x1 - x0 - 40) // 3
    items = [("서초구", "31.2%"), ("영등포구", "35.8%"), ("중구", "33.0%")]
    for i, (name, value) in enumerate(items):
        left = x0 + i * (card_w + 20)
        right = left + card_w
        draw.rounded_rectangle((left, y0, right, y1), radius=28, fill=CARD)
        draw.text((left + 28, y0 + 30), name, font=CARD_TITLE_FONT, fill=MUTED)
        draw.text((left + 28, y0 + 92), value, font=BIG_NUMBER_FONT, fill=WHITE)


def draw_metric_table(draw: ImageDraw.ImageDraw, box: Tuple[int, int, int, int], rows: Sequence[Tuple[str, str]]) -> None:
    x0, y0, x1, y1 = box
    draw.rounded_rectangle(box, radius=28, fill=CARD)
    draw.text((x0 + 24, y0 + 20), "Driver Snapshot", font=CARD_TITLE_FONT, fill=WHITE)
    row_y = y0 + 82
    for label, value in rows:
        draw.rounded_rectangle((x0 + 22, row_y, x1 - 22, row_y + 58), radius=18, fill=CARD_2)
        draw.text((x0 + 42, row_y + 12), label, font=BODY_FONT, fill=MUTED)
        draw.text((x1 - 220, row_y + 12), value, font=BODY_FONT, fill=WHITE)
        row_y += 72


def draw_json_card(draw: ImageDraw.ImageDraw, box: Tuple[int, int, int, int]) -> None:
    x0, y0, x1, y1 = box
    draw.rounded_rectangle(box, radius=28, fill=(18, 28, 58))
    draw.text((x0 + 28, y0 + 20), "AI_COMPLETE Structured Output", font=CARD_TITLE_FONT, fill=ACCENT)
    json_lines = [
        '{',
        '  "recommended_district": "영등포구",',
        '  "allocation_pct": 42.0,',
        '  "drivers": ["전입 증가", "카드소비 강세"],',
        '  "risk": "중구 관광 편중 변동성",',
        '  "next_action": "설치 인력과 체험 오퍼 우선 배치"',
        '}',
    ]
    y = y0 + 80
    mono_font = get_font(27)
    for line in json_lines:
        draw.text((x0 + 36, y), line, font=mono_font, fill=GREEN)
        y += 44


def draw_sim_sliders(draw: ImageDraw.ImageDraw, box: Tuple[int, int, int, int]) -> None:
    x0, y0, x1, y1 = box
    draw.rounded_rectangle(box, radius=28, fill=CARD)
    draw.text((x0 + 28, y0 + 20), "Simulation Controls", font=CARD_TITLE_FONT, fill=WHITE)
    sliders = [("서초구", 0.31), ("영등포구", 0.38), ("중구", 0.31)]
    y = y0 + 100
    for label, pct in sliders:
        draw.text((x0 + 28, y - 12), label, font=BODY_FONT, fill=WHITE)
        draw.rounded_rectangle((x0 + 180, y, x1 - 60, y + 20), radius=10, fill=GRID)
        knob_x = x0 + 180 + int((x1 - x0 - 240) * pct)
        draw.rounded_rectangle((x0 + 180, y, knob_x, y + 20), radius=10, fill=ACCENT)
        draw.ellipse((knob_x - 16, y - 12, knob_x + 16, y + 20), fill=WHITE)
        draw.text((x1 - 160, y - 16), f"{pct*100:.1f}%", font=BODY_FONT, fill=MUTED)
        y += 110


def draw_pill(draw: ImageDraw.ImageDraw, xy: Tuple[int, int, int, int], text: str, fill: Tuple[int, int, int], text_fill: Tuple[int, int, int]) -> None:
    draw.rounded_rectangle(xy, radius=22, fill=fill)
    bbox = draw.textbbox((0, 0), text, font=SMALL_FONT)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    draw.text((xy[0] + (xy[2] - xy[0] - tw) / 2, xy[1] + (xy[3] - xy[1] - th) / 2 - 2), text, font=SMALL_FONT, fill=text_fill)


def render_scene(scene: Scene) -> Path:
    image = gradient_background()
    draw = ImageDraw.Draw(image)
    draw_header(draw, scene)
    draw_bullets(draw, scene.bullets)

    if scene.visual == "title":
        draw.rounded_rectangle((1080, 240, 1820, 860), radius=36, fill=CARD)
        draw.text((1140, 320), "Action Loop", font=get_font(42, True), fill=ACCENT)
        steps = ["Forecast", "Explain", "Recommend", "Simulate", "Operate"]
        for idx, step in enumerate(steps):
            top = 420 + idx * 82
            draw.rounded_rectangle((1140, top, 1740, top + 58), radius=18, fill=CARD_2)
            draw.text((1180, top + 10), f"{idx+1}. {step}", font=BODY_FONT, fill=WHITE)
    elif scene.visual == "data":
        draw_card(draw, (1040, 250, 1820, 840), "Source Mix")
        rows = [
            ("Marketplace", "SPH + Richgo"),
            ("Open Data", "Holiday / Demo / Tourism / Commerce"),
            ("Optional", "AJD extension layer"),
            ("Unit", "3 districts, monthly decisioning"),
        ]
        draw_metric_table(draw, (1080, 310, 1780, 780), rows)
        draw_pill(draw, (1080, 800, 1320, 848), "Sponsor-safe", (29, 44, 94), WHITE)
        draw_pill(draw, (1340, 800, 1650, 848), "Korean compliance", (29, 44, 94), WHITE)
    elif scene.visual == "architecture":
        draw_card(draw, (980, 210, 1840, 900), "Architecture Flow")
        boxes = [
            ("Sources", "SPH + Richgo + Open Data"),
            ("Feature", "DT_FEATURE_MART"),
            ("Serving", "FEATURE_MART_V2"),
            ("Model", "DISTRICTPILOT_FORECAST_V2"),
            ("AI", "Search + Complete"),
            ("App", "Streamlit + Ops"),
        ]
        y = 290
        for title, desc in boxes:
            draw.rounded_rectangle((1060, y, 1760, y + 78), radius=22, fill=CARD_2)
            draw.text((1095, y + 12), title, font=CARD_TITLE_FONT, fill=ACCENT)
            draw.text((1300, y + 18), desc, font=BODY_FONT, fill=WHITE)
            if y < 720:
                draw.line((1410, y + 78, 1410, y + 108), fill=ACCENT, width=6)
                draw.polygon([(1410, y + 108), (1398, y + 92), (1422, y + 92)], fill=ACCENT)
            y += 110
    elif scene.visual == "validation":
        draw_bar_chart(draw, (980, 230, 1820, 860), ["A", "B", "C", "D", "E"], [11.8, 10.7, 9.9, 9.1, 8.4])
    elif scene.visual == "allocation":
        draw_kpi_cards(draw, (940, 240, 1820, 440))
        draw_line_chart(draw, (940, 480, 1820, 860))
    elif scene.visual == "analysis":
        draw_metric_table(
            draw,
            (980, 240, 1820, 860),
            [
                ("전입 세대", "5,842"),
                ("카드소비", "₩14.8B"),
                ("순이동", "+1,284"),
                ("20-39 비중", "34.7%"),
                ("관광지수", "118.2"),
                ("상권안정도", "0.81"),
            ],
        )
    elif scene.visual == "agent":
        draw_json_card(draw, (980, 240, 1820, 860))
    elif scene.visual == "simulation":
        draw_sim_sliders(draw, (980, 240, 1820, 860))
        draw_pill(draw, (1020, 760, 1320, 810), "AI recommendation", (25, 36, 78), WHITE)
        draw_pill(draw, (1340, 760, 1630, 810), "User scenario", (54, 79, 140), WHITE)
    elif scene.visual == "ops":
        draw_metric_table(
            draw,
            (980, 240, 1820, 860),
            [
                ("DT_FEATURE_MART", "1h target lag"),
                ("TASK_REFRESH_PIPELINE", "Daily 06:00"),
                ("TASK_RETRAIN_FORECAST", "Weekly Mon 07:00"),
                ("Semantic validation", "PASS"),
                ("V_APP_HEALTH", "Healthy"),
                ("Query tag", "Tracked"),
            ],
        )
    elif scene.visual == "depth":
        draw_card(draw, (980, 220, 1820, 870), "Snowflake Stack")
        badges = [
            "Marketplace",
            "ML Forecast",
            "Semantic View",
            "Cortex Search",
            "AI_COMPLETE",
            "Dynamic Tables",
            "Tasks",
            "Streamlit",
        ]
        positions = [
            (1040, 300, 1340, 360),
            (1380, 300, 1700, 360),
            (1040, 390, 1380, 450),
            (1420, 390, 1740, 450),
            (1040, 480, 1360, 540),
            (1400, 480, 1780, 540),
            (1110, 570, 1370, 630),
            (1410, 570, 1700, 630),
        ]
        for badge, pos in zip(badges, positions):
            draw_pill(draw, pos, badge, CARD_2, WHITE)
        draw.text((1080, 700), "Integrated into one decision flow", font=get_font(36, True), fill=ACCENT_ALT)
    elif scene.visual == "impacts":
        draw_card(draw, (980, 220, 1380, 860), "획득 전략")
        draw_card(draw, (1420, 220, 1820, 860), "운영 실행")
        private_items = ["집행 강도", "채널 믹스", "오퍼 전략", "B2B/B2C 우선순위"]
        public_items = ["설치 인력", "재고 선배치", "운영 제약 반영", "CS 에스컬레이션"]
        y = 320
        for item in private_items:
            draw.text((1016, y), f"• {item}", font=BODY_FONT, fill=WHITE)
            y += 90
        y = 320
        for item in public_items:
            draw.text((1456, y), f"• {item}", font=BODY_FONT, fill=WHITE)
            y += 90
    elif scene.visual == "closing":
        draw.rounded_rectangle((960, 280, 1820, 800), radius=42, fill=CARD)
        draw.text((1050, 380), "Submission Hardening", font=get_font(48, True), fill=ACCENT)
        draw.text((1050, 470), "12_final_precheck.sql", font=get_font(40), fill=WHITE)
        draw.text((1050, 540), "14_judge_fastpath.sql", font=get_font(40), fill=WHITE)
        draw.text((1050, 610), "13_live_app_compatibility_patch.sql", font=get_font(40), fill=WHITE)
        draw.text((1050, 700), "README / PPT / Demo video aligned", font=get_font(34), fill=MUTED)

    draw_footer(draw, scene)
    out = ASSET_DIR / f"{scene.key}.png"
    image.save(out)
    return out


def synthesize_voice(scene: Scene) -> Path:
    out = ASSET_DIR / f"{scene.key}.aiff"
    subprocess.run(
        [
            "say",
            "-v",
            "Yuna",
            "-r",
            "178",
            "-o",
            str(out),
            scene.narration,
        ],
        check=True,
    )
    return out


def write_narration_files(scenes: Sequence[Scene]) -> None:
    md_lines = ["# DistrictPilot AI Demo Narration", ""]
    for idx, scene in enumerate(scenes, 1):
        md_lines.append(f"## {idx}. {scene.title}")
        md_lines.append("")
        md_lines.append(scene.narration)
        md_lines.append("")
    (DELIVERABLE_DIR / "DistrictPilot_AI_Demo_Narration.md").write_text(
        "\n".join(md_lines),
        encoding="utf-8",
    )


def seconds_to_srt(seconds: float) -> str:
    ms = int(round(seconds * 1000))
    hh = ms // 3_600_000
    ms %= 3_600_000
    mm = ms // 60_000
    ms %= 60_000
    ss = ms // 1000
    ms %= 1000
    return f"{hh:02d}:{mm:02d}:{ss:02d},{ms:03d}"


def build_video(scenes: Sequence[Scene]) -> Path:
    clips = []
    srt_lines: List[str] = []
    current = 0.0
    for idx, scene in enumerate(scenes, 1):
        image_path = render_scene(scene)
        audio_path = synthesize_voice(scene)
        audio = AudioFileClip(str(audio_path))
        duration = audio.duration + 0.4
        clip = (
            ImageClip(str(image_path))
            .with_duration(duration)
            .with_audio(audio)
            .with_effects([vfx.FadeIn(0.2), vfx.FadeOut(0.2)])
        )
        clips.append(clip)

        caption = scene.narration
        srt_lines.extend(
            [
                str(idx),
                f"{seconds_to_srt(current)} --> {seconds_to_srt(current + audio.duration)}",
                caption,
                "",
            ]
        )
        current += duration

    final = concatenate_videoclips(clips, method="compose")
    out = DELIVERABLE_DIR / "DistrictPilot_AI_Demo_Narrated.mp4"
    final.write_videofile(
        str(out),
        fps=FPS,
        codec="libx264",
        audio_codec="aac",
        preset="medium",
        threads=4,
    )
    final.close()
    for clip in clips:
        clip.close()

    (DELIVERABLE_DIR / "DistrictPilot_AI_Demo_Narrated.srt").write_text(
        "\n".join(srt_lines),
        encoding="utf-8",
    )
    return out


def main() -> None:
    ensure_dirs()
    write_narration_files(SCENES)
    video_path = build_video(SCENES)
    print(f"Built video: {video_path}")


if __name__ == "__main__":
    main()
