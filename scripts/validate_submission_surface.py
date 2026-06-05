"""Validate DistrictPilot submission artifacts stay reviewable offline."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SUBMISSION = ROOT / "submission"

SQL_ARTIFACTS = [
    "00_rename_database.sql",
    "02_feature_mart_v4.sql",
    "03_ml_and_cortex_v2.sql",
    "06_semantic_view.sql",
    "07_dynamic_tables_tasks.sql",
    "08_ajd_integration.sql",
    "09_cortex_search_agent.sql",
    "10_external_data.sql",
    "11_ablation_study.sql",
    "12_final_precheck.sql",
    "13_live_app_compatibility_patch.sql",
    "14_judge_fastpath.sql",
    "MASTER_DEPLOY.sql",
]

SUBMISSION_DOCS = [
    "README.md",
    "DOMAIN_POSITIONING.md",
    "JUDGE_FASTPATH.md",
    "DEMO_SCRIPT.md",
    "SUBMISSION_CHECKLIST.md",
    "FINAL_PRE_SUBMISSION_RUNBOOK.md",
]

EXPECTED_TABS = [
    "Capture Plan",
    "Move-in Signals",
    "AI Playbook",
    "Scenario Lab",
    "Ops / Trust",
]

REQUIRED_SQL_MARKERS = [
    "DISTRICTPILOT",
    "FEATURE_MART",
    "FORECAST",
]


def fail(message: str) -> None:
    raise SystemExit(f"submission surface validation failed: {message}")


def assert_same_file(relative_path: str) -> None:
    root_file = ROOT / relative_path
    submission_file = SUBMISSION / relative_path
    if not root_file.is_file():
        fail(f"missing root artifact: {relative_path}")
    if not submission_file.is_file():
        fail(f"missing submission artifact: {relative_path}")
    if root_file.read_bytes() != submission_file.read_bytes():
        fail(f"submission copy drifted from root artifact: {relative_path}")


def assert_submission_doc(relative_path: str) -> None:
    root_file = ROOT / relative_path
    submission_file = SUBMISSION / relative_path
    if not root_file.is_file():
        fail(f"missing root document: {relative_path}")
    if not submission_file.is_file():
        fail(f"missing submission document: {relative_path}")

    text = submission_file.read_text(encoding="utf-8")
    for marker in ("DistrictPilot", "Snowflake"):
        if marker not in text:
            fail(f"submission document missing marker {marker}: {relative_path}")


def main() -> None:
    if not SUBMISSION.is_dir():
        fail("missing submission directory")

    for artifact in SQL_ARTIFACTS:
        assert_same_file(artifact)

    for doc in SUBMISSION_DOCS:
        assert_submission_doc(doc)

    streamlit = (ROOT / "streamlit_app_v8.py").read_text(encoding="utf-8")
    for tab in EXPECTED_TABS:
        if tab not in streamlit:
            fail(f"Streamlit app missing expected tab: {tab}")

    for artifact in SQL_ARTIFACTS:
        text = (ROOT / artifact).read_text(encoding="utf-8")
        if not any(marker in text for marker in REQUIRED_SQL_MARKERS):
            fail(f"SQL artifact has no DistrictPilot marker: {artifact}")

    print("submission surface validation ok")


if __name__ == "__main__":
    main()
