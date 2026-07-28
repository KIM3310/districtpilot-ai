"""Regression tests for credential-free imports and lazy Snowflake access."""

from __future__ import annotations

import ast
import builtins
import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SESSION_MODULES = (
    ROOT / "session_access.py",
    ROOT / "submission" / "session_access.py",
)
APP_MODULES = (
    ROOT / "streamlit_app_v8.py",
    ROOT / "submission" / "streamlit_app_v8.py",
)


class DummyValue:
    empty = True
    columns = ()

    def __call__(self, *args, **kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def __getattr__(self, name):
        return self

    def __getitem__(self, key):
        return self

    def __setitem__(self, key, value):
        return None

    def __iter__(self):
        return iter(())

    def __bool__(self):
        return False

    def __len__(self):
        return 0


class DummyFrame(DummyValue):
    def __init__(self, *args, **kwargs):
        pass


class StreamlitStub(types.ModuleType):
    def __init__(self):
        super().__init__("streamlit")
        self.session_state = {}

    def cache_data(self, *args, **kwargs):
        if args and callable(args[0]):
            return args[0]

        def decorator(function):
            return function

        return decorator

    def tabs(self, labels):
        return [DummyValue() for _ in labels]

    def columns(self, spec):
        count = spec if isinstance(spec, int) else len(spec)
        return [DummyValue() for _ in range(count)]

    def selectbox(self, label, options, index=0, **kwargs):
        return options[index] if options else None

    def slider(self, *args, **kwargs):
        return kwargs.get("value", 0)

    def text_area(self, *args, **kwargs):
        return ""

    def button(self, *args, **kwargs):
        return False

    def form_submit_button(self, *args, **kwargs):
        return False

    def __getattr__(self, name):
        return DummyValue()


class CapturingQuery:
    def __init__(self, payload):
        self.payload = payload

    def collect(self):
        return [{"RESULTS": json.dumps({"results": self.payload})}]


class CapturingSession:
    def __init__(self):
        self.calls = []

    def sql(self, sql, params=None):
        self.calls.append({"sql": sql, "params": params})
        return CapturingQuery([{"chunk": "grounded"}])


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class LazySessionAccessTests(unittest.TestCase):
    def test_app_import_does_not_acquire_snowflake_session(self):
        calls = []
        context = types.ModuleType("snowflake.snowpark.context")

        def fake_get_active_session():
            calls.append("called")
            return DummyValue()

        context.get_active_session = fake_get_active_session
        snowflake = types.ModuleType("snowflake")
        snowpark = types.ModuleType("snowflake.snowpark")
        streamlit = StreamlitStub()
        runtime = types.ModuleType("streamlit.runtime")
        runner = types.ModuleType("streamlit.runtime.scriptrunner")
        runner.get_script_run_ctx = lambda suppress_warning=True: None
        components_package = types.ModuleType("streamlit.components")
        components = types.ModuleType("streamlit.components.v1")
        components.html = lambda *args, **kwargs: None
        pandas = types.ModuleType("pandas")
        pandas.DataFrame = DummyFrame
        pandas.isna = lambda value: value is None
        pandas.to_datetime = lambda *args, **kwargs: DummyValue()

        common_modules = {
            "pandas": pandas,
            "snowflake": snowflake,
            "snowflake.snowpark": snowpark,
            "snowflake.snowpark.context": context,
            "streamlit": streamlit,
            "streamlit.runtime": runtime,
            "streamlit.runtime.scriptrunner": runner,
            "streamlit.components": components_package,
            "streamlit.components.v1": components,
        }

        for index, (app_path, session_path) in enumerate(
            zip(APP_MODULES, SESSION_MODULES)
        ):
            session_module = load_module(
                session_path, f"districtpilot_session_app_{index}"
            )
            modules = dict(common_modules, session_access=session_module)
            with self.subTest(path=app_path), patch.dict(sys.modules, modules):
                load_module(app_path, f"districtpilot_app_import_{index}")

        self.assertEqual(calls, [])

    def test_import_does_not_import_snowflake(self):
        real_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name == "snowflake" or name.startswith("snowflake."):
                raise AssertionError(
                    "Snowflake must not be imported at module import time"
                )
            return real_import(name, *args, **kwargs)

        for index, path in enumerate(SESSION_MODULES):
            with self.subTest(path=path), patch("builtins.__import__", guarded_import):
                module = load_module(path, f"districtpilot_session_import_{index}")
                self.assertTrue(callable(module.get_snowflake_session))

    def test_session_is_acquired_only_when_requested(self):
        calls = []
        marker = object()
        context = types.ModuleType("snowflake.snowpark.context")

        def fake_get_active_session():
            calls.append("called")
            return marker

        context.get_active_session = fake_get_active_session
        snowflake = types.ModuleType("snowflake")
        snowpark = types.ModuleType("snowflake.snowpark")

        with patch.dict(
            sys.modules,
            {
                "snowflake": snowflake,
                "snowflake.snowpark": snowpark,
                "snowflake.snowpark.context": context,
            },
        ):
            module = load_module(SESSION_MODULES[0], "districtpilot_session_request")
            self.assertEqual(calls, [])
            self.assertIs(module.get_snowflake_session(), marker)
            self.assertEqual(calls, ["called"])

    def test_streamlit_runtime_detection_is_credential_free(self):
        runner = types.ModuleType("streamlit.runtime.scriptrunner")
        runner.get_script_run_ctx = lambda suppress_warning=True: object()
        streamlit = types.ModuleType("streamlit")
        runtime = types.ModuleType("streamlit.runtime")

        with patch.dict(
            sys.modules,
            {
                "streamlit": streamlit,
                "streamlit.runtime": runtime,
                "streamlit.runtime.scriptrunner": runner,
            },
        ):
            module = load_module(SESSION_MODULES[0], "districtpilot_runtime_detection")
            self.assertTrue(module.in_streamlit_runtime())

    def test_apps_have_no_eager_snowflake_session_acquisition(self):
        for path in APP_MODULES:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
            with self.subTest(path=path):
                self.assertNotIn("get_active_session", source)
                self.assertIn("get_snowflake_session", source)
                for node in tree.body:
                    if isinstance(node, (ast.Import, ast.ImportFrom)):
                        module = getattr(node, "module", "") or ""
                        self.assertFalse(module.startswith("snowflake"))
                    if isinstance(node, (ast.Assign, ast.AnnAssign)):
                        value = getattr(node, "value", None)
                        self.assertFalse(
                            isinstance(value, ast.Call)
                            and getattr(value.func, "id", "") == "get_snowflake_session"
                        )

    def test_submission_copies_match_runtime_sources(self):
        self.assertEqual(APP_MODULES[0].read_bytes(), APP_MODULES[1].read_bytes())
        self.assertEqual(
            SESSION_MODULES[0].read_bytes(), SESSION_MODULES[1].read_bytes()
        )


class SearchPolicyContextHardeningTests(unittest.TestCase):
    def load_app(self, app_path: Path, session_path: Path, index: int):
        session_module = load_module(
            session_path, f"districtpilot_search_session_{index}"
        )
        streamlit = StreamlitStub()
        runner = types.ModuleType("streamlit.runtime.scriptrunner")
        runner.get_script_run_ctx = lambda suppress_warning=True: None
        components_package = types.ModuleType("streamlit.components")
        components = types.ModuleType("streamlit.components.v1")
        components.html = lambda *args, **kwargs: None
        pandas = types.ModuleType("pandas")
        pandas.DataFrame = DummyFrame
        pandas.isna = lambda value: value is None
        pandas.to_datetime = lambda *args, **kwargs: DummyValue()
        modules = {
            "pandas": pandas,
            "session_access": session_module,
            "streamlit": streamlit,
            "streamlit.runtime": types.ModuleType("streamlit.runtime"),
            "streamlit.runtime.scriptrunner": runner,
            "streamlit.components": components_package,
            "streamlit.components.v1": components,
        }
        with patch.dict(sys.modules, modules):
            return load_module(app_path, f"districtpilot_search_app_{index}")

    def test_search_policy_context_binds_malicious_query_and_clamps_high_limit(self):
        malicious_query = "전입'; DROP TABLE policy_docs; --"
        for index, (app_path, session_path) in enumerate(
            zip(APP_MODULES, SESSION_MODULES)
        ):
            with self.subTest(path=app_path):
                app = self.load_app(app_path, session_path, index)
                session = CapturingSession()
                app.get_snowflake_session = lambda: session

                results = app.search_policy_context(malicious_query, "999")

                self.assertEqual(results, [{"chunk": "grounded"}])
                self.assertEqual(len(session.calls), 1)
                call = session.calls[0]
                self.assertEqual(
                    call["sql"],
                    "SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(?, ?) AS RESULTS",
                )
                self.assertEqual(call["params"][0], app.SEARCH_SERVICE_FQN)
                query_parameters = json.loads(call["params"][1])
                self.assertEqual(query_parameters["query"], malicious_query)
                self.assertEqual(query_parameters["limit"], 10)
                self.assertNotIn(malicious_query, call["sql"])
                self.assertNotIn("DROP TABLE", call["sql"])

    def test_search_policy_context_clamps_low_and_invalid_limits(self):
        app = self.load_app(APP_MODULES[0], SESSION_MODULES[0], 99)
        session = CapturingSession()
        app.get_snowflake_session = lambda: session

        app.search_policy_context("policy", 0)
        app.search_policy_context("policy", "1; DROP TABLE")

        self.assertEqual(json.loads(session.calls[0]["params"][1])["limit"], 1)
        self.assertEqual(json.loads(session.calls[1]["params"][1])["limit"], 3)

    def test_search_service_identifier_validation_rejects_uncontrolled_names(self):
        app = self.load_app(APP_MODULES[0], SESSION_MODULES[0], 100)

        with self.assertRaises(ValueError):
            app.quote_snowflake_identifier_path(
                'DISTRICTPILOT_AI.ANALYTICS.SVC"; DROP TABLE X; --'
            )


if __name__ == "__main__":
    unittest.main()
