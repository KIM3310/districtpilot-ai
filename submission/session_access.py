"""Credential-free runtime detection and lazy Snowflake session access."""

from __future__ import annotations

from typing import Any


def in_streamlit_runtime() -> bool:
    """Return whether this module is executing inside a Streamlit script run."""
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
    except ImportError:
        try:
            from streamlit.scriptrunner.script_run_context import get_script_run_ctx
        except ImportError:
            return False

    try:
        return get_script_run_ctx(suppress_warning=True) is not None
    except TypeError:
        return get_script_run_ctx() is not None


def get_snowflake_session() -> Any:
    """Acquire the active Snowpark session only when a query needs it."""
    from snowflake.snowpark.context import get_active_session

    return get_active_session()
