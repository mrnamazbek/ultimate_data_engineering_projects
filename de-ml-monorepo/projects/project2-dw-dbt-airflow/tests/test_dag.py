"""Unit tests for the Airflow DAG (AST-based + optional live import)."""

import ast
import os
import sys

import pytest

DAG_FILE = os.path.join(
    os.path.dirname(__file__), "..", "airflow", "dags", "dbt_and_load.py"
)


# ── AST-based tests (no Airflow installation needed) ─────────────────────────


def _dag_source() -> str:
    with open(DAG_FILE) as f:
        return f.read()


def test_dag_id_in_source():
    """dag_id 'dbt_and_load' must appear in the DAG file."""
    assert "dbt_and_load" in _dag_source()


def test_task_ids_in_source():
    """Required task IDs must appear in the DAG file."""
    src = _dag_source()
    for task_id in ("dbt_deps", "dbt_run", "dbt_test"):
        assert task_id in src, f"task_id '{task_id}' not found in DAG"


def test_catchup_false_in_source():
    """catchup=False must be set in the DAG file."""
    src = _dag_source()
    assert "catchup=False" in src or "catchup = False" in src


def test_daily_schedule_in_source():
    """Schedule must be @daily."""
    src = _dag_source()
    assert "@daily" in src


def test_dag_file_is_valid_python():
    """DAG file must be syntactically valid Python."""
    with open(DAG_FILE) as f:
        source = f.read()
    ast.parse(source)  # raises SyntaxError if invalid


# ── Live-import tests (skipped when Airflow is not importable) ────────────────

# Try to import the DAG module once and cache the result.
_dag_module = None
_dag_import_error: str = ""

try:
    _dags_dir = os.path.join(os.path.dirname(__file__), "..", "airflow", "dags")
    sys.path.insert(0, _dags_dir)
    import importlib as _il

    _dag_module = _il.import_module("dbt_and_load")
except Exception as _exc:  # noqa: BLE001
    _dag_import_error = str(_exc)

_live_skip = pytest.mark.skipif(
    _dag_module is None,
    reason=f"Airflow not importable in this environment: {_dag_import_error}",
)


@_live_skip
def test_live_dag_id():
    assert _dag_module.dag.dag_id == "dbt_and_load"


@_live_skip
def test_live_dag_tasks():
    task_ids = {t.task_id for t in _dag_module.dag.tasks}
    assert {"dbt_deps", "dbt_run", "dbt_test"} == task_ids


@_live_skip
def test_live_dag_no_catchup():
    assert _dag_module.dag.catchup is False
