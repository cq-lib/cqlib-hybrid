import importlib
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cqlib_hybrid.fusion.config import resolve_login_key
from cqlib_hybrid.fusion.runtime.quantum.tianyan import TianYanQuantumRunner


def test_login_key_precedence(monkeypatch):
    for key in ("TIANYAN_API_KEY", "CQFUSION_LOGIN_KEY", "LOGIN_KEY"):
        monkeypatch.delenv(key, raising=False)
    assert resolve_login_key() is None
    monkeypatch.setenv("LOGIN_KEY", "old")
    assert resolve_login_key() == "old"
    monkeypatch.setenv("CQFUSION_LOGIN_KEY", "compat")
    assert resolve_login_key() == "compat"
    monkeypatch.setenv("TIANYAN_API_KEY", "new")
    assert resolve_login_key() == "new"
    assert resolve_login_key("explicit") == "explicit"


def runner(results):
    task = SimpleNamespace(task_ids=["t1", "t2"], wait=Mock(return_value=results))
    backend = SimpleNamespace(run=Mock(return_value=task))
    platform = SimpleNamespace(get_backend=Mock(return_value=backend))
    return TianYanQuantumRunner(platform, "machine", transpile=False), task, backend


def test_batch_quantum_preserves_all_results_without_legacy_import(monkeypatch):
    import builtins

    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        assert name != "cqlib" and not name.startswith("cqlib."), (
            "companion cqlib imported without transpilation"
        )
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    r, task, backend = runner(
        [
            SimpleNamespace(task_id="t1", counts={"0": 2}, shots=2),
            SimpleNamespace(task_id="t2", counts={"1": 2}, shots=2),
        ]
    )
    result = r.run("quantum", {"qcis": ["H Q1", "X Q1"], "shots": 2})
    backend.run.assert_called_once_with(["H Q1", "X Q1"], shots=2)
    task.wait.assert_called_once_with(timeout=3600.0, poll_interval=5.0)
    assert result.output["task_ids"] == ["t1", "t2"]
    assert [item["counts"] for item in result.output["results"]] == [{"0": 2}, {"1": 2}]
    assert "counts" not in result.output
    json.dumps(result.output, allow_nan=False)


def test_single_result_keeps_counts():
    r, _, _ = runner([SimpleNamespace(task_id="t1", counts={"00": 4}, shots=4)])
    assert r.run("q", {"qcis": "M Q1"}).output["counts"] == {"00": 4}


def test_quantum_failure_does_not_become_success():
    r, task, _ = runner([])
    task.wait.side_effect = TimeoutError("still running")
    with pytest.raises(TimeoutError):
        r.run("q", {"qcis": "M Q1"})


def test_invalid_quantum_payload_never_submits():
    r, _, backend = runner([])
    with pytest.raises(ValueError):
        r.run("q", {"qcis": [""], "shots": 0})
    backend.run.assert_not_called()
