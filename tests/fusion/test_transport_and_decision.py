"""Retry/refresh semantics of the transport and the pure scheduling rules."""

import json
from collections import deque
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("requests")
import requests  # noqa: E402

from cqlib_hybrid.fusion.client import _decision as decision  # noqa: E402
from cqlib_hybrid.fusion.client import _transport  # noqa: E402
from cqlib_hybrid.fusion.client.fusion_client import (  # noqa: E402
    FusionClient,
    download_name,
    safe_basename,
)
from cqlib_hybrid.fusion.exceptions import CqfusionRequestError  # noqa: E402


class Reply:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status
        self.headers = headers or {}
        self.text = body if isinstance(body, str) else json.dumps(body)

    def json(self):
        return json.loads(self.text)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class FakeSession:
    def __init__(self, *replies):
        self.replies = deque(replies)
        self.calls = []
        self.logins = []

    def _next(self):
        reply = self.replies.popleft()
        if isinstance(reply, Exception):
            raise reply
        return reply

    def post(self, url, data=None, **kwargs):
        self.logins.append(data)
        return Reply(
            200, {"code": 0, "data": {"access_token": f"token-{len(self.logins)}"}}
        )

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self._next()


def ok(data):
    return Reply(200, {"code": 200, "data": data})


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(_transport.time, "sleep", lambda seconds: None)


def client_for(session, **kwargs):
    kwargs.setdefault("token", "t0")
    return FusionClient(host="http://example.test", session=session, **kwargs)


def test_unauthorized_refreshes_once_and_retries():
    session = FakeSession(Reply(401, {}), ok({"status": 8}))
    client = client_for(session, login_key="key")
    assert client.progress("1") == {"status": 8}
    assert session.logins == [
        {"grant_type": "openId", "openId": "key", "account_type": "member"}
    ]
    assert session.calls[0][2]["headers"]["basicToken"] == "t0"
    assert session.calls[1][2]["headers"]["basicToken"] == "token-1"
    assert session.calls[1][2]["headers"]["Authorization"] == "Bearer token-1"


def test_unauthorized_without_login_key_is_an_error():
    session = FakeSession(Reply(401, {"msg": "expired"}))
    with pytest.raises(CqfusionRequestError, match="token expired"):
        client_for(session).progress("1")
    assert len(session.calls) == 1


def test_reads_retry_server_errors_but_mutations_do_not():
    session = FakeSession(Reply(503, "busy"), Reply(500, "busy"), ok({"status": 8}))
    assert client_for(session).progress("1") == {"status": 8}
    assert len(session.calls) == 3

    session = FakeSession(Reply(503, "busy"), ok({}))
    with pytest.raises(CqfusionRequestError) as caught:
        client_for(session).delete_job("1")
    assert (caught.value.kind, caught.value.status_code) == ("api", 503)
    assert len(session.calls) == 1


def test_reads_give_up_after_two_retries():
    session = FakeSession(*[Reply(500, "x") for _ in range(3)])
    with pytest.raises(CqfusionRequestError):
        client_for(session).progress("1")
    assert len(session.calls) == 3


def test_mutation_is_not_replayed_after_connection_loss():
    session = FakeSession(requests.ConnectionError("reset"))
    with pytest.raises(CqfusionRequestError) as caught:
        client_for(session).delete_job("1")
    assert caught.value.kind == "transport"
    assert len(session.calls) == 1


def test_timeout_is_reported_as_timeout():
    session = FakeSession(requests.Timeout("slow"))
    with pytest.raises(CqfusionRequestError) as caught:
        client_for(session).delete_job("1")
    assert caught.value.kind == "timeout"


def test_unconfirmed_submit_retries_the_same_job_with_the_same_body():
    session = FakeSession(
        ok({"submissionState": "UNCONFIRMED", "id": "7"}),
        ok({"id": "7", "status": 2}),
    )
    handle = client_for(session).submit_decision(
        "demo", decision.FusionDecision("file", "qpu", "cpu", "MANUAL")
    )
    assert handle.task_id() == "7"
    first, retry = session.calls
    assert first[1].endswith("/hybrid/job/v2/submit")
    assert retry[1].endswith("/hybrid/job/v2/tasks/7/retry")
    assert first[2]["json"] == retry[2]["json"]


def test_submit_decision_validates_before_any_request():
    session = FakeSession()
    client = client_for(session)
    for name, item in [
        ("", decision.FusionDecision("f", "q", "c", "MANUAL")),
        ("x" * 51, decision.FusionDecision("f", "q", "c", "MANUAL")),
        ("demo", decision.FusionDecision("", "q", "c", "MANUAL")),
        ("demo", decision.FusionDecision("f", "q", "c", "OTHER")),
    ]:
        with pytest.raises(CqfusionRequestError):
            client.submit_decision(name, item)
    assert not session.calls


def test_host_must_be_an_origin():
    for host in ("ftp://x", "http://u:p@x", "http://x/path", "http://x?a=1"):
        with pytest.raises(CqfusionRequestError):
            FusionClient(host=host, token="t")


def test_missing_credentials_are_reported_on_first_use(monkeypatch):
    for name in ("TIANYAN_API_KEY", "CQFUSION_LOGIN_KEY", "LOGIN_KEY"):
        monkeypatch.delenv(name, raising=False)
    client = FusionClient(host="http://example.test", session=FakeSession())
    with pytest.raises(CqfusionRequestError, match="TIANYAN_API_KEY"):
        client.progress("1")


def test_task_id_is_escaped_and_dots_rejected():
    session = FakeSession(ok({"status": 8}))
    client_for(session).progress("a/b c")
    assert session.calls[0][1].endswith("/tasks/a%2Fb%20c/progress")
    for bad in ("", ".", ".."):
        with pytest.raises(CqfusionRequestError):
            client_for(FakeSession()).progress(bad)


# -- scheduling rules ---------------------------------------------------------


def choices(available=True):
    return decision.ResourceChoices(
        [decision.ResourceChoice("qpu-1", available)],
        [decision.ResourceChoice("cpu-1")],
    )


def candidate(rank, meets=True):
    qpu, cpu, selectable, reason = decision.annotate("qpu-1", "cpu-1", meets, choices())
    return decision.FusionCandidate(
        rank, f"rg-{rank}", "qpu-1", "cpu-1", qpu, cpu, meets, {"q": 1}, selectable, reason
    )


def test_only_three_lowest_ranks_are_kept():
    ranked = decision.top_candidates([candidate(r) for r in (4, 1, 3, 2)])
    assert [item.rank for item in ranked] == [1, 2, 3]


def test_select_rejects_unmet_constraints_and_unknown_rank():
    schedule = decision.FusionSchedule("f", [candidate(1, meets=False)], choices())
    with pytest.raises(CqfusionRequestError, match="constraints"):
        schedule.select(1)
    with pytest.raises(CqfusionRequestError):
        schedule.select(2)


def test_manual_matches_codes_not_display_names():
    schedule = decision.FusionSchedule("f", [], choices())
    assert schedule.manual("qpu-1", "cpu-1").scheduling_mode == "MANUAL"
    with pytest.raises(CqfusionRequestError):
        schedule.manual("量子资源 A", "cpu-1")


def test_unavailable_resource_fails_before_submit():
    with pytest.raises(CqfusionRequestError, match="unavailable"):
        decision.match_resources("qpu-1", "cpu-1", choices(available=False))


def test_candidates_are_plain_dictionaries():
    schedule = decision.FusionSchedule("f", [candidate(1)], choices())
    assert schedule.candidates()[0]["selectable"] is True


def test_wait_signal():
    assert decision.wait_signal(6) == decision.READY
    assert decision.wait_signal(8) == decision.CONTINUE
    assert decision.wait_signal(3) == decision.CONTINUE
    for bad in (9, 0, None, "6"):
        with pytest.raises(CqfusionRequestError):
            decision.wait_signal(bad)


def test_download_name_is_reduced_to_a_safe_basename():
    assert download_name('attachment; filename="../../x.zip"') == "x.zip"
    assert download_name("attachment; filename*=UTF-8''r%C3%A9sum%C3%A9.json") == "résumé.json"
    assert download_name('attachment; filename="a:b.zip"') is None
    assert download_name("") is None
    assert safe_basename("..") is None


def test_wait_job_times_out(monkeypatch):
    session = FakeSession(*[ok({"status": 8}) for _ in range(50)])
    ticks = iter(range(0, 1000))
    monkeypatch.setattr(
        "cqlib_hybrid.fusion.client.fusion_client.time.monotonic", lambda: next(ticks)
    )
    monkeypatch.setattr(
        "cqlib_hybrid.fusion.client.fusion_client.time.sleep", lambda s: None
    )
    with pytest.raises(CqfusionRequestError) as caught:
        client_for(session).wait_job("1", timeout=3, poll_interval=1)
    assert caught.value.kind == "timeout"


def test_wait_rejects_non_positive_timeout():
    with pytest.raises(CqfusionRequestError):
        client_for(FakeSession()).wait_job("1", timeout=-1)


def test_qrmi_runner_runs_circuits_as_consecutive_iterations():
    from cqlib_hybrid.exceptions import QRMIQuantumError
    from cqlib_hybrid.fusion.runtime.quantum.qrmi import QRMIQuantumRunner

    client = SimpleNamespace(
        submit_quantum=Mock(side_effect=[1, 2]),
        wait_quantum_result=Mock(
            side_effect=[
                {"taskStatus": 3, "result": {"counts": {"0": 5}}},
                {"taskStatus": 3, "result": [1, 2]},
            ]
        ),
    )
    runner = QRMIQuantumRunner(client, timeout=30)
    result = runner.run("q", {"qcis": ["H Q0", "X Q0"], "shots": 5})
    assert result.output["task_ids"] == ["qrmi-1", "qrmi-2"]
    assert [r["result"] for r in result.output["results"]] == [{"counts": {"0": 5}}, [1, 2]]
    assert "counts" not in result.output
    client.wait_quantum_result.assert_called_with(2, timeout=30.0)

    single = SimpleNamespace(
        submit_quantum=Mock(return_value=1),
        wait_quantum_result=Mock(
            return_value={"taskStatus": 3, "result": {"counts": {"1": 3}}}
        ),
    )
    out = QRMIQuantumRunner(single).run("q", {"qcis": "M Q0", "shots": 3}).output
    assert out["counts"] == {"1": 3}

    failing = SimpleNamespace(
        submit_quantum=Mock(return_value=1),
        wait_quantum_result=Mock(
            return_value={"taskStatus": 4, "result": None, "iterationNo": 1}
        ),
    )
    with pytest.raises(QRMIQuantumError):
        QRMIQuantumRunner(failing).run("q", {"qcis": "M Q0"})

    untouched = SimpleNamespace(submit_quantum=Mock(), wait_quantum_result=Mock())
    with pytest.raises(ValueError):
        QRMIQuantumRunner(untouched).run("q", {"qcis": [""], "shots": 0})
    untouched.submit_quantum.assert_not_called()


def test_host_comes_from_environment(monkeypatch):
    monkeypatch.delenv("CQFUSION_HOST", raising=False)
    monkeypatch.setenv("CQLIB_HYBRID_HOST", "http://10.0.0.1:8849/")
    assert FusionClient(token="t").host == "http://10.0.0.1:8849"
    assert FusionClient(host="http://other.test", token="t").host == "http://other.test"
    monkeypatch.delenv("CQLIB_HYBRID_HOST")
    assert FusionClient(token="t").host == "https://qc.zdxlz.com"
