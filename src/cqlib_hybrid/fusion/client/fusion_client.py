"""Pure-Python client for the member hybrid-job (v2) cloud API."""

from __future__ import annotations

import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any, Optional, Union
from urllib.parse import quote, unquote

from ..config import resolve_login_key
from ..exceptions import ConfirmationRequired, CqfusionRequestError
from ._archive import validate_archive
from ._decision import (
    READY,
    FusionCandidate,
    FusionDecision,
    FusionSchedule,
    ResourceChoice,
    ResourceChoices,
    annotate,
    top_candidates,
    wait_signal,
)
from ._transport import (
    Transport,
    api_error,
    fail,
    invalid,
    parse_origin,
    positive_seconds,
    require_requests,
)
from .models import FusionJob, ResourcePlan

DEFAULT_HOST = "https://qc.zdxlz.com"
JOBS = "/qccp-hybrid/hybrid/job/v2"
SCHEDULE = "/qccp-quantum/fusion/strategy/schedule"
DISPATCH_MODES = ("intelligent", "manual")
PREFERENCES = ("quality", "speed", "cost", "balanced")

PathLike = Union[Path, str]


class FusionJobHandle:
    """A submitted job: the task ID, its first snapshot, ``wait`` and ``download``."""

    def __init__(self, client: "FusionClient", task: dict):
        self._client = client
        self._snapshot = task
        self._task_id = str(task["id"])

    def task_id(self) -> str:
        return self._task_id

    def status(self) -> dict:
        """The job as returned by the submit call."""
        return dict(self._snapshot)

    def wait(self, timeout: float = 3600.0, poll_interval: float = 5.0) -> FusionJob:
        return self._client.wait_job(
            self._task_id, timeout=timeout, poll_interval=poll_interval
        )

    def download(self, dest_dir: PathLike) -> Path:
        return self._client.download_job_result(self._task_id, dest_dir)


class FusionClient:
    """Fusion cloud API. Authenticate with ``token`` and/or ``login_key``.

    The login key is read from ``TIANYAN_API_KEY``, ``CQFUSION_LOGIN_KEY`` or
    ``LOGIN_KEY`` when not given, and the host from ``CQLIB_HYBRID_HOST`` (then
    ``CQFUSION_HOST``) before falling back to the production platform. Login
    happens on first use.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        token: str = "",
        login_key: Optional[str] = None,
        timeout: float = 120,
        *,
        session: Any = None,
    ):
        host = (
            host
            or os.environ.get("CQLIB_HYBRID_HOST")
            or os.environ.get("CQFUSION_HOST")
            or DEFAULT_HOST
        )
        self.host = host.rstrip("/")
        parse_origin(self.host)
        self.login_key = resolve_login_key(login_key)
        self.token = token
        self.timeout = positive_seconds(timeout, "timeout")
        self._session = session
        self._transport: Optional[Transport] = None

    # -- plumbing -----------------------------------------------------------

    def _http(self) -> Transport:
        if self._transport is None:
            if not self.token and not self.login_key:
                raise CqfusionRequestError(
                    "set TIANYAN_API_KEY, CQFUSION_LOGIN_KEY or LOGIN_KEY, or pass token"
                )
            self._transport = Transport(
                self.host,
                token=self.token,
                api_key=self.login_key or "",
                timeout=self.timeout,
                session=self._session,
            )
        return self._transport

    def _exchange(
        self, method, path, query=None, *, json_body=None, upload=None, timeout=None
    ):
        response = self._http().request(
            method,
            path,
            query=query,
            json_body=json_body,
            upload=upload,
            timeout=timeout,
        )
        with response:
            return response.status_code, response.text

    def _member(
        self, method, path, query=None, *, json_body=None, upload=None, timeout=None
    ) -> Any:
        status, text = self._exchange(
            method, path, query, json_body=json_body, upload=upload, timeout=timeout
        )
        if not text:
            return _member_payload(status, None)
        try:
            payload = json.loads(text)
        except ValueError:
            raise fail(
                "invalid_input",
                "fusion response is not valid JSON",
                http_status=status,
            ) from None
        return _member_payload(status, payload)

    # -- authentication -----------------------------------------------------

    def login(self, login_key: Optional[str] = None) -> str:
        key = resolve_login_key(login_key or self.login_key)
        if not key:
            raise CqfusionRequestError("login_key is required")
        self.login_key = key
        if self._transport is None:
            self._transport = Transport(
                self.host,
                token="",
                api_key=key,
                timeout=self.timeout,
                session=self._session,
            )
            return self._transport.access_token
        return self._transport.login(key, self.timeout)

    # -- scheduling and submission -------------------------------------------

    def upload_job_package(self, package: PathLike) -> dict:
        """Validated multipart upload; nothing is sent for an invalid archive."""
        validate_archive(package)
        return self._member("POST", f"{JOBS}/upload", upload=Path(package))

    def schedule_package(
        self, package: PathLike, strategy: str = "balanced"
    ) -> FusionSchedule:
        """Upload and assess a ZIP without creating a job."""
        strategy = _schedule_strategy(strategy)
        file_id = _file_id(self.upload_job_package(package))
        assessed = self._member(
            "POST",
            SCHEDULE,
            [("refresh", "false"), ("details", "false")],
            json_body={"fileId": file_id, "strategy": strategy},
        )
        choices = self._resource_choices()
        assessed = assessed if isinstance(assessed, dict) else {}
        candidates: list = []
        if assessed.get("recommended_group_id"):
            ranked = assessed.get("ranked_candidates") or []
            candidates = top_candidates(
                [
                    _candidate_from_json(item, index, choices)
                    for index, item in enumerate(ranked)
                ]
            )
        return FusionSchedule(file_id, candidates, choices)

    def submit_decision(self, name: str, decision: FusionDecision) -> FusionJobHandle:
        """Submit a confirmed decision and return the job handle."""
        task = self._submit_decision(name, decision)
        if not isinstance(task, dict) or not task.get("id"):
            raise invalid("submit response is missing id")
        return FusionJobHandle(self, task)

    def _submit_decision(self, name: str, decision: FusionDecision) -> Any:
        name = name.strip()
        if not name or len(name) > 50:
            raise invalid("task name must be 1 to 50 characters")
        _nonempty(decision.file_id, "fileId")
        _nonempty(decision.qpu_code, "qpuResourceCode")
        _nonempty(decision.cpu_code, "cpuResourceCode")
        if decision.scheduling_mode not in ("MANUAL", "SMART"):
            raise invalid("schedulingMode must be MANUAL or SMART")
        body = {
            "taskName": name,
            "fileId": decision.file_id,
            "qpuResourceCode": decision.qpu_code,
            "cpuResourceCode": decision.cpu_code,
            "schedulingMode": decision.scheduling_mode,
            "clientRequestId": f"sdk-{time.time_ns():x}",
        }
        if decision.scheduling_mode == "SMART" and decision.scheduling_preference:
            body["schedulingPreference"] = decision.scheduling_preference
        status, text = self._exchange("POST", f"{JOBS}/submit", json_body=body)
        try:
            payload = json.loads(text)
        except ValueError:
            payload = None
        data = payload.get("data", payload) if isinstance(payload, dict) else None
        pending = _unconfirmed_id(data)
        if pending is None:
            return _member_payload(status, payload if text else None)
        # The server accepted the request but could not confirm it: retry that
        # same job with the same client request ID instead of creating another.
        return self._member(
            "POST", _job_path(pending, "/retry"), json_body=body
        )

    def submit_job(
        self,
        *,
        name: str,
        package: PathLike,
        dispatch_mode: str = "intelligent",
        dispatch_preference: str = "balanced",
        quantum_resource: Optional[str] = None,
        hpc_resource: Optional[str] = None,
        auto_confirm: bool = False,
        plan_id: Optional[str] = None,
    ) -> FusionJob:
        """Submit using the compatibility-shaped options.

        Without ``plan_id`` or ``auto_confirm`` in intelligent mode this raises
        :class:`ConfirmationRequired` carrying the recommendations; no job is
        created in that case.
        """
        _nonempty(name, "name")
        if dispatch_mode not in DISPATCH_MODES:
            raise invalid("unknown dispatch_mode")
        if dispatch_preference not in PREFERENCES:
            raise invalid("unknown dispatch_preference")
        if len(name.strip()) > 50:
            raise invalid("task name must be 1 to 50 characters")
        manual = dispatch_mode == "manual"
        if manual and not (
            (quantum_resource or "").strip() and (hpc_resource or "").strip()
        ):
            raise invalid("manual dispatch requires quantum_resource and hpc_resource")
        if manual:
            schedule = FusionSchedule(
                _file_id(self.upload_job_package(package)),
                [],
                self._resource_choices(),
            )
        else:
            schedule = self.schedule_package(package, dispatch_preference)
        plans = [
            {
                "plan_id": c.group_id,
                "rank": c.rank,
                "quantum_resource": c.qpu_code,
                "hpc_resource": c.cpu_code,
                "selectable": c.selectable,
                "reject_reason": c.reject_reason,
                "fileId": schedule.file_id,
            }
            for c in schedule._candidates
        ]
        decision: Optional[FusionDecision] = None
        if manual:
            decision = schedule.manual(quantum_resource, hpc_resource)
        elif plan_id:
            chosen = next(
                (c for c in schedule._candidates if c.group_id == plan_id), None
            )
            if chosen is None:
                raise invalid(
                    "unknown plan_id; schedule again and choose a current recommendation"
                )
            decision = schedule.select(chosen.rank)
        elif auto_confirm:
            chosen = next((c for c in schedule._candidates if c.selectable), None)
            if chosen is None:
                raise invalid(
                    "no selectable recommendation; choose available resource codes manually"
                )
            decision = schedule.select(chosen.rank)
        if decision is None:
            raise ConfirmationRequired(
                FusionJob.from_api(
                    {
                        "fileId": schedule.file_id,
                        "name": name,
                        "status": "awaiting_confirm",
                    }
                ),
                [ResourcePlan.from_api(p, rank=i) for i, p in enumerate(plans, 1)],
            )
        if decision.scheduling_mode == "SMART":
            decision.scheduling_preference = _schedule_strategy(
                dispatch_preference
            ).upper()
        task = self._submit_decision(name, decision)
        if not isinstance(task, dict):
            raise invalid("expected task object")
        return FusionJob.from_api(task)

    # -- jobs ---------------------------------------------------------------

    def list_jobs(
        self, *, page_no: int = 1, page_size: int = 20, view: str = "ALL"
    ) -> dict:
        """One page of parent jobs: ``total``, ``pageNo``, ``pageSize``, ``records``."""
        if page_no < 1 or not 1 <= page_size <= 100 or view not in ("ALL", "DRAFT"):
            raise invalid("pageNo must be positive, pageSize 1..100, view ALL or DRAFT")
        return self._member(
            "GET",
            f"{JOBS}/tasks",
            [("pageNo", str(page_no)), ("pageSize", str(page_size)), ("view", view)],
        )

    def presets(self) -> list:
        return self._member("GET", f"{JOBS}/presets")

    def progress(self, task_id: str, *, _timeout: Optional[float] = None) -> dict:
        return self._member(
            "GET", _job_path(task_id, "/progress"), timeout=_timeout
        )

    def get_job(self, task_id: str) -> FusionJob:
        return FusionJob.from_api(self._member("GET", _job_path(task_id, "")))

    def delete_job(self, task_id: str) -> dict:
        """Soft-delete a draft or finished job; the server checks eligibility."""
        return self._member("DELETE", _job_path(task_id, ""))

    def wait_job(
        self, task_id: str, *, poll_interval: float = 5.0, timeout: float = 3600.0
    ) -> FusionJob:
        timeout = positive_seconds(timeout, "timeout")
        interval = positive_seconds(poll_interval, "poll_interval")
        started = time.monotonic()
        while True:
            left = timeout - (time.monotonic() - started)
            if left <= 0:
                raise fail("timeout", "timeout waiting for fusion task")
            task = self.progress(task_id, _timeout=min(self.timeout, left))
            if not isinstance(task, dict):
                raise invalid("task status is missing")
            if wait_signal(task.get("status")) == READY:
                # /progress carries no ID; keep the one that was requested.
                task["id"] = task_id
                return FusionJob.from_api(task)
            time.sleep(max(min(interval, timeout - (time.monotonic() - started)), 0))

    def download_job_result(self, task_id: str, dest_dir: PathLike) -> Path:
        return self._download(task_id, "result", dest_dir, "json")

    def download_job_log(self, task_id: str, dest_dir: PathLike) -> Path:
        return self._download(task_id, "log", dest_dir, "log")

    def _download(self, task_id: str, kind: str, dest_dir: PathLike, ext: str) -> Path:
        requests = require_requests()
        path = _job_path(task_id, f"/{kind}")
        response = self._http().request("GET", path, stream=True)
        with response:
            name = download_name(response.headers.get("Content-Disposition", ""))
            if name is None:
                name = f"fusion-{safe_basename(task_id) or 'task'}-{kind}.{ext}"
            dest = Path(dest_dir)
            try:
                dest.mkdir(parents=True, exist_ok=True)
                # Stream into a temporary file and replace the target only once
                # the whole body arrived, so a failed download keeps old files.
                fd, temp = tempfile.mkstemp(dir=dest, prefix=".download-")
                try:
                    with os.fdopen(fd, "wb") as out:
                        for chunk in response.iter_content(1 << 16):
                            out.write(chunk)
                        out.flush()
                        os.fsync(out.fileno())
                    os.replace(temp, dest / name)
                except BaseException:
                    if os.path.exists(temp):
                        os.unlink(temp)
                    raise
            except OSError as exc:
                if isinstance(exc, requests.RequestException):
                    raise fail("transport", str(exc)) from exc
                raise fail("io", str(exc)) from exc
        return dest / name

    # -- resources ----------------------------------------------------------

    def _resource_choices(self) -> ResourceChoices:
        qpu: list = []
        cpu: list = []
        page = 1
        while True:
            data = self._member(
                "GET",
                f"{JOBS}/resources/choices",
                [("pageNo", str(page)), ("pageSize", "100")],
            )
            data = data if isinstance(data, dict) else {}
            qpu.extend(_choice_page(data.get("qpu")))
            cpu.extend(_choice_page(data.get("cpu")))
            qpu_total = _as_count(data.get("qpuTotal"), len(qpu))
            cpu_total = _as_count(data.get("cpuTotal"), len(cpu))
            page += 1
            if (len(qpu) >= qpu_total and len(cpu) >= cpu_total) or page > 100:
                break
        return ResourceChoices(qpu, cpu)


# -- helpers -----------------------------------------------------------------


def _nonempty(value: Any, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise invalid(f"{field} is required")


def _file_id(upload: Any) -> str:
    file_id = upload.get("fileId") if isinstance(upload, dict) else None
    if not file_id or not isinstance(file_id, str):
        raise invalid("upload response is missing fileId")
    return file_id


def _job_path(task_id: str, suffix: str) -> str:
    task_id = str(task_id)
    _nonempty(task_id, "task_id")
    if task_id in (".", ".."):
        raise invalid("invalid task_id")
    return f"{JOBS}/tasks/{quote(task_id, safe='')}{suffix}"


def _member_payload(status: int, payload: Any) -> Any:
    envelope = isinstance(payload, dict) and "code" in payload
    if status >= 400 or (envelope and payload["code"] != 200):
        raise api_error(status, payload, "fusion request failed")
    return payload.get("data") if envelope else payload


def _schedule_strategy(value: str) -> str:
    table = {
        "balanced": "balanced",
        "BALANCED": "balanced",
        "quality": "quality_first",
        "quality_first": "quality_first",
        "QUALITY_FIRST": "quality_first",
        "speed": "speed_first",
        "speed_first": "speed_first",
        "SPEED_FIRST": "speed_first",
        "cost": "cost_first",
        "cost_first": "cost_first",
        "COST_FIRST": "cost_first",
        "unified": "unified",
    }
    try:
        return table[value]
    except (KeyError, TypeError):
        raise invalid("unknown scheduling strategy") from None


def _unconfirmed_id(data: Any) -> Optional[str]:
    if not isinstance(data, dict):
        return None
    state = data.get("submissionState")
    unconfirmed = (
        isinstance(state, str) and state.upper() == "UNCONFIRMED"
    ) or data.get("errorCode") == "FUSION_QRMI_UNCONFIRMED"
    if not unconfirmed:
        return None
    for key in ("id", "rootId"):
        if isinstance(data.get(key), str) and data[key]:
            return data[key]
    return None


def _as_count(value: Any, default: int) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else default


def _choice_page(items: Any) -> list:
    choices = []
    for item in items if isinstance(items, list) else []:
        code = item.get("code") if isinstance(item, dict) else None
        if isinstance(code, str) and code:
            available = item.get("available", True)
            choices.append(
                ResourceChoice(code, available if isinstance(available, bool) else True)
            )
    return choices


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _candidate_from_json(
    value: dict, index: int, choices: ResourceChoices
) -> FusionCandidate:
    qpu = value.get("qpu") if isinstance(value.get("qpu"), dict) else {}
    classic = value.get("classic") if isinstance(value.get("classic"), dict) else {}
    device_id = _text(qpu.get("device_id"))
    resource_id = _text(classic.get("resource_id"))
    meets = value.get("meets_all_constraints") is True
    qpu_code, cpu_code, selectable, reason = annotate(
        device_id, resource_id, meets, choices
    )
    rank = value.get("rank")
    return FusionCandidate(
        rank=rank if isinstance(rank, int) and not isinstance(rank, bool) else index + 1,
        group_id=_text(value.get("group_id")),
        device_id=device_id,
        resource_id=resource_id,
        qpu_code=qpu_code,
        cpu_code=cpu_code,
        meets_all_constraints=meets,
        scores=value.get("scores"),
        selectable=selectable,
        reject_reason=reason,
    )


def safe_basename(name: str) -> Optional[str]:
    base = name.replace("\\", "/").rsplit("/", 1)[-1].strip()
    if (
        not base
        or base in (".", "..")
        or re.search(r'[\x00-\x1f\x7f:*?"<>|]', base)
    ):
        return None
    return base


def download_name(header: str) -> Optional[str]:
    """File name from Content-Disposition, reduced to a safe basename."""
    fallback = None
    for item in header.split(";"):
        key, sep, value = item.strip().partition("=")
        if not sep:
            continue
        value = value.strip().strip('"')
        key = key.strip().lower()
        if key == "filename*":
            parts = value.split("'", 2)
            if len(parts) == 3 and parts[0].lower() == "utf-8":
                try:
                    decoded = unquote(parts[2], errors="strict")
                except UnicodeDecodeError:
                    continue
                name = safe_basename(decoded)
                if name:
                    return name
        elif key == "filename":
            fallback = safe_basename(value)
    return fallback
