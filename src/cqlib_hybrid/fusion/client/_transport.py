"""Authenticated HTTP transport for the fusion cloud API.

Retry policy: only reads (GET) retry transient failures. A mutation may refresh
the token once after a 401, but is never replayed after a timeout, connection
loss, 429 or 5xx.
"""

from __future__ import annotations

import math
import threading
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit

from ..exceptions import CqfusionRequestError

LOGIN_PATH = "/qccp-auth/oauth2/sdk/opnId"
# The platform does not publish a token lifetime; be conservative (3 hours).
TOKEN_VALID_SECS = 3 * 60 * 60
MAX_READ_RETRIES = 2


def fail(
    kind: str,
    message: str,
    *,
    http_status: Optional[int] = None,
    code: Any = None,
    data: Any = None,
) -> CqfusionRequestError:
    return CqfusionRequestError(
        message, http_status, kind=kind, code=code, data=data
    )


def invalid(message: str) -> CqfusionRequestError:
    return fail("invalid_input", message)


def api_error(status: int, payload: Any, fallback: str) -> CqfusionRequestError:
    body = payload if isinstance(payload, dict) else {}
    message = fallback
    for key in ("message", "msg", "error"):
        if isinstance(body.get(key), str):
            message = body[key]
            break
    return fail(
        "api", message, http_status=status, code=body.get("code"), data=body.get("data")
    )


def require_requests():
    try:
        import requests
    except ImportError:
        raise invalid(
            "the fusion client needs 'requests': pip install 'cqlib-hybrid[fusion]'"
        ) from None
    return requests


def positive_seconds(value: float, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise invalid(f"{field} must be finite and positive")
    return float(value)


def parse_origin(host: str) -> str:
    try:
        url = urlsplit(host)
        port = url.port  # noqa: F841 - validates the port
    except ValueError as exc:
        raise invalid(str(exc)) from exc
    if (
        url.scheme not in ("http", "https")
        or not url.hostname
        or url.username is not None
        or url.password is not None
        or url.query
        or url.fragment
        or url.path not in ("", "/")
    ):
        raise invalid("host must be an HTTP(S) origin without credentials, path or query")
    return f"{url.scheme}://{url.netloc}"


class Transport:
    """Token-holding HTTP client; safe to share between threads."""

    def __init__(
        self,
        host: str,
        *,
        token: str = "",
        api_key: str = "",
        timeout: float = 120.0,
        session: Any = None,
    ):
        requests = require_requests()

        self.base_url = parse_origin(host)
        self.timeout = positive_seconds(timeout, "timeout")
        self._session = session or requests.Session()
        self._lock = threading.Lock()
        self._api_key = api_key or ""
        self._token = token or ""
        self._obtained_at = time.time() if token else 0.0
        if not self._token:
            if not self._api_key:
                raise invalid("API key or token is required")
            self.login(self._api_key, self.timeout)

    # -- authentication -------------------------------------------------

    @property
    def access_token(self) -> str:
        with self._lock:
            return self._token

    def login(self, api_key: str, timeout: Optional[float] = None) -> str:
        """Exchange an API key (openId) for an access token and remember both."""
        requests = require_requests()
        timeout = self.timeout if timeout is None else timeout
        try:
            response = self._session.post(
                self.base_url + LOGIN_PATH,
                data={
                    "grant_type": "openId",
                    "openId": api_key,
                    "account_type": "member",
                },
                timeout=timeout,
            )
        except requests.RequestException as exc:
            raise _transport_error(exc) from exc
        if response.status_code >= 400:
            raise fail(
                "transport",
                f"login failed with HTTP {response.status_code}",
                http_status=response.status_code,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise fail("decode", "login response is not valid JSON") from exc
        if not isinstance(payload, dict) or payload.get("code") != 0:
            message = None
            if isinstance(payload, dict):
                message = payload.get("msg") or payload.get("message")
            raise fail("platform", str(message or "login failed"))
        data = payload.get("data")
        token = data.get("access_token") if isinstance(data, dict) else None
        if not token:
            raise fail("platform", "login response missing 'data.access_token'")
        with self._lock:
            self._api_key = api_key
            self._token = token
            self._obtained_at = time.time()
        return token

    def _current_token(self, timeout: float) -> str:
        with self._lock:
            token, expired = self._token, (
                time.time() - self._obtained_at >= TOKEN_VALID_SECS
            )
        return self._refresh(timeout) if expired else token

    def _refresh(self, timeout: float) -> str:
        with self._lock:
            api_key = self._api_key
        if not api_key:
            raise fail(
                "platform", "token expired; provide an API key or a fresh token"
            )
        return self.login(api_key, timeout)

    # -- requests -------------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Optional[list] = None,
        json_body: Any = None,
        upload: Optional[Path] = None,
        timeout: Optional[float] = None,
        stream: bool = False,
    ):
        """Send a request and return a successful ``requests.Response``."""
        requests = require_requests()
        total = self.timeout if timeout is None else timeout
        started = time.monotonic()
        read_only = method == "GET"
        token = self._current_token(total)
        refreshed = False
        retries = 0

        def remaining() -> float:
            return total - (time.monotonic() - started)

        while True:
            left = remaining()
            if left <= 0:
                raise fail("timeout", "fusion request timed out")
            kwargs: dict = {
                "headers": {"basicToken": token, "Authorization": f"Bearer {token}"},
                "params": query or None,
                "timeout": left,
                "stream": stream,
            }
            handle = None
            try:
                if json_body is not None:
                    kwargs["json"] = json_body
                if upload is not None:
                    handle = open(upload, "rb")
                    kwargs["files"] = {
                        "file": (Path(upload).name or "task.zip", handle, "application/zip")
                    }
                try:
                    response = self._session.request(
                        method, self.base_url + path, **kwargs
                    )
                finally:
                    if handle is not None:
                        handle.close()
            except OSError as exc:
                if isinstance(exc, requests.RequestException):
                    if read_only and retries < MAX_READ_RETRIES:
                        _sleep(min(1 << retries, max(remaining(), 0)))
                        retries += 1
                        continue
                    raise _transport_error(exc) from exc
                raise fail("io", str(exc)) from exc

            status = response.status_code
            if status == 401 and not refreshed:
                if remaining() <= 0:
                    raise fail("timeout", "fusion authentication timed out")
                response.close()
                token = self._refresh(remaining())
                refreshed = True
                continue
            if (
                read_only
                and retries < MAX_READ_RETRIES
                and (status >= 500 or status == 429)
            ):
                try:
                    delay = int(response.headers.get("Retry-After", ""))
                except ValueError:
                    delay = 1 << retries
                response.close()
                retries += 1
                _sleep(min(delay, 30, max(remaining(), 0)))
                continue
            if not 200 <= status < 300:
                text = response.text
                response.close()
                try:
                    payload = _loads(text)
                except ValueError:
                    payload = None
                raise api_error(status, payload, text)
            return response


def _loads(text: str) -> Any:
    import json

    return json.loads(text)


def _sleep(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


def _transport_error(exc: Exception) -> CqfusionRequestError:
    requests = require_requests()

    kind = "timeout" if isinstance(exc, requests.Timeout) else "transport"
    return fail(kind, str(exc))
