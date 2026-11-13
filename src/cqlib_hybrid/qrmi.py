"""QRMI container-side JSON/WebSocket client.

One I/O thread owns the connection and keeps receiving and heartbeating while
the calling thread performs classical work. Public operations are synchronous;
only one quantum iteration may be in flight. No credentials are persisted.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
import logging
import math
import os
import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

import websocket

from .exceptions import (
    HybridAuthenticationError,
    HybridProtocolError,
    HybridRuntimeError,
    HybridValidationError,
    QRMIConnectionError,
    QRMIQuantumError,
    QRMIRequestError,
    QRMITimeoutError,
)
from .qrmi_protocol import (
    completed_message,
    decode_message,
    encode_message,
    failed_message,
    progress_message,
    quantum_submit_message,
    started_message,
)

logger = logging.getLogger(__name__)


def _positive_seconds(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise HybridValidationError(f"{name} must be a positive finite number")
    try:
        valid = math.isfinite(value) and value > 0
    except OverflowError:
        valid = False
    if not valid:
        raise HybridValidationError(f"{name} must be a positive finite number")
    return float(value)


@dataclass
class _Request:
    message: Dict[str, Any]
    encoded: str
    deadline: float
    sent: bool = False
    next_send: Optional[float] = 0.0
    accepted: bool = False


class _Disconnected(Exception):
    """Internal signal for a recoverable transport interruption."""


class QRMIClient:
    """Run a hybrid algorithm over the QRMI container WebSocket protocol.

    Use :meth:`from_env` for platform-injected connection settings. ``connect``
    (also called by the context manager) waits for STARTED acceptance. Request
    acknowledgement waits are bounded by ``request_timeout``. Quantum result
    waits may be unlimited, and a result wait timeout does not cancel the job.

    ``max_reconnect_attempts`` is a total budget for this client's lifetime,
    excluding the initial connection. Authentication and non-retryable protocol
    errors stop immediately. A closed client cannot be reopened.
    """

    def __init__(
        self,
        url: str,
        ticket: str,
        heartbeat_seconds: float,
        *,
        request_timeout: float = 30,
        connect_timeout: float = 10,
        retry_interval: float = 1,
        max_reconnect_attempts: int = 3,
    ):
        try:
            parsed = urlsplit(url) if isinstance(url, str) else None
            valid_url = (
                parsed is not None and parsed.scheme in ("ws", "wss")
                and parsed.hostname and parsed.username is None
                and parsed.password is None and not parsed.fragment
                and not any(character.isspace() for character in url)
            )
            if parsed is not None:
                parsed.port  # Validate malformed and out-of-range ports.
        except ValueError:
            valid_url = False
        if not valid_url:
            raise HybridValidationError("url must be a ws:// or wss:// URL without credentials or fragment")
        if not isinstance(ticket, str) or not ticket or any(c.isspace() for c in ticket):
            raise HybridValidationError("ticket must be a non-empty token without whitespace")
        self.url = url
        self._ticket = ticket
        self.heartbeat_seconds = _positive_seconds(heartbeat_seconds, "heartbeat_seconds")
        self.request_timeout = _positive_seconds(request_timeout, "request_timeout")
        self.connect_timeout = _positive_seconds(connect_timeout, "connect_timeout")
        self.retry_interval = _positive_seconds(retry_interval, "retry_interval")
        if type(max_reconnect_attempts) is not int or max_reconnect_attempts < 0:
            raise HybridValidationError("max_reconnect_attempts must be a non-negative integer")
        self.max_reconnect_attempts = max_reconnect_attempts
        self._cv = threading.Condition()
        self._operation_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._socket = None
        self._ready = False
        self._fatal: Optional[Exception] = None
        self._terminal: Optional[str] = None
        self._pending: Optional[_Request] = None
        self._latest_iteration = 0
        self._submissions: Dict[int, str] = {}
        self._acknowledged = set()
        self._results: Dict[int, Dict[str, Any]] = {}
        self._progress: Optional[int] = None
        self._progress_version = 0

    @classmethod
    def from_env(cls, **kwargs: Any) -> "QRMIClient":
        """Read injected credentials and heartbeat, including the deployment URL."""
        url = os.environ.get("QRMI_HYBRID_CPU_CONNECTION_URL")
        if not url:
            raise HybridRuntimeError("QRMI_HYBRID_CPU_CONNECTION_URL environment variable is missing or empty")
        names = (
            "QRMI_HYBRID_CPU_TICKET",
            "QRMI_HYBRID_CPU_HEARTBEAT_SECONDS",
        )
        values = []
        for name in names:
            value = os.environ.get(name)
            if not value:
                raise HybridRuntimeError(f"{name} environment variable is missing or empty")
            values.append(value)
        try:
            heartbeat_seconds = int(values[1])
        except ValueError:
            raise HybridValidationError(f"{names[1]} must contain a positive integer") from None
        return cls(url, values[0], heartbeat_seconds, **kwargs)

    def __enter__(self) -> "QRMIClient":
        return self.connect()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def _check_available(self) -> None:
        # Called with the condition held. Preserve the original background error.
        if self._fatal is not None:
            raise self._fatal
        if self._stop.is_set():
            raise QRMIConnectionError("QRMI client is closed")
        if self._thread is None:
            raise QRMIConnectionError("Call connect() before using the QRMI client")

    def connect(self, iteration_no: Optional[int] = None) -> "QRMIClient":
        """Connect and start, optionally resuming an already accepted iteration.

        For recovery with a new client, supply the outstanding iteration number.
        Its original circuit is unknown, so this client can wait for the result
        but cannot resubmit that iteration with different or reconstructed data.
        """
        started_message(iteration_no)  # Validate before starting a thread.
        with self._operation_lock:
            with self._cv:
                if self._thread is not None:
                    self._check_available()
                    if iteration_no is not None:
                        raise HybridValidationError("iteration_no can only be set on the first connect")
                else:
                    if self._stop.is_set():
                        raise QRMIConnectionError("QRMI client is closed")
                    logger.info("Connecting to %s (resume iteration=%s)", self.url, iteration_no)
                    if iteration_no is not None:
                        self._latest_iteration = iteration_no
                        self._acknowledged.add(iteration_no)
                    self._thread = threading.Thread(
                        target=self._run, name="qrmi-connection", daemon=True,
                    )
                    self._thread.start()
                deadline = time.monotonic() + self.request_timeout
                while not self._ready:
                    self._check_available()
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        error = QRMITimeoutError("Timed out waiting for STARTED acknowledgement")
                        self._abort(error)
                        raise error
                    self._cv.wait(remaining)
        return self

    def progress(self, progress: Optional[int] = None) -> None:
        """Schedule a PROGRESS heartbeat, optionally reporting 0..100 percent.

        This does not block on an acknowledgement. Background errors surface on
        the next synchronous operation. Automatic heartbeats use this value.
        """
        progress_message(progress)
        if progress is not None:
            logger.info("Progress updated to %d%%", progress)
        with self._cv:
            self._check_available()
            self._progress = progress
            self._progress_version += 1
            self._cv.notify_all()

    def submit_quantum(
        self,
        circuit_content: str,
        shots: int,
        *,
        iteration_no: Optional[int] = None,
        topology_mapping_enabled: Optional[bool] = None,
    ) -> int:
        """Submit QCIS and wait for acceptance, returning its iteration number."""
        with self._operation_lock:
            with self._cv:
                self._check_available()
                number = self._latest_iteration + 1 if iteration_no is None else iteration_no
                message = quantum_submit_message(number, circuit_content, shots, topology_mapping_enabled)
                encoded = encode_message(message)
                if number in self._submissions:
                    if self._submissions[number] != encoded:
                        raise HybridValidationError("The same iteration cannot use different circuit content or options")
                    if number in self._acknowledged:
                        return number
                else:
                    if number != self._latest_iteration + 1:
                        raise HybridValidationError("iteration_no must increase consecutively, starting at 1")
                    self._require_last_success()
                    self._submissions[number] = encoded
                    self._latest_iteration = number
            logger.info("Submitting quantum iteration %d (shots=%d)", number, shots)
            self._request(message, encoded)
            return number

    def _require_last_success(self) -> None:
        if self._latest_iteration:
            result = self._results.get(self._latest_iteration)
            if result is None:
                raise HybridValidationError("Wait for the outstanding quantum iteration before continuing")
            if result["taskStatus"] != 3:
                raise QRMIQuantumError(copy.deepcopy(result))

    def wait_quantum_result(
        self, iteration_no: Optional[int] = None, timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Return the QPU_RESULT envelope, including failure/cancellation states.

        Results are cached by iteration number and returned as independent JSON
        values. A timeout leaves the request and background connection active.
        """
        if timeout is not None:
            timeout = _positive_seconds(timeout, "timeout")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cv:
            if self._fatal is not None:
                raise self._fatal
            number = self._latest_iteration if iteration_no is None else iteration_no
            if type(number) is not int or number <= 0:
                raise HybridValidationError("iteration_no must refer to a submitted positive iteration")
            if number not in self._submissions and number not in self._acknowledged:
                raise HybridValidationError("Unknown quantum iteration")
            while number not in self._results:
                self._check_available()
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise QRMITimeoutError(f"Timed out waiting for quantum iteration {number}; it has not been cancelled")
                self._cv.wait(remaining)
            return copy.deepcopy(self._results[number])

    def run_quantum(
        self,
        circuit_content: str,
        shots: int,
        *,
        topology_mapping_enabled: Optional[bool] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Submit the next iteration and return its result, raising on 4 or 5."""
        if timeout is not None:
            _positive_seconds(timeout, "timeout")
        number = self.submit_quantum(
            circuit_content, shots, topology_mapping_enabled=topology_mapping_enabled,
        )
        response = self.wait_quantum_result(number, timeout=timeout)
        if response["taskStatus"] != 3:
            raise QRMIQuantumError(response)
        return response["result"]

    def complete(
        self, result: Any, *, stdout: Optional[str] = None,
        stderr: Optional[str] = None, exit_code: int = 0,
    ) -> None:
        """Report the final JSON result and close after COMPLETED acceptance."""
        message = completed_message(result, stdout=stdout, stderr=stderr, exit_code=exit_code)
        encoded = encode_message(message)
        logger.info("Reporting final COMPLETED result: %r", result)
        with self._operation_lock:
            with self._cv:
                self._check_available()
                self._require_last_success()
            self._request(message, encoded)
        self.close()

    def fail(
        self, error_message: str, *, error_code: str = "ALGORITHM_FAILED",
        exit_code: int = 1, stdout: Optional[str] = None, stderr: Optional[str] = None,
    ) -> None:
        """Report algorithm failure and close after FAILED acceptance.

        The caller controls process exit. Unlike COMPLETED, the protocol does not
        promise FAILED replay after a lost acknowledgement, so that case raises.
        """
        message = failed_message(
            error_message, error_code=error_code, exit_code=exit_code, stdout=stdout, stderr=stderr,
        )
        logger.info("Reporting FAILED: %s (errorCode=%s)", error_message, error_code)
        with self._operation_lock:
            self._request(message, encode_message(message))
        self.close()

    def _request(self, message: Dict[str, Any], encoded: str) -> None:
        with self._cv:
            self._check_available()
            request = _Request(message, encoded, time.monotonic() + self.request_timeout)
            self._pending = request
            self._cv.notify_all()
            while not request.accepted:
                self._check_available()
                remaining = request.deadline - time.monotonic()
                if remaining <= 0:
                    error = QRMITimeoutError(
                        f"Timed out waiting for {message['eventType']} acknowledgement; "
                        "the remote request may have been accepted"
                    )
                    self._abort(error)
                    raise error
                self._cv.wait(remaining)
            self._pending = None

    def _abort(self, error: Exception) -> None:
        logger.error("QRMI client aborted: %s", error)
        with self._cv:
            if self._fatal is None:
                self._fatal = error
            self._stop.set()
            self._ready = False
            self._cv.notify_all()
            connection = self._socket
        if connection is not None:
            try:
                connection.abort()
            except (OSError, websocket.WebSocketException):
                pass

    def close(self) -> None:
        """Release resources and wake waiters; closing does not report success."""
        with self._cv:
            self._stop.set()
            self._ready = False
            self._cv.notify_all()
            connection = self._socket
            thread = self._thread
        if connection is not None:
            try:
                connection.abort()
            except (OSError, websocket.WebSocketException):
                pass
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=self.connect_timeout + 1)

    def _run(self) -> None:
        reconnects = 0
        try:
            while not self._stop.is_set():
                connection = None
                try:
                    connection = websocket.create_connection(
                        self.url, header={"Authorization": "Bearer " + self._ticket},
                        timeout=self.connect_timeout, enable_multithread=True,
                        # A container script has no browser origin. Synthesizing
                        # one from a proxy URL can cause the QRMI upgrade to fail.
                        suppress_origin=True, redirect_limit=0,
                    )
                    # websocket-client can return an unupgraded redirect when
                    # redirect_limit=0; never send the ticket to another URL.
                    if connection.getstatus() != 101:
                        raise QRMIConnectionError("QRMI did not upgrade the WebSocket connection (redirects are disabled)")
                    logger.info("WebSocket connection established")
                    with self._cv:
                        self._socket = connection
                    if self._stop.is_set():
                        break
                    self._session(connection)
                except websocket.WebSocketBadStatusException as exc:
                    status = exc.status_code
                    if status in (401, 403):
                        raise HybridAuthenticationError("QRMI rejected the job ticket", status_code=status) from None
                    if status is not None and 400 <= status < 500 and status not in (408, 429):
                        raise QRMIConnectionError(f"QRMI WebSocket handshake failed (HTTP {status})") from None
                    # Other HTTP errors, including transient 5xx, use the same
                    # bounded reconnect budget as interrupted transport.
                except (OSError, websocket.WebSocketConnectionClosedException,
                        websocket.WebSocketTimeoutException, _Disconnected):
                    pass
                finally:
                    with self._cv:
                        self._socket = None
                        self._ready = False
                        self._cv.notify_all()
                    if connection is not None:
                        try:
                            connection.close(timeout=0)
                        except (OSError, websocket.WebSocketException):
                            pass
                if self._stop.is_set():
                    break
                with self._cv:
                    pending = self._pending
                    if pending is not None and pending.sent and pending.message["eventType"] == "FAILED":
                        raise QRMIConnectionError("FAILED acknowledgement was lost; automatic replay is not defined by the protocol")
                    if pending is not None:
                        pending.next_send = 0.0
                if reconnects >= self.max_reconnect_attempts:
                    logger.error("Reconnect attempts exhausted (%d)", self.max_reconnect_attempts)
                    raise QRMIConnectionError("QRMI connection lost; reconnect attempts exhausted")
                reconnects += 1
                logger.warning("Connection lost; reconnecting (%d/%d)", reconnects, self.max_reconnect_attempts)
                if self._stop.wait(self.retry_interval):
                    break
        except Exception as exc:
            if not self._stop.is_set():
                if isinstance(exc, (HybridRuntimeError, HybridProtocolError, HybridAuthenticationError)):
                    self._abort(exc)
                else:
                    # Transport exceptions may include handshake headers. Keep
                    # credentials out of the public error and exception chain.
                    self._abort(QRMIConnectionError(f"QRMI connection failed ({type(exc).__name__})"))
        finally:
            with self._cv:
                self._ready = False
                self._cv.notify_all()

    def _send(self, connection, encoded: str) -> None:
        connection.settimeout(self.connect_timeout)
        connection.send(encoded)
        connection.settimeout(min(0.2, self.heartbeat_seconds, self.retry_interval))

    def _session(self, connection) -> None:
        with self._cv:
            number = self._latest_iteration
            resume = number if number in self._acknowledged and number not in self._results else None
        started = encode_message(started_message(resume))
        start_deadline = time.monotonic() + self.request_timeout
        start_next_send: Optional[float] = 0.0
        heartbeat = None
        heartbeat_next_send = 0.0
        heartbeat_version = -1
        heartbeat_last_sent = 0.0
        ready = False
        while not self._stop.is_set():
            now = time.monotonic()
            if not ready:
                if now >= start_deadline:
                    raise QRMITimeoutError("Timed out waiting for STARTED acknowledgement")
                if start_next_send is not None and now >= start_next_send:
                    self._send(connection, started)
                    start_next_send = None
            else:
                with self._cv:
                    pending = self._pending
                    if pending is not None and not pending.accepted and pending.next_send is not None and now >= pending.next_send:
                        pending.sent = True
                        pending.next_send = (
                            now + self.retry_interval
                            if pending.message["eventType"] in ("QUANTUM_SUBMIT", "COMPLETED")
                            else None
                        )
                        encoded = pending.encoded
                    else:
                        encoded = None
                    progress = self._progress
                    version = self._progress_version
                    terminal_pending = (
                        pending is not None and pending.sent
                        and pending.message["eventType"] in ("COMPLETED", "FAILED")
                    )
                if encoded is not None:
                    self._send(connection, encoded)
                # Keep the exact heartbeat while its acknowledgement is pending.
                # Updated progress takes effect after that heartbeat is accepted.
                if not terminal_pending and heartbeat is None and (now >= heartbeat_next_send or version != heartbeat_version):
                    heartbeat = encode_message(progress_message(progress))
                    heartbeat_version = version
                    heartbeat_next_send = now
                if not terminal_pending and heartbeat is not None and now >= heartbeat_next_send:
                    logger.debug("Sending heartbeat (progress=%s)", progress)
                    self._send(connection, heartbeat)
                    heartbeat_last_sent = now
                    heartbeat_next_send = now + self.heartbeat_seconds
            try:
                opcode, raw = connection.recv_data(control_frame=True)
            except websocket.WebSocketTimeoutException:
                continue
            if opcode == websocket.ABNF.OPCODE_CLOSE:
                code = int.from_bytes(raw[:2], "big") if len(raw) >= 2 else None
                if code in (1002, 1003, 1007, 1008, 1009):
                    raise QRMIConnectionError(f"QRMI closed the connection with non-retryable WebSocket code {code}")
                raise _Disconnected()
            if opcode in (websocket.ABNF.OPCODE_PING, websocket.ABNF.OPCODE_PONG):
                continue  # websocket-client handles the WebSocket pong itself.
            if opcode != websocket.ABNF.OPCODE_TEXT:
                raise HybridProtocolError("QRMI messages must be UTF-8 JSON text frames")
            if isinstance(raw, bytes):
                try:
                    raw = raw.decode("utf-8")
                except UnicodeError:
                    raise HybridProtocolError("QRMI sent invalid UTF-8") from None
            message = decode_message(raw)
            event = message["eventType"]
            if event == "QPU_RESULT":
                self._receive_result(message)
                continue
            accepted = message["accepted"]
            if event == "STARTED" and not ready:
                if accepted:
                    ready = True
                    logger.info("STARTED accepted; connection is ready")
                    with self._cv:
                        self._ready = True
                        self._cv.notify_all()
                elif message["retryable"]:
                    logger.warning(
                        "STARTED rejected as retryable (%s: %s); retrying",
                        message["errorCode"], message["errorMessage"],
                    )
                    start_next_send = time.monotonic() + self.retry_interval
                else:
                    raise QRMIRequestError(message)
                continue
            if event == "PROGRESS":
                if accepted:
                    logger.debug("Heartbeat acknowledged")
                    heartbeat = None
                    heartbeat_next_send = heartbeat_last_sent + self.heartbeat_seconds
                elif message["retryable"] and heartbeat is not None:
                    heartbeat_next_send = time.monotonic() + self.retry_interval
                else:
                    raise QRMIRequestError(message)
                continue
            with self._cv:
                pending = self._pending
                matches = (
                    ready and pending is not None and pending.sent
                    and event == pending.message["eventType"]
                    and (event != "QUANTUM_SUBMIT"
                         or message.get("iterationNo") == pending.message["iterationNo"]
                         or (not accepted and "iterationNo" not in message))
                )
                if not matches:
                    if not accepted:
                        if (event == "QUANTUM_SUBMIT" and message["retryable"]
                                and message.get("iterationNo") in self._acknowledged):
                            continue  # A stale retry instruction for an accepted round.
                        raise QRMIRequestError(message)
                    continue  # A delayed duplicate acknowledgement.
                if not accepted:
                    if message["retryable"]:
                        logger.warning(
                            "%s rejected as retryable (%s: %s); resending identical request",
                            event, message["errorCode"], message["errorMessage"],
                        )
                        pending.sent = False
                        pending.next_send = time.monotonic() + self.retry_interval
                    else:
                        logger.error("%s rejected (%s: %s)", event, message["errorCode"], message["errorMessage"])
                        raise QRMIRequestError(message)
                else:
                    pending.accepted = True
                    if event == "QUANTUM_SUBMIT":
                        self._acknowledged.add(message["iterationNo"])
                        logger.info(
                            "QUANTUM_SUBMIT iteration %d accepted; waiting for QPU_RESULT",
                            message["iterationNo"],
                        )
                    elif event in ("COMPLETED", "FAILED"):
                        logger.info("%s acknowledged by the server; closing the connection", event)
                        self._terminal = event
                        self._stop.set()
                    self._cv.notify_all()

    def _receive_result(self, message: Dict[str, Any]) -> None:
        with self._cv:
            number = message["iterationNo"]
            if number not in self._submissions and number not in self._acknowledged:
                raise HybridProtocolError("QRMI returned a result for an unknown iteration")
            previous = self._results.get(number)
            if previous is not None:
                if previous["taskStatus"] != message["taskStatus"] or previous.get("result") != message.get("result"):
                    raise HybridProtocolError("QRMI returned conflicting results for the same iteration")
                logger.debug("Duplicate result for iteration %d ignored", number)
                return
            self._results[number] = message
            logger.info(
                "Quantum result received for iteration %d: taskStatus=%d, result=%r",
                number, message["taskStatus"], message.get("result"),
            )
            self._cv.notify_all()
