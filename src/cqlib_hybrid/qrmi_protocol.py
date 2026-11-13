"""The simplified QRMI container WebSocket JSON protocol.

These helpers validate complete messages before a transport sends them.  The
legacy identity/Base64 protocol is intentionally kept out of this module.
"""

from __future__ import annotations

import base64
import binascii
import json
import math
from typing import Any, Dict, Optional, Set

from .exceptions import HybridProtocolError, HybridValidationError


MAX_MESSAGE_BYTES = 3 * 1024 * 1024
MAX_BUSINESS_BYTES = 1024 * 1024
MAX_LOG_BYTES = 1024 * 1024
CLIENT_EVENTS = frozenset({"STARTED", "PROGRESS", "QUANTUM_SUBMIT", "COMPLETED", "FAILED"})
QPU_SUCCESS = 3
QPU_FAILED = 4
QPU_CANCELLED = 5


def _validate_json_value(
    value: Any,
    *,
    location: str = "$",
    active_containers: Optional[Set[int]] = None,
) -> None:
    """Validate that *value* contains only native, finite JSON values."""
    if value is None or isinstance(value, (str, bool, int)):
        return

    if isinstance(value, float):
        if not math.isfinite(value):
            raise HybridValidationError(
                f"Value at {location} is not valid JSON: non-finite numbers are not supported"
            )
        return

    if active_containers is None:
        active_containers = set()

    if isinstance(value, list):
        identity = id(value)
        if identity in active_containers:
            raise HybridValidationError(f"Value at {location} contains a circular reference")
        active_containers.add(identity)
        try:
            for index, item in enumerate(value):
                _validate_json_value(
                    item,
                    location=f"{location}[{index}]",
                    active_containers=active_containers,
                )
        finally:
            active_containers.remove(identity)
        return

    if isinstance(value, dict):
        identity = id(value)
        if identity in active_containers:
            raise HybridValidationError(f"Value at {location} contains a circular reference")
        active_containers.add(identity)
        try:
            for key, item in value.items():
                if not isinstance(key, str):
                    raise HybridValidationError(
                        f"Object key at {location} is not a string: {key!r}"
                    )
                _validate_json_value(
                    item,
                    location=f"{location}.{key}",
                    active_containers=active_containers,
                )
        finally:
            active_containers.remove(identity)
        return

    raise HybridValidationError(
        f"Value at {location} has unsupported type {type(value).__name__}"
    )


def _reject_non_standard_number(value: str) -> None:
    """Reject JavaScript-style NaN and Infinity tokens accepted by ``json``."""
    raise ValueError(f"non-standard numeric value {value}")


def _integer(value: Any, field: str, *, positive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise HybridValidationError(f"{field} must be an integer")
    if positive and value <= 0:
        raise HybridValidationError(f"{field} must be a positive integer")


def _text(
    value: Any,
    field: str,
    *,
    nonempty: bool = False,
    max_characters: Optional[int] = None,
) -> None:
    if not isinstance(value, str):
        raise HybridValidationError(f"{field} must be a string")
    if nonempty and not value.strip():
        raise HybridValidationError(f"{field} must not be empty")
    if max_characters is not None and len(value) > max_characters:
        raise HybridValidationError(f"{field} must be at most {max_characters} characters")


def _json_text(value: Any) -> str:
    try:
        _validate_json_value(value)
        serialized = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        serialized.encode("utf-8")
        return serialized
    except HybridValidationError:
        raise
    except (TypeError, ValueError, OverflowError, RecursionError, UnicodeError) as exc:
        raise HybridValidationError(f"Message is not valid UTF-8 JSON: {exc}") from exc


def _check_size(value: str, maximum: int, field: str) -> None:
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError as exc:
        raise HybridValidationError(f"{field} must contain valid UTF-8 text") from exc
    if size > maximum:
        raise HybridValidationError(f"{field} exceeds the {maximum}-byte limit")


def encode_message(message: Dict[str, Any]) -> str:
    """Validate and encode a client message using compact UTF-8 JSON.

    Business size includes its JSON wrapper (``quantumCircuit`` or ``result``).
    Logs are measured as UTF-8 text; the entire serialized message also has to
    fit the independent 3 MiB limit, including JSON escaping and field names.
    """
    if not isinstance(message, dict):
        raise HybridValidationError("Client message must be an object")
    serialized = _json_text(message)
    event = message.get("eventType")
    if not isinstance(event, str) or event not in CLIENT_EVENTS:
        raise HybridValidationError("Unsupported client eventType")

    if event == "STARTED":
        if "iterationNo" in message:
            _integer(message["iterationNo"], "iterationNo", positive=True)
    elif event == "PROGRESS":
        if "progress" in message:
            _integer(message["progress"], "progress")
            if not 0 <= message["progress"] <= 100:
                raise HybridValidationError("progress must be between 0 and 100")
    elif event == "QUANTUM_SUBMIT":
        _integer(message.get("iterationNo"), "iterationNo", positive=True)
        circuit = message.get("quantumCircuit")
        if not isinstance(circuit, dict):
            raise HybridValidationError("quantumCircuit must be an object")
        if circuit.get("circuitFormat") != "QCIS":
            raise HybridValidationError("quantumCircuit.circuitFormat must be QCIS")
        _text(circuit.get("circuitContent"), "quantumCircuit.circuitContent", nonempty=True)
        _integer(circuit.get("shots"), "quantumCircuit.shots", positive=True)
        if "topologyMappingEnabled" in circuit and not isinstance(circuit["topologyMappingEnabled"], bool):
            raise HybridValidationError("quantumCircuit.topologyMappingEnabled must be a boolean")
        _check_size(_json_text({"quantumCircuit": circuit}), MAX_BUSINESS_BYTES, "quantumCircuit")
    elif event == "COMPLETED":
        if "result" not in message:
            raise HybridValidationError("COMPLETED requires result")
        if "exitCode" in message:
            _integer(message["exitCode"], "exitCode")
            if message["exitCode"] != 0:
                raise HybridValidationError("COMPLETED exitCode must be 0")
        _check_size(_json_text({"result": message["result"]}), MAX_BUSINESS_BYTES, "result")
    elif event == "FAILED":
        _text(message.get("errorMessage"), "errorMessage", nonempty=True, max_characters=2048)
        if "errorCode" in message:
            _text(message["errorCode"], "errorCode", max_characters=128)
        if "exitCode" in message:
            _integer(message["exitCode"], "exitCode")
            if message["exitCode"] == 0:
                raise HybridValidationError("FAILED exitCode must not be 0")

    for field in ("stdout", "stderr"):
        if field in message:
            _text(message[field], field)
            _check_size(message[field], MAX_LOG_BYTES, field)
    _check_size(serialized, MAX_MESSAGE_BYTES, "Client message")
    return serialized


def _checked(message: Dict[str, Any]) -> Dict[str, Any]:
    encode_message(message)
    return message


def started_message(iteration_no: Optional[int] = None) -> Dict[str, Any]:
    """Build STARTED, optionally resuming the indicated quantum iteration."""
    message: Dict[str, Any] = {"eventType": "STARTED"}
    if iteration_no is not None:
        message["iterationNo"] = iteration_no
    return _checked(message)


def progress_message(progress: Optional[int] = None) -> Dict[str, Any]:
    """Build a heartbeat, optionally reporting an integer percentage."""
    message: Dict[str, Any] = {"eventType": "PROGRESS"}
    if progress is not None:
        message["progress"] = progress
    return _checked(message)


def quantum_submit_message(
    iteration_no: int,
    circuit_content: str,
    shots: int,
    topology_mapping_enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    """Build a QCIS submission without adding platform identity fields."""
    circuit: Dict[str, Any] = {
        "circuitFormat": "QCIS",
        "circuitContent": circuit_content,
        "shots": shots,
    }
    if topology_mapping_enabled is not None:
        circuit["topologyMappingEnabled"] = topology_mapping_enabled
    return _checked({"eventType": "QUANTUM_SUBMIT", "iterationNo": iteration_no, "quantumCircuit": circuit})


def completed_message(
    result: Any,
    stdout: Optional[str] = None,
    stderr: Optional[str] = None,
    exit_code: int = 0,
) -> Dict[str, Any]:
    """Build the final result, preserving every valid JSON value, including null."""
    message = {"eventType": "COMPLETED", "result": result, "exitCode": exit_code}
    if stdout is not None:
        message["stdout"] = stdout
    if stderr is not None:
        message["stderr"] = stderr
    return _checked(message)


def failed_message(
    error_message: str,
    error_code: str = "ALGORITHM_FAILED",
    exit_code: int = 1,
    stdout: Optional[str] = None,
    stderr: Optional[str] = None,
) -> Dict[str, Any]:
    """Build an algorithm failure and optional archived output."""
    message = {
        "eventType": "FAILED",
        "errorMessage": error_message,
        "errorCode": error_code,
        "exitCode": exit_code,
    }
    if stdout is not None:
        message["stdout"] = stdout
    if stderr is not None:
        message["stderr"] = stderr
    return _checked(message)


def _decode_experiment_results(result: Dict[str, Any]) -> None:
    """Decode the platform Base64 ``experimentResults`` into ``counts``/``shots``.

    The platform encodes per-shot measurement outcomes as a Base64 ASCII
    string of '0'/'1'. Outcomes are grouped by shot: each shot holds one
    character per measured qubit, in ``measureQubitList`` order. The original
    Base64 field and ``waveforms`` are preserved; the decoded histogram is
    added as ``counts`` (outcome bitstring -> frequency) with ``shots``.
    """
    encoded = result["experimentResults"]
    try:
        decoded = base64.b64decode(encoded, validate=True).decode("ascii")
    except (binascii.Error, ValueError, UnicodeDecodeError):
        raise HybridValidationError(
            "result.experimentResults must be Base64-encoded ASCII measurement outcomes"
        ) from None
    if not decoded or any(character not in "01" for character in decoded):
        raise HybridValidationError(
            "result.experimentResults must decode to a non-empty '0'/'1' outcome string"
        )
    measure_list = result.get("measureQubitList")
    if measure_list is None:
        num_measure = 1
    elif isinstance(measure_list, list) and measure_list:
        num_measure = len(measure_list)
    else:
        raise HybridValidationError("result.measureQubitList must be a non-empty list")
    if len(decoded) % num_measure:
        raise HybridValidationError(
            "result.experimentResults length must be a multiple of the measured qubit count"
        )
    shots = len(decoded) // num_measure
    counts: Dict[str, int] = {}
    for start in range(0, len(decoded), num_measure):
        outcome = decoded[start:start + num_measure]
        counts[outcome] = counts.get(outcome, 0) + 1
    result["counts"] = counts
    result["shots"] = shots


def decode_message(raw: str) -> Dict[str, Any]:
    """Decode a server acknowledgement or QPU result with protocol validation.

    Failed/cancelled QPU messages may omit ``result``; it is normalized to
    ``None``.  Unknown/null event types are permitted in rejections so that a
    server can report an unrecognized or unparseable client request.  A
    successful platform result carrying Base64 ``experimentResults`` (with
    ``waveforms`` or ``measureQubitList``) is decoded in place, adding the
    ``counts`` histogram and ``shots`` while preserving the original fields.
    """
    if not isinstance(raw, str):
        raise HybridProtocolError("QRMI messages must be UTF-8 JSON text")
    try:
        raw.encode("utf-8")
        message = json.loads(raw, parse_constant=_reject_non_standard_number)
        if not isinstance(message, dict):
            raise HybridValidationError("Server message must be an object")
        _json_text(message)
        if "eventType" not in message:
            raise HybridValidationError("Server message requires eventType")
        event = message["eventType"]
        if event == "QPU_RESULT":
            _integer(message.get("iterationNo"), "iterationNo", positive=True)
            _integer(message.get("taskStatus"), "taskStatus")
            if message["taskStatus"] not in (QPU_SUCCESS, QPU_FAILED, QPU_CANCELLED):
                raise HybridValidationError("QPU_RESULT taskStatus must be 3, 4 or 5")
            if message["taskStatus"] == QPU_SUCCESS and "result" not in message:
                raise HybridValidationError("Successful QPU_RESULT requires result")
            message.setdefault("result", None)
            result = message["result"]
            if (
                message["taskStatus"] == QPU_SUCCESS
                and isinstance(result, dict)
                and isinstance(result.get("experimentResults"), str)
                and ("waveforms" in result or "measureQubitList" in result)
            ):
                _decode_experiment_results(result)
        else:
            accepted = message.get("accepted")
            if not isinstance(accepted, bool):
                raise HybridValidationError("Acknowledgement accepted must be a boolean")
            if accepted:
                if not isinstance(event, str) or event not in CLIENT_EVENTS:
                    raise HybridValidationError("Acknowledgement has an unsupported eventType")
                if event == "QUANTUM_SUBMIT" and "iterationNo" not in message:
                    raise HybridValidationError("QUANTUM_SUBMIT acknowledgement requires iterationNo")
            else:
                if event is not None and not isinstance(event, str):
                    raise HybridValidationError("Rejected eventType must be a string or null")
                if not isinstance(message.get("retryable"), bool):
                    raise HybridValidationError("Rejected acknowledgement retryable must be a boolean")
                _text(message.get("errorCode"), "errorCode")
                _text(message.get("errorMessage"), "errorMessage")
            if "iterationNo" in message:
                _integer(message["iterationNo"], "iterationNo", positive=True)
        return message
    except (HybridValidationError, ValueError, TypeError, OverflowError, RecursionError, UnicodeError) as exc:
        raise HybridProtocolError(f"Invalid QRMI server message: {exc}") from exc
