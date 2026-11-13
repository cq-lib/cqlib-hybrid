"""Quantum execution inside a QRMI container through :class:`QRMIClient`.

This lets the same workflow run locally (mock or Tianyan runners) and inside a
fusion task container, where quantum circuits go over the QRMI long connection.
"""

from __future__ import annotations

import json
import math
from typing import Any

from cqlib_hybrid.exceptions import QRMIQuantumError
from cqlib_hybrid.qrmi import QRMIClient

from .mock import QuantumJobResult

DEFAULT_SHOTS = 1000
SUCCESS = 3


class QRMIQuantumRunner:
    """Run quantum stages through a connected :class:`QRMIClient`.

    The payload follows the other runners: ``qcis`` (or ``circuit``) holds one
    QCIS string or a list of them, plus optional ``shots`` and ``max_wait_time``.
    Each circuit is one QRMI iteration, submitted after the previous one finished.
    """

    def __init__(
        self,
        client: QRMIClient,
        *,
        timeout: float | None = 600,
        topology_mapping_enabled: bool | None = None,
    ):
        self.client = client
        self.timeout = timeout
        self.topology_mapping_enabled = topology_mapping_enabled

    @classmethod
    def from_env(cls, **kwargs: Any) -> "QRMIQuantumRunner":
        """Connect with the injected QRMI environment (see ``QRMIClient.from_env``)."""
        timeout = kwargs.pop("timeout", 600)
        mapping = kwargs.pop("topology_mapping_enabled", None)
        client = QRMIClient.from_env(**kwargs)
        client.connect()
        return cls(client, timeout=timeout, topology_mapping_enabled=mapping)

    def run(self, stage_name: str, payload: dict[str, Any]) -> QuantumJobResult:
        qcis = payload.get("qcis") or payload.get("circuit")
        circuits = [qcis] if isinstance(qcis, str) else qcis
        if (
            not isinstance(circuits, list)
            or not circuits
            or any(not isinstance(c, str) or not c.strip() for c in circuits)
        ):
            raise ValueError(
                "quantum stage payload requires nonempty QCIS text or a list of QCIS strings"
            )
        shots = int(payload.get("shots", DEFAULT_SHOTS))
        timeout = payload.get("max_wait_time", self.timeout)
        if shots <= 0 or (
            timeout is not None
            and not (math.isfinite(float(timeout)) and float(timeout) > 0)
        ):
            raise ValueError("shots and max_wait_time must be positive and finite")
        results = []
        for index, circuit in enumerate(circuits):
            iteration = self.client.submit_quantum(
                circuit,
                shots,
                topology_mapping_enabled=self.topology_mapping_enabled,
            )
            message = self.client.wait_quantum_result(
                iteration, timeout=None if timeout is None else float(timeout)
            )
            if message["taskStatus"] != SUCCESS:
                raise QRMIQuantumError(message)
            results.append(
                {
                    "result_index": index,
                    "task_id": f"qrmi-{iteration}",
                    "iteration_no": iteration,
                    "result": message["result"],
                    "shots": shots,
                }
            )
        output: dict[str, Any] = {
            "stage": stage_name,
            "engine": "qrmi",
            "task_ids": [item["task_id"] for item in results],
            "results": results,
        }
        if len(results) == 1:
            output["result"] = results[0]["result"]
            if isinstance(results[0]["result"], dict) and isinstance(
                results[0]["result"].get("counts"), dict
            ):
                output["counts"] = results[0]["result"]["counts"]
        # Fail now rather than losing a finished stage at checkpoint time.
        output = json.loads(json.dumps(output, allow_nan=False))
        return QuantumJobResult(query_id=output["task_ids"][0], output=output)
