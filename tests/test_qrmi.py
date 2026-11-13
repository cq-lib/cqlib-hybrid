"""QRMI worker contract tests using a deterministic in-memory WebSocket peer."""

from __future__ import annotations

import json
import os
import queue
import threading
import unittest
from unittest import mock

import websocket

from cqlib_hybrid.exceptions import (
    HybridAuthenticationError,
    HybridProtocolError,
    HybridRuntimeError,
    HybridValidationError,
)
from cqlib_hybrid.qrmi import (
    QRMIClient,
    QRMIConnectionError,
    QRMIQuantumError,
    QRMIRequestError,
    QRMITimeoutError,
)


URL = "wss://qrmi.example.test/internal/workflow/cpu-executions"
TICKET = "test-job-ticket"
CIRCUIT = "H Q0\nM Q0"


class FakeSocket:
    """Queue frames while preserving the real transport's blocking receive API."""

    def __init__(self, handler=None):
        self.handler = handler
        self.incoming = queue.Queue()
        self.sent = []
        self.changed = threading.Condition()
        self.timeout = 0.01
        self.closed = False

    def settimeout(self, timeout):
        self.timeout = timeout

    def getstatus(self):
        return 101

    def send(self, contents, *args, **kwargs):
        if self.closed:
            raise websocket.WebSocketConnectionClosedException("socket closed")
        message = json.loads(contents)
        with self.changed:
            self.sent.append(message)
            self.changed.notify_all()
        if self.handler is None or not self.handler(self, message):
            self.ack(message)

    def recv_data(self, control_frame=False):
        try:
            frame = self.incoming.get(timeout=min(self.timeout, 0.01))
        except queue.Empty:
            raise websocket.WebSocketTimeoutException("no frame") from None
        if isinstance(frame, Exception):
            raise frame
        return frame

    def push(self, message):
        self.incoming.put((websocket.ABNF.OPCODE_TEXT, json.dumps(message)))

    def ack(self, request, **fields):
        message = {"eventType": request["eventType"], "accepted": True}
        if request["eventType"] == "QUANTUM_SUBMIT":
            message["iterationNo"] = request["iterationNo"]
        message.update(fields)
        self.push(message)

    def result(self, iteration=1, result=None, status=3):
        self.push({
            "eventType": "QPU_RESULT",
            "iterationNo": iteration,
            "taskStatus": status,
            "result": result,
        })

    def disconnect(self):
        self.incoming.put((websocket.ABNF.OPCODE_CLOSE, b""))

    def abort(self):
        self.closed = True
        self.incoming.put(websocket.WebSocketConnectionClosedException("aborted"))

    def close(self, *args, **kwargs):
        self.abort()

    def messages(self, event_type):
        with self.changed:
            return [message for message in self.sent if message["eventType"] == event_type]

    def wait_for(self, event_type, count=1, timeout=1):
        with self.changed:
            if not self.changed.wait_for(
                lambda: len(self.messages(event_type)) >= count, timeout=timeout
            ):
                raise AssertionError(f"Did not receive {count} {event_type} messages: {self.sent}")
            return self.messages(event_type)


class QRMIClientTests(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch("cqlib_hybrid.qrmi.websocket.create_connection")
        self.create_connection = patcher.start()
        self.addCleanup(patcher.stop)

    def client(self, *sockets, **kwargs):
        self.create_connection.side_effect = list(sockets or [FakeSocket()])
        options = {
            "heartbeat_seconds": 60,
            "request_timeout": 0.5,
            "connect_timeout": 0.1,
            "retry_interval": 0.01,
            "max_reconnect_attempts": 2,
        }
        options.update(kwargs)
        client = QRMIClient(URL, TICKET, **options)
        self.addCleanup(client.close)
        return client

    def test_connect_authenticates_and_started_is_first_message(self):
        peer = FakeSocket()
        client = self.client(peer)
        self.assertIs(client.connect(), client)
        self.create_connection.assert_called_once()
        args, kwargs = self.create_connection.call_args
        self.assertEqual(args[0], URL)
        self.assertEqual(kwargs["header"], {"Authorization": "Bearer " + TICKET})
        self.assertEqual(kwargs["timeout"], 0.1)
        self.assertTrue(kwargs["enable_multithread"])
        self.assertEqual(kwargs["redirect_limit"], 0)
        self.assertEqual(peer.sent[0], {"eventType": "STARTED"})
        self.assertNotIn(TICKET, json.dumps(peer.sent))

    def test_from_environment_reads_connection_settings(self):
        peer = FakeSocket()
        self.create_connection.return_value = peer
        with mock.patch.dict(os.environ, {
            "QRMI_HYBRID_CPU_CONNECTION_URL": URL,
            "QRMI_HYBRID_CPU_TICKET": TICKET,
            "QRMI_HYBRID_CPU_HEARTBEAT_SECONDS": "7",
        }, clear=True):
            client = QRMIClient.from_env(request_timeout=0.5)
        self.addCleanup(client.close)
        client.connect()
        self.assertEqual(self.create_connection.call_args.args[0], URL)
        self.assertEqual(
            self.create_connection.call_args.kwargs["header"]["Authorization"],
            "Bearer " + TICKET,
        )

    def test_missing_environment_settings_fail_before_network(self):
        for environment in ({}, {"QRMI_HYBRID_CPU_CONNECTION_URL": URL}):
            with self.subTest(environment=environment):
                with mock.patch.dict(os.environ, environment, clear=True):
                    with self.assertRaises((HybridRuntimeError, HybridValidationError)):
                        QRMIClient.from_env()
        self.create_connection.assert_not_called()

    def test_invalid_connection_options_fail_before_network(self):
        for overrides in (
            {"url": "https://qrmi.example.test"},
            {"url": "wss://"},
            {"ticket": ""},
            {"ticket": "ticket\r\nInjected: yes"},
            {"heartbeat_seconds": 0},
            {"heartbeat_seconds": True},
            {"request_timeout": -1},
            {"connect_timeout": 0},
            {"retry_interval": -1},
            {"max_reconnect_attempts": -1},
        ):
            options = {"url": URL, "ticket": TICKET, "heartbeat_seconds": 1}
            options.update(overrides)
            with self.subTest(overrides=overrides):
                with self.assertRaises(HybridValidationError):
                    QRMIClient(**options)
        self.create_connection.assert_not_called()

    def test_context_manager_connects_and_closes_idempotently(self):
        peer = FakeSocket()
        client = self.client(peer)
        with client as connected:
            self.assertIs(connected, client)
            self.assertFalse(peer.closed)
        self.assertTrue(peer.closed)
        client.close()

    def test_two_round_feedback_loop_reports_progress_and_completes(self):
        """Use a QPU result to choose the next circuit, then report completion."""
        quantum_results = [
            {"counts": {"0": 600, "1": 400}},
            {"counts": {"1": 1000}},
        ]
        progress_received = threading.Event()

        def respond(peer, message):
            if message["eventType"] == "QUANTUM_SUBMIT":
                iteration = message["iterationNo"]
                peer.ack(message)
                peer.result(iteration, quantum_results[iteration - 1])
                return True
            if message["eventType"] == "PROGRESS" and message.get("progress") == 50:
                peer.ack(message)
                progress_received.set()
                return True
            return False  # Acknowledge STARTED, PROGRESS and COMPLETED normally.

        peer = FakeSocket(respond)
        client = self.client(peer, request_timeout=3, retry_interval=1)
        with client:
            first = client.run_quantum(CIRCUIT, shots=1000, timeout=3)
            self.assertEqual(first, quantum_results[0])

            # Classical work uses the first measurement to prepare the next round.
            next_circuit = "X Q0\nM Q0" if first["counts"]["0"] > first["counts"]["1"] else "M Q0"
            client.progress(50)
            self.assertTrue(progress_received.wait(timeout=3), "Progress was not sent")

            second = client.run_quantum(
                next_circuit, shots=1000, topology_mapping_enabled=False, timeout=3,
            )
            self.assertEqual(second, quantum_results[1])
            final_result = {"iterations": 2, "initial": first, "final": second}
            client.complete(final_result, stdout="两轮计算完成\n")
            self.assertTrue(peer.closed)

        self.create_connection.assert_called_once()
        self.assertEqual(
            self.create_connection.call_args.kwargs["header"],
            {"Authorization": "Bearer " + TICKET},
        )
        self.assertEqual(peer.sent[0], {"eventType": "STARTED"})
        self.assertEqual(
            [message["eventType"] for message in peer.sent if message["eventType"] != "PROGRESS"],
            ["STARTED", "QUANTUM_SUBMIT", "QUANTUM_SUBMIT", "COMPLETED"],
        )
        self.assertEqual(peer.messages("QUANTUM_SUBMIT"), [
            {
                "eventType": "QUANTUM_SUBMIT", "iterationNo": 1,
                "quantumCircuit": {
                    "circuitFormat": "QCIS", "circuitContent": CIRCUIT, "shots": 1000,
                },
            },
            {
                "eventType": "QUANTUM_SUBMIT", "iterationNo": 2,
                "quantumCircuit": {
                    "circuitFormat": "QCIS", "circuitContent": "X Q0\nM Q0",
                    "shots": 1000, "topologyMappingEnabled": False,
                },
            },
        ])
        self.assertEqual(peer.messages("COMPLETED"), [{
            "eventType": "COMPLETED", "result": final_result,
            "stdout": "两轮计算完成\n", "exitCode": 0,
        }])
        submit_positions = [
            index for index, message in enumerate(peer.sent)
            if message["eventType"] == "QUANTUM_SUBMIT"
        ]
        self.assertTrue(any(
            submit_positions[0] < index < submit_positions[1]
            and message == {"eventType": "PROGRESS", "progress": 50}
            for index, message in enumerate(peer.sent)
        ))
        self.assertEqual(peer.sent[-1]["eventType"], "COMPLETED")

    def test_retryable_started_is_retried_on_same_connection(self):
        def respond(peer, message):
            if message["eventType"] == "STARTED" and len(peer.messages("STARTED")) == 1:
                peer.ack(message, accepted=False, retryable=True,
                         errorCode="400003", errorMessage="not ready")
                return True
            return False

        peer = FakeSocket(respond)
        self.client(peer).connect()
        self.assertEqual(peer.messages("STARTED"), [{"eventType": "STARTED"}] * 2)
        self.create_connection.assert_called_once()

    def test_submit_waits_for_ack_and_preserves_simplified_payload(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        self.assertEqual(client.submit_quantum(CIRCUIT, 1000, topology_mapping_enabled=False), 1)
        self.assertEqual(peer.messages("QUANTUM_SUBMIT"), [{
            "eventType": "QUANTUM_SUBMIT",
            "iterationNo": 1,
            "quantumCircuit": {
                "circuitFormat": "QCIS",
                "circuitContent": CIRCUIT,
                "shots": 1000,
                "topologyMappingEnabled": False,
            },
        }])

    def test_accepted_submit_is_not_a_quantum_result_and_timeout_does_not_cancel(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        client.submit_quantum(CIRCUIT, 100)
        with self.assertRaises(QRMITimeoutError):
            client.wait_quantum_result(timeout=0.03)
        self.assertFalse(peer.closed)
        self.assertEqual(len(peer.messages("QUANTUM_SUBMIT")), 1)
        peer.result(result={"counts": {"0": 100}})
        self.assertEqual(client.wait_quantum_result(timeout=0.5)["result"], {"counts": {"0": 100}})
        self.assertFalse(peer.messages("FAILED"))

    def test_next_iteration_requires_previous_success_and_cannot_skip(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        with self.assertRaises(HybridValidationError):
            client.submit_quantum(CIRCUIT, 100, iteration_no=2)
        self.assertEqual(client.submit_quantum(CIRCUIT, 100), 1)
        with self.assertRaises(HybridValidationError):
            client.submit_quantum("X Q0\nM Q0", 100)
        with self.assertRaises(HybridValidationError):
            client.complete({"energy": -1})
        peer.result(result=0)
        client.wait_quantum_result(timeout=0.5)
        with self.assertRaises(HybridValidationError):
            client.submit_quantum(CIRCUIT, 100, iteration_no=3)
        self.assertEqual(client.submit_quantum("X Q0\nM Q0", 100), 2)

    def test_same_iteration_cannot_change_circuit_or_shots(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        client.submit_quantum(CIRCUIT, 100)
        self.assertEqual(client.submit_quantum(CIRCUIT, 100, iteration_no=1), 1)
        for circuit, shots in (("X Q0\nM Q0", 100), (CIRCUIT, 101)):
            with self.subTest(circuit=circuit, shots=shots):
                with self.assertRaises(HybridValidationError):
                    client.submit_quantum(circuit, shots, iteration_no=1)

    def test_result_before_submit_ack_is_matched_and_cached(self):
        expected = {"counts": {"0": 42}}

        def respond(peer, message):
            if message["eventType"] == "QUANTUM_SUBMIT":
                peer.result(message["iterationNo"], expected)
            return False

        peer = FakeSocket(respond)
        client = self.client(peer).connect()
        self.assertEqual(client.run_quantum(CIRCUIT, 42, timeout=0.5), expected)
        self.assertEqual(client.wait_quantum_result(1, timeout=0.5), {
            "eventType": "QPU_RESULT", "iterationNo": 1,
            "taskStatus": 3, "result": expected,
        })

    def test_qpu_result_is_not_base64_or_json_string_decoded(self):
        values = [None, False, 0, [], '"raw JSON string"', {"result": "unchanged"}]

        def respond(peer, message):
            if message["eventType"] == "QUANTUM_SUBMIT":
                peer.ack(message)
                peer.result(message["iterationNo"], values[message["iterationNo"] - 1])
                return True
            return False

        peer = FakeSocket(respond)
        client = self.client(peer).connect()
        for expected in values:
            with self.subTest(expected=expected):
                self.assertEqual(client.run_quantum(CIRCUIT, 10, timeout=0.5), expected)

    def test_duplicate_result_does_not_advance_iteration_or_replace_result(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        client.submit_quantum(CIRCUIT, 10)
        peer.result(result={"energy": -1})
        peer.result(result={"energy": -1})
        self.assertEqual(client.wait_quantum_result(timeout=0.5)["result"], {"energy": -1})
        self.assertEqual(client.submit_quantum(CIRCUIT, 20), 2)
        peer.result(iteration=2, result={"energy": -2})
        self.assertEqual(client.wait_quantum_result(timeout=0.5)["iterationNo"], 2)
        self.assertEqual(client.wait_quantum_result(1, timeout=0.5)["result"], {"energy": -1})

    def test_failed_and_cancelled_results_stop_normal_iteration(self):
        for status in (4, 5):
            with self.subTest(status=status):
                peer = FakeSocket()
                client = self.client(peer).connect()
                client.submit_quantum(CIRCUIT, 100)
                peer.result(status=status)
                self.assertEqual(client.wait_quantum_result(timeout=0.5)["taskStatus"], status)
                with self.assertRaises((HybridValidationError, QRMIQuantumError)):
                    client.submit_quantum(CIRCUIT, 100)
                with self.assertRaises((HybridValidationError, QRMIQuantumError)):
                    client.complete({"energy": -1})
                client.close()

    def test_run_quantum_raises_result_error_with_status_and_iteration(self):
        def respond(peer, message):
            if message["eventType"] == "QUANTUM_SUBMIT":
                peer.ack(message)
                peer.result(message["iterationNo"], {"reason": "cancelled"}, status=5)
                return True
            return False

        client = self.client(FakeSocket(respond)).connect()
        with self.assertRaises(QRMIQuantumError) as caught:
            client.run_quantum(CIRCUIT, 100, timeout=0.5)
        self.assertEqual(caught.exception.iteration_no, 1)
        self.assertEqual(caught.exception.task_status, 5)
        self.assertEqual(caught.exception.result, {"reason": "cancelled"})

    def test_automatic_heartbeats_run_during_cpu_work_and_qpu_wait(self):
        peer = FakeSocket()
        client = self.client(peer, heartbeat_seconds=0.02).connect()
        peer.wait_for("PROGRESS", count=2)
        client.progress(30)
        client.submit_quantum(CIRCUIT, 100)
        previous_count = len(peer.messages("PROGRESS"))
        peer.wait_for("PROGRESS", count=previous_count + 2)
        self.assertEqual(peer.messages("PROGRESS")[-1]["progress"], 30)
        self.assertEqual(peer.sent[0]["eventType"], "STARTED")
        peer.result(result="done")
        self.assertEqual(client.wait_quantum_result(timeout=0.5)["result"], "done")

    def test_retryable_submit_resends_identical_iteration_and_payload(self):
        def respond(peer, message):
            if message["eventType"] == "QUANTUM_SUBMIT" and len(peer.messages("QUANTUM_SUBMIT")) == 1:
                peer.ack(message, accepted=False, retryable=True,
                         errorCode="400003", errorMessage="temporarily busy")
                return True
            return False

        peer = FakeSocket(respond)
        client = self.client(peer).connect()
        self.assertEqual(client.submit_quantum(CIRCUIT, 100), 1)
        messages = peer.messages("QUANTUM_SUBMIT")
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0], messages[1])
        self.create_connection.assert_called_once()

    def test_lost_submit_ack_is_replayed_while_connection_remains_open(self):
        def respond(peer, message):
            return (
                message["eventType"] == "QUANTUM_SUBMIT"
                and len(peer.messages("QUANTUM_SUBMIT")) == 1
            )

        peer = FakeSocket(respond)
        client = self.client(peer).connect()
        self.assertEqual(client.submit_quantum(CIRCUIT, 100), 1)
        submitted = peer.messages("QUANTUM_SUBMIT")
        self.assertEqual(len(submitted), 2)
        self.assertEqual(submitted[0], submitted[1])
        peer.result(result=42)
        self.assertEqual(client.wait_quantum_result(timeout=0.5)["result"], 42)
        self.create_connection.assert_called_once()

    def test_fatal_submit_error_is_exposed_without_retry(self):
        def respond(peer, message):
            if message["eventType"] == "QUANTUM_SUBMIT":
                peer.ack(message, accepted=False, retryable=False,
                         errorCode="400002", errorMessage="different circuit")
                return True
            return False

        peer = FakeSocket(respond)
        client = self.client(peer).connect()
        with self.assertRaises(QRMIRequestError) as caught:
            client.submit_quantum(CIRCUIT, 100)
        error = caught.exception
        self.assertEqual(error.event_type, "QUANTUM_SUBMIT")
        self.assertEqual(error.iteration_no, 1)
        self.assertFalse(error.retryable)
        self.assertEqual(error.error_code, "400002")
        self.assertEqual(error.error_message, "different circuit")
        self.assertEqual(len(peer.messages("QUANTUM_SUBMIT")), 1)
        self.create_connection.assert_called_once()

    def test_background_progress_rejection_reaches_result_waiter(self):
        def respond(peer, message):
            if message["eventType"] == "PROGRESS" and message.get("progress") == 10:
                peer.ack(message, accepted=False, retryable=False,
                         errorCode="400003", errorMessage="execution expired")
                return True
            return False

        peer = FakeSocket(respond)
        client = self.client(peer).connect()
        client.submit_quantum(CIRCUIT, 100)
        client.progress(10)
        with self.assertRaises(QRMIRequestError) as caught:
            client.wait_quantum_result(timeout=0.5)
        self.assertEqual(caught.exception.error_code, "400003")
        self.assertEqual(caught.exception.event_type, "PROGRESS")

    def test_reconnect_after_accepted_submit_resumes_wait_without_resubmitting(self):
        first = FakeSocket()

        def respond(peer, message):
            if message["eventType"] == "STARTED":
                peer.ack(message)
                peer.result(result={"counts": {"1": 100}})
                return True
            return False

        second = FakeSocket(respond)
        client = self.client(first, second).connect()
        client.submit_quantum(CIRCUIT, 100)
        first.disconnect()
        self.assertEqual(client.wait_quantum_result(timeout=0.5)["result"], {"counts": {"1": 100}})
        self.assertEqual(second.sent[0], {"eventType": "STARTED", "iterationNo": 1})
        self.assertFalse(second.messages("QUANTUM_SUBMIT"))
        for call in self.create_connection.call_args_list:
            self.assertEqual(call.kwargs["header"]["Authorization"], "Bearer " + TICKET)

    def test_reconnect_after_lost_submit_ack_replays_same_request(self):
        def disconnect_before_ack(peer, message):
            if message["eventType"] == "QUANTUM_SUBMIT":
                peer.disconnect()
                return True
            return False

        first, second = FakeSocket(disconnect_before_ack), FakeSocket()
        client = self.client(first, second).connect()
        self.assertEqual(client.submit_quantum(CIRCUIT, 100), 1)
        self.assertEqual(second.sent[0], {"eventType": "STARTED"})
        self.assertEqual(first.messages("QUANTUM_SUBMIT"), second.messages("QUANTUM_SUBMIT"))
        second.result(result=42)
        self.assertEqual(client.wait_quantum_result(timeout=0.5)["result"], 42)

    def test_explicit_recovery_iteration_is_sent_in_started(self):
        def respond(peer, message):
            if message["eventType"] == "STARTED":
                peer.ack(message)
                peer.result(iteration=2, result="recovered")
                return True
            return False

        peer = FakeSocket(respond)
        client = self.client(peer).connect(iteration_no=2)
        self.assertEqual(peer.sent[0], {"eventType": "STARTED", "iterationNo": 2})
        self.assertEqual(client.wait_quantum_result(2, timeout=0.5)["result"], "recovered")
        self.assertEqual(client.submit_quantum(CIRCUIT, 100), 3)

    def test_unauthorized_handshake_is_not_retried(self):
        client = self.client()
        self.create_connection.side_effect = websocket.WebSocketBadStatusException(
            "unauthorized", status_code=401
        )
        with self.assertRaises(HybridAuthenticationError) as caught:
            client.connect()
        self.assertEqual(caught.exception.status_code, 401)
        self.create_connection.assert_called_once()

    def test_submit_ack_timeout_stops_future_transmissions(self):
        def respond(peer, message):
            return message["eventType"] == "QUANTUM_SUBMIT"

        peer = FakeSocket(respond)
        client = self.client(peer, request_timeout=0.05).connect()
        with self.assertRaises(QRMITimeoutError):
            client.submit_quantum(CIRCUIT, 100)
        self.assertTrue(peer.closed)
        transmitted = peer.messages("QUANTUM_SUBMIT")
        self.assertGreaterEqual(len(transmitted), 2)
        self.assertTrue(all(message == transmitted[0] for message in transmitted))
        with self.assertRaises(QRMITimeoutError):
            client.submit_quantum(CIRCUIT, 100, iteration_no=1)
        self.assertEqual(peer.messages("QUANTUM_SUBMIT"), transmitted)
        self.create_connection.assert_called_once()

    def test_policy_close_stops_without_reconnecting(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        client.submit_quantum(CIRCUIT, 100)
        peer.incoming.put((websocket.ABNF.OPCODE_CLOSE, (1008).to_bytes(2, "big")))
        with self.assertRaises(QRMIConnectionError):
            client.wait_quantum_result(timeout=0.5)
        self.create_connection.assert_called_once()

    def test_retryable_submit_rejection_without_identifiable_iteration(self):
        def respond(peer, message):
            if message["eventType"] == "QUANTUM_SUBMIT" and len(peer.messages("QUANTUM_SUBMIT")) == 1:
                peer.push({
                    "eventType": "QUANTUM_SUBMIT", "accepted": False,
                    "retryable": True, "errorCode": "400003", "errorMessage": "temporarily unavailable",
                })
                return True
            return False

        peer = FakeSocket(respond)
        client = self.client(peer).connect()
        self.assertEqual(client.submit_quantum(CIRCUIT, 10), 1)
        submitted = peer.messages("QUANTUM_SUBMIT")
        self.assertEqual(submitted, [submitted[0]] * 2)

    def test_stale_retryable_rejection_does_not_interrupt_the_next_round(self):
        def respond(peer, message):
            if message["eventType"] == "QUANTUM_SUBMIT":
                if message["iterationNo"] == 2:
                    peer.push({
                        "eventType": "QUANTUM_SUBMIT", "iterationNo": 1,
                        "accepted": False, "retryable": True,
                        "errorCode": "400003", "errorMessage": "delayed retry response",
                    })
                peer.ack(message)
                peer.result(message["iterationNo"], {"ok": True})
                return True
            return False

        peer = FakeSocket(respond)
        client = self.client(peer).connect()
        self.assertEqual(client.run_quantum(CIRCUIT, 10, timeout=0.5), {"ok": True})
        self.assertEqual(client.run_quantum(CIRCUIT, 10, timeout=0.5), {"ok": True})

    def test_cached_result_does_not_hide_a_background_protocol_error(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        client.submit_quantum(CIRCUIT, 10)
        peer.result(result={"energy": 1})
        self.assertEqual(client.wait_quantum_result(timeout=0.5)["result"], {"energy": 1})
        peer.result(result={"energy": 2})
        with client._cv:
            self.assertTrue(client._cv.wait_for(lambda: client._fatal is not None, timeout=0.5))
        with self.assertRaises(HybridProtocolError):
            client.wait_quantum_result(1, timeout=0.5)

    def test_malformed_json_is_reported_to_the_waiter(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        client.submit_quantum(CIRCUIT, 100)
        peer.incoming.put((websocket.ABNF.OPCODE_TEXT, "{not-json"))
        with self.assertRaises(HybridProtocolError):
            client.wait_quantum_result(timeout=0.5)
        self.create_connection.assert_called_once()

    def test_returned_result_does_not_mutate_the_cached_envelope(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        client.submit_quantum(CIRCUIT, 100)
        peer.result(result={"counts": {"0": 100}})
        response = client.wait_quantum_result(timeout=0.5)
        response["taskStatus"] = 4
        response["result"]["counts"]["0"] = 0
        self.assertEqual(client.wait_quantum_result(1, timeout=0.5), {
            "eventType": "QPU_RESULT", "iterationNo": 1, "taskStatus": 3,
            "result": {"counts": {"0": 100}},
        })
        self.assertEqual(client.submit_quantum(CIRCUIT, 100), 2)

    def test_reconnect_attempts_are_bounded(self):
        first = FakeSocket()
        client = self.client(first, max_reconnect_attempts=2).connect()
        client.submit_quantum(CIRCUIT, 100)
        self.create_connection.side_effect = OSError("network unavailable")
        first.disconnect()
        with self.assertRaises(QRMIConnectionError):
            client.wait_quantum_result(timeout=0.5)
        self.assertLessEqual(self.create_connection.call_count, 3)

    def test_complete_archives_result_and_logs_then_closes(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        client.complete({"energy": -1.5}, stdout="finished\n", stderr="", exit_code=0)
        completed = peer.messages("COMPLETED")
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0]["result"], {"energy": -1.5})
        self.assertEqual(completed[0]["stdout"], "finished\n")
        self.assertEqual(completed[0]["stderr"], "")
        self.assertEqual(completed[0].get("exitCode", 0), 0)
        self.assertTrue(peer.closed)

    def test_lost_completed_ack_replays_identical_final_result(self):
        def respond(peer, message):
            if message["eventType"] == "COMPLETED":
                peer.disconnect()
                return True
            return False

        first, second = FakeSocket(respond), FakeSocket()
        client = self.client(first, second).connect()
        client.complete({"energy": -1.5}, stdout="finished")
        self.assertEqual(second.sent[0], {"eventType": "STARTED"})
        self.assertEqual(first.messages("COMPLETED"), second.messages("COMPLETED"))
        self.assertTrue(second.closed)

    def test_lost_completed_ack_is_replayed_while_connection_remains_open(self):
        def respond(peer, message):
            return (
                message["eventType"] == "COMPLETED"
                and len(peer.messages("COMPLETED")) == 1
            )

        peer = FakeSocket(respond)
        client = self.client(peer).connect()
        client.complete({"energy": -1.5}, stdout="finished")
        completed = peer.messages("COMPLETED")
        self.assertEqual(len(completed), 2)
        self.assertEqual(completed[0], completed[1])
        self.assertTrue(peer.closed)
        self.create_connection.assert_called_once()

    def test_terminal_ack_is_read_before_sending_another_heartbeat(self):
        for terminal_event in ("COMPLETED", "FAILED"):
            with self.subTest(terminal_event=terminal_event):
                terminal_sent = threading.Event()
                heartbeat_ack_received = threading.Event()
                release_heartbeat_ack = threading.Event()
                self.addCleanup(release_heartbeat_ack.set)

                def respond(peer, message):
                    if terminal_sent.is_set():
                        # The server has queued its final ack and stopped reading.
                        raise websocket.WebSocketConnectionClosedException("server finished")
                    if message["eventType"] == terminal_event:
                        terminal_sent.set()
                    return False

                peer = FakeSocket(respond)
                original_receive = peer.recv_data

                def receive(control_frame=False):
                    frame = original_receive(control_frame=control_frame)
                    if (
                        frame[0] == websocket.ABNF.OPCODE_TEXT
                        and json.loads(frame[1])["eventType"] == "PROGRESS"
                        and not heartbeat_ack_received.is_set()
                    ):
                        heartbeat_ack_received.set()
                        if not release_heartbeat_ack.wait(timeout=1):
                            raise AssertionError("Heartbeat ack was not released")
                    return frame

                peer.recv_data = receive
                client = self.client(peer).connect()
                self.assertTrue(heartbeat_ack_received.wait(timeout=0.5))
                client.progress(75)
                outcome = queue.Queue()

                def finish():
                    try:
                        if terminal_event == "COMPLETED":
                            client.complete({"done": True})
                        else:
                            client.fail("solver failed")
                        outcome.put(None)
                    except Exception as error:
                        outcome.put(error)

                worker = threading.Thread(target=finish, daemon=True)
                worker.start()
                # Synchronize the race: both a new progress value and the final
                # request must be ready when the I/O thread consumes this ack.
                with client._cv:
                    self.assertTrue(client._cv.wait_for(
                        lambda: client._pending is not None, timeout=0.5,
                    ))
                release_heartbeat_ack.set()
                self.assertIsNone(outcome.get(timeout=1))
                worker.join(timeout=0.5)
                self.assertFalse(worker.is_alive())
                self.assertEqual(peer.sent[-1]["eventType"], terminal_event)
                self.assertTrue(peer.closed)
                self.assertEqual(self.create_connection.call_count, 1)
                self.create_connection.reset_mock()

    def test_fail_reports_error_and_closes_after_ack(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        client.fail("solver failed", error_code="SOLVER_ERROR", exit_code=2,
                    stdout="iteration 1", stderr="traceback")
        self.assertEqual(peer.messages("FAILED"), [{
            "eventType": "FAILED", "errorMessage": "solver failed",
            "errorCode": "SOLVER_ERROR", "exitCode": 2,
            "stdout": "iteration 1", "stderr": "traceback",
        }])
        self.assertTrue(peer.closed)

    def test_lost_failed_ack_is_not_replayed_without_idempotency_guarantee(self):
        def respond(peer, message):
            if message["eventType"] == "FAILED":
                peer.disconnect()
                return True
            return False

        peer = FakeSocket(respond)
        client = self.client(peer).connect()
        with self.assertRaises(QRMIConnectionError):
            client.fail("solver failed")
        self.assertEqual(len(peer.messages("FAILED")), 1)
        self.create_connection.assert_called_once()

    def test_close_wakes_a_thread_waiting_for_quantum_result(self):
        peer = FakeSocket()
        client = self.client(peer).connect()
        client.submit_quantum(CIRCUIT, 100)
        entered = threading.Event()
        errors = queue.Queue()

        def wait_for_result():
            entered.set()
            try:
                client.wait_quantum_result()
            except Exception as error:
                errors.put(error)

        waiter = threading.Thread(target=wait_for_result, daemon=True)
        waiter.start()
        self.assertTrue(entered.wait(timeout=0.5))
        client.close()
        waiter.join(timeout=0.5)
        self.assertFalse(waiter.is_alive())
        self.assertIsInstance(errors.get(timeout=0.5), QRMIConnectionError)


if __name__ == "__main__":
    unittest.main()
