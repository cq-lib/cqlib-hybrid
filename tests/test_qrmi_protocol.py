"""Contract and byte-boundary tests for QRMI's simplified JSON protocol."""

from __future__ import annotations

import base64
import json
import unittest

from cqlib_hybrid.exceptions import HybridProtocolError, HybridValidationError
from cqlib_hybrid.qrmi_protocol import (
    MAX_BUSINESS_BYTES,
    MAX_LOG_BYTES,
    MAX_MESSAGE_BYTES,
    completed_message,
    decode_message,
    encode_message,
    failed_message,
    progress_message,
    quantum_submit_message,
    started_message,
)


class OutgoingMessageTests(unittest.TestCase):
    def test_document_startup_and_heartbeat_examples(self) -> None:
        self.assertEqual(encode_message(started_message()), '{"eventType":"STARTED"}')
        self.assertEqual(started_message(2), {"eventType": "STARTED", "iterationNo": 2})
        self.assertEqual(progress_message(), {"eventType": "PROGRESS"})
        for percentage in (0, 30, 100):
            self.assertEqual(progress_message(percentage)["progress"], percentage)

    def test_quantum_example_and_optional_mapping(self) -> None:
        message = quantum_submit_message(1, "H Q0\nM Q0", 1000)
        self.assertEqual(message, {
            "eventType": "QUANTUM_SUBMIT",
            "iterationNo": 1,
            "quantumCircuit": {
                "circuitFormat": "QCIS",
                "circuitContent": "H Q0\nM Q0",
                "shots": 1000,
            },
        })
        for enabled in (False, True):
            mapped = quantum_submit_message(1, "M Q0", 10, enabled)
            self.assertIs(mapped["quantumCircuit"]["topologyMappingEnabled"], enabled)

    def test_rejects_invalid_positive_integer_fields(self) -> None:
        for value in (0, -1, True, False, 1.0, "1", [], {}):
            with self.subTest(value=value):
                with self.assertRaises(HybridValidationError):
                    started_message(value)
                with self.assertRaises(HybridValidationError):
                    quantum_submit_message(value, "M Q0", 10)
                with self.assertRaises(HybridValidationError):
                    quantum_submit_message(1, "M Q0", value)

    def test_rejects_invalid_percentage_and_circuit_fields(self) -> None:
        for value in (-1, 101, True, False, 50.0, "50"):
            with self.subTest(progress=value), self.assertRaises(HybridValidationError):
                progress_message(value)
        for value in (None, "", " \n\t", 123):
            with self.subTest(circuit=value), self.assertRaises(HybridValidationError):
                quantum_submit_message(1, value, 10)
        for value in (0, 1, "true"):
            with self.subTest(mapping=value), self.assertRaises(HybridValidationError):
                quantum_submit_message(1, "M Q0", 10, value)

    def test_final_result_preserves_all_json_types_and_unicode(self) -> None:
        for result in (None, False, 0, 1.5, "量子结果", [], {}, [1, "0", None]):
            with self.subTest(result=result):
                message = completed_message(result, stdout="", stderr="日志")
                self.assertEqual(json.loads(encode_message(message))["result"], result)
                self.assertEqual(message["stdout"], "")
                self.assertEqual(message["stderr"], "日志")
        self.assertIn("量子", encode_message(completed_message("量子")))
        self.assertNotIn("\\u", encode_message(completed_message("量子")))

    def test_failure_fields_and_character_limits(self) -> None:
        message = failed_message("算法失败", stdout="输出", stderr="错误")
        self.assertEqual(message["errorCode"], "ALGORITHM_FAILED")
        self.assertEqual(message["exitCode"], 1)
        self.assertEqual(message["stdout"], "输出")
        self.assertEqual(message["stderr"], "错误")
        failed_message("错" * 2048, "错" * 128, -1)
        for error in (None, "", "  ", 1, "错" * 2049):
            with self.subTest(error=error), self.assertRaises(HybridValidationError):
                failed_message(error)
        for code in (None, 123, "错" * 129):
            with self.subTest(code=code), self.assertRaises(HybridValidationError):
                failed_message("failed", code)

    def test_exit_codes_obey_terminal_status(self) -> None:
        for value in (1, -1, True, False, 0.0, "0", None):
            with self.subTest(completed_exit=value), self.assertRaises(HybridValidationError):
                completed_message({}, exit_code=value)
        for value in (0, True, False, 1.0, "1", None):
            with self.subTest(failed_exit=value), self.assertRaises(HybridValidationError):
                failed_message("failed", exit_code=value)

    def test_strict_json_rejects_invalid_values(self) -> None:
        circular = []
        circular.append(circular)
        for result in (float("nan"), float("inf"), {1: "bad key"}, (1, 2), {1, 2}, circular, "\ud800"):
            with self.subTest(result_type=type(result).__name__), self.assertRaises(HybridValidationError):
                completed_message(result)

    def test_direct_encoder_validates_client_shape(self) -> None:
        invalid_messages = (
            [], {}, {"eventType": None}, {"eventType": []}, {"eventType": "QPU_RESULT"},
            {"eventType": "COMPLETED"},
            {"eventType": "QUANTUM_SUBMIT", "iterationNo": 1, "quantumCircuit": []},
            {"eventType": "QUANTUM_SUBMIT", "iterationNo": 1, "quantumCircuit": {
                "circuitFormat": "QASM", "circuitContent": "M Q0", "shots": 1,
            }},
            {"eventType": "PROGRESS", "progress": None},
        )
        for message in invalid_messages:
            with self.subTest(message=message), self.assertRaises(HybridValidationError):
                encode_message(message)

    def test_result_business_size_includes_json_wrapper(self) -> None:
        overhead = len('{"result":""}'.encode("utf-8"))
        result = "a" * (MAX_BUSINESS_BYTES - overhead)
        completed_message(result)
        with self.assertRaisesRegex(HybridValidationError, "result exceeds"):
            completed_message(result + "a")

    def test_circuit_business_size_includes_json_wrapper(self) -> None:
        seed = quantum_submit_message(1, "M", 1)["quantumCircuit"]
        overhead = len(json.dumps({"quantumCircuit": seed}, separators=(",", ":"))) - 1
        circuit = "a" * (MAX_BUSINESS_BYTES - overhead)
        quantum_submit_message(1, circuit, 1)
        with self.assertRaisesRegex(HybridValidationError, "quantumCircuit exceeds"):
            quantum_submit_message(1, circuit + "a", 1)

    def test_business_limit_counts_escaped_json_bytes(self) -> None:
        # A newline requires two bytes inside JSON, even though the input is one.
        with self.assertRaisesRegex(HybridValidationError, "result exceeds"):
            completed_message("\n" * (MAX_BUSINESS_BYTES // 2))

    def test_log_limits_count_utf8_bytes_and_allow_exact_boundary(self) -> None:
        for field in ("stdout", "stderr"):
            with self.subTest(field=field):
                completed_message(None, **{field: "a" * MAX_LOG_BYTES})
                unicode_log = "量" * (MAX_LOG_BYTES // 3) + "a"
                self.assertEqual(len(unicode_log.encode("utf-8")), MAX_LOG_BYTES)
                failed_message("failed", **{field: unicode_log})
                with self.assertRaisesRegex(HybridValidationError, field + " exceeds"):
                    completed_message(None, **{field: unicode_log + "a"})
                with self.assertRaises(HybridValidationError):
                    failed_message("failed", **{field: 123})

    def test_aggregate_limit_includes_envelope_and_json_escaping(self) -> None:
        message = completed_message("", stdout="a" * MAX_LOG_BYTES, stderr="a" * MAX_LOG_BYTES)
        overhead = len(encode_message(message).encode("utf-8"))
        message["result"] = "a" * (MAX_MESSAGE_BYTES - overhead)
        self.assertEqual(len(encode_message(message).encode("utf-8")), MAX_MESSAGE_BYTES)
        message["result"] += "a"
        with self.assertRaisesRegex(HybridValidationError, "Client message exceeds"):
            encode_message(message)
        with self.assertRaisesRegex(HybridValidationError, "Client message exceeds"):
            failed_message("failed", stdout="\n" * MAX_LOG_BYTES, stderr="a" * MAX_LOG_BYTES)


class IncomingMessageTests(unittest.TestCase):
    def test_document_acknowledgements(self) -> None:
        for event in ("STARTED", "PROGRESS", "COMPLETED", "FAILED"):
            message = {"eventType": event, "accepted": True}
            self.assertEqual(decode_message(json.dumps(message)), message)
        message = {"eventType": "QUANTUM_SUBMIT", "iterationNo": 1, "accepted": True}
        self.assertEqual(decode_message(json.dumps(message)), message)

    def test_document_rejections_and_unparseable_request(self) -> None:
        for event in ("QUANTUM_SUBMIT", None, "UNKNOWN"):
            for retryable in (True, False):
                message = {
                    "eventType": event,
                    "accepted": False,
                    "retryable": retryable,
                    "errorCode": "400001",
                    "errorMessage": "消息参数不合法",
                }
                self.assertEqual(decode_message(json.dumps(message)), message)

    def test_rejections_require_boolean_flags_and_error_strings(self) -> None:
        valid = {"eventType": "STARTED", "accepted": False, "retryable": True,
                 "errorCode": "400003", "errorMessage": "not ready"}
        for key, invalid in (("accepted", 0), ("retryable", 1), ("retryable", "false"),
                             ("errorCode", 400003), ("errorMessage", None), ("eventType", [])):
            with self.subTest(field=key), self.assertRaises(HybridProtocolError):
                decode_message(json.dumps(dict(valid, **{key: invalid})))
        for key in valid:
            with self.subTest(missing=key), self.assertRaises(HybridProtocolError):
                decode_message(json.dumps({field: value for field, value in valid.items() if field != key}))

    def test_success_requires_valid_request_type_and_quantum_iteration(self) -> None:
        for event in (None, "UNKNOWN", [], 123):
            with self.subTest(event=event), self.assertRaises(HybridProtocolError):
                decode_message(json.dumps({"eventType": event, "accepted": True}))
        with self.assertRaises(HybridProtocolError):
            decode_message('{"eventType":"QUANTUM_SUBMIT","accepted":true}')
        for iteration in (0, -1, True, 1.0, "1", None):
            with self.subTest(iteration=iteration), self.assertRaises(HybridProtocolError):
                decode_message(json.dumps({"eventType": "QUANTUM_SUBMIT", "accepted": True,
                                           "iterationNo": iteration}))

    def test_qpu_result_accepts_arbitrary_json_and_terminal_states(self) -> None:
        for result in ({"counts": {"0": 600, "1": 400}}, "QPU output", [0, 1], 0, False, None):
            with self.subTest(result=result):
                message = {"eventType": "QPU_RESULT", "iterationNo": 1, "taskStatus": 3, "result": result}
                self.assertEqual(decode_message(json.dumps(message)), message)
        for status in (4, 5):
            message = {"eventType": "QPU_RESULT", "iterationNo": 2, "taskStatus": status}
            self.assertIsNone(decode_message(json.dumps(message))["result"])

    def test_qpu_result_decodes_platform_base64_experiment_results(self) -> None:
        # Real platform payload: one measured qubit, Base64 '0'/'1' outcomes.
        encoded = base64.b64encode(b"001011").decode("ascii")
        result = {
            "experimentResults": encoded,
            "waveforms": {"0": 0.5, "1": 0.5},
            "measureQubitList": ["Q0"],
            "experimentTaskId": "T123",
            "runningStatus": 1,
            "explain": "任务执行成功",
        }
        message = {"eventType": "QPU_RESULT", "iterationNo": 1, "taskStatus": 3, "result": result}
        decoded = decode_message(json.dumps(message))
        self.assertEqual(decoded["result"]["counts"], {"0": 3, "1": 3})
        self.assertEqual(decoded["result"]["shots"], 6)
        # Original fields are preserved, including the Base64 payload.
        self.assertEqual(decoded["result"]["experimentResults"], encoded)
        self.assertEqual(decoded["result"]["waveforms"], {"0": 0.5, "1": 0.5})
        self.assertEqual(decoded["result"]["measureQubitList"], ["Q0"])

    def test_qpu_result_decodes_multi_qubit_outcomes_per_shot(self) -> None:
        # Each shot holds one character per measured qubit, in list order.
        encoded = base64.b64encode(b"011001").decode("ascii")
        result = {
            "experimentResults": encoded,
            "measureQubitList": ["Q0", "Q1"],
        }
        message = {"eventType": "QPU_RESULT", "iterationNo": 1, "taskStatus": 3, "result": result}
        decoded = decode_message(json.dumps(message))
        self.assertEqual(decoded["result"]["counts"], {"01": 2, "10": 1})
        self.assertEqual(decoded["result"]["shots"], 3)

    def test_qpu_result_without_measure_list_assumes_one_qubit(self) -> None:
        encoded = base64.b64encode(b"0110").decode("ascii")
        message = {
            "eventType": "QPU_RESULT", "iterationNo": 1, "taskStatus": 3,
            "result": {"experimentResults": encoded, "waveforms": {}},
        }
        decoded = decode_message(json.dumps(message))
        self.assertEqual(decoded["result"]["counts"], {"0": 2, "1": 2})
        self.assertEqual(decoded["result"]["shots"], 4)

    def test_qpu_result_opaque_experiment_results_stay_untouched(self) -> None:
        # Without platform markers the field is not treated as Base64.
        for result in ({"experimentResults": "0011"}, {"experimentResults": "not-base64!!!"}):
            with self.subTest(result=result):
                message = {"eventType": "QPU_RESULT", "iterationNo": 1, "taskStatus": 3, "result": result}
                self.assertEqual(decode_message(json.dumps(message)), message)

    def test_qpu_result_rejects_invalid_platform_experiment_results(self) -> None:
        invalid_payloads = (
            "not-base64!!!",                                          # invalid Base64
            base64.b64encode(b"").decode("ascii"),                    # empty outcomes
            base64.b64encode(b"012").decode("ascii"),                 # non-binary outcome
            base64.b64encode(b"010").decode("ascii"),                 # not a multiple of 2 qubits
        )
        for encoded in invalid_payloads:
            with self.subTest(encoded=encoded), self.assertRaises(HybridProtocolError):
                decode_message(json.dumps({
                    "eventType": "QPU_RESULT", "iterationNo": 1, "taskStatus": 3,
                    "result": {"experimentResults": encoded, "measureQubitList": ["Q0", "Q1"]},
                }))
        with self.assertRaises(HybridProtocolError):
            decode_message(json.dumps({
                "eventType": "QPU_RESULT", "iterationNo": 1, "taskStatus": 3,
                "result": {
                    "experimentResults": base64.b64encode(b"01").decode("ascii"),
                    "measureQubitList": [],
                },
            }))

    def test_qpu_result_requires_success_payload_and_valid_status(self) -> None:
        with self.assertRaises(HybridProtocolError):
            decode_message('{"eventType":"QPU_RESULT","iterationNo":1,"taskStatus":3}')
        for status in (None, 0, 1, 2, 6, True, False, 3.0, "3"):
            with self.subTest(status=status), self.assertRaises(HybridProtocolError):
                decode_message(json.dumps({"eventType": "QPU_RESULT", "iterationNo": 1,
                                           "taskStatus": status, "result": {}}))
        for iteration in (None, 0, -1, True, 1.0, "1"):
            with self.subTest(iteration=iteration), self.assertRaises(HybridProtocolError):
                decode_message(json.dumps({"eventType": "QPU_RESULT", "iterationNo": iteration,
                                           "taskStatus": 4}))

    def test_non_json_binary_and_nonfinite_payloads_are_protocol_errors(self) -> None:
        for raw in (b'{"eventType":"STARTED","accepted":true}', None, [], "[]", "null", "{bad",
                    '{"eventType":"QPU_RESULT","iterationNo":1,"taskStatus":3,"result":NaN}',
                    '{"eventType":"QPU_RESULT","iterationNo":1,"taskStatus":3,"result":Infinity}',
                    '{"eventType":"QPU_RESULT","iterationNo":1,"taskStatus":3,"result":1e999}',
                    '{"eventType":"QPU_RESULT","iterationNo":1,"taskStatus":3,"result":"\\ud800"}'):
            with self.subTest(raw=raw), self.assertRaises(HybridProtocolError):
                decode_message(raw)


if __name__ == "__main__":
    unittest.main()
