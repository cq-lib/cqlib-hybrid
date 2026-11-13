"""Exercise the real websocket-client transport against a local RFC 6455 peer.

The test server uses only the standard library and binds an ephemeral loopback
port. No QRMI deployment, credentials or external network access are required.
"""

import base64
import hashlib
import json
import queue
import socket
import struct
import threading
import unittest
from unittest.mock import patch

from cqlib_hybrid import QRMIClient


_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class _LocalQRMIServer:
    """One-connection peer that releases each QPU result after a heartbeat."""

    def __init__(self, results):
        self.results = results
        self.messages = []
        self.headers = {}
        self.request_line = None
        self.cpu_heartbeat = threading.Event()
        self.closed_after_completion = threading.Event()
        self.pong_received = threading.Event()
        self.quantum_heartbeats = []
        self.errors = queue.Queue()
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.listener.bind(("127.0.0.1", 0))
            self.listener.listen(1)
            self.listener.settimeout(5)
        except BaseException:
            self.listener.close()
            raise
        self.url = (
            f"ws://127.0.0.1:{self.listener.getsockname()[1]}"
            "/internal/workflow/cpu-executions"
        )
        self.connection = None
        self.buffer = bytearray()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def close(self):
        if self.connection is not None:
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.connection.close()
        self.listener.close()
        if self.thread.ident is not None:
            self.thread.join(timeout=5)

    def _read_exact(self, size):
        while len(self.buffer) < size:
            chunk = self.connection.recv(max(4096, size - len(self.buffer)))
            if not chunk:
                raise EOFError("Client closed the WebSocket transport")
            self.buffer.extend(chunk)
        result = bytes(self.buffer[:size])
        del self.buffer[:size]
        return result

    def _handshake(self):
        while b"\r\n\r\n" not in self.buffer:
            chunk = self.connection.recv(4096)
            if not chunk:
                raise EOFError("Client closed before HTTP upgrade")
            self.buffer.extend(chunk)
        boundary = self.buffer.index(b"\r\n\r\n") + 4
        request = bytes(self.buffer[:boundary]).decode("ascii")
        del self.buffer[:boundary]
        lines = request.split("\r\n")
        self.request_line = lines[0]
        self.headers = {
            name.lower(): value.strip()
            for name, value in (line.split(":", 1) for line in lines[1:] if line)
        }
        if self.headers.get("upgrade", "").lower() != "websocket":
            raise AssertionError("Expected a native WebSocket upgrade")
        if self.headers.get("sec-websocket-version") != "13":
            raise AssertionError("Expected WebSocket protocol version 13")
        accept = base64.b64encode(hashlib.sha1(
            (self.headers["sec-websocket-key"] + _GUID).encode("ascii")
        ).digest()).decode("ascii")
        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
        )
        self.connection.sendall(response.encode("ascii"))

    def _receive_frame(self):
        first, second = self._read_exact(2)
        if first & 0x70:
            raise AssertionError("Unexpected reserved WebSocket frame flags")
        if not second & 0x80:
            raise AssertionError("Client WebSocket frames must be masked")
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._read_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._read_exact(8))[0]
        if length > 3 * 1024 * 1024:
            raise AssertionError("Unexpected oversized client frame")
        mask = self._read_exact(4)
        data = self._read_exact(length)
        payload = bytes(value ^ mask[index % 4] for index, value in enumerate(data))
        return first & 0x0F, bool(first & 0x80), payload

    def _send_frame(self, payload, opcode=1, final=True):
        header = bytes([(0x80 if final else 0) | opcode])
        length = len(payload)
        if length < 126:
            header += bytes([length])
        elif length < 65536:
            header += b"\x7e" + struct.pack("!H", length)
        else:
            header += b"\x7f" + struct.pack("!Q", length)
        self.connection.sendall(header + payload)

    def _send_json(self, message, fragmented=False):
        payload = json.dumps(message, ensure_ascii=False).encode("utf-8")
        if fragmented:
            # Split inside a multibyte character, and interleave a control frame.
            split = payload.index("量".encode("utf-8")) + 1
            self._send_frame(payload[:split], final=False)
            self._send_frame(b"qrmi-ping", opcode=9)
            self._send_frame(payload[split:], opcode=0)
        else:
            self._send_frame(payload)

    def _run(self):
        completed = False
        completed_message = None
        accepted_submissions = {}
        pending_iteration = None
        try:
            self.connection, _address = self.listener.accept()
            self.connection.settimeout(5)
            self._handshake()
            while True:
                try:
                    opcode, final, payload = self._receive_frame()
                except (EOFError, ConnectionResetError):
                    if not completed:
                        raise AssertionError("Client disconnected before COMPLETED acknowledgement")
                    self.closed_after_completion.set()
                    return
                if opcode == 8:
                    if not completed:
                        raise AssertionError("Client sent CLOSE before completing the task")
                    self.closed_after_completion.set()
                    return
                if opcode == 10:
                    if payload != b"qrmi-ping":
                        raise AssertionError("Unexpected WebSocket pong payload")
                    self.pong_received.set()
                    continue
                if opcode != 1 or not final:
                    raise AssertionError("Expected a complete UTF-8 JSON text message")
                message = json.loads(payload.decode("utf-8"))
                self.messages.append(message)
                event = message["eventType"]
                if len(self.messages) == 1 and message != {"eventType": "STARTED"}:
                    raise AssertionError("STARTED must be the first application message")
                ack = {"eventType": event, "accepted": True}
                if event == "QUANTUM_SUBMIT":
                    number = message["iterationNo"]
                    ack["iterationNo"] = number
                    if number in accepted_submissions:
                        if message != accepted_submissions[number]:
                            raise AssertionError("A repeated quantum iteration changed its content")
                    else:
                        if pending_iteration is not None or number != len(accepted_submissions) + 1:
                            raise AssertionError("Expected consecutive, non-overlapping quantum iterations")
                        accepted_submissions[number] = message
                        pending_iteration = number
                elif event == "COMPLETED":
                    if completed_message is not None and message != completed_message:
                        raise AssertionError("A repeated completion changed its content")
                    completed_message = message
                    completed = True
                elif event not in ("STARTED", "PROGRESS"):
                    raise AssertionError(f"Unexpected event: {event}")
                try:
                    self._send_json(ack)
                except (BrokenPipeError, ConnectionResetError):
                    # A queued duplicate acknowledgement can race with the
                    # client closing after it receives the first terminal ack.
                    if not completed:
                        raise
                    self.closed_after_completion.set()
                    return
                if event == "PROGRESS":
                    if pending_iteration is None:
                        self.cpu_heartbeat.set()
                    else:
                        number = pending_iteration
                        self.quantum_heartbeats.append(number)
                        self._send_json({
                            "eventType": "QPU_RESULT", "iterationNo": number,
                            "taskStatus": 3, "result": self.results[number - 1],
                        }, fragmented=number == 1)
                        pending_iteration = None
        except BaseException as error:
            self.errors.put(error)


class QRMITransportIntegrationTests(unittest.TestCase):
    def test_real_websocket_handshake_heartbeats_utf8_results_and_completion(self):
        expected_results = [
            {"counts": {"0": 600, "1": 400}, "description": "量子结果" * 20},
            ["原始结果", None, 0, False],
        ]
        server = _LocalQRMIServer(expected_results)
        self.addCleanup(server.close)
        server.start()
        client = QRMIClient(
            server.url, "integration-job-ticket", heartbeat_seconds=0.03,
            request_timeout=3, connect_timeout=1, retry_interval=0.1,
            max_reconnect_attempts=0,
        )
        self.addCleanup(client.close)
        # Keep local traffic independent of proxy configuration on the machine.
        with patch.dict("os.environ", {"no_proxy": "127.0.0.1", "NO_PROXY": "127.0.0.1"}):
            with client:
                self.assertTrue(server.cpu_heartbeat.wait(timeout=3))
                first = client.run_quantum("H Q0\nM Q0", shots=1000, timeout=3)
                self.assertEqual(first, expected_results[0])
                client.progress(50)
                second = client.run_quantum(
                    "X Q0\nM Q0", shots=1000,
                    topology_mapping_enabled=False, timeout=3,
                )
                self.assertEqual(second, expected_results[1])
                final_result = {"说明": "两轮计算完成", "results": [first, second]}
                client.complete(final_result, stdout="计算结束\n", stderr="")

        self.assertTrue(server.closed_after_completion.wait(timeout=3))
        server.thread.join(timeout=3)
        self.assertFalse(server.thread.is_alive())
        if not server.errors.empty():
            raise server.errors.get_nowait()
        self.assertEqual(server.request_line, "GET /internal/workflow/cpu-executions HTTP/1.1")
        self.assertEqual(server.headers["authorization"], "Bearer integration-job-ticket")
        self.assertNotIn("origin", server.headers)
        self.assertEqual(server.messages[0], {"eventType": "STARTED"})
        self.assertEqual(server.quantum_heartbeats, [1, 2])
        self.assertTrue(server.pong_received.is_set())
        self.assertTrue(any(
            message.get("progress") == 50
            for message in server.messages if message["eventType"] == "PROGRESS"
        ))
        submissions = [message for message in server.messages if message["eventType"] == "QUANTUM_SUBMIT"]
        unique_submissions = {}
        for message in submissions:
            original = unique_submissions.setdefault(message["iterationNo"], message)
            self.assertEqual(message, original)
        self.assertEqual(list(unique_submissions.values()), [
            {
                "eventType": "QUANTUM_SUBMIT", "iterationNo": 1,
                "quantumCircuit": {
                    "circuitFormat": "QCIS", "circuitContent": "H Q0\nM Q0", "shots": 1000,
                },
            },
            {
                "eventType": "QUANTUM_SUBMIT", "iterationNo": 2,
                "quantumCircuit": {
                    "circuitFormat": "QCIS", "circuitContent": "X Q0\nM Q0", "shots": 1000,
                    "topologyMappingEnabled": False,
                },
            },
        ])
        completed = [message for message in server.messages if message["eventType"] == "COMPLETED"]
        self.assertTrue(completed)
        for message in completed:
            self.assertEqual(message, completed[0])
        self.assertEqual(completed[0]["result"], final_result)
        self.assertEqual(completed[0]["stdout"], "计算结束\n")
        self.assertEqual(completed[0]["stderr"], "")
        self.assertNotIn("integration-job-ticket", json.dumps(server.messages))


if __name__ == "__main__":
    unittest.main()
