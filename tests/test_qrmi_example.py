"""Exercise the QRMI container example without a deployed QRMI service."""

from pathlib import Path
import runpy
import unittest
from unittest.mock import call, patch


SCRIPT = Path(__file__).parents[1] / "examples/programs/qrmi_job.py"


class QRMIExampleTests(unittest.TestCase):
    def test_two_rounds_report_progress_and_complete_with_original_results(self):
        with patch("cqlib_hybrid.QRMIClient") as client_class:
            client = client_class.from_env.return_value.__enter__.return_value
            client.run_quantum.side_effect = [{"counts": {"0": 1000}}, [1, 2]]
            runpy.run_path(str(SCRIPT), run_name="__main__")

        client_class.from_env.assert_called_once_with()
        self.assertEqual(client.run_quantum.call_args_list, [
            call("RY Q0 0.0\nM Q0", shots=1000, timeout=600),
            call("RY Q0 0.5\nM Q0", shots=1000, timeout=600),
        ])
        self.assertEqual(client.progress.call_args_list, [call(50), call(100)])
        client.complete.assert_called_once_with({
            "shots": 1000,
            "records": [
                {"iteration_no": 1, "angle": 0.0, "quantum_result": {"counts": {"0": 1000}}},
                {"iteration_no": 2, "angle": 0.5, "quantum_result": [1, 2]},
            ],
        })
        client.fail.assert_not_called()

    def test_algorithm_failure_is_reported_and_reraised(self):
        error = RuntimeError("quantum failed")
        with patch("cqlib_hybrid.QRMIClient") as client_class:
            client = client_class.from_env.return_value.__enter__.return_value
            client.run_quantum.side_effect = error
            with self.assertRaises(RuntimeError) as caught:
                runpy.run_path(str(SCRIPT), run_name="__main__")

        self.assertIs(caught.exception, error)
        client.fail.assert_called_once_with("quantum failed")
        client.complete.assert_not_called()

    def test_failure_report_error_does_not_hide_original_error(self):
        error = RuntimeError("original error")
        with patch("cqlib_hybrid.QRMIClient") as client_class:
            client = client_class.from_env.return_value.__enter__.return_value
            client.run_quantum.side_effect = error
            client.fail.side_effect = ConnectionError("report failed")
            with self.assertRaises(RuntimeError) as caught:
                runpy.run_path(str(SCRIPT), run_name="__main__")

        self.assertIs(caught.exception, error)
        client.complete.assert_not_called()

    def test_keyboard_interrupt_is_not_reported_as_algorithm_failure(self):
        with patch("cqlib_hybrid.QRMIClient") as client_class:
            client = client_class.from_env.return_value.__enter__.return_value
            client.run_quantum.side_effect = KeyboardInterrupt()
            with self.assertRaises(KeyboardInterrupt):
                runpy.run_path(str(SCRIPT), run_name="__main__")

        client.fail.assert_not_called()
        client.complete.assert_not_called()

    def test_completion_error_does_not_send_competing_failure(self):
        error = ConnectionError("completion acknowledgement lost")
        with patch("cqlib_hybrid.QRMIClient") as client_class:
            client = client_class.from_env.return_value.__enter__.return_value
            client.run_quantum.return_value = None
            client.complete.side_effect = error
            with self.assertRaises(ConnectionError) as caught:
                runpy.run_path(str(SCRIPT), run_name="__main__")

        self.assertIs(caught.exception, error)
        client.fail.assert_not_called()


if __name__ == "__main__":
    unittest.main()
