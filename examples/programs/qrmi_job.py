"""Run two QCIS rounds inside a QRMI-managed container.

The execution image needs cqlib-hybrid. The default connection URL is
ws://223.244.15.5:30002/qrmi/internal/workflow/cpu-executions; a non-empty
QRMI_HYBRID_CPU_CONNECTION_URL environment variable overrides it. QRMI must
still supply QRMI_HYBRID_CPU_TICKET and QRMI_HYBRID_CPU_HEARTBEAT_SECONDS.
No cqlib dependency or argument/result files are required.
"""

from cqlib_hybrid import QRMIClient

import logging


def main():
    # Show the connection/iteration flow on the container's stdout.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    with QRMIClient.from_env() as client:
        try:
            records = []
            angles = [0.0, 0.5]
            for iteration, angle in enumerate(angles, start=1):
                # Replace the fixed angles with the algorithm's classical work.
                circuit = f"RY Q0 {angle}\nM Q0"
                result = client.run_quantum(circuit, shots=1000, timeout=600)
                records.append({
                    "iteration_no": iteration,
                    "angle": angle,
                    "quantum_result": result,
                })
                client.progress(iteration * 100 // len(angles))
        except Exception as error:
            # Reporting failure must never replace the original exception.
            # KeyboardInterrupt/SystemExit propagate without reporting an
            # algorithm failure because they are not Exception subclasses.
            try:
                client.fail((str(error) or type(error).__name__)[:2048])
            except Exception:
                pass
            raise

        # A lost completion acknowledgement is handled by the client. Do not
        # replace this terminal report with FAILED if acknowledgement fails.
        client.complete({"shots": 1000, "records": records})


if __name__ == "__main__":
    main()
