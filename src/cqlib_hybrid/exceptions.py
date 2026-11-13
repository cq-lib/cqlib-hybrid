"""Errors exposed by the QRMI container SDK."""


class HybridError(Exception):
    """Base error for cqlib-hybrid."""


class HybridValidationError(HybridError, ValueError):
    """Invalid local input; the request has not been submitted."""


class HybridRequestError(HybridError):
    """Base request error with an optional HTTP handshake status."""

    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class HybridAuthenticationError(HybridRequestError):
    """The QRMI WebSocket handshake rejected the job ticket."""


class HybridProtocolError(HybridError):
    """The service returned an unexpected response."""


class HybridRuntimeError(HybridError):
    """A QRMI container runtime operation failed."""


class QRMIConnectionError(HybridRuntimeError):
    """The QRMI connection closed or could not be recovered."""


class QRMITimeoutError(HybridRuntimeError, TimeoutError):
    """A QRMI acknowledgement or quantum result did not arrive in time."""


class QRMIRequestError(HybridRuntimeError):
    """QRMI rejected a request; structured server details are preserved."""

    def __init__(self, response):
        self.event_type = response.get("eventType")
        self.iteration_no = response.get("iterationNo")
        self.retryable = response["retryable"]
        self.error_code = response["errorCode"]
        self.error_message = response["errorMessage"]
        super().__init__(
            f"QRMI rejected {self.event_type or 'message'} "
            f"({self.error_code}): {self.error_message}"
        )


class QRMIQuantumError(HybridRuntimeError):
    """A quantum iteration failed (4) or was cancelled (5)."""

    def __init__(self, response):
        self.iteration_no = response["iterationNo"]
        self.task_status = response["taskStatus"]
        self.result = response.get("result")
        super().__init__(
            f"Quantum iteration {self.iteration_no} "
            f"{'failed' if self.task_status == 4 else 'was cancelled'} "
            f"(taskStatus={self.task_status})"
        )

