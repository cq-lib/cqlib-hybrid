"""QRMI WebSocket client for classical-quantum task containers."""

import logging

# Libraries stay silent unless the application configures logging.
logging.getLogger("cqlib_hybrid").addHandler(logging.NullHandler())

from .exceptions import (
    HybridAuthenticationError,
    HybridError,
    HybridProtocolError,
    HybridRequestError,
    HybridRuntimeError,
    HybridValidationError,
    QRMIConnectionError,
    QRMIQuantumError,
    QRMIRequestError,
    QRMITimeoutError,
)
from .qrmi import QRMIClient

__all__ = [
    "QRMIClient", "HybridError",
    "HybridValidationError", "HybridAuthenticationError", "HybridProtocolError",
    "HybridRequestError", "HybridRuntimeError",
    "QRMIConnectionError", "QRMIQuantumError", "QRMIRequestError", "QRMITimeoutError",
]
