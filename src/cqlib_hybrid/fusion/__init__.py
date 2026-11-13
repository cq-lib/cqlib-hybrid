"""Hybrid quantum-classical fusion workflow SDK."""

from __future__ import annotations

from cqlib_hybrid.fusion.workflow.base import FusionWorkflow
from cqlib_hybrid.fusion.workflow.context import Context
from cqlib_hybrid.fusion.workflow.decorators import classic_stage, quantum_stage
from cqlib_hybrid.fusion.spec.profile import TaskProfile

__all__ = [
    "FusionWorkflow",
    "Context",
    "classic_stage",
    "quantum_stage",
    "TaskProfile",
]

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("cqlib-hybrid")
except PackageNotFoundError:
    __version__ = "0+uninstalled"
