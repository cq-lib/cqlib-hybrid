"""Pure selection rules for hybrid-job scheduling.

Resource codes match ``candidateResources`` of the platform: ``code`` equals the
recommendation ``device_id`` or ``resource_id``. Display names are not used.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from ._transport import invalid

READY = "ready"
CONTINUE = "continue"


@dataclass
class ResourceChoice:
    code: str
    available: bool = True


@dataclass
class ResourceChoices:
    qpu: list = field(default_factory=list)
    cpu: list = field(default_factory=list)


@dataclass
class FusionCandidate:
    rank: int
    group_id: str
    device_id: str
    resource_id: str
    qpu_code: Optional[str]
    cpu_code: Optional[str]
    meets_all_constraints: bool
    scores: Any
    selectable: bool
    reject_reason: Optional[str]


@dataclass
class FusionDecision:
    file_id: str
    qpu_code: str
    cpu_code: str
    scheduling_mode: str
    scheduling_preference: Optional[str] = None


@dataclass
class FusionSchedule:
    """Uploaded package plus the current recommendations and resource list."""

    file_id: str
    _candidates: list = field(default_factory=list)
    _choices: ResourceChoices = field(default_factory=ResourceChoices, repr=False)

    def candidates(self) -> list:
        """Top recommendations as plain dictionaries."""
        return [asdict(item) for item in self._candidates]

    def select(self, rank: int) -> FusionDecision:
        return select_rank(self, rank)

    def manual(self, qpu: str, cpu: str) -> FusionDecision:
        return select_manual(self, qpu, cpu)


def top_candidates(ranked: list) -> list:
    return sorted(ranked, key=lambda item: item.rank)[:3]


def select_rank(schedule: FusionSchedule, rank: int) -> FusionDecision:
    candidate = next((c for c in schedule._candidates if c.rank == rank), None)
    if candidate is None:
        raise invalid("selected rank is not in the top 3 recommendations")
    if not candidate.meets_all_constraints:
        raise invalid("selected recommendation does not meet all constraints")
    qpu, cpu = match_resources(
        candidate.device_id, candidate.resource_id, schedule._choices
    )
    return FusionDecision(schedule.file_id, qpu, cpu, "SMART")


def select_manual(schedule: FusionSchedule, qpu: str, cpu: str) -> FusionDecision:
    qpu_code, cpu_code = match_resources(qpu, cpu, schedule._choices)
    return FusionDecision(schedule.file_id, qpu_code, cpu_code, "MANUAL")


def wait_signal(status: Any) -> str:
    if isinstance(status, bool) or not isinstance(status, int):
        raise invalid("task status is missing")
    if 5 <= status <= 7:
        return READY
    if status in (1, 2, 3, 4, 8):
        return CONTINUE
    if status == 9:
        raise invalid("fusion task status is unknown")
    raise invalid(f"unexpected fusion task status {status}")


def annotate(
    device_id: str, resource_id: str, meets: bool, choices: ResourceChoices
) -> tuple:
    """Return ``(qpu_code, cpu_code, selectable, reject_reason)``."""
    try:
        qpu, cpu = match_resources(device_id, resource_id, choices)
    except Exception as exc:  # CqfusionRequestError carries the reason
        return None, None, False, str(exc)
    if meets:
        return qpu, cpu, True, None
    return qpu, cpu, False, "selected recommendation does not meet all constraints"


def match_resources(qpu: str, cpu: str, choices: ResourceChoices) -> tuple:
    return (
        _available_code(choices.qpu, qpu, "QPU"),
        _available_code(choices.cpu, cpu, "CPU"),
    )


def _available_code(choices: list, code: str, kind: str) -> str:
    found = next((item for item in choices if item.code == code), None)
    if found is None:
        raise invalid(f"{kind} resource {code} is not in the resource list")
    if not found.available:
        raise invalid(f"{kind} resource {code} is unavailable")
    return found.code
