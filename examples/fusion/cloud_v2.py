"""Evaluate a task ZIP, pick a resource explicitly, then submit and wait.

Needs ``pip install 'cqlib-hybrid[fusion]'`` and TIANYAN_API_KEY in the environment.
Nothing is submitted until ``submit_selected`` is called with a chosen rank.
"""

from pathlib import Path

from cqlib_hybrid.fusion.client import FusionClient


def evaluate(client: FusionClient, package: Path):
    schedule = client.schedule_package(package, strategy="balanced")
    for candidate in schedule.candidates():
        print(candidate)
    return schedule


def submit_selected(client: FusionClient, schedule, rank: int, name: str = "fusion-demo"):
    decision = schedule.select(rank)  # or schedule.manual(qpu_code, cpu_code)
    return client.submit_decision(name, decision)


if __name__ == "__main__":
    client = FusionClient()
    schedule = evaluate(client, Path("task.zip"))
    handle = submit_selected(client, schedule, rank=1)
    job = handle.wait(timeout=3600)
    print(job.status, handle.download("results"))
