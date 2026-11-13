"""Regression coverage for public release security and task isolation."""

import shutil
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from cqlib_hybrid import QRMIClient
from cqlib_hybrid.exceptions import HybridRuntimeError
from cqlib_hybrid.fusion import Context, FusionWorkflow, classic_stage, quantum_stage
from cqlib_hybrid.fusion.cli import _resolve_task_dir, main
from cqlib_hybrid.fusion.client._transport import Transport
from cqlib_hybrid.fusion.exceptions import (
    CqfusionRequestError,
    PackageValidationError,
    WorkflowError,
)
from cqlib_hybrid.fusion.package.builder import build_package
from cqlib_hybrid.fusion.package.validator import validate_package_dir
from cqlib_hybrid.fusion.runtime.classic.slurm import SlurmClassicRunner
from cqlib_hybrid.fusion.runtime.orchestrator import Orchestrator
from cqlib_hybrid.fusion.spec.manifest import load_manifest

SAMPLE = Path(__file__).resolve().parents[2] / "examples/fusion/iterative"


@pytest.mark.parametrize("login", [False, True])
def test_authenticated_transport_does_not_follow_redirects(monkeypatch, login):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    received = []

    class Sink(BaseHTTPRequestHandler):
        def do_GET(self):
            received.append(True)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{}")

        do_POST = do_GET

        def log_message(self, *args):
            pass

    sink = ThreadingHTTPServer(("127.0.0.1", 0), Sink)

    class Redirect(Sink):
        def do_GET(self):
            self.send_response(307)
            self.send_header("Location", f"http://127.0.0.1:{sink.server_port}/sink")
            self.end_headers()

        do_POST = do_GET

    origin = ThreadingHTTPServer(("127.0.0.1", 0), Redirect)
    for server in (sink, origin):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with pytest.raises(CqfusionRequestError):
            transport = Transport(
                f"http://127.0.0.1:{origin.server_port}",
                token="" if login else "dummy",
                api_key="dummy",
                timeout=2,
            )
            transport.request("GET", "/redirect")
        assert not received
    finally:
        for server in (origin, sink):
            server.shutdown()
            server.server_close()


def test_builder_excludes_secret_variants_and_ignored_python(tmp_path):
    root = tmp_path / "task"
    shutil.copytree(SAMPLE, root)
    for name in (".env.production", ".env.dev", "id_rsa", "private.pem"):
        (root / name).write_text("dummy secret")
    (root / ".venv").mkdir()
    (root / ".venv/broken.py").write_text("invalid python !")
    assert validate_package_dir(root).ok
    out = build_package(root, tmp_path / "task.zip")
    with zipfile.ZipFile(out) as archive:
        assert not any(
            n in archive.namelist()
            for n in (
                ".env.production",
                ".env.dev",
                "id_rsa",
                "private.pem",
                ".venv/broken.py",
            )
        )


def test_build_rejects_extra_circuit(tmp_path):
    shutil.copytree(SAMPLE, tmp_path / "task")
    (tmp_path / "task/extra.qcis").write_text("H Q0")
    with pytest.raises(PackageValidationError, match="exactly one"):
        build_package(tmp_path / "task", tmp_path / "task.zip")


def test_cli_uses_bounded_zip_extractor(tmp_path, monkeypatch):
    monkeypatch.setattr("cqlib_hybrid.fusion.package.loader.MAX_EXTRACTED_BYTES", 10)
    archive = tmp_path / "large.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("large.bin", b"0" * 11)
    with pytest.raises(PackageValidationError):
        _resolve_task_dir(archive)


def test_cli_cleans_extracted_package_on_cancel(tmp_path, monkeypatch):
    archive = build_package(SAMPLE, tmp_path / "task.zip")
    extracted = tmp_path / "extracted"
    from cqlib_hybrid.fusion import cli

    original = cli._resolve_task_dir

    def resolve(path):
        root, source = original(path)
        shutil.move(root, extracted)
        return extracted, source

    monkeypatch.setattr(cli, "_resolve_task_dir", resolve)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    assert main(["run-local", str(archive)]) == 2
    assert not extracted.exists()


def test_from_env_requires_explicit_deployment_url(monkeypatch):
    monkeypatch.delenv("QRMI_HYBRID_CPU_CONNECTION_URL", raising=False)
    monkeypatch.setenv("QRMI_HYBRID_CPU_TICKET", "dummy")
    monkeypatch.setenv("QRMI_HYBRID_CPU_HEARTBEAT_SECONDS", "5")
    with pytest.raises(HybridRuntimeError, match="CONNECTION_URL"):
        QRMIClient.from_env()


class Interrupted(FusionWorkflow):
    @classic_stage()
    def prepare(self, ctx: Context):
        return {"task": "first"}

    @quantum_stage()
    def execute(self, ctx: Context):
        raise ValueError("interrupted")


class Other(FusionWorkflow):
    @classic_stage()
    def execute(self, ctx: Context):
        return {"task": "other"}


def test_checkpoint_requires_explicit_resume_and_matching_workflow(tmp_path):
    manifest = load_manifest(SAMPLE / "fusion.yaml")
    with pytest.raises(WorkflowError):
        Orchestrator(Interrupted(), manifest, tmp_path).run()
    with pytest.raises(WorkflowError, match="resume"):
        Orchestrator(Other(), manifest, tmp_path).run()
    with pytest.raises(WorkflowError, match="match"):
        Orchestrator(Other(), manifest, tmp_path, resume=True).run()


def test_slurm_runner_does_not_reuse_previous_job_directory(tmp_path):
    first = SlurmClassicRunner(tmp_path, dry_run=True).run("first", {"old": 1})
    second = SlurmClassicRunner(tmp_path, dry_run=True).run("second", {"new": 1})
    assert first.output["stage"] == "first"
    assert second.output["stage"] == "second"
    outputs = list((tmp_path / "slurm").glob("*/output.json"))
    assert len(outputs) == 2


def test_resume_rejects_changed_manifest(tmp_path):
    manifest = load_manifest(SAMPLE / "fusion.yaml")
    with pytest.raises(WorkflowError):
        Orchestrator(Interrupted(), manifest, tmp_path).run()
    manifest.name = "different-task"
    with pytest.raises(WorkflowError, match="match"):
        Orchestrator(Interrupted(), manifest, tmp_path, resume=True).run()


def test_slurm_failure_is_not_hidden_by_output_file(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from cqlib_hybrid.fusion.runtime.classic import slurm

    output = tmp_path / "output.json"
    output.write_text('{"stale": true}')
    monkeypatch.setattr(slurm, "_command_exists", lambda name: True)
    monkeypatch.setattr(slurm, "_slurm_job_state", lambda job: "FAILED")
    monkeypatch.setattr(
        slurm.subprocess,
        "run",
        lambda *a, **kw: SimpleNamespace(stdout="Submitted batch job 123"),
    )
    runner = SlurmClassicRunner(tmp_path)
    with pytest.raises(RuntimeError, match="FAILED"):
        runner._submit_and_wait(tmp_path / "run.sh", tmp_path / "submit.slurm", output)


def test_failed_build_preserves_existing_output(tmp_path):
    shutil.copytree(SAMPLE, tmp_path / "task")
    (tmp_path / "task/extra.qcis").write_text("H Q0")
    output = tmp_path / "task.zip"
    output.write_bytes(b"previous archive")
    with pytest.raises(PackageValidationError):
        build_package(tmp_path / "task", output)
    assert output.read_bytes() == b"previous archive"
    assert not list(tmp_path.glob(".task-*.zip"))
