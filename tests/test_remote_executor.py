"""Deterministic scheduling and actual task round trips without cloud services."""

import json
import os
import shutil
import subprocess
import threading
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest
from dvc.repo import Repo

from dvx.batch.base import JobHandle, JobState
from dvx.batch.remote import RemoteRunner, apply_overlay, validate_image
from dvx.batch.storage import write_json_url
from dvx.run.artifact import Artifact, Computation
from dvx.run.dvc_files import write_dvc_file
from dvx.run.executor import ExecutionConfig, ExecutionResult, ParallelExecutor, run
from dvx.run.hash import compute_md5


def test_ready_set_starts_consumer_before_unrelated_slow_stage():
    a_started, b_started, c_done = threading.Event(), threading.Event(), threading.Event()
    events = []
    a = Artifact("a", computation=Computation(cmd="a"))
    b = Artifact("b", computation=Computation(cmd="b"))
    c = Artifact("c", computation=Computation(cmd="c", deps=[a]))
    executor = ParallelExecutor([a, b, c], ExecutionConfig(max_workers=2, commit="never"))

    def execute(artifact):
        name = artifact.path
        if name == "b":
            assert a_started.wait(5)
        events.append(("start", name))
        if name == "a":
            a_started.set()
            assert b_started.wait(5)
        elif name == "b":
            b_started.set()
            assert c_done.wait(5)
        events.append(("done", name))
        if name == "c":
            c_done.set()
        return ExecutionResult(name, True)

    executor._execute_artifact = execute
    results = executor.execute()
    assert sorted((r.path, r.success) for r in results) == [("a", True), ("b", True), ("c", True)]
    assert events == [
        ("start", "a"),
        ("start", "b"),
        ("done", "a"),
        ("start", "c"),
        ("done", "c"),
        ("done", "b"),
    ]


def test_ready_set_does_not_admit_queued_work_after_failure():
    stages = [Artifact(name, computation=Computation(cmd=name)) for name in ("a", "b", "c")]
    executor = ParallelExecutor(stages, ExecutionConfig(max_workers=1, commit="never"))
    calls = []

    def execute(artifact):
        calls.append(artifact.path)
        return ExecutionResult(artifact.path, False)

    executor._execute_artifact = execute
    assert [(r.path, r.success) for r in executor.execute()] == [("a", False)]
    assert calls == ["a"]


@pytest.fixture
def remote_pipeline(tmp_path, monkeypatch):
    root, remote = tmp_path / "driver", tmp_path / "cache"
    root.mkdir()
    remote.mkdir()
    monkeypatch.chdir(root)
    Repo.init(no_scm=True).close()
    with Repo() as repo, repo.config.edit() as config:
        config["core"]["remote"] = "storage"
        config["remote"]["storage"] = {"url": str(remote)}
    (root / "in.txt").write_text("new input\n")
    from dvx.cache import cache_blob, collect_push_objects, push_targets

    md5 = compute_md5(Path("in.txt"))
    write_dvc_file(Path("in.txt"), md5=md5, size=10)
    cache_blob(Path("in.txt"), md5)
    keys, _ = collect_push_objects(["in.txt.dvc"])
    push_targets(keys)
    write_dvc_file(
        Path("out.txt"), md5="0" * 32, size=0, cmd="cp in.txt out.txt", deps={"in.txt": md5}
    )
    (root / "dvx.yml").write_text(
        f"batch:\n  profiles:\n    test:\n      image: fake\n      results_url: {tmp_path / 'results'}\n"
    )
    return root, tmp_path


class LocalTaskBackend:
    def __init__(self, driver: Path, scratch: Path):
        self.driver, self.scratch = driver, scratch
        self.requests = []
        self.code = 0

    def submit(self, request):
        self.requests.append(request)
        task = self.scratch / f"task-{len(self.requests)}"
        task.mkdir()
        # Fresh image: sidecars/config only, no workspace data or local cache.
        shutil.copytree(
            self.driver / ".dvc",
            task / ".dvc",
            ignore=shutil.ignore_patterns("cache", "tmp", "dvx.db*"),
        )
        for path in self.driver.glob("*.dvc"):
            if path.is_file():
                shutil.copy2(path, task / path.name)
        (task / "dvx.yml").write_text("run:\n  stages:\n    out.txt:\n      executor: gcp:not-configured\n")
        env = {**os.environ, **request.env}
        completed = subprocess.run(
            [os.sys.executable, "-c", "from dvx.cli import main; main()", *request.command],
            cwd=task,
            env=env,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
        self.code = completed.returncode
        return JobHandle("fake", "job-1", "uid", request.profile)

    def status(self, handle):
        return JobState("SUCCEEDED", self.code)


def test_remote_task_reconciles_metadata_and_downstream_local_input(remote_pipeline, monkeypatch):
    import dvx.batch.remote as remote_module

    driver, scratch = remote_pipeline
    backend = LocalTaskBackend(driver, scratch)
    monkeypatch.setattr(
        remote_module, "watch", lambda provider, handle, **kw: provider.status(handle).exit_code
    )
    config = ExecutionConfig(executor="gcp:test", allow_image_mismatch=True, commit="never")
    executor_factory = RemoteRunner(config, backend_factory=lambda name, **kw: backend)
    monkeypatch.setattr(remote_module, "RemoteRunner", lambda config: executor_factory)
    results = run([Path("out.txt.dvc")], config)
    assert [(r.path, r.success, r.reason) for r in results] == [
        ("out.txt", True, "completed remotely")
    ]
    remote_sidecar = Path(scratch / "task-1/out.txt.dvc").read_bytes()
    assert Path("out.txt.dvc").read_bytes() == remote_sidecar
    assert Path("out.txt").exists() is False
    # A local consumer restores the remotely-produced output on demand.
    write_dvc_file(
        Path("consumer.txt"),
        md5="1" * 32,
        size=0,
        cmd="cp out.txt consumer.txt",
        deps={"out.txt": "0" * 32},
    )
    local = run([Path("consumer.txt.dvc")], ExecutionConfig(commit="never"))
    assert [(r.path, r.success) for r in local] == [("out.txt", True), ("consumer.txt", True)]
    assert Path("consumer.txt").read_text() == "new input\n"
    assert len(backend.requests) == 1


def test_reattach_same_input_key_and_changed_key(remote_pipeline, monkeypatch):
    import dvx.batch.remote as module

    driver, scratch = remote_pipeline
    provider = LocalTaskBackend(driver, scratch)
    monkeypatch.setattr(module, "watch", lambda backend, handle, **kw: 0)
    config = ExecutionConfig(allow_image_mismatch=True, commit="never", force=True)
    runner = RemoteRunner(config, backend_factory=lambda *a, **kw: provider)
    artifact = Artifact.from_dvc(Path("out.txt"))
    runner.execute([artifact], "gcp:test", lambda line: None)
    artifact = Artifact.from_dvc(Path("out.txt"))
    runner.execute([artifact], "gcp:test", lambda line: None)
    assert len(provider.requests) == 1
    # A new command is a different input key and must not attach.
    artifact.computation.cmd = "cat in.txt > out.txt"
    import yaml

    data = yaml.safe_load(Path("out.txt.dvc").read_text())
    data["meta"]["computation"]["cmd"] = artifact.computation.cmd
    Path("out.txt.dvc").write_text(yaml.safe_dump(data, sort_keys=False))
    runner.execute([artifact], "gcp:test", lambda line: None)
    assert len(provider.requests) == 2


def test_overlay_rejects_path_escape_before_any_write(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    url = str(tmp_path / "overlay.json")
    write_json_url(
        url, {"version": 1, "files": {"ok.dvc": "outs: []\n", "../escape.dvc": "outs: []\n"}}
    )
    with pytest.raises(ValueError, match="Invalid sidecar path"):
        apply_overlay(url)
    assert Path("ok.dvc").exists() is False


def test_image_revision_mismatch_refuses(monkeypatch):
    def fake_run(command, **kwargs):
        if command[:2] == ["git", "rev-parse"]:
            return MagicMock(stdout="abc\n")
        if command[:2] == ["git", "diff"]:
            return MagicMock(stdout="")
        return MagicMock(
            stdout=json.dumps(
                {"Config": {"Labels": {"org.opencontainers.image.revision": "other"}}}
            )
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(ValueError, match="Image revision 'other' differs from HEAD abc"):
        validate_image({"image": "image"})


def test_reattach_failed_job_requires_retry_flag(remote_pipeline, monkeypatch):
    import dvx.batch.remote as module

    driver, scratch = remote_pipeline
    provider = LocalTaskBackend(driver, scratch)
    monkeypatch.setattr(module, "watch", lambda *args, **kw: 0)
    config = ExecutionConfig(allow_image_mismatch=True, commit="never", force=True)
    runner = RemoteRunner(config, backend_factory=lambda *a, **kw: provider)
    runner.execute([Artifact.from_dvc(Path("out.txt"))], "gcp:test", lambda line: None)
    monkeypatch.setattr(provider, "status", lambda handle: JobState("FAILED", 2))
    with pytest.raises(ValueError, match="Previous remote job failed: job-1; use --retry-failed"):
        runner.execute([Artifact.from_dvc(Path("out.txt"))], "gcp:test", lambda line: None)
    assert len(provider.requests) == 1


def test_stop_preserves_handles_for_cancel_after_detach():
    runner = RemoteRunner(ExecutionConfig())
    provider = MagicMock()
    handle = JobHandle("fake", "job")
    runner._active[handle.name] = (provider, handle)
    assert runner.stop() is True
    runner._active.clear()
    assert runner.stop(cancel=True) is True
    assert provider.cancel.call_args_list == [call(handle)]


def test_image_validation_returns_registry_digest(monkeypatch):
    def fake_run(command, **kwargs):
        if command[:2] == ["git", "rev-parse"]:
            return MagicMock(stdout="abc\n")
        if command[:2] == ["git", "diff"]:
            return MagicMock(stdout="meta/out.dvc\n")
        return MagicMock(
            stdout=json.dumps(
                {
                    "Config": {"Labels": {"org.opencontainers.image.revision": "abc"}},
                    "RepoDigests": ["repo/app@sha256:digest"],
                }
            )
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert validate_image({"image": "repo/app:latest"}) == "repo/app@sha256:digest"
