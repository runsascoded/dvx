"""Tests that `dvx run --push each|end` also pushes cache blobs to the remote.

Uses a local directory as the remote (DVC treats all remotes uniformly).
"""

import os
import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dvx.cli import cli
from dvx.run.hash import compute_md5


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def repo_with_remote(tmp_path, monkeypatch):
    """DVC+git repo with a local remote at ``remote/``."""
    repo = tmp_path / "repo"
    remote = tmp_path / "remote"
    repo.mkdir()
    remote.mkdir()

    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["dvc", "init"], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["dvc", "remote", "add", "-d", "local", str(remote)],
        cwd=repo, check=True, capture_output=True,
    )
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True, capture_output=True)

    monkeypatch.chdir(repo)
    return repo, remote


def _write_stage(repo: Path, name: str, cmd: str) -> Path:
    """Write a .dvc file with a cmd that produces ``name``. Returns dvc path."""
    dvc_path = repo / f"{name}.dvc"
    with open(dvc_path, "w") as f:
        yaml.dump({"outs": [{"path": name}], "meta": {"computation": {"cmd": cmd}}}, f)
    return dvc_path


def _remote_has_blob(remote: Path, md5: str) -> bool:
    return (remote / "files" / "md5" / md5[:2] / md5[2:]).exists()


def test_push_each_uploads_blob_to_remote(runner, repo_with_remote):
    repo, remote = repo_with_remote
    _write_stage(repo, "out.txt", "echo hello > out.txt")

    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output

    md5 = compute_md5(repo / "out.txt")
    assert _remote_has_blob(remote, md5), f"blob {md5} missing from remote; output:\n{result.output}"
    assert "cache pushed" in result.output


def test_push_end_batches_cache_pushes(runner, repo_with_remote):
    repo, remote = repo_with_remote
    _write_stage(repo, "a.txt", "echo aaa > a.txt")
    _write_stage(repo, "b.txt", "echo bbb > b.txt")

    result = runner.invoke(cli, ["run", "--commit", "--push", "end"])
    assert result.exit_code == 0, result.output

    for name in ("a.txt", "b.txt"):
        md5 = compute_md5(repo / name)
        assert _remote_has_blob(remote, md5), f"blob for {name} ({md5}) missing; output:\n{result.output}"


def test_no_cache_push_opt_out(runner, repo_with_remote):
    repo, remote = repo_with_remote
    _write_stage(repo, "out.txt", "echo opt-out > out.txt")

    result = runner.invoke(cli, ["run", "--commit", "--push", "each", "--no-cache-push"])
    assert result.exit_code == 0, result.output

    md5 = compute_md5(repo / "out.txt")
    assert not _remote_has_blob(remote, md5), "blob should NOT be in remote with --no-cache-push"
    assert "cache pushed" not in result.output


def test_skipped_stages_dont_push(runner, repo_with_remote):
    """A fresh (skipped) stage must not trigger cache push."""
    repo, remote = repo_with_remote

    # Create output first, then a .dvc matching it — stage will be considered fresh
    (repo / "fresh.txt").write_text("already here\n")
    md5 = compute_md5(repo / "fresh.txt")
    size = (repo / "fresh.txt").stat().st_size
    with open(repo / "fresh.txt.dvc", "w") as f:
        yaml.dump({
            "outs": [{"md5": md5, "size": size, "path": "fresh.txt"}],
            "meta": {"computation": {"cmd": "echo already here > fresh.txt"}},
        }, f)

    result = runner.invoke(cli, ["run", "--commit", "--push", "end"])
    assert result.exit_code == 0, result.output

    # No stage was executed → no cache push should have happened
    assert not _remote_has_blob(remote, md5)
    # end-mode git push is also gated on executed stages, so neither message appears
    assert "cache pushed" not in result.output


def test_cache_push_failure_is_non_fatal(runner, repo_with_remote, monkeypatch):
    """A failing cache push logs a warning but doesn't abort the run."""
    repo, remote = repo_with_remote
    _write_stage(repo, "resilient.txt", "echo survive > resilient.txt")

    # Make Repo.push raise
    from dvx import repo as repo_module
    original_push = repo_module.Repo.push

    def boom(self, *args, **kwargs):
        raise RuntimeError("simulated remote outage")

    monkeypatch.setattr(repo_module.Repo, "push", boom)

    try:
        result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
        assert result.exit_code == 0, result.output
        assert "cache push failed" in result.output
        assert "simulated remote outage" in result.output
        # .dvc commit happened despite cache push failure
        commits = subprocess.run(
            ["git", "log", "--oneline"], cwd=repo, capture_output=True, text=True,
        ).stdout
        assert "Run resilient" in commits or "resilient" in commits
    finally:
        monkeypatch.setattr(repo_module.Repo, "push", original_push)
