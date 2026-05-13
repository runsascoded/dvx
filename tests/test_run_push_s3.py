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


def test_push_each_uploads_all_co_output_blobs(runner, repo_with_remote):
    """``--push each`` must push EVERY co-output's blob, not just the primary's.

    Regression: when one cmd produces multiple outputs (co-outputs), the
    primary stage's ``_handle_stage_output`` builds the cache-push manifest
    from only its own ``.dvc``, dropping the co-output's blob. The git
    commit + git-push pick up both ``.dvc`` files (``git add -u``), so the
    bug is silent: the .dvc updates land on the remote git ref with new
    md5s, but the co-output's blob never reaches the data remote. The
    next day's ``dvx pull`` fails on the dangling md5.

    See ``specs/done/co-output-push-half-blob.md``.
    """
    repo, remote = repo_with_remote
    # Same cmd in both .dvc files triggers the co-output dedup path.
    # ``sleep`` widens the race window so the second artifact reliably
    # enters the dedup check while the first is still running.
    cmd = "sleep 0.1 && echo aaa > a.txt && echo bbb > b.txt"
    _write_stage(repo, "a.txt", cmd)
    _write_stage(repo, "b.txt", cmd)
    # Track the .dvc stubs so ``git add -u`` (in ``_handle_stage_output``)
    # picks up the md5 updates when DVX rewrites the files.
    subprocess.run(["git", "add", "a.txt.dvc", "b.txt.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stubs"], cwd=repo, check=True, capture_output=True)

    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output
    # Sanity: the co-output dedup path actually fired (vs. both stages
    # racing through ``run cmd`` independently — which would also
    # produce 2 blobs, masking the bug).
    assert "co-output ready" in result.output, (
        f"co-output dedup path did not fire — test is invalid:\n{result.output}"
    )

    md5_a = compute_md5(repo / "a.txt")
    md5_b = compute_md5(repo / "b.txt")
    missing = [
        name for name, md5 in [("a.txt", md5_a), ("b.txt", md5_b)]
        if not _remote_has_blob(remote, md5)
    ]
    assert not missing, (
        f"co-output blob(s) missing from remote: {missing}\n"
        f"a.txt md5={md5_a} b.txt md5={md5_b}\n"
        f"output:\n{result.output}"
    )


def test_push_each_uploads_all_co_output_blobs_3way(runner, repo_with_remote):
    """Same cmd → 3 outputs. All 3 blobs must reach the remote.

    Generalizes ``test_push_each_uploads_all_co_output_blobs`` past the
    2-output case the spec reported: ``_wait_for_co_outputs`` has to
    barrier on every co-output, not just the first one to finish.
    """
    repo, remote = repo_with_remote
    cmd = "sleep 0.1 && echo aaa > a.txt && echo bbb > b.txt && echo ccc > c.txt"
    for name in ("a.txt", "b.txt", "c.txt"):
        _write_stage(repo, name, cmd)
    subprocess.run(
        ["git", "add", "a.txt.dvc", "b.txt.dvc", "c.txt.dvc"],
        cwd=repo, check=True, capture_output=True,
    )
    subprocess.run(["git", "commit", "-m", "stubs"], cwd=repo, check=True, capture_output=True)

    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output
    # Two co-outputs (one primary + two waiters).
    assert result.output.count("co-output ready") == 2, (
        f"expected 2 co-outputs, got {result.output.count('co-output ready')}:\n{result.output}"
    )

    missing = [
        name for name in ("a.txt", "b.txt", "c.txt")
        if not _remote_has_blob(remote, compute_md5(repo / name))
    ]
    assert not missing, f"missing blobs: {missing}\noutput:\n{result.output}"


def test_push_each_co_output_failure_does_not_hang(runner, repo_with_remote):
    """If one co-output isn't produced by the cmd, the primary must still
    commit + push its own blob — not hang waiting on a dvc-done event
    that would never fire without the ``try/finally`` in
    ``_handle_co_output``.
    """
    repo, remote = repo_with_remote
    # cmd produces a.txt but NOT b.txt — b's `_handle_co_output` returns
    # ``co-output not produced``, must still set its dvc-done event so
    # the primary's ``_wait_for_co_outputs`` returns.
    cmd = "sleep 0.1 && echo aaa > a.txt"
    _write_stage(repo, "a.txt", cmd)
    _write_stage(repo, "b.txt", cmd)
    subprocess.run(["git", "add", "a.txt.dvc", "b.txt.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stubs"], cwd=repo, check=True, capture_output=True)

    # Run the executor in a worker thread with a hard timeout — without
    # the ``try/finally`` in ``_handle_co_output``, the primary's wait
    # would block forever and a regression would hang CI rather than
    # fail.
    import threading
    result_holder: list = []
    def _go():
        result_holder.append(runner.invoke(cli, ["run", "--commit", "--push", "each"]))
    t = threading.Thread(target=_go, daemon=True)
    t.start()
    t.join(timeout=30)
    assert not t.is_alive(), (
        "executor hung — likely missing dvc-done signal from a failed "
        "co-output (regression of try/finally in _handle_co_output)"
    )
    assert result_holder, "worker produced no result"
    result = result_holder[0]
    # The run reports a partial failure (b.txt missing), so exit != 0
    # is expected. What matters: a.txt's blob still made it to remote.
    assert "co-output not produced" in result.output, result.output
    md5_a = compute_md5(repo / "a.txt")
    assert _remote_has_blob(remote, md5_a), (
        f"a.txt blob missing despite primary's stage completing:\n{result.output}"
    )


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
