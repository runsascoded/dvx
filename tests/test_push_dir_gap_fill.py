"""Tests for ``push_dir_inner_blobs`` and its wiring into ``dvx push`` and
``dvx run --push``.

Regression of ``specs/done/dir-push-shallow-existence-check.md``: DVC's
``repo.push`` short-circuits when a ``.dir`` manifest is already present in
the remote, even if inner blobs the manifest references are missing. Both
``dvx push`` and ``dvx run --push`` now run a gap-fill pass that walks
manifests locally and uploads missing inner blobs from local cache.
"""

import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dvx.cli import cli


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

    for cmd in (
        ["git", "init", "-b", "main"],
        ["git", "config", "user.email", "t@t"],
        ["git", "config", "user.name", "t"],
        ["dvc", "init"],
        ["dvc", "remote", "add", "-d", "local", str(remote)],
        ["git", "add", "."],
        ["git", "commit", "-m", "init"],
    ):
        subprocess.run(cmd, cwd=repo, check=True, capture_output=True)

    monkeypatch.chdir(repo)
    return repo, remote


def _write_stage(repo: Path, name: str, cmd: str) -> Path:
    """Write a .dvc file whose cmd produces ``name``. Returns the .dvc path."""
    dvc_path = repo / f"{name}.dvc"
    with open(dvc_path, "w") as f:
        yaml.dump({"outs": [{"path": name}], "meta": {"computation": {"cmd": cmd}}}, f)
    return dvc_path


def _list_remote(remote: Path) -> list[str]:
    """Sorted list of remote blob paths relative to ``remote``."""
    return sorted(str(p.relative_to(remote)) for p in remote.rglob("*") if p.is_file())


def _populate_dir_output(repo: Path, remote: Path, runner: CliRunner) -> tuple[Path, list[str]]:
    """Produce a single dir output ``d`` with 3 inner files and push everything.

    Returns ``(repo_root, sorted_remote_paths_after_push)``.
    """
    cmd = (
        "mkdir -p d && "
        "echo aaa > d/a.txt && "
        "echo bbb > d/b.txt && "
        "echo ccc > d/c.txt"
    )
    _write_stage(repo, "d", cmd)
    subprocess.run(["git", "add", "d.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stub"], cwd=repo, check=True, capture_output=True)

    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output
    return repo, _list_remote(remote)


def test_dvx_push_backfills_missing_inner_blob(runner, repo_with_remote):
    """`dvx push <dir.dvc>` backfills inner blobs missing from remote.

    Reproduces the silent-breakage state: the ``.dir`` manifest is present
    in remote but one inner blob has been deleted (or was never pushed).
    DVC's ``repo.push`` reports ``0 file(s) pushed`` and the gap stays
    unfilled. With the gap-fill pass, the missing inner blob is detected
    and uploaded from local cache; ``dvx push`` reports backfill count.
    """
    repo, remote = repo_with_remote
    _, full = _populate_dir_output(repo, remote, runner)

    # Drop one inner blob from the remote to simulate the silently-broken state.
    inner_paths = [
        p for p in full
        if not p.endswith(".dir") and p.startswith("files/md5/")
    ]
    assert len(inner_paths) == 3, full
    dropped = inner_paths[0]
    (remote / dropped).unlink()
    assert _list_remote(remote) == sorted(set(full) - {dropped})

    # Re-push: DVC sees manifest in remote and short-circuits, but the
    # gap-fill pass should restore the missing inner blob.
    result = runner.invoke(cli, ["push", "d.dvc"])
    assert result.exit_code == 0, result.output
    assert result.output.rstrip().split("\n") == [
        "0 file(s) pushed.",
        "Backfilled 1 dir blob(s) missing from remote.",
    ]
    assert _list_remote(remote) == full


def test_dvx_push_noop_when_remote_complete(runner, repo_with_remote):
    """`dvx push` reports no backfill when all dir inner blobs are present."""
    repo, remote = repo_with_remote
    _, full = _populate_dir_output(repo, remote, runner)

    result = runner.invoke(cli, ["push", "d.dvc"])
    assert result.exit_code == 0, result.output
    # No "Backfilled" line — single "0 file(s) pushed." line only.
    assert result.output.rstrip().split("\n") == ["0 file(s) pushed."]
    assert _list_remote(remote) == full


def test_dvx_push_warns_when_local_cache_also_missing(runner, repo_with_remote):
    """If an inner blob is missing from BOTH remote AND local cache, warn."""
    repo, remote = repo_with_remote
    _, full = _populate_dir_output(repo, remote, runner)

    # Drop one inner blob from BOTH remote and local cache.
    inner_paths = [
        p for p in full
        if not p.endswith(".dir") and p.startswith("files/md5/")
    ]
    dropped = inner_paths[0]
    (remote / dropped).unlink()
    local_path = repo / ".dvc" / "cache" / dropped
    if local_path.exists():
        local_path.unlink()

    result = runner.invoke(cli, ["push", "d.dvc"])
    assert result.exit_code == 0, result.output
    # stdout: ``0 file(s) pushed.`` (no backfill since local also missing).
    # stderr: warning about local cache gap.
    # CliRunner merges by default; result.output has both.
    lines = result.output.rstrip().split("\n")
    assert lines == [
        "0 file(s) pushed.",
        "⚠ 1 dir blob(s) missing from remote AND local cache; "
        "run `dvx pull` to repopulate, then re-push.",
    ]
    # Remote unchanged — gap couldn't be filled.
    assert _list_remote(remote) == sorted(set(full) - {dropped})


def test_dvx_run_push_each_fills_pre_existing_remote_gap(runner, repo_with_remote):
    """``dvx run --push each`` backfills dir inner-blob gaps left by prior runs.

    Models the historical ``hccs/crashes`` state: a previous run cached the
    manifest in remote (and pushed it), but inner blobs never made it (the
    `cache_blob` dir bug from `co-output-push-half-blob.md` / `dir-co-output-push-missing.md`).
    A subsequent ``dvx run --push each`` that regenerates byte-identical
    content (same manifest) was a no-op before this fix; now it detects
    and uploads the missing inner blobs.
    """
    repo, remote = repo_with_remote
    cmd = (
        "mkdir -p d && "
        "echo aaa > d/a.txt && "
        "echo bbb > d/b.txt && "
        "echo ccc > d/c.txt"
    )
    _write_stage(repo, "d", cmd)
    subprocess.run(["git", "add", "d.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stub"], cwd=repo, check=True, capture_output=True)

    # First run pushes everything correctly (post-`099855640` fix).
    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output
    full = _list_remote(remote)

    # Simulate the historical breakage by removing all inner blobs from
    # remote but leaving the manifest.
    dir_blob = next(p for p in full if p.endswith(".dir"))
    inner_blobs = sorted(set(full) - {dir_blob})
    assert len(inner_blobs) == 3
    for p in inner_blobs:
        (remote / p).unlink()
    assert _list_remote(remote) == [dir_blob]

    # Re-run with no content change. The cmd skips (output is fresh), so
    # no commit fires and no push happens. Force the cmd to re-execute by
    # touching a dep so the stage isn't skipped, then re-push.
    # Easiest: just run `dvx push` (already covered by an earlier test).
    # Here we force a non-skipped re-run by adding a dummy dep:
    (repo / "force-rerun.txt").write_text("v1\n")
    # Add an externally-changing dep so the stage is stale.
    dvc_path = repo / "d.dvc"
    dvc_data = yaml.safe_load(dvc_path.read_text())
    dvc_data["meta"]["computation"]["deps"] = {"force-rerun.txt": "deadbeef"}
    dvc_path.write_text(yaml.dump(dvc_data))
    subprocess.run(
        ["git", "add", "d.dvc", "force-rerun.txt"], cwd=repo, check=True, capture_output=True,
    )
    subprocess.run(["git", "commit", "-m", "force"], cwd=repo, check=True, capture_output=True)

    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output
    # After re-run + gap-fill, all 4 blobs (manifest + 3 inner) are back.
    assert _list_remote(remote) == full
