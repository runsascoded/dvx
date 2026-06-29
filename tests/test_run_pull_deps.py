"""Tests for ``dvx run --pull-deps`` (default-on) auto-fetch of materializable
trans-deps before rerunning a stage.

Regression of ``specs/done/run-auto-pull.md``: in CI / fresh checkouts the
graph walker hits stages whose deps are fresh per their ``.dvc`` files but
whose output is missing locally. Pre-fix, those rerun the cmd; post-fix,
the executor tries ``repo.pull(targets=[<dvc>])`` first and skips if the
remote has the bit-identical output.
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
    dvc_path = repo / f"{name}.dvc"
    with open(dvc_path, "w") as f:
        yaml.dump({"outs": [{"path": name}], "meta": {"computation": {"cmd": cmd}}}, f)
    return dvc_path


def _wipe_local(repo: Path, name: str) -> None:
    """Simulate a fresh checkout: remove the workspace output AND local cache."""
    output = repo / name
    if output.exists():
        output.unlink()
    cache = repo / ".dvc" / "cache"
    if cache.exists():
        import shutil
        shutil.rmtree(cache)


def _summary_line(output: str, key: str) -> int:
    """Extract `Executed:`/`Skipped:` count from `dvx run` summary."""
    for line in output.split("\n"):
        line = line.strip()
        if line.startswith(f"{key}:"):
            return int(line.split(":", 1)[1].strip())
    raise AssertionError(f"no {key!r} line in:\n{output}")


def _stage_status_lines(output: str) -> list[str]:
    """Stage status lines from `dvx run` output (lines starting with two-space + glyph)."""
    return [
        line for line in output.split("\n")
        if any(line.startswith(f"  {g}") for g in ("⟳", "✓", "✗", "◐", "○"))
    ]


def test_pull_deps_skips_rerun_when_remote_has_output(runner, repo_with_remote):
    """Default-on `--pull-deps`: a stage with output missing locally but
    present on remote is skipped (fetched, not re-executed)."""
    repo, _remote = repo_with_remote
    _write_stage(repo, "out.txt", "echo 'v1' > out.txt")
    subprocess.run(["git", "add", "out.txt.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stub"], cwd=repo, check=True, capture_output=True)

    # First run pushes the blob to remote.
    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output
    assert _summary_line(result.output, "Executed") == 1

    # Wipe local state, then re-run with the default `--pull-deps`.
    _wipe_local(repo, "out.txt")
    result = runner.invoke(cli, ["run"])
    assert result.exit_code == 0, result.output

    # Stage is skipped with "fetched (...)" reason — no cmd execution.
    assert _stage_status_lines(result.output) == ["  ○ out.txt: fetched (up-to-date)"]
    assert _summary_line(result.output, "Executed") == 0
    assert _summary_line(result.output, "Skipped") == 1
    # And the workspace file was materialized.
    assert (repo / "out.txt").read_text() == "v1\n"


def test_no_pull_deps_reruns_when_output_missing(runner, repo_with_remote):
    """`--no-pull-deps`: same setup as above, but the stage re-executes
    because the executor doesn't try the remote pull pre-pass."""
    repo, _remote = repo_with_remote
    _write_stage(repo, "out.txt", "echo 'v1' > out.txt")
    subprocess.run(["git", "add", "out.txt.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stub"], cwd=repo, check=True, capture_output=True)

    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output

    _wipe_local(repo, "out.txt")
    result = runner.invoke(cli, ["run", "--no-pull-deps"])
    assert result.exit_code == 0, result.output

    assert _stage_status_lines(result.output) == [
        "  ⟳ out.txt: running...",
        "  ✓ out.txt: completed (0.0s)",
    ]
    assert _summary_line(result.output, "Executed") == 1
    assert _summary_line(result.output, "Skipped") == 0
    assert (repo / "out.txt").read_text() == "v1\n"


def test_pull_deps_falls_through_when_remote_missing_blob(runner, repo_with_remote):
    """`--pull-deps` cleanly degrades to rerun when the remote also lacks the
    blob (no remote configured, blob never pushed, network error, etc.)."""
    repo, _remote = repo_with_remote
    _write_stage(repo, "out.txt", "echo 'v1' > out.txt")
    subprocess.run(["git", "add", "out.txt.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stub"], cwd=repo, check=True, capture_output=True)

    # Run WITHOUT pushing — blob stays only in local cache, never reaches remote.
    result = runner.invoke(cli, ["run", "--commit"])
    assert result.exit_code == 0, result.output

    _wipe_local(repo, "out.txt")
    # Default --pull-deps tries the pull, fails (remote has no blob), falls
    # through to rerunning the cmd.
    result = runner.invoke(cli, ["run"])
    assert result.exit_code == 0, result.output

    assert _stage_status_lines(result.output) == [
        "  ⟳ out.txt: running...",
        "  ✓ out.txt: completed (0.0s)",
    ]
    assert _summary_line(result.output, "Executed") == 1
    assert (repo / "out.txt").read_text() == "v1\n"


def test_pull_deps_does_not_interfere_with_forced_rerun(runner, repo_with_remote):
    """`--force` bypasses the pull pre-pass — forced stages always re-execute."""
    repo, _remote = repo_with_remote
    _write_stage(repo, "out.txt", "echo 'v1' > out.txt")
    subprocess.run(["git", "add", "out.txt.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stub"], cwd=repo, check=True, capture_output=True)

    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output

    # With --force, even though remote has the blob, cmd re-executes.
    result = runner.invoke(cli, ["run", "--force"])
    assert result.exit_code == 0, result.output
    assert _stage_status_lines(result.output) == [
        "  ⟳ out.txt: running...",
        "  ✓ out.txt: completed (0.0s)",
    ]
    assert _summary_line(result.output, "Executed") == 1
