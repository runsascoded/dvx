"""Tests for ``dvx run --pull-deps`` (default-on) auto-fetch of materializable
trans-deps before rerunning a stage.

Regression of ``specs/done/run-auto-pull.md``: in CI / fresh checkouts the
graph walker hits stages whose deps are fresh per their ``.dvc`` files but
whose output is missing locally. Pre-fix, those rerun the cmd; post-fix,
the executor tries ``repo.pull(targets=[<dvc>])`` first and skips if the
remote has the bit-identical output.
"""

import re
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


_DURATION_RE = re.compile(r"\(\d+\.\d+s\)")


def _stage_status_lines(output: str) -> list[str]:
    """Stage status lines from `dvx run` output (lines starting with two-space + glyph).

    Durations (``(0.1s)``) are normalized to ``(<duration>s)`` so tests can
    assert on shape rather than the actual wall-clock (which flakes across
    macOS / Linux CI, and even across consecutive runs on the same host).
    Per the CLAUDE.md testing rules: normalize variable parts, then compare.
    """
    return [
        _DURATION_RE.sub("(<duration>s)", line)
        for line in output.split("\n")
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
        "  ✓ out.txt: completed (<duration>s)",
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
        "  ✓ out.txt: completed (<duration>s)",
    ]
    assert _summary_line(result.output, "Executed") == 1
    assert (repo / "out.txt").read_text() == "v1\n"


def test_no_commit_still_pushes_cache_blobs(runner, repo_with_remote):
    """`--no-commit --push each` flushes each stage's cache blobs to the
    remote even though nothing is git-committed.

    Regression (specs/done/batch-run-command-cli-mismatch.md §2): the whole
    per-stage push block — including `_push_cache_blobs` — lived under
    `if commit_msg:`, so the `dvx batch` container default (no git writes)
    silently skipped every per-stage cache push, and a Spot reclaim lost
    everything since job start instead of one stage.
    """
    from dvx.run.hash import compute_md5

    repo, remote = repo_with_remote
    _write_stage(repo, "out.txt", "echo 'v1' > out.txt")

    head_before = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True,
    ).stdout.strip()

    result = runner.invoke(cli, ["run", "--no-commit", "--push", "each"])
    assert result.exit_code == 0, result.output
    assert _summary_line(result.output, "Executed") == 1

    # No git commit happened.
    head_after = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True,
    ).stdout.strip()
    assert head_after == head_before

    # But the stage's blob IS in the remote.
    md5 = compute_md5(repo / "out.txt")
    blob = remote / "files" / "md5" / md5[:2] / md5[2:]
    assert blob.exists()
    assert blob.read_text() == "v1\n"


def test_fresh_clone_dep_inside_tracked_dir_materializes(runner, repo_with_remote):
    """Fresh-clone invariant: a stage whose recorded closure exists in the
    remote executes zero cmds — including when its dep lives inside a
    DVC-tracked *directory* whose stage isn't in the plan.

    Regression (specs/done/batch-run-command-cli-mismatch.md §3, nj-crashes
    Fargate run `dvx-cells-smoke`): auto-pull fired only on `output missing`;
    after pulling the stage's own output, freshness re-classified as
    `dep missing` (the dep file lives in an unmaterialized tracked dir),
    which fell through to a rerun against inputs that don't exist —
    `FileNotFoundError` on a fresh machine, nondeterministically across
    scheduling orders.
    """
    repo, _remote = repo_with_remote

    # Upstream: a tracked *directory* stage containing the dep file.
    with open(repo / "data.dvc", "w") as f:
        yaml.dump({
            "outs": [{"path": "data"}],
            "meta": {"computation": {
                "cmd": "mkdir -p data && echo 'hi' > data/input.txt",
            }},
        }, f)

    # Downstream: deps on the file INSIDE the tracked dir (not on `data/`
    # itself) — the nj-crashes shape. The dir stage is thus absent from
    # `dvx run out.txt.dvc`'s plan (nothing deps on `data` as recorded).
    with open(repo / "out.txt.dvc", "w") as f:
        yaml.dump({
            "outs": [{"path": "out.txt"}],
            "meta": {"computation": {
                "cmd": "cat data/input.txt > out.txt",
                "deps": {"data/input.txt": ""},
            }},
        }, f)

    subprocess.run(["git", "add", "data.dvc", "out.txt.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stubs"], cwd=repo, check=True, capture_output=True)

    # Build + push everything.
    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output
    assert _summary_line(result.output, "Executed") == 2

    # Simulate a fresh clone: workspace outputs + local cache gone; .dvc
    # files (with recorded md5s) remain.
    import shutil
    (repo / "out.txt").unlink()
    shutil.rmtree(repo / "data")
    shutil.rmtree(repo / ".dvc" / "cache")

    # Target ONLY the downstream stage — the dir stage is not in the plan.
    result = runner.invoke(cli, ["run", "out.txt.dvc"])
    assert result.exit_code == 0, result.output

    # Zero cmds executed; the stage (and its dep closure) was materialized
    # from the remote.
    assert _stage_status_lines(result.output) == ["  ○ out.txt: fetched (up-to-date)"]
    assert _summary_line(result.output, "Executed") == 0
    assert _summary_line(result.output, "Skipped") == 1
    assert (repo / "out.txt").read_text() == "hi\n"
    assert (repo / "data" / "input.txt").read_text() == "hi\n"


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
        "  ✓ out.txt: completed (<duration>s)",
    ]
    assert _summary_line(result.output, "Executed") == 1
