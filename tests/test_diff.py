"""Tests for ``dvx diff``.

In particular: ``-p/--pull`` should fetch missing cache blobs for either
revision from the configured remote (or ``--remote``) before erroring.

Assertions parse CLI output into structured form and use exact equality
where shape is fixed; substring ``in`` checks are avoided.
"""

import os
import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dvx.cli import cli


# ────────────────────────────────────────────────────────────────────────────
# Notes on assertion strategy
# ────────────────────────────────────────────────────────────────────────────
#
# ``dvx diff`` shells out to ``diff(1)`` with the inherited stdout (no
# capture), so the unified-diff text does NOT land in ``CliRunner.invoke``'s
# ``result.output``. We assert on the substantive signals instead:
#
#   - ``result.exit_code == 1`` → ``diff(1)`` ran and the files differed
#     (vs ``0`` = identical, ``2`` = bad arg / cache-missing exception).
#   - The blob the test evicted now exists in the local cache → ``--pull``
#     actually fetched from the remote (not just that ``diff(1)`` shrugged
#     and produced empty output).
#
# Error tests still parse ``result.output`` since ``ClickException`` writes
# through click's captured stream.


# ────────────────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def dvc_repo_with_remote(tmp_path):
    """Initialize a git+dvc repo with a local remote."""
    repo_path = tmp_path / "repo"
    remote_path = tmp_path / "remote"
    repo_path.mkdir()
    remote_path.mkdir()

    def run(*args):
        subprocess.run(args, cwd=repo_path, capture_output=True, check=True)

    run("git", "init")
    run("git", "config", "user.email", "test@test.com")
    run("git", "config", "user.name", "Test")
    run("dvc", "init")
    run("dvc", "remote", "add", "-d", "local", str(remote_path))
    run("git", "add", ".")
    run("git", "commit", "-m", "init")
    return repo_path, remote_path


@pytest.fixture
def two_commits(dvc_repo_with_remote):
    """Two commits of ``data.txt`` (``v1``, ``v2``) tracked by DVC and
    fully pushed to the remote. Returns ``(repo, remote, sha_v1, sha_v2)``.
    """
    repo_path, remote_path = dvc_repo_with_remote

    def run(*args):
        subprocess.run(args, cwd=repo_path, capture_output=True, check=True)

    def rev(spec):
        return subprocess.run(
            ["git", "rev-parse", spec],
            cwd=repo_path, capture_output=True, text=True, check=True,
        ).stdout.strip()

    (repo_path / "data.txt").write_text("v1 line 1\nv1 line 2\n")
    run("dvc", "add", "data.txt")
    run("git", "add", "data.txt.dvc", ".gitignore")
    run("git", "commit", "-m", "v1")
    sha_v1 = rev("HEAD")
    # Push v1's blob before overwriting it locally — `dvc push` only ships
    # what's in the workspace, so we have to push between each version to
    # land both blobs on the remote.
    run("dvc", "push")

    (repo_path / "data.txt").write_text("v2 line 1\nv2 line 2\n")
    run("dvc", "add", "data.txt")
    run("git", "add", "data.txt.dvc")
    run("git", "commit", "-m", "v2")
    sha_v2 = rev("HEAD")
    run("dvc", "push")
    return repo_path, remote_path, sha_v1, sha_v2


def _cache_md5_for_ref(repo_path: Path, dvc_path: str, ref: str) -> str:
    """Read the md5 from a ``.dvc`` file at a git ref."""
    out = subprocess.run(
        ["git", "show", f"{ref}:{dvc_path}"],
        cwd=repo_path, capture_output=True, text=True, check=True,
    ).stdout
    content = yaml.safe_load(out)
    return content["outs"][0]["md5"]


def _cache_path(repo_path: Path, md5: str) -> Path:
    base = md5.replace(".dir", "")
    suffix = ".dir" if md5.endswith(".dir") else ""
    return repo_path / ".dvc" / "cache" / "files" / "md5" / base[:2] / (base[2:] + suffix)


def _evict(repo_path: Path, md5: str) -> None:
    p = _cache_path(repo_path, md5)
    if p.exists():
        p.unlink()


# ────────────────────────────────────────────────────────────────────────────
# Tests
# ────────────────────────────────────────────────────────────────────────────


def _run_diff_in(repo_path: Path, runner: CliRunner, *args):
    cwd = os.getcwd()
    os.chdir(repo_path)
    try:
        return runner.invoke(cli, ["diff", *args])
    finally:
        os.chdir(cwd)


class TestDiffPull:
    def test_pull_fetches_missing_before_blob(self, runner, two_commits):
        """``--pull`` fetches the parent-commit blob from the remote."""
        repo_path, _remote, sha_v1, sha_v2 = two_commits
        md5_v1 = _cache_md5_for_ref(repo_path, "data.txt.dvc", sha_v1)
        _evict(repo_path, md5_v1)
        assert not _cache_path(repo_path, md5_v1).exists()

        result = _run_diff_in(repo_path, runner, "-p", "-r", f"{sha_v1}..{sha_v2}", "data.txt")

        # diff(1) exit 1 = files differ; cache populated proves --pull worked.
        assert (result.exit_code, _cache_path(repo_path, md5_v1).exists()) == (1, True)

    def test_pull_fetches_missing_after_blob(self, runner, two_commits):
        """``--pull`` fetches the newer-commit blob from the remote."""
        repo_path, _remote, sha_v1, sha_v2 = two_commits
        md5_v2 = _cache_md5_for_ref(repo_path, "data.txt.dvc", sha_v2)
        _evict(repo_path, md5_v2)
        assert not _cache_path(repo_path, md5_v2).exists()

        result = _run_diff_in(repo_path, runner, "-p", "-r", f"{sha_v1}..{sha_v2}", "data.txt")

        assert (result.exit_code, _cache_path(repo_path, md5_v2).exists()) == (1, True)

    def test_pull_fetches_both_missing_blobs(self, runner, two_commits):
        """Both refs' blobs missing locally → ``-p`` pulls both before diffing."""
        repo_path, _remote, sha_v1, sha_v2 = two_commits
        md5_v1 = _cache_md5_for_ref(repo_path, "data.txt.dvc", sha_v1)
        md5_v2 = _cache_md5_for_ref(repo_path, "data.txt.dvc", sha_v2)
        _evict(repo_path, md5_v1)
        _evict(repo_path, md5_v2)

        result = _run_diff_in(repo_path, runner, "-p", "-r", f"{sha_v1}..{sha_v2}", "data.txt")

        assert (
            result.exit_code,
            _cache_path(repo_path, md5_v1).exists(),
            _cache_path(repo_path, md5_v2).exists(),
        ) == (1, True, True)

    def test_no_pull_errors_with_hint(self, runner, two_commits):
        """Without ``-p``, missing cache should error and the hint should
        name the new flag rather than ``dvc pull``."""
        repo_path, _remote, sha_v1, sha_v2 = two_commits
        md5_v1 = _cache_md5_for_ref(repo_path, "data.txt.dvc", sha_v1)
        _evict(repo_path, md5_v1)

        result = _run_diff_in(repo_path, runner, "-r", f"{sha_v1}..{sha_v2}", "data.txt")

        # ClickException prints to click's captured stream as "Error: <msg>"
        # — assert the two-line message verbatim.
        assert (result.exit_code, result.output.rstrip().split("\n")) == (
            1,
            [
                f"Error: Cache missing for '{sha_v1}': Cache file missing: {md5_v1}",
                "Run with -p/--pull to fetch from remote (or 'dvx pull -R <ref> <path>').",
            ],
        )

    def test_pull_fails_when_remote_also_missing(self, runner, two_commits):
        """If a blob is missing both locally AND from the remote, ``-p``
        must still report a clean cache-missing error (and the hint should
        acknowledge that the remote is the problem)."""
        repo_path, remote_path, sha_v1, sha_v2 = two_commits
        md5_v1 = _cache_md5_for_ref(repo_path, "data.txt.dvc", sha_v1)
        # Evict locally...
        _evict(repo_path, md5_v1)
        # ...and remove from the remote storage too.
        base = md5_v1.replace(".dir", "")
        suffix = ".dir" if md5_v1.endswith(".dir") else ""
        remote_blob = remote_path / "files" / "md5" / base[:2] / (base[2:] + suffix)
        if remote_blob.exists():
            remote_blob.unlink()

        result = _run_diff_in(repo_path, runner, "-p", "-r", f"{sha_v1}..{sha_v2}", "data.txt")

        assert (result.exit_code, result.output.rstrip().split("\n")) == (
            1,
            [
                f"Error: Cache missing for '{sha_v1}': Cache file missing: {md5_v1}",
                "Cache is missing from the remote.",
            ],
        )
