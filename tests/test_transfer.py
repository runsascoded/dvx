"""Tests for dvx push/pull with dry-run.

Assertions parse ``dvx push|pull``'s output into structured form
(``TransferFile``, ``DryRunResult``, ``TransferResult``) and assert
exact equality / counts. Avoid bare ``in result.output`` checks.
"""

import os
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dvx.cli import cli


# ────────────────────────────────────────────────────────────────────────────
# Output parsing
# ────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TransferFile:
    """One ``  <path>  (<size>)  <md5-prefix>...`` line in dry-run output."""
    path: str
    md5_prefix: str  # 8 hex chars before the "..."


@dataclass
class DryRunResult:
    """Parsed ``dvx push|pull --dry-run`` output.

    ``would`` carries the count + per-file detail when the run found
    things to transfer; ``nothing`` is True when the output reported
    "Nothing to push/pull"; the two are mutually exclusive.
    ``already`` is the count from the optional "Already in remote /
    Already cached" subsection.
    """
    would_count: int | None = None
    would_files: list[TransferFile] = field(default_factory=list)
    nothing: bool = False
    already_count: int | None = None
    warnings: list[str] = field(default_factory=list)  # "<target>: <reason>"


_WOULD_RE = re.compile(r"Would (?:push|pull) (\d+) file\(s\) ")
_NOTHING_RE = re.compile(r"Nothing to (?:push|pull)")
_ALREADY_RE = re.compile(r"Already (?:in remote|cached): (\d+) file\(s\)")
# File / warning lines start at column 0 of their own line (no progress
# bar pollution there), so anchor with ^.
_FILE_LINE_RE = re.compile(r"^  (\S.*?)  \(.+?\)  ([0-9a-f]+)\.\.\.$")
_WARN_RE = re.compile(r"^  ⚠ (\S+): (.+)$")


def parse_dry_run(output: str) -> DryRunResult:
    r = DryRunResult()
    # Summary lines (count / nothing / already) may share a line with the
    # tail of a ``\r``-cleared progress bar — search across the whole
    # output rather than anchoring on line start.
    if (m := _WOULD_RE.search(output)):
        r.would_count = int(m.group(1))
    if _NOTHING_RE.search(output):
        r.nothing = True
    if (m := _ALREADY_RE.search(output)):
        r.already_count = int(m.group(1))
    # Per-file and warning lines occupy their own lines without progress
    # pollution, so anchored line-by-line parsing is precise here.
    for line in output.split("\n"):
        if (m := _FILE_LINE_RE.match(line)):
            r.would_files.append(TransferFile(path=m.group(1), md5_prefix=m.group(2)))
        elif (m := _WARN_RE.match(line)):
            r.warnings.append(f"{m.group(1)}: {m.group(2)}")
    return r


@dataclass
class TransferResult:
    """Parsed non-dry ``dvx push|pull`` output."""
    fetched: int | None = None
    added: int | None = None
    pushed: int | None = None
    nothing: bool = False
    warnings: list[str] = field(default_factory=list)


_PULL_DONE_RE = re.compile(r"(\d+) file\(s\) fetched, (\d+) file\(s\) added\.")
_PUSH_DONE_RE = re.compile(r"(\d+) file\(s\) pushed\.")
_NOTHING_PLAIN_RE = re.compile(r"Nothing to (?:push|pull)\.")


def parse_transfer(output: str) -> TransferResult:
    r = TransferResult()
    if (m := _PULL_DONE_RE.search(output)):
        r.fetched = int(m.group(1))
        r.added = int(m.group(2))
    if (m := _PUSH_DONE_RE.search(output)):
        r.pushed = int(m.group(1))
    if _NOTHING_PLAIN_RE.search(output):
        r.nothing = True
    for line in output.split("\n"):
        if (m := _WARN_RE.match(line)):
            r.warnings.append(f"{m.group(1)}: {m.group(2)}")
    return r


@pytest.fixture
def runner():
    """Create a Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def dvc_repo_with_remote(tmp_path):
    """Create a temporary DVC repository with a local remote.

    Returns:
        tuple: (repo_path, remote_path) - paths to repo and remote storage
    """
    repo_path = tmp_path / "repo"
    remote_path = tmp_path / "remote"
    repo_path.mkdir()
    remote_path.mkdir()

    # Initialize git repo
    subprocess.run(["git", "init"], cwd=repo_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=repo_path,
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=repo_path,
        capture_output=True,
        check=True,
    )

    # Initialize DVC
    subprocess.run(["dvc", "init"], cwd=repo_path, capture_output=True, check=True)

    # Add local remote
    subprocess.run(
        ["dvc", "remote", "add", "-d", "local", str(remote_path)],
        cwd=repo_path,
        capture_output=True,
        check=True,
    )

    # Commit DVC init
    subprocess.run(["git", "add", "."], cwd=repo_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "init"],
        cwd=repo_path,
        capture_output=True,
        check=True,
    )

    return repo_path, remote_path


@pytest.fixture
def dvc_repo_with_files(dvc_repo_with_remote):
    """Create a DVC repo with some tracked files.

    Returns:
        tuple: (repo_path, remote_path, files) - paths and list of tracked file names
    """
    repo_path, remote_path = dvc_repo_with_remote

    # Create and track test files
    files = []
    for name, content, size in [
        ("small.txt", "hello world", 11),
        ("medium.txt", "x" * 1000, 1000),
        ("large.txt", "y" * 10000, 10000),
    ]:
        file_path = repo_path / name
        file_path.write_text(content)
        subprocess.run(
            ["dvc", "add", name],
            cwd=repo_path,
            capture_output=True,
            check=True,
        )
        files.append(name)

    # Commit .dvc files
    subprocess.run(["git", "add", "."], cwd=repo_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "add files"],
        cwd=repo_path,
        capture_output=True,
        check=True,
    )

    return repo_path, remote_path, files


class TestPushDryRun:
    """Tests for dvx push --dry-run."""

    def test_push_dry_run_shows_files_to_push(self, runner, dvc_repo_with_files):
        """Test that dry-run shows files that would be pushed."""
        repo_path, _remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        result = runner.invoke(cli, ["push", "-n"])
        assert result.exit_code == 0

        parsed = parse_dry_run(result.output)
        assert parsed.would_count == len(files)
        assert parsed.nothing is False
        assert parsed.already_count is None
        # Each tracked file appears as one ``TransferFile`` (path basename
        # matches one of the seeded files).
        listed_paths = {Path(f.path).name for f in parsed.would_files}
        assert listed_paths == set(files)

    def test_push_dry_run_nothing_to_push(self, runner, dvc_repo_with_files):
        """Test dry-run after files are already pushed."""
        repo_path, _remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        # Actually push first
        subprocess.run(["dvc", "push"], cwd=repo_path, capture_output=True, check=True)

        # Now dry-run reports nothing to push + everything already in remote.
        result = runner.invoke(cli, ["push", "-n"])
        assert result.exit_code == 0

        parsed = parse_dry_run(result.output)
        assert parsed.nothing is True
        assert parsed.would_count is None
        assert parsed.would_files == []
        assert parsed.already_count == len(files)

    def test_push_dry_run_specific_target(self, runner, dvc_repo_with_files):
        """Test dry-run with specific target."""
        repo_path, _remote_path, _files = dvc_repo_with_files
        os.chdir(repo_path)

        result = runner.invoke(cli, ["push", "-n", "small.txt"])
        assert result.exit_code == 0

        parsed = parse_dry_run(result.output)
        assert parsed.would_count == 1
        assert [Path(f.path).name for f in parsed.would_files] == ["small.txt"]


class TestPullDryRun:
    """Tests for dvx pull --dry-run."""

    def test_pull_dry_run_nothing_to_pull(self, runner, dvc_repo_with_files):
        """Test dry-run when all files are already cached locally."""
        repo_path, _remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        result = runner.invoke(cli, ["pull", "-n"])
        assert result.exit_code == 0

        parsed = parse_dry_run(result.output)
        assert parsed.nothing is True
        assert parsed.would_count is None
        assert parsed.would_files == []
        assert parsed.already_count == len(files)

    def test_pull_dry_run_shows_missing_files(self, runner, dvc_repo_with_files):
        """Test dry-run shows files that need to be pulled."""
        repo_path, _remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        subprocess.run(["dvc", "push"], cwd=repo_path, capture_output=True, check=True)
        cache_dir = repo_path / ".dvc" / "cache"
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)

        result = runner.invoke(cli, ["pull", "-n"])
        assert result.exit_code == 0

        parsed = parse_dry_run(result.output)
        assert parsed.would_count == len(files)
        assert parsed.nothing is False
        assert {Path(f.path).name for f in parsed.would_files} == set(files)

    def test_pull_dry_run_specific_target(self, runner, dvc_repo_with_files):
        """Test dry-run with specific target after clearing cache."""
        repo_path, _remote_path, _files = dvc_repo_with_files
        os.chdir(repo_path)

        subprocess.run(["dvc", "push"], cwd=repo_path, capture_output=True, check=True)
        cache_dir = repo_path / ".dvc" / "cache"
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)

        result = runner.invoke(cli, ["pull", "-n", "small.txt"])
        assert result.exit_code == 0

        parsed = parse_dry_run(result.output)
        assert parsed.would_count == 1
        assert [Path(f.path).name for f in parsed.would_files] == ["small.txt"]


class TestTargetedPull:
    """Tests for dvx pull <target> (targeted pull via .dvc file resolution)."""

    def test_pull_specific_file(self, runner, dvc_repo_with_files):
        """Test pulling a specific file by output path."""
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        # Push to remote, then remove the output file and clear cache
        subprocess.run(["dvc", "push"], cwd=repo_path, capture_output=True, check=True)
        (repo_path / "small.txt").unlink()
        import shutil
        cache_dir = repo_path / ".dvc" / "cache"
        if cache_dir.exists():
            shutil.rmtree(cache_dir)

        result = runner.invoke(cli, ["pull", "small.txt"])
        assert result.exit_code == 0
        parsed = parse_transfer(result.output)
        assert parsed.fetched == 1
        assert parsed.added == 1
        assert parsed.warnings == []
        assert (repo_path / "small.txt").exists()
        assert (repo_path / "small.txt").read_text() == "hello world"

    def test_pull_by_dvc_path(self, runner, dvc_repo_with_files):
        """Test pulling by .dvc file path."""
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        subprocess.run(["dvc", "push"], cwd=repo_path, capture_output=True, check=True)
        (repo_path / "small.txt").unlink()
        import shutil
        cache_dir = repo_path / ".dvc" / "cache"
        if cache_dir.exists():
            shutil.rmtree(cache_dir)

        result = runner.invoke(cli, ["pull", "small.txt.dvc"])
        assert result.exit_code == 0
        assert (repo_path / "small.txt").exists()

    def test_pull_nonexistent_target(self, runner, dvc_repo_with_files):
        """Test pulling a target with no .dvc file."""
        repo_path, _remote_path, _files = dvc_repo_with_files
        os.chdir(repo_path)

        result = runner.invoke(cli, ["pull", "nonexistent.txt"])
        # Resolver emits a warning, then reports "Nothing to pull." since
        # no resolvable targets remain.
        parsed = parse_transfer(result.output)
        assert parsed.warnings == ["nonexistent.txt: no .dvc file found"]
        assert parsed.nothing is True
        assert parsed.fetched is None
        assert parsed.pushed is None

    def test_pull_already_up_to_date(self, runner, dvc_repo_with_files):
        """Test pulling when file already matches cache."""
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        # File already exists and matches — should be a no-op
        result = runner.invoke(cli, ["pull", "small.txt"])
        assert result.exit_code == 0

    def test_pull_glob_expands_pattern(self, runner, dvc_repo_with_files):
        """`dvx pull --glob '<pattern>' <target>` must expand the pattern.

        Regression: introduced in 65f993aa8 ("Fix targeted dvx pull for .dvc
        files"), persisted through 536816f1b's refactor. The targeted-pull
        path dropped the ``glob`` flag, so the literal pattern string was
        passed to DVC as a path target → "<pattern> does not exist".
        """
        repo_path, _remote_path, _files = dvc_repo_with_files
        os.chdir(repo_path)

        # Push to remote, remove outputs + clear cache so a real pull is needed
        subprocess.run(["dvc", "push"], cwd=repo_path, capture_output=True, check=True)
        for name in ("small.txt", "medium.txt", "large.txt"):
            (repo_path / name).unlink()
        import shutil
        cache_dir = repo_path / ".dvc" / "cache"
        if cache_dir.exists():
            shutil.rmtree(cache_dir)

        # `--glob 'small*.dvc'` must match `small.txt.dvc` only
        result = runner.invoke(cli, ["pull", "--glob", "small*.dvc"])
        assert result.exit_code == 0, f"output:\n{result.output}"
        parsed = parse_transfer(result.output)
        # Exactly one file fetched (small.txt) — no "does not exist" path.
        assert parsed.fetched == 1
        assert parsed.added == 1
        assert parsed.warnings == []
        assert (repo_path / "small.txt").exists()
        assert (repo_path / "small.txt").read_text() == "hello world"
        assert not (repo_path / "medium.txt").exists()
        assert not (repo_path / "large.txt").exists()


class TestDryRunDoesNotTransfer:
    """Tests to verify dry-run doesn't actually transfer files."""

    def test_push_dry_run_does_not_push(self, runner, dvc_repo_with_files):
        """Verify dry-run doesn't actually push files."""
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        # Remote should be empty initially
        remote_files = list(remote_path.rglob("*"))
        initial_count = len([f for f in remote_files if f.is_file()])

        # Run dry-run
        result = runner.invoke(cli, ["push", "-n"])
        assert result.exit_code == 0

        # Remote should still be empty
        remote_files = list(remote_path.rglob("*"))
        final_count = len([f for f in remote_files if f.is_file()])
        assert final_count == initial_count

    def test_pull_dry_run_does_not_pull(self, runner, dvc_repo_with_files):
        """Verify dry-run doesn't actually pull files."""
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        # Push and clear cache
        subprocess.run(["dvc", "push"], cwd=repo_path, capture_output=True, check=True)
        cache_dir = repo_path / ".dvc" / "cache"
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)

        # Cache should be empty
        assert not cache_dir.exists() or not any(cache_dir.rglob("*"))

        # Run dry-run
        result = runner.invoke(cli, ["pull", "-n"])
        assert result.exit_code == 0

        # Cache should still be empty (or only have the structure, not files)
        cache_files = list(cache_dir.rglob("*")) if cache_dir.exists() else []
        file_count = len([f for f in cache_files if f.is_file()])
        assert file_count == 0
