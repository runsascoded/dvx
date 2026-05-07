"""Tests for dvx push/pull with dry-run."""

import os
import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dvx.cli import cli


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
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        result = runner.invoke(cli, ["push", "-n"])
        assert result.exit_code == 0
        assert "Would push" in result.output
        assert f"{len(files)} file(s)" in result.output

        # Check each file is mentioned
        for name in files:
            assert name in result.output

    def test_push_dry_run_nothing_to_push(self, runner, dvc_repo_with_files):
        """Test dry-run after files are already pushed."""
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        # Actually push first
        subprocess.run(["dvc", "push"], cwd=repo_path, capture_output=True, check=True)

        # Now dry-run should show nothing to push
        result = runner.invoke(cli, ["push", "-n"])
        assert result.exit_code == 0
        assert "Nothing to push" in result.output or "Already in remote" in result.output

    def test_push_dry_run_specific_target(self, runner, dvc_repo_with_files):
        """Test dry-run with specific target."""
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        result = runner.invoke(cli, ["push", "-n", "small.txt"])
        assert result.exit_code == 0
        assert "small.txt" in result.output
        # Should only show 1 file
        assert "1 file(s)" in result.output


class TestPullDryRun:
    """Tests for dvx pull --dry-run."""

    def test_pull_dry_run_nothing_to_pull(self, runner, dvc_repo_with_files):
        """Test dry-run when all files are already cached locally."""
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        result = runner.invoke(cli, ["pull", "-n"])
        assert result.exit_code == 0
        assert "Nothing to pull" in result.output or "Already cached" in result.output

    def test_pull_dry_run_shows_missing_files(self, runner, dvc_repo_with_files):
        """Test dry-run shows files that need to be pulled."""
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        # Push to remote, then clear local cache
        subprocess.run(["dvc", "push"], cwd=repo_path, capture_output=True, check=True)

        # Clear local cache
        cache_dir = repo_path / ".dvc" / "cache"
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)

        # Now dry-run should show files to pull
        result = runner.invoke(cli, ["pull", "-n"])
        assert result.exit_code == 0
        assert "Would pull" in result.output
        assert f"{len(files)} file(s)" in result.output

    def test_pull_dry_run_specific_target(self, runner, dvc_repo_with_files):
        """Test dry-run with specific target after clearing cache."""
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        # Push and clear cache
        subprocess.run(["dvc", "push"], cwd=repo_path, capture_output=True, check=True)
        cache_dir = repo_path / ".dvc" / "cache"
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)

        result = runner.invoke(cli, ["pull", "-n", "small.txt"])
        assert result.exit_code == 0
        assert "small.txt" in result.output
        assert "1 file(s)" in result.output


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
        assert "fetched" in result.output
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
        repo_path, remote_path, files = dvc_repo_with_files
        os.chdir(repo_path)

        result = runner.invoke(cli, ["pull", "nonexistent.txt"])
        assert "no .dvc file found" in result.output or "Nothing to pull" in result.output

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
        assert "does not exist" not in result.output
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
