"""Tests for dvx CLI commands."""

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
def temp_dvc_repo(tmp_path):
    """Create a temporary DVC repository."""
    # Initialize git repo
    subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=tmp_path,
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=tmp_path,
        capture_output=True,
        check=True,
    )

    # Initialize DVC
    subprocess.run(["dvc", "init"], cwd=tmp_path, capture_output=True, check=True)

    return tmp_path


def test_cli_help(runner):
    """Test CLI shows help."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "DVX - Minimal data version control" in result.output


def test_cli_version(runner):
    """Test version command."""
    result = runner.invoke(cli, ["version"])
    assert result.exit_code == 0
    assert "DVX version:" in result.output
    assert "DVC version:" in result.output


def test_cache_help(runner):
    """Test cache subcommand help."""
    result = runner.invoke(cli, ["cache", "--help"])
    assert result.exit_code == 0
    assert "Manage DVC cache" in result.output


def test_cache_md5(runner, tmp_path):
    """Test cache md5 command."""
    os.chdir(tmp_path)

    # Create .dvc file
    dvc_content = {
        "outs": [
            {
                "md5": "abc123def456",
                "size": 100,
                "path": "data.txt",
            }
        ]
    }
    dvc_file = tmp_path / "data.txt.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    result = runner.invoke(cli, ["cache", "md5", "data.txt"])
    assert result.exit_code == 0
    assert "abc123def456" in result.output


def test_cache_path(runner, tmp_path):
    """Test cache path command."""
    os.chdir(tmp_path)

    # Create .dvc directory structure
    dvc_dir = tmp_path / ".dvc"
    dvc_dir.mkdir()

    # Create .dvc file
    dvc_content = {
        "outs": [
            {
                "md5": "abc123def456789",
                "size": 100,
                "path": "data.txt",
            }
        ]
    }
    dvc_file = tmp_path / "data.txt.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    result = runner.invoke(cli, ["cache", "path", "data.txt"])
    assert result.exit_code == 0
    # Should contain hash structure
    assert "ab" in result.output  # First 2 chars of hash
    assert "c123def456789" in result.output  # Rest of hash


def test_root_command(runner, temp_dvc_repo):
    """Test root command."""
    os.chdir(temp_dvc_repo)

    result = runner.invoke(cli, ["root"])
    assert result.exit_code == 0
    assert "." in result.output or str(temp_dvc_repo) in result.output


def test_run_help(runner):
    """Test run command help."""
    result = runner.invoke(cli, ["run", "--help"])
    assert result.exit_code == 0
    assert "Execute artifact computations" in result.output
    assert "--dry-run" in result.output
    assert "--force" in result.output
    assert "--jobs" in result.output


def test_run_no_dvc_files(runner, tmp_path):
    """Test run command with no .dvc files."""
    os.chdir(tmp_path)

    result = runner.invoke(cli, ["run"])
    assert result.exit_code != 0
    assert "No .dvc files found" in result.output


def test_run_dry_run(runner, tmp_path):
    """Test run command with --dry-run."""
    os.chdir(tmp_path)

    # Create a simple .dvc file with computation
    dvc_content = {
        "outs": [
            {
                "md5": "",
                "size": 0,
                "path": "output.txt",
            }
        ],
        "meta": {
            "computation": {
                "cmd": "echo hello > output.txt",
            }
        },
    }
    dvc_file = tmp_path / "output.txt.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    result = runner.invoke(cli, ["run", "--dry-run"])
    # Dry run should succeed (even if file doesn't exist)
    assert "Dry run" in result.output or "Summary" in result.output


def test_cat_missing_cache(runner, tmp_path):
    """Test cat command with missing cache file."""
    os.chdir(tmp_path)

    # Create .dvc directory
    dvc_dir = tmp_path / ".dvc"
    dvc_dir.mkdir()

    # Create .dvc file
    dvc_content = {
        "outs": [
            {
                "md5": "abc123",
                "size": 100,
                "path": "data.txt",
            }
        ]
    }
    dvc_file = tmp_path / "data.txt.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    result = runner.invoke(cli, ["cat", "data.txt"])
    assert result.exit_code != 0
    assert "Cache file not found" in result.output


def test_init_command(runner, tmp_path):
    """Test init command."""
    os.chdir(tmp_path)

    # Initialize git first
    subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)

    result = runner.invoke(cli, ["init"])
    assert result.exit_code == 0
    assert "Initialized DVX repository" in result.output
    assert (tmp_path / ".dvc").exists()


def test_add_command(runner, temp_dvc_repo):
    """Test add command."""
    os.chdir(temp_dvc_repo)

    # Create a file to track
    data_file = temp_dvc_repo / "data.txt"
    data_file.write_text("test data\n")

    result = runner.invoke(cli, ["add", "data.txt"])
    assert result.exit_code == 0

    # Should create .dvc file
    assert (temp_dvc_repo / "data.txt.dvc").exists()


def test_status_command(runner, temp_dvc_repo):
    """Test status command."""
    os.chdir(temp_dvc_repo)

    result = runner.invoke(cli, ["status"])
    # Should succeed even with no tracked files
    assert result.exit_code == 0


def test_diff_help(runner):
    """Test diff command help."""
    result = runner.invoke(cli, ["diff", "--help"])
    assert result.exit_code == 0
    assert "Diff DVC-tracked files between commits" in result.output
