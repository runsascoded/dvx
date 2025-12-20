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
    expected_hash = "abc123def456"
    dvc_content = {
        "outs": [
            {
                "md5": expected_hash,
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
    assert result.output.strip() == expected_hash


def test_cache_path(runner, tmp_path):
    """Test cache path command."""
    os.chdir(tmp_path)

    # Create .dvc directory structure
    dvc_dir = tmp_path / ".dvc"
    dvc_dir.mkdir()

    # Create .dvc file
    md5_hash = "abc123def456789"
    dvc_content = {
        "outs": [
            {
                "md5": md5_hash,
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
    # DVC cache path structure: .dvc/cache/files/md5/<first2>/<rest>
    expected_path = f".dvc/cache/files/md5/{md5_hash[:2]}/{md5_hash[2:]}"
    assert result.output.strip() == expected_path


def test_root_command(runner, temp_dvc_repo):
    """Test root command."""
    os.chdir(temp_dvc_repo)

    result = runner.invoke(cli, ["root"])
    assert result.exit_code == 0
    # Root command outputs the repo root path - could be "." or absolute path
    output_path = result.output.strip()
    assert output_path in (".", str(temp_dvc_repo))


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


def test_add_recursive_flag(runner, temp_dvc_repo):
    """Test add command with --recursive flag."""
    from dvx.cache import _hash_single_file

    os.chdir(temp_dvc_repo)

    # Create dep file
    dep_file = temp_dvc_repo / "input.txt"
    dep_file.write_text("input data\n")
    dep_hash = _hash_single_file(dep_file)

    # Create dep .dvc with WRONG hash (stale)
    dep_dvc = temp_dvc_repo / "input.txt.dvc"
    dvc_content = {
        "outs": [{"md5": "wrong_hash_123", "size": 10, "hash": "md5", "path": "input.txt"}]
    }
    with open(dep_dvc, "w") as f:
        yaml.dump(dvc_content, f)

    # Create output file
    output_file = temp_dvc_repo / "output.txt"
    output_file.write_text("output\n")

    # Create output .dvc with dep
    output_dvc = temp_dvc_repo / "output.txt.dvc"
    output_content = {
        "outs": [{"md5": "placeholder", "size": 7, "path": "output.txt"}],
        "meta": {
            "computation": {
                "cmd": "cat input.txt > output.txt",
                "deps": {"input.txt": "wrong_hash_123"},
            }
        },
    }
    with open(output_dvc, "w") as f:
        yaml.dump(output_content, f)

    # Without -r, should fail with stale dep error
    stale_hash = "wrong_hash_123"
    result = runner.invoke(cli, ["add", "output.txt"])
    assert result.exit_code == 1
    expected_lines = [
        "Error: Failed to add output.txt: Cannot add output.txt: 1 stale dep(s):",
        f"  input.txt: .dvc={stale_hash[:8]}... file={dep_hash[:8]}...",
        "Run `dvx add` on deps first, or use --recursive",
    ]
    assert result.output.strip().split("\n") == expected_lines

    # With -r, should succeed
    result = runner.invoke(cli, ["add", "-r", "output.txt"])
    assert result.exit_code == 0

    # Verify dep .dvc was updated with correct hash
    with open(dep_dvc) as f:
        dep_result = yaml.safe_load(f)
    assert dep_result["outs"][0]["md5"] == dep_hash


def test_status_shows_fresh_and_stale(runner, temp_dvc_repo):
    """Test status command shows correct freshness indicators."""
    from dvx.cache import _hash_single_file

    os.chdir(temp_dvc_repo)

    # Create a fresh file (hash matches .dvc)
    fresh_file = temp_dvc_repo / "fresh.txt"
    fresh_file.write_text("fresh data\n")
    fresh_hash = _hash_single_file(fresh_file)

    fresh_dvc = temp_dvc_repo / "fresh.txt.dvc"
    with open(fresh_dvc, "w") as f:
        yaml.dump({
            "outs": [{"md5": fresh_hash, "size": 11, "hash": "md5", "path": "fresh.txt"}]
        }, f)

    # Create a stale file (hash doesn't match .dvc)
    stale_file = temp_dvc_repo / "stale.txt"
    stale_file.write_text("stale data\n")
    stale_actual_hash = _hash_single_file(stale_file)

    stale_dvc = temp_dvc_repo / "stale.txt.dvc"
    wrong_hash = "00000000000000000000000000000000"
    with open(stale_dvc, "w") as f:
        yaml.dump({
            "outs": [{"md5": wrong_hash, "size": 11, "hash": "md5", "path": "stale.txt"}]
        }, f)

    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0

    # Parse output lines
    lines = result.output.strip().split("\n")

    # Find the stale.txt line - should have ✗ and show data changed
    stale_lines = [l for l in lines if "stale.txt" in l]
    assert len(stale_lines) == 1
    stale_line = stale_lines[0]
    assert stale_line.startswith("✗")
    assert "data changed" in stale_line


def test_status_json_output(runner, temp_dvc_repo):
    """Test status command with --json flag."""
    import json
    from dvx.cache import _hash_single_file

    os.chdir(temp_dvc_repo)

    # Create a file and track it
    data_file = temp_dvc_repo / "data.txt"
    data_file.write_text("test data\n")
    file_hash = _hash_single_file(data_file)

    dvc_file = temp_dvc_repo / "data.txt.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump({
            "outs": [{"md5": file_hash, "size": 10, "hash": "md5", "path": "data.txt"}]
        }, f)

    result = runner.invoke(cli, ["status", "--json"])
    assert result.exit_code == 0

    assert json.loads(result.output) == [
        {"path": "data.txt.dvc", "status": "fresh", "reason": None}
    ]


def test_status_dep_changed(runner, temp_dvc_repo):
    """Test status shows dep changed vs data changed."""
    from dvx.cache import _hash_single_file

    os.chdir(temp_dvc_repo)

    # Create dep file and .dvc (fresh)
    dep_file = temp_dvc_repo / "input.txt"
    dep_file.write_text("input\n")
    dep_hash = _hash_single_file(dep_file)

    dep_dvc = temp_dvc_repo / "input.txt.dvc"
    with open(dep_dvc, "w") as f:
        yaml.dump({
            "outs": [{"md5": dep_hash, "size": 6, "hash": "md5", "path": "input.txt"}]
        }, f)

    # Create output file (fresh data)
    output_file = temp_dvc_repo / "output.txt"
    output_file.write_text("output\n")
    output_hash = _hash_single_file(output_file)

    # Create output .dvc with WRONG dep hash (dep changed scenario)
    output_dvc = temp_dvc_repo / "output.txt.dvc"
    with open(output_dvc, "w") as f:
        yaml.dump({
            "outs": [{"md5": output_hash, "size": 7, "hash": "md5", "path": "output.txt"}],
            "meta": {
                "computation": {
                    "cmd": "cat input.txt > output.txt",
                    "deps": {"input.txt": "old_wrong_hash"},
                }
            }
        }, f)

    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0

    # Parse output lines
    lines = result.output.strip().split("\n")

    # Find output.txt line - should show dep changed, not data changed
    output_lines = [l for l in lines if "output.txt" in l]
    assert len(output_lines) == 1
    output_line = output_lines[0]
    assert output_line.startswith("✗")
    assert "dep changed" in output_line
    assert "input.txt" in output_line
