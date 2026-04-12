"""Tests for dvx.gc module."""

import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from dvx.gc import (
    ArtifactVersion,
    compute_gc_plan,
    format_size,
    get_artifact_versions,
    get_referenced_hashes,
    list_cache_blobs,
    parse_duration,
)


def test_parse_duration():
    """Parse duration strings."""
    assert parse_duration("7d") == timedelta(days=7)
    assert parse_duration("24h") == timedelta(hours=24)
    assert parse_duration("1w") == timedelta(weeks=1)
    assert parse_duration("30d") == timedelta(days=30)


def test_parse_duration_invalid():
    """Invalid duration raises ValueError."""
    with pytest.raises(ValueError):
        parse_duration("abc")
    with pytest.raises(ValueError):
        parse_duration("30m")  # minutes not supported


def test_format_size():
    """Format bytes as human-readable."""
    assert format_size(100) == "100 B"
    assert format_size(1024) == "1.0 KB"
    assert format_size(1048576) == "1.0 MB"
    assert format_size(1073741824) == "1.0 GB"


def test_list_cache_blobs(tmp_path):
    """List blobs in the local cache."""
    # Create a fake cache structure
    cache_dir = tmp_path / ".dvc" / "cache" / "files" / "md5"
    (cache_dir / "ab").mkdir(parents=True)
    (cache_dir / "cd").mkdir(parents=True)

    (cache_dir / "ab" / "c123def456").write_text("blob1")
    (cache_dir / "cd" / "e789f01234").write_text("blob2data")

    blobs = list_cache_blobs(tmp_path)
    assert len(blobs) == 2

    md5s = {md5 for md5, _, _ in blobs}
    assert "abc123def456" in md5s
    assert "cde789f01234" in md5s


@pytest.fixture
def git_repo_with_versions(tmp_path):
    """Create a git repo with multiple versions of a .dvc file."""
    repo = tmp_path / "repo"
    repo.mkdir()

    subprocess.run(["git", "init"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.email", "test@test.com"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, capture_output=True, check=True)

    # Create .dvc dir
    (repo / ".dvc").mkdir()

    # Version 1
    dvc_content = {"outs": [{"md5": "aaaa1111bbbb2222cccc3333dddd4444", "size": 100, "path": "data.txt"}]}
    with open(repo / "data.txt.dvc", "w") as f:
        yaml.dump(dvc_content, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "v1"], cwd=repo, capture_output=True, check=True)

    # Version 2
    dvc_content["outs"][0]["md5"] = "eeee5555ffff6666aaaa7777bbbb8888"
    with open(repo / "data.txt.dvc", "w") as f:
        yaml.dump(dvc_content, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "v2"], cwd=repo, capture_output=True, check=True)

    # Version 3
    dvc_content["outs"][0]["md5"] = "cccc9999dddd0000eeee1111ffff2222"
    with open(repo / "data.txt.dvc", "w") as f:
        yaml.dump(dvc_content, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "v3"], cwd=repo, capture_output=True, check=True)

    return repo


def test_get_artifact_versions(git_repo_with_versions):
    """List all versions of an artifact from git history."""
    versions = get_artifact_versions("data.txt.dvc", repo_path=git_repo_with_versions)

    assert len(versions) == 3
    md5s = [v.md5 for v in versions]
    # Newest first
    assert md5s[0] == "cccc9999dddd0000eeee1111ffff2222"
    assert md5s[1] == "eeee5555ffff6666aaaa7777bbbb8888"
    assert md5s[2] == "aaaa1111bbbb2222cccc3333dddd4444"


def test_get_referenced_hashes(git_repo_with_versions):
    """Get hashes referenced at HEAD."""
    hashes = get_referenced_hashes(repo_path=git_repo_with_versions)

    # Only the current (v3) hash should be referenced at HEAD
    assert "cccc9999dddd0000eeee1111ffff2222" in hashes
    assert "aaaa1111bbbb2222cccc3333dddd4444" not in hashes


def test_compute_gc_plan_keep(git_repo_with_versions):
    """--keep N retains the N newest versions."""
    repo = git_repo_with_versions
    os.chdir(repo)

    # Create cache blobs for all 3 versions
    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in [
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
        "cccc9999dddd0000eeee1111ffff2222",
    ]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    # Keep 2 → oldest version should be deletable
    keep_hashes, delete_hashes, deletable = compute_gc_plan(keep=2, repo_path=repo)

    assert "aaaa1111bbbb2222cccc3333dddd4444" in delete_hashes
    assert "eeee5555ffff6666aaaa7777bbbb8888" in keep_hashes
    assert "cccc9999dddd0000eeee1111ffff2222" in keep_hashes
    assert len(deletable) == 1


def test_compute_gc_plan_no_flags(git_repo_with_versions):
    """No --keep/--older-than: keep only HEAD-referenced hashes."""
    repo = git_repo_with_versions
    os.chdir(repo)

    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in [
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
        "cccc9999dddd0000eeee1111ffff2222",
    ]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    # No retention flags → only HEAD hash kept
    keep_hashes, delete_hashes, deletable = compute_gc_plan(repo_path=repo)

    assert "cccc9999dddd0000eeee1111ffff2222" in keep_hashes
    assert len(deletable) == 2


def test_compute_gc_plan_older_than(git_repo_with_versions):
    """--older-than retains versions newer than the cutoff."""
    repo = git_repo_with_versions
    os.chdir(repo)

    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in [
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
        "cccc9999dddd0000eeee1111ffff2222",
    ]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    # All commits just happened → all are within 1d
    keep_hashes, delete_hashes, deletable = compute_gc_plan(
        older_than="1d", repo_path=repo,
    )
    # Everything recent → nothing to delete
    assert len(deletable) == 0

    # older_than=0h → everything is "older than 0 hours"... but HEAD is always kept
    keep_hashes, delete_hashes, deletable = compute_gc_plan(
        older_than="0d", repo_path=repo,
    )
    # HEAD hash always kept (referenced), but older versions deletable
    # Actually 0d means keep nothing by age, but HEAD is still referenced
    assert "cccc9999dddd0000eeee1111ffff2222" in keep_hashes


def test_compute_gc_plan_keep_and_older_than(git_repo_with_versions):
    """--keep and --older-than combine: keep if EITHER criterion matches."""
    repo = git_repo_with_versions
    os.chdir(repo)

    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in [
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
        "cccc9999dddd0000eeee1111ffff2222",
    ]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    # --keep 1 --older-than 1d: keep 1 newest by count + all within 1d by age
    # Since all commits are recent, all 3 are within 1d → all kept
    keep_hashes, _, deletable = compute_gc_plan(
        keep=1, older_than="1d", repo_path=repo,
    )
    assert len(deletable) == 0


def test_compute_gc_plan_target_specific(git_repo_with_versions):
    """GC targeting a specific .dvc file only considers that artifact."""
    repo = git_repo_with_versions
    os.chdir(repo)

    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in [
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
        "cccc9999dddd0000eeee1111ffff2222",
    ]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    keep_hashes, delete_hashes, deletable = compute_gc_plan(
        keep=1, targets=["data.txt.dvc"], repo_path=repo,
    )
    # Keep newest + HEAD (same), delete 2 older
    assert len(deletable) == 2
    assert "cccc9999dddd0000eeee1111ffff2222" in keep_hashes


def test_compute_gc_plan_all_branches(tmp_path):
    """--all-branches considers hashes from all local branches."""
    repo = tmp_path / "repo"
    repo.mkdir()

    subprocess.run(["git", "init", "-b", "main"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.email", "t@t.com"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.name", "T"], cwd=repo, capture_output=True, check=True)
    (repo / ".dvc").mkdir()

    # Main branch: hash A
    dvc = {"outs": [{"md5": "aaaa1111bbbb2222cccc3333dddd4444", "size": 100, "path": "data.txt"}]}
    with open(repo / "data.txt.dvc", "w") as f:
        yaml.dump(dvc, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "main"], cwd=repo, capture_output=True, check=True)

    # Feature branch: hash B
    subprocess.run(["git", "checkout", "-b", "feat"], cwd=repo, capture_output=True, check=True)
    dvc["outs"][0]["md5"] = "bbbb2222cccc3333dddd4444eeee5555"
    with open(repo / "data.txt.dvc", "w") as f:
        yaml.dump(dvc, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "feat"], cwd=repo, capture_output=True, check=True)

    subprocess.run(["git", "checkout", "main"], cwd=repo, capture_output=True, check=True)
    os.chdir(repo)

    # Create cache for both
    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in ["aaaa1111bbbb2222cccc3333dddd4444", "bbbb2222cccc3333dddd4444eeee5555"]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    # Without --all-branches: only main's hash kept, feat's deleted
    _, delete_hashes, deletable = compute_gc_plan(repo_path=repo)
    assert "bbbb2222cccc3333dddd4444eeee5555" in delete_hashes
    assert len(deletable) == 1

    # With --all-branches: both kept
    _, delete_hashes, deletable = compute_gc_plan(all_branches=True, repo_path=repo)
    assert len(deletable) == 0


def test_gc_cli_dry_run(tmp_path):
    """CLI dvx gc --keep --dry shows plan without deleting."""
    from click.testing import CliRunner
    from dvx.cli import cli

    repo = tmp_path / "repo"
    repo.mkdir()

    subprocess.run(["git", "init"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.email", "t@t.com"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.name", "T"], cwd=repo, capture_output=True, check=True)

    subprocess.run(["dvc", "init"], cwd=repo, capture_output=True, check=True)

    dvc = {"outs": [{"md5": "aaaa1111bbbb2222cccc3333dddd4444", "size": 100, "path": "d.txt"}]}
    with open(repo / "d.txt.dvc", "w") as f:
        yaml.dump(dvc, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "v1"], cwd=repo, capture_output=True, check=True)

    dvc["outs"][0]["md5"] = "bbbb2222cccc3333dddd4444eeee5555"
    with open(repo / "d.txt.dvc", "w") as f:
        yaml.dump(dvc, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "v2"], cwd=repo, capture_output=True, check=True)

    # Create cache blobs
    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in ["aaaa1111bbbb2222cccc3333dddd4444", "bbbb2222cccc3333dddd4444eeee5555"]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    os.chdir(repo)
    runner = CliRunner()
    result = runner.invoke(cli, ["gc", "--keep", "1", "--dry"])
    assert result.exit_code == 0
    assert "Would delete 1 blob" in result.output
    assert "aaaa1111bbbb" in result.output

    # Blob should NOT be deleted (dry run)
    assert (cache_dir / "aa" / "aa1111bbbb2222cccc3333dddd4444").exists()
