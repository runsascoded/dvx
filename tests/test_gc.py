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
