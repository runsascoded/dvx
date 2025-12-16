"""Tests for dvx.cache module."""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from dvx.cache import get_cache_path, get_hash


@pytest.fixture
def temp_dvc_repo(tmp_path):
    """Create a temporary DVC repository with a tracked file."""
    # Create .dvc directory structure
    dvc_dir = tmp_path / ".dvc"
    dvc_dir.mkdir()
    cache_dir = dvc_dir / "cache" / "files" / "md5"
    cache_dir.mkdir(parents=True)

    # Create a data file
    data_file = tmp_path / "data.txt"
    data_file.write_text("test data\n")

    # The MD5 of "test data\n" is 39a870a194a787550b6b5d1f49629236
    md5_hash = "39a870a194a787550b6b5d1f49629236"

    # Create .dvc file
    dvc_file = tmp_path / "data.txt.dvc"
    dvc_content = {
        "outs": [
            {
                "md5": md5_hash,
                "size": 10,
                "path": "data.txt",
            }
        ]
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    # Create cache file
    cache_subdir = cache_dir / md5_hash[:2]
    cache_subdir.mkdir()
    cache_file = cache_subdir / md5_hash[2:]
    cache_file.write_text("test data\n")

    return tmp_path, md5_hash


def test_get_hash(temp_dvc_repo):
    """Test get_hash returns correct MD5 from .dvc file."""
    repo_path, expected_hash = temp_dvc_repo
    os.chdir(repo_path)

    # Test with .dvc extension
    assert get_hash("data.txt.dvc") == expected_hash

    # Test without .dvc extension (auto-added)
    assert get_hash("data.txt") == expected_hash


def test_get_hash_missing_file(tmp_path):
    """Test get_hash raises for missing .dvc file."""
    os.chdir(tmp_path)

    with pytest.raises(FileNotFoundError):
        get_hash("nonexistent.txt")


def test_get_cache_path(temp_dvc_repo):
    """Test get_cache_path returns correct path."""
    repo_path, md5_hash = temp_dvc_repo
    os.chdir(repo_path)

    path = get_cache_path("data.txt")

    # Should be relative path
    assert not path.startswith("/")

    # Should contain the hash structure
    assert md5_hash[:2] in path
    assert md5_hash[2:] in path


def test_get_cache_path_absolute(temp_dvc_repo):
    """Test get_cache_path with absolute=True."""
    repo_path, md5_hash = temp_dvc_repo
    os.chdir(repo_path)

    path = get_cache_path("data.txt", absolute=True)

    # Should be absolute path
    assert path.startswith("/") or (len(path) > 1 and path[1] == ":")  # Windows


def test_get_hash_with_computation_block(tmp_path):
    """Test get_hash works with DVX computation block."""
    os.chdir(tmp_path)

    # Create .dvc file with computation block
    dvc_content = {
        "outs": [
            {
                "md5": "abc123def456",
                "size": 100,
                "path": "output.txt",
            }
        ],
        "meta": {
            "computation": {
                "cmd": "python process.py",
                "code_ref": "deadbeef",
                "deps": {"input.txt": "111222333"},
            }
        },
    }
    dvc_file = tmp_path / "output.txt.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    assert get_hash("output.txt") == "abc123def456"


def test_get_hash_directory(tmp_path):
    """Test get_hash strips .dir suffix for directories."""
    os.chdir(tmp_path)

    # Create .dvc file for a directory (hash ends with .dir)
    dvc_content = {
        "outs": [
            {
                "md5": "abc123def456.dir",
                "size": 1000,
                "nfiles": 5,
                "path": "data_dir",
            }
        ]
    }
    dvc_file = tmp_path / "data_dir.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    # get_hash should return hash without .dir suffix
    assert get_hash("data_dir") == "abc123def456.dir"
