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


def test_add_to_cache_updates_dep_hashes_when_fresh(tmp_path):
    """Test that add_to_cache updates dep hashes from current .dvc files.

    When an output is (re)generated, the dep hashes recorded should reflect
    what was actually used - the current state of deps. This updates the
    provenance to match reality.

    The key constraint: deps must be fresh (file hash == .dvc hash) before
    adding. This ensures the recorded dep hashes are accurate.
    """
    from dvx.cache import add_to_cache, _hash_single_file

    os.chdir(tmp_path)

    # Create .dvc directory structure
    dvc_dir = tmp_path / ".dvc"
    dvc_dir.mkdir()
    cache_dir = dvc_dir / "cache" / "files" / "md5"
    cache_dir.mkdir(parents=True)

    # Create a dependency FILE first
    dep_file = tmp_path / "input.txt"
    dep_file.write_text("input data v2\n")
    new_dep_hash = _hash_single_file(dep_file)

    # Create a dependency .dvc file matching the file (fresh state)
    dep_dvc = tmp_path / "input.txt.dvc"
    dep_content = {
        "outs": [{"md5": new_dep_hash, "size": 14, "hash": "md5", "path": "input.txt"}]
    }
    with open(dep_dvc, "w") as f:
        yaml.dump(dep_content, f)

    # Create output file
    output_file = tmp_path / "output.txt"
    output_file.write_text("original output\n")

    # Create output .dvc file with meta.computation.deps pointing to OLD dep hash
    output_dvc = tmp_path / "output.txt.dvc"
    old_dep_hash = "aaa111bbb222ccc333ddd444eee55566"
    output_content = {
        "outs": [{"md5": "old_output_hash_placeholder", "size": 16, "path": "output.txt"}],
        "meta": {
            "computation": {
                "cmd": "cat input.txt > output.txt",
                "deps": {"input.txt": old_dep_hash},
            }
        },
    }
    with open(output_dvc, "w") as f:
        yaml.dump(output_content, f)

    # Run add_to_cache - deps are fresh, so this should update dep hashes
    md5, size, is_dir = add_to_cache("output.txt")

    # Read back the .dvc file
    with open(output_dvc) as f:
        result = yaml.safe_load(f)

    # Verify output was updated with correct hash, size, and dep hash
    assert result["outs"][0]["md5"] == md5
    assert result["outs"][0]["size"] == size
    assert result["meta"]["computation"]["deps"]["input.txt"] == new_dep_hash


def test_add_to_cache_errors_on_stale_deps(tmp_path):
    """Test that add_to_cache errors when deps are stale (file != .dvc).

    If a dep file has been modified but not added, adding an output would
    record incorrect provenance - the output was built from the modified file,
    but we'd record the old .dvc hash.

    Instead, we should error and require deps to be fresh before adding.
    """
    from dvx.cache import add_to_cache, _hash_single_file

    os.chdir(tmp_path)

    # Create .dvc directory structure
    dvc_dir = tmp_path / ".dvc"
    dvc_dir.mkdir()
    cache_dir = dvc_dir / "cache" / "files" / "md5"
    cache_dir.mkdir(parents=True)

    # Create a dependency FILE
    dep_file = tmp_path / "input.txt"
    dep_file.write_text("input data v2\n")
    file_hash = _hash_single_file(dep_file)

    # Create a dependency .dvc file with DIFFERENT hash (stale state)
    dep_dvc = tmp_path / "input.txt.dvc"
    stale_dvc_hash = "aaa111bbb222ccc333ddd444eee55566"
    dep_content = {
        "outs": [{"md5": stale_dvc_hash, "size": 10, "hash": "md5", "path": "input.txt"}]
    }
    with open(dep_dvc, "w") as f:
        yaml.dump(dep_content, f)

    assert file_hash != stale_dvc_hash, "Test setup: hashes should differ"

    # Create output file
    output_file = tmp_path / "output.txt"
    output_file.write_text("output\n")

    # Create output .dvc file with dep
    output_dvc = tmp_path / "output.txt.dvc"
    output_content = {
        "outs": [{"md5": "placeholder", "size": 7, "path": "output.txt"}],
        "meta": {
            "computation": {
                "cmd": "cat input.txt > output.txt",
                "deps": {"input.txt": stale_dvc_hash},
            }
        },
    }
    with open(output_dvc, "w") as f:
        yaml.dump(output_content, f)

    # Run add_to_cache - should ERROR because dep is stale
    with pytest.raises(ValueError) as exc_info:
        add_to_cache("output.txt")

    expected_lines = [
        "Cannot add output.txt: 1 stale dep(s):",
        f"  input.txt: .dvc={stale_dvc_hash[:8]}... file={file_hash[:8]}...",
        "Run `dvx add` on deps first, or use --recursive",
    ]
    assert str(exc_info.value).strip().split("\n") == expected_lines


def test_add_to_cache_recursive_adds_stale_deps(tmp_path):
    """Test that add_to_cache with recursive=True auto-adds stale deps first.

    When recursive=True, stale deps should be added (depth-first) before
    adding the output. This ensures consistent state across the DAG.
    """
    from dvx.cache import add_to_cache, _hash_single_file

    os.chdir(tmp_path)

    # Create .dvc directory structure
    dvc_dir = tmp_path / ".dvc"
    dvc_dir.mkdir()
    cache_dir = dvc_dir / "cache" / "files" / "md5"
    cache_dir.mkdir(parents=True)

    # Create a dependency FILE
    dep_file = tmp_path / "input.txt"
    dep_file.write_text("input data v2\n")
    new_file_hash = _hash_single_file(dep_file)

    # Create a dependency .dvc file with DIFFERENT hash (stale state)
    dep_dvc = tmp_path / "input.txt.dvc"
    old_dvc_hash = "aaa111bbb222ccc333ddd444eee55566"
    dep_content = {
        "outs": [{"md5": old_dvc_hash, "size": 10, "hash": "md5", "path": "input.txt"}]
    }
    with open(dep_dvc, "w") as f:
        yaml.dump(dep_content, f)

    # Create output file
    output_file = tmp_path / "output.txt"
    output_file.write_text("output\n")

    # Create output .dvc file with dep pointing to old hash
    output_dvc = tmp_path / "output.txt.dvc"
    output_content = {
        "outs": [{"md5": "placeholder", "size": 7, "path": "output.txt"}],
        "meta": {
            "computation": {
                "cmd": "cat input.txt > output.txt",
                "deps": {"input.txt": old_dvc_hash},
            }
        },
    }
    with open(output_dvc, "w") as f:
        yaml.dump(output_content, f)

    # Run add_to_cache with recursive=True
    md5, size, is_dir = add_to_cache("output.txt", recursive=True)

    # Verify dep .dvc was updated with correct file hash
    with open(dep_dvc) as f:
        dep_result = yaml.safe_load(f)
    assert dep_result["outs"][0]["md5"] == new_file_hash

    # Verify output .dvc has new dep hash
    with open(output_dvc) as f:
        output_result = yaml.safe_load(f)
    assert output_result["meta"]["computation"]["deps"]["input.txt"] == new_file_hash
