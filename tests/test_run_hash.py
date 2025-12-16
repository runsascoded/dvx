"""Tests for dvx.run.hash module."""

import tempfile
from pathlib import Path

import pytest

from dvx.run.hash import compute_file_size, compute_md5


def test_compute_md5_file(tmp_path):
    """Test MD5 hash computation for a file."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test data\n")

    # Known MD5 of "test data\n"
    expected = "39a870a194a787550b6b5d1f49629236"
    assert compute_md5(test_file) == expected


def test_compute_md5_empty_file(tmp_path):
    """Test MD5 hash of empty file."""
    test_file = tmp_path / "empty.txt"
    test_file.write_text("")

    # Known MD5 of empty string
    expected = "d41d8cd98f00b204e9800998ecf8427e"
    assert compute_md5(test_file) == expected


def test_compute_md5_binary(tmp_path):
    """Test MD5 hash of binary content."""
    test_file = tmp_path / "binary.bin"
    test_file.write_bytes(b"\x00\x01\x02\x03")

    # Should work without error
    result = compute_md5(test_file)
    assert len(result) == 32  # MD5 hex digest is 32 chars


def test_compute_md5_directory(tmp_path):
    """Test MD5 hash computation for a directory."""
    # Create directory with files
    subdir = tmp_path / "mydir"
    subdir.mkdir()
    (subdir / "a.txt").write_text("file a\n")
    (subdir / "b.txt").write_text("file b\n")

    # Should compute a hash for the directory
    result = compute_md5(subdir)
    assert len(result) == 32


def test_compute_md5_directory_order_independent(tmp_path):
    """Test that directory hash is consistent regardless of creation order."""
    # Create two directories with same content but different creation order
    dir1 = tmp_path / "dir1"
    dir1.mkdir()
    (dir1 / "z.txt").write_text("z content\n")
    (dir1 / "a.txt").write_text("a content\n")

    dir2 = tmp_path / "dir2"
    dir2.mkdir()
    (dir2 / "a.txt").write_text("a content\n")
    (dir2 / "z.txt").write_text("z content\n")

    # Both should have the same hash (sorted by relpath)
    assert compute_md5(dir1) == compute_md5(dir2)


def test_compute_md5_missing_file(tmp_path):
    """Test that missing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        compute_md5(tmp_path / "nonexistent.txt")


def test_compute_file_size(tmp_path):
    """Test file size computation."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello")

    assert compute_file_size(test_file) == 5


def test_compute_file_size_directory(tmp_path):
    """Test directory size computation."""
    subdir = tmp_path / "mydir"
    subdir.mkdir()
    (subdir / "a.txt").write_text("aaa")  # 3 bytes
    (subdir / "b.txt").write_text("bb")  # 2 bytes

    assert compute_file_size(subdir) == 5


def test_compute_file_size_nested_directory(tmp_path):
    """Test nested directory size computation."""
    subdir = tmp_path / "parent"
    subdir.mkdir()
    (subdir / "file.txt").write_text("hello")  # 5 bytes

    nested = subdir / "child"
    nested.mkdir()
    (nested / "nested.txt").write_text("world")  # 5 bytes

    assert compute_file_size(subdir) == 10
