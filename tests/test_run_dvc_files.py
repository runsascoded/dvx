"""Tests for dvx.run.dvc_files module."""

import os
from pathlib import Path

import pytest
import yaml

from dvx.run.dvc_files import (
    DVCFileInfo,
    get_dvc_file_path,
    read_dvc_file,
    write_dvc_file,
)


def test_write_dvc_file_basic(tmp_path):
    """Test basic .dvc file writing."""
    output_path = tmp_path / "output.txt"

    dvc_path = write_dvc_file(
        output_path=output_path,
        md5="abc123",
        size=100,
    )

    assert dvc_path == tmp_path / "output.txt.dvc"
    assert dvc_path.exists()

    with open(dvc_path) as f:
        data = yaml.safe_load(f)

    assert data["outs"][0]["md5"] == "abc123"
    assert data["outs"][0]["size"] == 100
    assert data["outs"][0]["path"] == "output.txt"


def test_write_dvc_file_with_computation(tmp_path):
    """Test .dvc file writing with computation block."""
    output_path = tmp_path / "output.txt"

    dvc_path = write_dvc_file(
        output_path=output_path,
        md5="abc123",
        size=100,
        cmd="python process.py",
        deps={"input.txt": "111222", "script.py": "333444"},
    )

    with open(dvc_path) as f:
        data = yaml.safe_load(f)

    assert "meta" in data
    assert "computation" in data["meta"]
    comp = data["meta"]["computation"]
    assert comp["cmd"] == "python process.py"
    assert comp["deps"]["input.txt"] == "111222"
    assert comp["deps"]["script.py"] == "333444"


def test_write_dvc_file_directory(tmp_path):
    """Test .dvc file writing for directories."""
    output_dir = tmp_path / "output_dir"
    output_dir.mkdir()
    (output_dir / "file1.txt").write_text("content1")
    (output_dir / "file2.txt").write_text("content2")

    dvc_path = write_dvc_file(
        output_path=output_dir,
        md5="abc123",
        size=1000,
        is_dir=True,
        nfiles=2,
    )

    with open(dvc_path) as f:
        data = yaml.safe_load(f)

    # Directory hash should have .dir suffix
    assert data["outs"][0]["md5"] == "abc123.dir"
    assert data["outs"][0]["nfiles"] == 2


def test_read_dvc_file_basic(tmp_path):
    """Test reading basic .dvc file."""
    dvc_file = tmp_path / "data.txt.dvc"
    dvc_content = {
        "outs": [
            {
                "md5": "abc123",
                "size": 100,
                "path": "data.txt",
            }
        ]
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    info = read_dvc_file(tmp_path / "data.txt")

    assert info is not None
    assert info.md5 == "abc123"
    assert info.size == 100
    assert info.path == "data.txt"
    assert info.cmd is None
    assert info.deps == {}


def test_read_dvc_file_with_computation(tmp_path):
    """Test reading .dvc file with computation block."""
    dvc_file = tmp_path / "output.txt.dvc"
    dvc_content = {
        "outs": [
            {
                "md5": "abc123",
                "size": 100,
                "path": "output.txt",
            }
        ],
        "meta": {
            "computation": {
                "cmd": "python process.py",
                "deps": {"input.txt": "111222"},
            }
        },
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    info = read_dvc_file(tmp_path / "output.txt")

    assert info is not None
    assert info.cmd == "python process.py"
    assert info.deps == {"input.txt": "111222"}


def test_read_dvc_file_directory(tmp_path):
    """Test reading .dvc file for directory (strips .dir suffix)."""
    dvc_file = tmp_path / "data_dir.dvc"
    dvc_content = {
        "outs": [
            {
                "md5": "abc123.dir",
                "size": 1000,
                "nfiles": 5,
                "path": "data_dir",
            }
        ]
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    info = read_dvc_file(tmp_path / "data_dir")

    assert info is not None
    assert info.md5 == "abc123"  # .dir suffix stripped
    assert info.is_dir is True
    assert info.nfiles == 5


def test_read_dvc_file_missing(tmp_path):
    """Test reading non-existent .dvc file returns None."""
    info = read_dvc_file(tmp_path / "nonexistent.txt")
    assert info is None


def test_read_dvc_file_direct_path(tmp_path):
    """Test reading .dvc file by direct .dvc path."""
    dvc_file = tmp_path / "data.txt.dvc"
    dvc_content = {
        "outs": [
            {
                "md5": "abc123",
                "size": 100,
                "path": "data.txt",
            }
        ]
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    # Pass .dvc file path directly
    info = read_dvc_file(dvc_file)

    assert info is not None
    assert info.md5 == "abc123"


def test_get_dvc_file_path():
    """Test get_dvc_file_path helper."""
    assert get_dvc_file_path(Path("data.txt")) == Path("data.txt.dvc")
    assert get_dvc_file_path(Path("path/to/file.csv")) == Path("path/to/file.csv.dvc")


def test_roundtrip(tmp_path):
    """Test write then read preserves data."""
    output_path = tmp_path / "output.parquet"

    write_dvc_file(
        output_path=output_path,
        md5="fedcba987654",
        size=12345,
        cmd="python train.py --model rf",
        deps={"data.csv": "111", "train.py": "222"},
    )

    info = read_dvc_file(output_path)

    assert info.md5 == "fedcba987654"
    assert info.size == 12345
    assert info.cmd == "python train.py --model rf"
    assert info.deps == {"data.csv": "111", "train.py": "222"}
