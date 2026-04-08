"""Tests for dvx.run.dvc_files module."""

import os
from pathlib import Path

import pytest
import yaml

from dvx.run.dvc_files import (
    DVCFileInfo,
    get_dvc_file_path,
    is_output_fresh,
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


def test_write_dvc_file_with_git_deps(tmp_path):
    """Test .dvc file writing with git_deps block."""
    output_path = tmp_path / "output.txt"

    dvc_path = write_dvc_file(
        output_path=output_path,
        md5="abc123",
        size=100,
        cmd="python process.py",
        deps={"input.txt": "111222"},
        git_deps={"script.py": "aabbccdd", "lib.py": "eeff0011"},
    )

    with open(dvc_path) as f:
        data = yaml.safe_load(f)

    comp = data["meta"]["computation"]
    assert comp["git_deps"]["script.py"] == "aabbccdd"
    assert comp["git_deps"]["lib.py"] == "eeff0011"
    assert comp["deps"]["input.txt"] == "111222"


def test_read_dvc_file_with_git_deps(tmp_path):
    """Test reading .dvc file with git_deps."""
    dvc_file = tmp_path / "output.txt.dvc"
    dvc_content = {
        "outs": [{"md5": "abc123", "size": 100, "path": "output.txt"}],
        "meta": {
            "computation": {
                "cmd": "python process.py",
                "deps": {"input.txt": "111222"},
                "git_deps": {"script.py": "aabbccdd"},
            }
        },
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    info = read_dvc_file(tmp_path / "output.txt")

    assert info is not None
    assert info.deps == {"input.txt": "111222"}
    assert info.git_deps == {"script.py": "aabbccdd"}


def test_roundtrip_with_git_deps(tmp_path):
    """Test write then read preserves git_deps."""
    output_path = tmp_path / "output.parquet"

    write_dvc_file(
        output_path=output_path,
        md5="fedcba987654",
        size=12345,
        cmd="python train.py",
        deps={"data.csv": "111"},
        git_deps={"train.py": "aaa", "utils.py": "bbb"},
    )

    info = read_dvc_file(output_path)

    assert info.deps == {"data.csv": "111"}
    assert info.git_deps == {"train.py": "aaa", "utils.py": "bbb"}


def test_write_dvc_file_git_deps_only(tmp_path):
    """Test .dvc file with git_deps but no deps still creates computation block."""
    output_path = tmp_path / "output.txt"

    dvc_path = write_dvc_file(
        output_path=output_path,
        md5="abc123",
        size=100,
        git_deps={"script.py": "aabbccdd"},
    )

    with open(dvc_path) as f:
        data = yaml.safe_load(f)

    assert "meta" in data
    comp = data["meta"]["computation"]
    assert "deps" not in comp
    assert comp["git_deps"]["script.py"] == "aabbccdd"


def test_read_dvc_file_no_git_deps(tmp_path):
    """Test reading .dvc file without git_deps returns empty dict."""
    dvc_file = tmp_path / "output.txt.dvc"
    dvc_content = {
        "outs": [{"md5": "abc123", "size": 100, "path": "output.txt"}],
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

    assert info.git_deps == {}


# =============================================================================
# Side-effect stage tests
# =============================================================================


def test_read_side_effect_dvc_file(tmp_path):
    """Test reading a side-effect .dvc file (no outs, has computation)."""
    dvc_file = tmp_path / "deploy.dvc"
    dvc_content = {
        "meta": {
            "computation": {
                "cmd": "wrangler pages deploy dist",
                "deps": {"dist/index.html": "abc123"},
            }
        }
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    info = read_dvc_file(dvc_file)

    assert info is not None
    assert info.path == "deploy"
    assert info.md5 is None
    assert info.size is None
    assert info.cmd == "wrangler pages deploy dist"
    assert info.deps == {"dist/index.html": "abc123"}
    assert info.is_side_effect is True


def test_read_no_outs_no_computation_returns_none(tmp_path):
    """Test that .dvc with no outs AND no computation returns None."""
    dvc_file = tmp_path / "empty.dvc"
    dvc_content = {"meta": {"some_key": "value"}}
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    assert read_dvc_file(dvc_file) is None


def test_is_side_effect_property():
    """Test DVCFileInfo.is_side_effect property."""
    # Side-effect: has cmd, no md5 (inferred)
    se = DVCFileInfo(path="deploy", cmd="deploy.sh", deps={"a": "b"})
    assert se.is_side_effect is True

    # Regular: has cmd and md5
    regular = DVCFileInfo(path="out.txt", md5="abc123", size=100, cmd="run.sh")
    assert regular.is_side_effect is False

    # Leaf: no cmd, no md5
    leaf = DVCFileInfo(path="raw.txt", md5="abc123", size=100)
    assert leaf.is_side_effect is False

    # Explicit side_effect=True overrides inference (even with md5)
    explicit = DVCFileInfo(path="out.txt", md5="abc123", size=100, cmd="run.sh", side_effect=True)
    assert explicit.is_side_effect is True

    # Explicit side_effect=False overrides inference (even without md5)
    explicit_false = DVCFileInfo(path="deploy", cmd="deploy.sh", side_effect=False)
    assert explicit_false.is_side_effect is False


def test_explicit_side_effect_roundtrip(tmp_path):
    """Test that explicit side_effect: true persists through write/read."""
    output_path = tmp_path / "deploy"

    write_dvc_file(
        output_path=output_path,
        cmd="wrangler pages deploy dist",
        deps={"dist/index.html": "abc123"},
        side_effect=True,
    )

    with open(tmp_path / "deploy.dvc") as f:
        data = yaml.safe_load(f)

    assert data["meta"]["computation"]["side_effect"] is True

    info = read_dvc_file(tmp_path / "deploy.dvc")
    assert info.side_effect is True
    assert info.is_side_effect is True


def test_write_side_effect_dvc_file(tmp_path):
    """Test writing a side-effect .dvc file (no outs)."""
    output_path = tmp_path / "deploy"

    dvc_path = write_dvc_file(
        output_path=output_path,
        cmd="wrangler pages deploy dist",
        deps={"dist/index.html": "abc123", "dist/app.js": "def456"},
    )

    assert dvc_path == tmp_path / "deploy.dvc"

    with open(dvc_path) as f:
        data = yaml.safe_load(f)

    assert "outs" not in data
    assert data["meta"]["computation"]["cmd"] == "wrangler pages deploy dist"
    assert data["meta"]["computation"]["deps"]["dist/index.html"] == "abc123"


def test_side_effect_roundtrip(tmp_path):
    """Test write then read of side-effect .dvc file."""
    output_path = tmp_path / "slack-post"

    write_dvc_file(
        output_path=output_path,
        cmd="njsp slack sync",
        deps={"data/crash-log.parquet": "aaa111"},
    )

    info = read_dvc_file(tmp_path / "slack-post.dvc")

    assert info is not None
    assert info.is_side_effect is True
    assert info.cmd == "njsp slack sync"
    assert info.deps == {"data/crash-log.parquet": "aaa111"}
    assert info.md5 is None
    assert info.size is None


def test_side_effect_freshness_deps_match(tmp_path):
    """Side-effect is fresh when all dep hashes match their .dvc files."""
    os.chdir(tmp_path)

    # Create a dep with matching .dvc
    dep_dvc = tmp_path / "dist.dvc"
    dep_content = {
        "outs": [{"md5": "abc123", "size": 100, "path": "dist"}]
    }
    with open(dep_dvc, "w") as f:
        yaml.dump(dep_content, f)

    # Create side-effect .dvc with matching dep hash
    se_dvc = tmp_path / "deploy.dvc"
    se_content = {
        "meta": {
            "computation": {
                "cmd": "deploy.sh",
                "deps": {"dist": "abc123"},
            }
        }
    }
    with open(se_dvc, "w") as f:
        yaml.dump(se_content, f)

    fresh, reason = is_output_fresh(Path("deploy"), use_mtime_cache=False)
    assert fresh is True
    assert reason == "up-to-date"


def test_side_effect_stale_when_dep_changed(tmp_path):
    """Side-effect is stale when a dep hash no longer matches."""
    os.chdir(tmp_path)

    # Create a dep with UPDATED hash
    dep_dvc = tmp_path / "dist.dvc"
    dep_content = {
        "outs": [{"md5": "new_hash_999", "size": 200, "path": "dist"}]
    }
    with open(dep_dvc, "w") as f:
        yaml.dump(dep_content, f)

    # Side-effect .dvc still references the OLD dep hash
    se_dvc = tmp_path / "deploy.dvc"
    se_content = {
        "meta": {
            "computation": {
                "cmd": "deploy.sh",
                "deps": {"dist": "old_hash_111"},
            }
        }
    }
    with open(se_dvc, "w") as f:
        yaml.dump(se_content, f)

    fresh, reason = is_output_fresh(Path("deploy"), use_mtime_cache=False)
    assert fresh is False
    assert "dep changed: dist" == reason
