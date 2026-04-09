"""Tests for dvx.run.dvc_files module."""

import os
from pathlib import Path

import pytest
import yaml

from dvx.run.dvc_files import (
    DVCFileInfo,
    find_parent_dvc_dir,
    get_dvc_file_path,
    get_freshness_details,
    is_output_fresh,
    read_dir_manifest,
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
    assert info.path == str(dvc_file)[:-4]  # Full path minus .dvc suffix
    assert info.md5 is None
    assert info.size is None
    assert info.cmd == "wrangler pages deploy dist"
    assert info.deps == {"dist/index.html": "abc123"}
    assert info.is_side_effect is True


def test_read_side_effect_subdirectory_path(tmp_path):
    """Side-effect .dvc in a subdirectory preserves the directory in path."""
    subdir = tmp_path / "njsp" / "data"
    subdir.mkdir(parents=True)
    dvc_file = subdir / "refresh.dvc"
    dvc_content = {
        "meta": {
            "computation": {
                "cmd": "njsp refresh_data",
                "fetch": {"schedule": "daily"},
            }
        }
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    info = read_dvc_file(dvc_file)
    assert info is not None
    assert info.path == str(subdir / "refresh")
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


# =============================================================================
# Fetch/cron schedule tests
# =============================================================================


def test_is_fetch_due_never_run():
    """Fetch is always due if never run (last_run=None)."""
    from dvx.run.dvc_files import is_fetch_due

    assert is_fetch_due("daily", None) is True
    assert is_fetch_due("hourly", None) is True
    assert is_fetch_due("weekly", None) is True


def test_is_fetch_due_manual():
    """Manual schedule is never auto-due."""
    from dvx.run.dvc_files import is_fetch_due

    assert is_fetch_due("manual", None) is False
    assert is_fetch_due("manual", "2026-01-01T00:00:00Z") is False


def test_is_fetch_due_daily():
    """Daily schedule: due after 24h, not before."""
    from datetime import datetime, timezone

    from dvx.run.dvc_files import is_fetch_due

    last = "2026-04-07T12:00:00+00:00"
    # 23 hours later → not due
    now_early = datetime(2026, 4, 8, 11, 0, 0, tzinfo=timezone.utc)
    assert is_fetch_due("daily", last, now=now_early) is False

    # 25 hours later → due
    now_late = datetime(2026, 4, 8, 13, 0, 0, tzinfo=timezone.utc)
    assert is_fetch_due("daily", last, now=now_late) is True


def test_is_fetch_due_hourly():
    """Hourly schedule: due after 1h."""
    from datetime import datetime, timezone

    from dvx.run.dvc_files import is_fetch_due

    last = "2026-04-07T12:00:00+00:00"
    now_early = datetime(2026, 4, 7, 12, 30, 0, tzinfo=timezone.utc)
    assert is_fetch_due("hourly", last, now=now_early) is False

    now_late = datetime(2026, 4, 7, 13, 1, 0, tzinfo=timezone.utc)
    assert is_fetch_due("hourly", last, now=now_late) is True


def test_read_fetch_schedule(tmp_path):
    """Test reading fetch schedule from .dvc file."""
    dvc_file = tmp_path / "data.xml.dvc"
    dvc_content = {
        "outs": [{"md5": "abc123", "size": 1000, "path": "data.xml"}],
        "meta": {
            "computation": {
                "cmd": "fetch-data",
                "fetch": {
                    "schedule": "daily",
                    "last_run": "2026-04-07T15:10:00Z",
                },
            }
        },
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    info = read_dvc_file(dvc_file)
    assert info is not None
    assert info.fetch_schedule == "daily"
    assert info.fetch_last_run == "2026-04-07T15:10:00Z"
    assert info.is_side_effect is False


def test_write_fetch_schedule(tmp_path):
    """Test writing fetch schedule to .dvc file."""
    output_path = tmp_path / "data.xml"

    dvc_path = write_dvc_file(
        output_path=output_path,
        md5="abc123",
        size=1000,
        cmd="fetch-data",
        fetch_schedule="daily",
        fetch_last_run="2026-04-07T15:10:00Z",
    )

    with open(dvc_path) as f:
        data = yaml.safe_load(f)

    fetch = data["meta"]["computation"]["fetch"]
    assert fetch["schedule"] == "daily"
    assert fetch["last_run"] == "2026-04-07T15:10:00Z"


def test_fetch_schedule_roundtrip(tmp_path):
    """Test write then read of fetch schedule."""
    output_path = tmp_path / "data.xml"

    write_dvc_file(
        output_path=output_path,
        md5="abc123",
        size=1000,
        cmd="fetch-data",
        fetch_schedule="0 15 * * *",
        fetch_last_run="2026-04-07T15:10:00Z",
    )

    info = read_dvc_file(output_path)
    assert info.fetch_schedule == "0 15 * * *"
    assert info.fetch_last_run == "2026-04-07T15:10:00Z"


def test_fetch_due_makes_output_stale(tmp_path):
    """Output with expired fetch schedule reports stale."""
    from datetime import datetime, timezone
    from unittest.mock import patch

    os.chdir(tmp_path)

    # Create output file
    output = tmp_path / "data.xml"
    output.write_text("<data/>")

    # Write .dvc with daily schedule, last run >24h ago
    dvc_path = write_dvc_file(
        output_path=output,
        md5="abc123",
        size=7,
        cmd="fetch-data",
        fetch_schedule="daily",
        fetch_last_run="2026-04-06T10:00:00Z",
    )

    # Mock now to be 2 days later
    fake_now = datetime(2026, 4, 8, 10, 0, 0, tzinfo=timezone.utc)
    with patch("dvx.run.dvc_files.is_fetch_due", wraps=lambda s, l, now=None: True):
        fresh, reason = is_output_fresh(Path("data.xml"), use_mtime_cache=False)

    assert fresh is False
    assert reason == "fetch schedule due"


def test_fetch_not_due_output_fresh(tmp_path):
    """Output with recent fetch schedule and matching hash is fresh."""
    os.chdir(tmp_path)

    # Create output file with known content
    output = tmp_path / "data.xml"
    output.write_text("<data/>")

    from dvx.run.hash import compute_md5
    md5 = compute_md5(output)

    # Write .dvc with daily schedule, last run just now
    write_dvc_file(
        output_path=output,
        md5=md5,
        size=output.stat().st_size,
        cmd="fetch-data",
        fetch_schedule="daily",
        fetch_last_run="2099-01-01T00:00:00Z",  # Far future → not due
    )

    fresh, reason = is_output_fresh(Path("data.xml"), use_mtime_cache=False)
    assert fresh is True
    assert reason == "up-to-date"


# =============================================================================
# Directory git_deps tests
# =============================================================================


@pytest.fixture
def git_repo(tmp_path):
    """Create a temporary git repo with files and directories."""
    import subprocess

    os.chdir(tmp_path)
    subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.email", "test@test.com"], cwd=tmp_path, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_path, capture_output=True, check=True)

    # Create files
    (tmp_path / "script.py").write_text("print('hello')\n")

    # Create a directory with files
    src = tmp_path / "src"
    src.mkdir()
    (src / "app.ts").write_text("export const app = 1;\n")
    (src / "utils.ts").write_text("export const util = 2;\n")

    subprocess.run(["git", "add", "."], cwd=tmp_path, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=tmp_path, capture_output=True, check=True)

    return tmp_path


def test_get_git_object_sha_file(git_repo):
    """get_git_object_sha returns blob SHA for files."""
    from dvx.run.dvc_files import get_git_blob_sha, get_git_object_sha

    file_sha = get_git_object_sha("script.py", "HEAD", git_repo)
    blob_sha = get_git_blob_sha("script.py", "HEAD", git_repo)

    assert file_sha is not None
    assert file_sha == blob_sha


def test_get_git_object_sha_directory(git_repo):
    """get_git_object_sha returns tree SHA for directories."""
    from dvx.run.dvc_files import get_git_blob_sha, get_git_object_sha

    dir_sha = get_git_object_sha("src", "HEAD", git_repo)
    blob_sha = get_git_blob_sha("src", "HEAD", git_repo)

    assert dir_sha is not None
    # Directories are NOT in the blob cache
    assert blob_sha is None
    # But get_git_object_sha finds them via rev-parse
    assert len(dir_sha) == 40


def test_get_git_object_sha_trailing_slash(git_repo):
    """get_git_object_sha strips trailing slash."""
    from dvx.run.dvc_files import get_git_object_sha

    with_slash = get_git_object_sha("src/", "HEAD", git_repo)
    without_slash = get_git_object_sha("src", "HEAD", git_repo)

    assert with_slash == without_slash


def test_directory_git_dep_freshness(git_repo):
    """Freshness check works with directory git_deps (tree SHAs)."""
    import subprocess

    from dvx.run.dvc_files import get_git_object_sha

    os.chdir(git_repo)

    # Get current tree SHA for src/
    tree_sha = get_git_object_sha("src", "HEAD", git_repo)
    assert tree_sha is not None

    # Create output file
    output = git_repo / "bundle.js"
    output.write_text("bundled\n")

    from dvx.run.hash import compute_md5
    md5 = compute_md5(output)

    # Write .dvc with directory git_dep
    write_dvc_file(
        output_path=output,
        md5=md5,
        size=output.stat().st_size,
        cmd="build",
        git_deps={"src": tree_sha},
    )

    # Should be fresh — tree SHA matches
    fresh, reason = is_output_fresh(Path("bundle.js"), use_mtime_cache=False)
    assert fresh is True

    # Modify a file in src/
    (git_repo / "src" / "app.ts").write_text("export const app = 99;\n")
    subprocess.run(["git", "add", "src/app.ts"], cwd=git_repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "update app"], cwd=git_repo, capture_output=True, check=True)

    # Clear blob cache so it re-reads
    from dvx.run.dvc_files import _blob_cache
    _blob_cache.clear()

    # Should now be stale — tree SHA changed
    fresh, reason = is_output_fresh(Path("bundle.js"), use_mtime_cache=False)
    assert fresh is False
    assert "git dep changed: src" == reason


# =============================================================================
# get_freshness_details tests for side-effect and fetch
# =============================================================================


def test_freshness_details_side_effect_fresh(tmp_path):
    """get_freshness_details returns fresh for side-effect with matching deps."""
    os.chdir(tmp_path)

    dep_dvc = tmp_path / "dist.dvc"
    with open(dep_dvc, "w") as f:
        yaml.dump({"outs": [{"md5": "abc123", "size": 100, "path": "dist"}]}, f)

    se_dvc = tmp_path / "deploy.dvc"
    with open(se_dvc, "w") as f:
        yaml.dump({
            "meta": {"computation": {"cmd": "deploy.sh", "deps": {"dist": "abc123"}}}
        }, f)

    details = get_freshness_details(Path("deploy"), use_mtime_cache=False)
    assert details.fresh is True
    assert details.reason == "up-to-date"


def test_freshness_details_side_effect_stale(tmp_path):
    """get_freshness_details returns stale for side-effect with changed deps."""
    os.chdir(tmp_path)

    dep_dvc = tmp_path / "dist.dvc"
    with open(dep_dvc, "w") as f:
        yaml.dump({"outs": [{"md5": "new_hash", "size": 200, "path": "dist"}]}, f)

    se_dvc = tmp_path / "deploy.dvc"
    with open(se_dvc, "w") as f:
        yaml.dump({
            "meta": {"computation": {"cmd": "deploy.sh", "deps": {"dist": "old_hash"}}}
        }, f)

    details = get_freshness_details(Path("deploy"), use_mtime_cache=False)
    assert details.fresh is False
    assert "dep changed: dist" in details.reason
    assert details.changed_deps is not None
    assert "dist" in details.changed_deps


def test_freshness_details_fetch_due(tmp_path):
    """get_freshness_details returns stale when fetch schedule is due."""
    os.chdir(tmp_path)

    output = tmp_path / "data.xml"
    output.write_text("<data/>")

    write_dvc_file(
        output_path=output,
        md5="abc123",
        size=7,
        cmd="fetch-data",
        fetch_schedule="daily",
        fetch_last_run="2020-01-01T00:00:00Z",  # Long ago → due
    )

    details = get_freshness_details(Path("data.xml"), use_mtime_cache=False)
    assert details.fresh is False
    assert details.reason == "fetch schedule due"


def test_freshness_details_fetch_not_due(tmp_path):
    """get_freshness_details returns fresh when fetch not due and hash matches."""
    os.chdir(tmp_path)

    output = tmp_path / "data.xml"
    output.write_text("<data/>")

    from dvx.run.hash import compute_md5
    md5 = compute_md5(output)

    write_dvc_file(
        output_path=output,
        md5=md5,
        size=output.stat().st_size,
        cmd="fetch-data",
        fetch_schedule="daily",
        fetch_last_run="2099-01-01T00:00:00Z",
    )

    details = get_freshness_details(Path("data.xml"), use_mtime_cache=False)
    assert details.fresh is True


def test_is_fetch_due_weekly():
    """Weekly schedule: due after 7 days, not before."""
    from datetime import datetime, timezone

    from dvx.run.dvc_files import is_fetch_due

    last = "2026-04-01T12:00:00+00:00"
    # 6 days later → not due
    assert is_fetch_due("weekly", last, now=datetime(2026, 4, 7, 12, 0, 0, tzinfo=timezone.utc)) is False
    # 8 days later → due
    assert is_fetch_due("weekly", last, now=datetime(2026, 4, 9, 12, 0, 0, tzinfo=timezone.utc)) is True


def test_is_fetch_due_naive_last_run():
    """last_run without timezone is treated as UTC."""
    from datetime import datetime, timezone

    from dvx.run.dvc_files import is_fetch_due

    last = "2026-04-07T12:00:00"  # No timezone
    now = datetime(2026, 4, 8, 13, 0, 0, tzinfo=timezone.utc)
    assert is_fetch_due("daily", last, now=now) is True


# =============================================================================
# find_parent_dvc_dir / read_dir_manifest tests
# =============================================================================


def test_find_parent_dvc_dir_basic(tmp_path):
    """find_parent_dvc_dir finds .dvc-tracked parent directory."""
    # Create a directory tracked by DVC
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "file1.txt").write_text("content1\n")
    (data_dir / "sub").mkdir()
    (data_dir / "sub" / "file2.txt").write_text("content2\n")

    # Write .dvc file for the directory
    dvc_file = tmp_path / "data.dvc"
    dvc_content = {
        "outs": [{"md5": "abc123.dir", "size": 1000, "nfiles": 2, "path": "data"}]
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    # Find parent for a file inside the directory
    result = find_parent_dvc_dir(tmp_path / "data" / "file1.txt")
    assert result is not None
    parent_dir, relpath = result
    assert parent_dir == tmp_path / "data"
    assert relpath == "file1.txt"

    # Find parent for a nested file
    result2 = find_parent_dvc_dir(tmp_path / "data" / "sub" / "file2.txt")
    assert result2 is not None
    parent_dir2, relpath2 = result2
    assert parent_dir2 == tmp_path / "data"
    assert relpath2 == "sub/file2.txt"


def test_find_parent_dvc_dir_not_found(tmp_path):
    """find_parent_dvc_dir returns None when no parent .dvc exists."""
    (tmp_path / "untracked.txt").write_text("hello\n")
    result = find_parent_dvc_dir(tmp_path / "untracked.txt")
    assert result is None


def test_read_dir_manifest(tmp_path):
    """read_dir_manifest reads .dir JSON manifest from cache."""
    import json

    # Create cache structure
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    subdir = cache_dir / "ab"
    subdir.mkdir()

    # Write manifest file (hash = "abc123...", prefix "ab", rest "c123...")
    manifest = [
        {"md5": "111222333", "relpath": "file1.txt"},
        {"md5": "444555666", "relpath": "sub/file2.txt"},
    ]
    manifest_file = subdir / "c123def456.dir"
    manifest_file.write_text(json.dumps(manifest))

    result = read_dir_manifest("abc123def456", cache_dir)

    assert result == {"file1.txt": "111222333", "sub/file2.txt": "444555666"}


def test_read_dir_manifest_with_dir_suffix(tmp_path):
    """read_dir_manifest handles hash with .dir suffix."""
    import json

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    subdir = cache_dir / "ab"
    subdir.mkdir()

    manifest = [{"md5": "aaa", "relpath": "data.csv"}]
    (subdir / "c123.dir").write_text(json.dumps(manifest))

    # Pass hash with .dir suffix
    result = read_dir_manifest("abc123.dir", cache_dir)
    assert result == {"data.csv": "aaa"}


def test_read_dir_manifest_missing(tmp_path):
    """read_dir_manifest returns empty dict for missing manifest."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    result = read_dir_manifest("nonexistent", cache_dir)
    assert result == {}
