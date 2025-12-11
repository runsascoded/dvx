"""Tests for .dvc file read/write with computation block."""

import tempfile
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


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestWriteDvcFile:
    """Tests for write_dvc_file."""

    def test_write_minimal(self, temp_dir):
        """Test writing .dvc file with only required fields."""
        output_path = temp_dir / "output.txt"
        output_path.write_text("hello")

        dvc_path = write_dvc_file(
            output_path=output_path,
            md5="abc123",
            size=5,
        )

        assert dvc_path == temp_dir / "output.txt.dvc"
        assert dvc_path.exists()

        with open(dvc_path) as f:
            data = yaml.safe_load(f)

        assert "outs" in data
        assert len(data["outs"]) == 1
        assert data["outs"][0]["md5"] == "abc123"
        assert data["outs"][0]["size"] == 5
        assert "computation" not in data

    def test_write_with_computation(self, temp_dir):
        """Test writing .dvc file with computation block."""
        output_path = temp_dir / "output.txt"
        output_path.write_text("hello")

        dvc_path = write_dvc_file(
            output_path=output_path,
            md5="abc123",
            size=5,
            cmd="python process.py",
            code_ref="deadbeef1234",
            deps={"input.txt": "def456", "script.py": "789abc"},
        )

        with open(dvc_path) as f:
            data = yaml.safe_load(f)

        assert "computation" in data
        assert data["computation"]["cmd"] == "python process.py"
        assert data["computation"]["code_ref"] == "deadbeef1234"
        assert data["computation"]["deps"] == {
            "input.txt": "def456",
            "script.py": "789abc",
        }

    def test_write_partial_computation(self, temp_dir):
        """Test writing .dvc file with only some computation fields."""
        output_path = temp_dir / "output.txt"
        output_path.write_text("hello")

        # Only cmd, no code_ref or deps
        dvc_path = write_dvc_file(
            output_path=output_path,
            md5="abc123",
            size=5,
            cmd="echo hello",
        )

        with open(dvc_path) as f:
            data = yaml.safe_load(f)

        assert "computation" in data
        assert data["computation"]["cmd"] == "echo hello"
        assert "code_ref" not in data["computation"]
        assert "deps" not in data["computation"]


class TestReadDvcFile:
    """Tests for read_dvc_file."""

    def test_read_nonexistent(self, temp_dir):
        """Test reading non-existent .dvc file."""
        output_path = temp_dir / "nonexistent.txt"
        result = read_dvc_file(output_path)
        assert result is None

    def test_read_computation_format(self, temp_dir):
        """Test reading .dvc file with computation block."""
        output_path = temp_dir / "output.txt"
        dvc_path = temp_dir / "output.txt.dvc"

        dvc_path.write_text("""
outs:
- md5: abc123
  size: 100
  path: output.txt
computation:
  cmd: python process.py
  code_ref: deadbeef1234
  deps:
    input.txt: def456
""")

        info = read_dvc_file(output_path)

        assert info is not None
        assert info.md5 == "abc123"
        assert info.size == 100
        assert info.cmd == "python process.py"
        assert info.code_ref == "deadbeef1234"
        assert info.deps == {"input.txt": "def456"}

    def test_read_legacy_meta_format(self, temp_dir):
        """Test reading legacy .dvc file with meta block."""
        output_path = temp_dir / "output.txt"
        dvc_path = temp_dir / "output.txt.dvc"

        dvc_path.write_text("""
outs:
- md5: abc123
  size: 100
  path: output.txt
meta:
  stage: my_stage
  cmd: old command
  deps:
    old_input.txt: abc111222
""")

        info = read_dvc_file(output_path)

        assert info is not None
        assert info.md5 == "abc123"
        assert info.cmd == "old command"
        assert info.deps == {"old_input.txt": "abc111222"}
        assert info.stage == "my_stage"
        assert info.code_ref is None  # Not in legacy format

    def test_read_computation_takes_precedence(self, temp_dir):
        """Test that computation block takes precedence over meta."""
        output_path = temp_dir / "output.txt"
        dvc_path = temp_dir / "output.txt.dvc"

        # File with both blocks (shouldn't happen, but test precedence)
        dvc_path.write_text("""
outs:
- md5: abc123
  size: 100
  path: output.txt
computation:
  cmd: new command
  deps:
    new_input.txt: aaa
meta:
  cmd: old command
  deps:
    old_input.txt: bbb
""")

        info = read_dvc_file(output_path)

        assert info.cmd == "new command"
        assert info.deps == {"new_input.txt": "aaa"}

    def test_roundtrip(self, temp_dir):
        """Test write then read produces same data."""
        output_path = temp_dir / "output.txt"
        output_path.write_text("test content")

        write_dvc_file(
            output_path=output_path,
            md5="abc123def",
            size=12,
            cmd="make output",
            code_ref="1234567890abcdef",
            deps={"a.txt": "aaa", "b.txt": "bbb"},
        )

        info = read_dvc_file(output_path)

        assert info.md5 == "abc123def"
        assert info.size == 12
        assert info.cmd == "make output"
        assert info.code_ref == "1234567890abcdef"
        assert info.deps == {"a.txt": "aaa", "b.txt": "bbb"}


class TestIsOutputFresh:
    """Tests for is_output_fresh."""

    def test_fresh_output_no_deps(self, temp_dir):
        """Test fresh check with matching output hash."""
        from dvx.run.hash import compute_md5

        output_path = temp_dir / "output.txt"
        output_path.write_text("hello world")
        md5 = compute_md5(output_path)

        write_dvc_file(output_path=output_path, md5=md5, size=11)

        fresh, reason = is_output_fresh(output_path)
        assert fresh is True
        assert reason == "up-to-date"

    def test_stale_output_hash_mismatch(self, temp_dir):
        """Test stale detection when output hash changed."""
        output_path = temp_dir / "output.txt"
        output_path.write_text("hello world")

        write_dvc_file(output_path=output_path, md5="wrong_hash", size=11)

        fresh, reason = is_output_fresh(output_path)
        assert fresh is False
        assert "output hash mismatch" in reason

    def test_stale_output_missing(self, temp_dir):
        """Test stale detection when output is missing."""
        output_path = temp_dir / "output.txt"
        dvc_path = temp_dir / "output.txt.dvc"

        dvc_path.write_text("""
outs:
- md5: abc123
  size: 100
  path: output.txt
""")

        fresh, reason = is_output_fresh(output_path)
        assert fresh is False
        assert reason == "output missing"

    def test_stale_dep_changed(self, temp_dir):
        """Test stale detection when dependency changed."""
        from dvx.run.hash import compute_md5

        # Create output and dep
        output_path = temp_dir / "output.txt"
        dep_path = temp_dir / "input.txt"

        output_path.write_text("output content")
        dep_path.write_text("original input")

        output_md5 = compute_md5(output_path)
        dep_md5 = compute_md5(dep_path)

        write_dvc_file(
            output_path=output_path,
            md5=output_md5,
            size=14,
            deps={str(dep_path): dep_md5},
        )

        # Initially fresh
        fresh, reason = is_output_fresh(output_path)
        assert fresh is True

        # Modify dependency
        dep_path.write_text("modified input")

        fresh, reason = is_output_fresh(output_path)
        assert fresh is False
        assert "dep changed" in reason

    def test_stale_dep_missing(self, temp_dir):
        """Test stale detection when dependency is missing."""
        from dvx.run.hash import compute_md5

        output_path = temp_dir / "output.txt"
        output_path.write_text("output content")
        output_md5 = compute_md5(output_path)

        write_dvc_file(
            output_path=output_path,
            md5=output_md5,
            size=14,
            deps={str(temp_dir / "missing.txt"): "abc123"},
        )

        fresh, reason = is_output_fresh(output_path)
        assert fresh is False
        assert "dep missing" in reason

    def test_skip_dep_check(self, temp_dir):
        """Test that check_deps=False skips dependency checking."""
        from dvx.run.hash import compute_md5

        output_path = temp_dir / "output.txt"
        output_path.write_text("output content")
        output_md5 = compute_md5(output_path)

        # Write with non-existent dep
        write_dvc_file(
            output_path=output_path,
            md5=output_md5,
            size=14,
            deps={str(temp_dir / "missing.txt"): "abc123"},
        )

        # With dep check, should be stale
        fresh, _ = is_output_fresh(output_path, check_deps=True)
        assert fresh is False

        # Without dep check, should be fresh
        fresh, reason = is_output_fresh(output_path, check_deps=False)
        assert fresh is True
        assert reason == "up-to-date"


class TestGetDvcFilePath:
    """Tests for get_dvc_file_path."""

    def test_simple_path(self, temp_dir):
        """Test getting .dvc path for simple file."""
        output_path = temp_dir / "output.txt"
        dvc_path = get_dvc_file_path(output_path)
        assert dvc_path == temp_dir / "output.txt.dvc"

    def test_nested_path(self, temp_dir):
        """Test getting .dvc path for nested file."""
        output_path = temp_dir / "subdir" / "output.parquet"
        dvc_path = get_dvc_file_path(output_path)
        assert dvc_path == temp_dir / "subdir" / "output.parquet.dvc"
