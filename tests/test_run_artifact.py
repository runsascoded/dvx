"""Tests for dvx.run.artifact module."""

from pathlib import Path

import pytest
import yaml

from dvx.run.artifact import Artifact, Computation, delayed, write_all_dvc


def test_artifact_basic(tmp_path):
    """Test basic Artifact creation."""
    artifact = Artifact(path="output.txt")

    assert artifact.path == "output.txt"
    assert artifact.computation is None
    assert artifact.md5 is None
    assert artifact.size is None


def test_artifact_with_computation():
    """Test Artifact with computation."""
    comp = Computation(
        cmd="python process.py",
        deps=["input.txt"],
    )
    artifact = Artifact(
        path="output.txt",
        computation=comp,
    )

    assert artifact.computation is not None
    assert artifact.computation.cmd == "python process.py"
    assert artifact.computation.deps == ["input.txt"]


def test_artifact_from_path(tmp_path):
    """Test Artifact.from_path creates artifact with hash."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello world\n")

    artifact = Artifact.from_path(test_file)

    assert artifact.md5 is not None
    assert artifact.size == 12
    assert len(artifact.md5) == 32


def test_artifact_from_path_missing():
    """Test Artifact.from_path raises for missing file."""
    with pytest.raises(FileNotFoundError):
        Artifact.from_path("/nonexistent/file.txt")


def test_artifact_from_dvc(tmp_path):
    """Test Artifact.from_dvc loads from .dvc file."""
    # Create .dvc file
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
                "code_ref": "deadbeef",
                "deps": {"input.txt": "111222"},
            }
        },
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    artifact = Artifact.from_dvc(tmp_path / "output.txt")

    assert artifact is not None
    assert artifact.md5 == "abc123"
    assert artifact.size == 100
    assert artifact.computation is not None
    assert artifact.computation.cmd == "python process.py"
    assert artifact.computation.code_ref == "deadbeef"


def test_artifact_from_dvc_missing(tmp_path):
    """Test Artifact.from_dvc returns None for missing .dvc file."""
    artifact = Artifact.from_dvc(tmp_path / "nonexistent.txt")
    assert artifact is None


def test_artifact_write_dvc(tmp_path):
    """Test Artifact.write_dvc creates .dvc file."""
    # Create output file
    output = tmp_path / "output.txt"
    output.write_text("result data\n")

    artifact = Artifact(
        path=str(output),
        computation=Computation(cmd="echo test > output.txt"),
    )

    dvc_path = artifact.write_dvc(capture_code_ref=False)

    assert dvc_path.exists()
    with open(dvc_path) as f:
        data = yaml.safe_load(f)

    assert data["outs"][0]["path"] == "output.txt"
    assert data["outs"][0]["md5"] is not None
    assert data["meta"]["computation"]["cmd"] == "echo test > output.txt"


def test_artifact_exists(tmp_path):
    """Test Artifact.exists() method."""
    existing = tmp_path / "existing.txt"
    existing.write_text("hello")

    artifact1 = Artifact(path=str(existing))
    assert artifact1.exists() is True

    artifact2 = Artifact(path=str(tmp_path / "nonexistent.txt"))
    assert artifact2.exists() is False


def test_artifact_is_computed():
    """Test Artifact.is_computed() method."""
    artifact1 = Artifact(path="test.txt", md5="abc123")
    assert artifact1.is_computed() is True

    artifact2 = Artifact(path="test.txt")
    assert artifact2.is_computed() is False


def test_computation_get_dep_paths():
    """Test Computation.get_dep_paths()."""
    dep1 = Artifact(path="input1.txt")
    comp = Computation(
        cmd="python process.py",
        deps=[dep1, "input2.txt", Path("input3.txt")],
    )

    paths = comp.get_dep_paths()

    assert len(paths) == 3
    assert Path("input1.txt") in paths
    assert Path("input2.txt") in paths
    assert Path("input3.txt") in paths


def test_computation_get_dep_hashes(tmp_path):
    """Test Computation.get_dep_hashes()."""
    # Create dep file
    dep_file = tmp_path / "input.txt"
    dep_file.write_text("input data\n")

    # Artifact with known hash - use a different path so it doesn't get overwritten
    dep1 = Artifact(path=str(tmp_path / "other.txt"), md5="known_hash")

    comp = Computation(
        cmd="python process.py",
        deps=[dep1, str(dep_file)],
    )

    hashes = comp.get_dep_hashes()

    # First dep should use known hash (since file doesn't exist, it uses the provided md5)
    assert hashes[str(tmp_path / "other.txt")] == "known_hash"
    # Second dep should compute hash from file
    assert len(hashes[str(dep_file)]) == 32  # MD5 hex digest length


def test_artifact_get_upstream():
    """Test Artifact.get_upstream() returns only Artifact deps."""
    dep1 = Artifact(path="input1.txt")
    dep2 = Artifact(path="input2.txt")

    artifact = Artifact(
        path="output.txt",
        computation=Computation(
            cmd="cat input*.txt > output.txt",
            deps=[dep1, "string_dep.txt", dep2],
        ),
    )

    upstream = artifact.get_upstream()

    assert len(upstream) == 2
    assert dep1 in upstream
    assert dep2 in upstream


def test_artifact_walk_upstream():
    """Test Artifact.walk_upstream() collects all ancestors."""
    # Build a simple DAG: leaf -> mid -> output
    leaf = Artifact(path="leaf.txt")
    mid = Artifact(
        path="mid.txt",
        computation=Computation(cmd="process leaf", deps=[leaf]),
    )
    output = Artifact(
        path="output.txt",
        computation=Computation(cmd="process mid", deps=[mid]),
    )

    ancestors = output.walk_upstream()

    # Should be in dependency order (leaf first)
    assert len(ancestors) == 3
    assert ancestors[0] == leaf
    assert ancestors[1] == mid
    assert ancestors[2] == output


def test_delayed_decorator():
    """Test @delayed decorator marks functions."""

    @delayed
    def make_artifact(name: str) -> Artifact:
        return Artifact(path=f"{name}.txt")

    # Should work normally
    result = make_artifact("test")
    assert isinstance(result, Artifact)
    assert result.path == "test.txt"

    # Should be marked as delayed
    assert hasattr(make_artifact, "_dvx_delayed")
    assert make_artifact._dvx_delayed is True


def test_artifact_hash_eq():
    """Test Artifact __hash__ and __eq__."""
    a1 = Artifact(path="same.txt")
    a2 = Artifact(path="same.txt")
    a3 = Artifact(path="different.txt")

    assert a1 == a2
    assert a1 != a3
    assert hash(a1) == hash(a2)

    # Should be usable in sets
    s = {a1, a2, a3}
    assert len(s) == 2
