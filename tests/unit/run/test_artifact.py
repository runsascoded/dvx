"""Tests for Python library API (Artifact, Computation, delayed)."""

import tempfile
from pathlib import Path

import pytest
import yaml

from dvx.run.artifact import (
    Artifact,
    Computation,
    delayed,
    materialize,
    write_all_dvc,
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestComputation:
    """Tests for Computation class."""

    def test_basic_computation(self):
        """Test creating a basic computation."""
        comp = Computation(cmd="python process.py")
        assert comp.cmd == "python process.py"
        assert comp.deps == []
        assert comp.params == {}

    def test_computation_with_deps(self, temp_dir):
        """Test computation with string path deps."""
        dep_file = temp_dir / "input.txt"
        dep_file.write_text("test data")

        comp = Computation(
            cmd="python process.py",
            deps=[str(dep_file)],
        )

        assert len(comp.deps) == 1
        paths = comp.get_dep_paths()
        assert len(paths) == 1
        assert paths[0] == dep_file

    def test_computation_with_artifact_deps(self, temp_dir):
        """Test computation with Artifact deps."""
        dep_file = temp_dir / "input.txt"
        dep_file.write_text("test data")

        dep_artifact = Artifact.from_path(dep_file)
        comp = Computation(
            cmd="python process.py",
            deps=[dep_artifact],
        )

        paths = comp.get_dep_paths()
        assert len(paths) == 1
        assert paths[0] == dep_file

    def test_get_dep_hashes(self, temp_dir):
        """Test computing dependency hashes."""
        dep_file = temp_dir / "input.txt"
        dep_file.write_text("hello world")

        comp = Computation(
            cmd="python process.py",
            deps=[str(dep_file)],
        )

        hashes = comp.get_dep_hashes()
        assert str(dep_file) in hashes
        assert len(hashes[str(dep_file)]) == 32  # MD5 hex length

    def test_get_dep_hashes_with_artifact(self, temp_dir):
        """Test that artifact with known hash is used."""
        dep_artifact = Artifact(path="input.txt", md5="known_hash_value")

        comp = Computation(cmd="echo", deps=[dep_artifact])
        hashes = comp.get_dep_hashes()

        assert hashes["input.txt"] == "known_hash_value"

    def test_computation_with_params(self):
        """Test computation with parameters."""
        comp = Computation(
            cmd="python train.py",
            params={"learning_rate": 0.01, "epochs": 100},
        )

        assert comp.params["learning_rate"] == 0.01
        assert comp.params["epochs"] == 100


class TestArtifact:
    """Tests for Artifact class."""

    def test_basic_artifact(self):
        """Test creating a basic artifact."""
        artifact = Artifact(path="output.txt")
        assert artifact.path == "output.txt"
        assert artifact.computation is None
        assert artifact.md5 is None
        assert artifact.size is None

    def test_artifact_with_path_object(self):
        """Test that Path objects are converted to strings."""
        artifact = Artifact(path=Path("output.txt"))
        assert artifact.path == "output.txt"
        assert isinstance(artifact.path, str)

    def test_from_path(self, temp_dir):
        """Test creating artifact from existing file."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("hello world")

        artifact = Artifact.from_path(test_file)

        assert artifact.path == str(test_file)
        assert artifact.md5 is not None
        assert artifact.size == 11

    def test_from_path_not_found(self, temp_dir):
        """Test from_path with non-existent file."""
        with pytest.raises(FileNotFoundError):
            Artifact.from_path(temp_dir / "nonexistent.txt")

    def test_from_dvc_nonexistent(self, temp_dir):
        """Test from_dvc with no .dvc file."""
        result = Artifact.from_dvc(temp_dir / "output.txt")
        assert result is None

    def test_from_dvc(self, temp_dir):
        """Test loading artifact from .dvc file."""
        dvc_file = temp_dir / "output.txt.dvc"
        dvc_file.write_text("""
outs:
- md5: abc123def456
  size: 100
  path: output.txt
computation:
  cmd: python process.py
  code_ref: deadbeef
  deps:
    input.txt: 111222333
""")

        artifact = Artifact.from_dvc(temp_dir / "output.txt")

        assert artifact is not None
        assert artifact.path == "output.txt"
        assert artifact.md5 == "abc123def456"
        assert artifact.size == 100
        assert artifact.computation is not None
        assert artifact.computation.cmd == "python process.py"
        assert artifact.computation.code_ref == "deadbeef"
        assert len(artifact.computation.deps) == 1

    def test_write_dvc_minimal(self, temp_dir):
        """Test writing .dvc file for simple artifact."""
        output_file = temp_dir / "output.txt"
        output_file.write_text("hello world")

        artifact = Artifact(path=str(output_file))
        dvc_path = artifact.write_dvc(capture_code_ref=False)

        assert dvc_path == temp_dir / "output.txt.dvc"
        assert dvc_path.exists()

        with open(dvc_path) as f:
            data = yaml.safe_load(f)

        assert "outs" in data
        assert data["outs"][0]["path"] == str(output_file)
        assert len(data["outs"][0]["md5"]) == 32

    def test_write_dvc_with_computation(self, temp_dir):
        """Test writing .dvc file with computation block."""
        output_file = temp_dir / "output.txt"
        output_file.write_text("hello world")

        dep_file = temp_dir / "input.txt"
        dep_file.write_text("input data")

        artifact = Artifact(
            path=str(output_file),
            computation=Computation(
                cmd="python process.py",
                deps=[str(dep_file)],
            ),
        )

        dvc_path = artifact.write_dvc(capture_code_ref=False)

        with open(dvc_path) as f:
            data = yaml.safe_load(f)

        assert "computation" in data
        assert data["computation"]["cmd"] == "python process.py"
        assert str(dep_file) in data["computation"]["deps"]

    def test_write_dvc_nonexistent_output(self, temp_dir):
        """Test writing .dvc for output that doesn't exist yet."""
        artifact = Artifact(
            path=str(temp_dir / "future_output.txt"),
            computation=Computation(cmd="echo hello"),
        )

        dvc_path = artifact.write_dvc(capture_code_ref=False)

        with open(dvc_path) as f:
            data = yaml.safe_load(f)

        # Should have empty placeholder hash
        assert data["outs"][0]["md5"] == ""
        assert data["outs"][0]["size"] == 0

    def test_is_computed(self):
        """Test is_computed check."""
        artifact_no_hash = Artifact(path="output.txt")
        artifact_with_hash = Artifact(path="output.txt", md5="abc123")

        assert artifact_no_hash.is_computed() is False
        assert artifact_with_hash.is_computed() is True

    def test_exists(self, temp_dir):
        """Test exists check."""
        existing_file = temp_dir / "exists.txt"
        existing_file.write_text("hello")

        artifact_exists = Artifact(path=str(existing_file))
        artifact_missing = Artifact(path=str(temp_dir / "missing.txt"))

        assert artifact_exists.exists() is True
        assert artifact_missing.exists() is False

    def test_get_upstream(self):
        """Test getting upstream artifact dependencies."""
        dep1 = Artifact(path="dep1.txt")
        dep2 = Artifact(path="dep2.txt")

        artifact = Artifact(
            path="output.txt",
            computation=Computation(
                cmd="process",
                deps=[dep1, dep2, "string_dep.txt"],
            ),
        )

        upstream = artifact.get_upstream()
        assert len(upstream) == 2
        assert dep1 in upstream
        assert dep2 in upstream

    def test_get_upstream_no_computation(self):
        """Test get_upstream for leaf node."""
        artifact = Artifact(path="leaf.txt")
        assert artifact.get_upstream() == []

    def test_walk_upstream(self):
        """Test recursive upstream traversal."""
        # Build a simple DAG:
        # leaf1 -> mid -> output
        # leaf2 -/
        leaf1 = Artifact(path="leaf1.txt")
        leaf2 = Artifact(path="leaf2.txt")
        mid = Artifact(
            path="mid.txt",
            computation=Computation(cmd="mid", deps=[leaf1, leaf2]),
        )
        output = Artifact(
            path="output.txt",
            computation=Computation(cmd="output", deps=[mid]),
        )

        walked = output.walk_upstream()

        # Should be in dependency order (leaves first)
        assert len(walked) == 4
        assert walked[-1] == output  # Output is last
        # Leaves should come before mid
        leaf_indices = [walked.index(leaf1), walked.index(leaf2)]
        mid_index = walked.index(mid)
        assert all(li < mid_index for li in leaf_indices)

    def test_artifact_equality(self):
        """Test artifact equality based on path."""
        a1 = Artifact(path="output.txt", md5="hash1")
        a2 = Artifact(path="output.txt", md5="hash2")
        a3 = Artifact(path="other.txt", md5="hash1")

        assert a1 == a2  # Same path
        assert a1 != a3  # Different path

    def test_artifact_hash(self):
        """Test artifact can be used in sets/dicts."""
        a1 = Artifact(path="output.txt")
        a2 = Artifact(path="output.txt")

        s = {a1, a2}
        assert len(s) == 1  # Same path, same hash


class TestDelayed:
    """Tests for delayed decorator."""

    def test_delayed_basic(self):
        """Test delayed decorator preserves function behavior."""

        @delayed
        def make_artifact(name: str) -> Artifact:
            return Artifact(path=f"{name}.txt")

        result = make_artifact("test")

        assert isinstance(result, Artifact)
        assert result.path == "test.txt"

    def test_delayed_marker(self):
        """Test delayed decorator adds marker attribute."""

        @delayed
        def make_artifact() -> Artifact:
            return Artifact(path="test.txt")

        assert hasattr(make_artifact, "_dvx_delayed")
        assert make_artifact._dvx_delayed is True

    def test_delayed_with_deps(self):
        """Test delayed functions can compose."""
        @delayed
        def leaf(name: str) -> Artifact:
            return Artifact(path=f"{name}.txt")

        @delayed
        def derived(src: Artifact) -> Artifact:
            return Artifact(
                path="derived.txt",
                computation=Computation(cmd="derive", deps=[src]),
            )

        src = leaf("input")
        result = derived(src)

        assert result.path == "derived.txt"
        assert result.computation is not None
        assert len(result.computation.deps) == 1
        assert result.computation.deps[0] == src


class TestWriteAllDvc:
    """Tests for write_all_dvc function."""

    def test_write_all_dvc(self, temp_dir):
        """Test writing .dvc files for multiple artifacts."""
        # Create source files
        input1 = temp_dir / "input1.txt"
        input1.write_text("data1")
        input2 = temp_dir / "input2.txt"
        input2.write_text("data2")
        output = temp_dir / "output.txt"
        output.write_text("result")

        leaf1 = Artifact.from_path(input1)
        leaf2 = Artifact.from_path(input2)

        computed = Artifact(
            path=str(output),
            computation=Computation(
                cmd="combine",
                deps=[leaf1, leaf2],
            ),
        )

        paths = write_all_dvc([computed], capture_code_ref=False)

        # Should only write .dvc for computed artifact (leaves have no computation)
        assert len(paths) == 1
        assert paths[0] == temp_dir / "output.txt.dvc"

    def test_write_all_dvc_with_chain(self, temp_dir):
        """Test writing .dvc files for a chain of computations."""
        # Create files
        (temp_dir / "a.txt").write_text("a")
        (temp_dir / "b.txt").write_text("b")
        (temp_dir / "c.txt").write_text("c")

        a = Artifact(
            path=str(temp_dir / "a.txt"),
            computation=Computation(cmd="make a"),
        )
        b = Artifact(
            path=str(temp_dir / "b.txt"),
            computation=Computation(cmd="make b", deps=[a]),
        )
        c = Artifact(
            path=str(temp_dir / "c.txt"),
            computation=Computation(cmd="make c", deps=[b]),
        )

        paths = write_all_dvc([c], capture_code_ref=False)

        # All three have computations
        assert len(paths) == 3

        # Should be in dependency order
        path_strs = [str(p) for p in paths]
        assert path_strs.index(str(temp_dir / "a.txt.dvc")) < path_strs.index(
            str(temp_dir / "b.txt.dvc")
        )
        assert path_strs.index(str(temp_dir / "b.txt.dvc")) < path_strs.index(
            str(temp_dir / "c.txt.dvc")
        )


class TestMaterialize:
    """Tests for materialize function."""

    def test_materialize_simple(self, temp_dir):
        """Test materializing a simple computation."""
        output_file = temp_dir / "output.txt"

        artifact = Artifact(
            path=str(output_file),
            computation=Computation(cmd=f"echo hello > {output_file}"),
        )

        computed = materialize([artifact], force=True)

        assert len(computed) == 1
        assert output_file.exists()
        assert "hello" in output_file.read_text()
        assert artifact.md5 is not None

    def test_materialize_skips_fresh(self, temp_dir):
        """Test that materialize skips fresh artifacts."""
        from dvx.run.hash import compute_md5

        output_file = temp_dir / "output.txt"
        output_file.write_text("existing content")

        # Create artifact with matching hash
        artifact = Artifact(
            path=str(output_file),
            md5=compute_md5(output_file),
            size=output_file.stat().st_size,
            computation=Computation(cmd="echo should not run"),
        )

        # Write matching .dvc file
        artifact.write_dvc(capture_code_ref=False)

        # Materialize should skip (already fresh)
        computed = materialize([artifact], force=False)
        assert len(computed) == 0

    def test_materialize_force(self, temp_dir):
        """Test that force=True recomputes even if fresh."""
        output_file = temp_dir / "output.txt"
        output_file.write_text("old")

        artifact = Artifact(
            path=str(output_file),
            computation=Computation(cmd=f"echo new > {output_file}"),
        )

        # Write .dvc with current hash
        artifact.write_dvc(capture_code_ref=False)

        # Force should recompute
        computed = materialize([artifact], force=True)
        assert len(computed) == 1
        assert "new" in output_file.read_text()

    def test_materialize_chain(self, temp_dir):
        """Test materializing a chain of dependent computations."""
        a_file = temp_dir / "a.txt"
        b_file = temp_dir / "b.txt"

        a = Artifact(
            path=str(a_file),
            computation=Computation(cmd=f"echo a > {a_file}"),
        )
        b = Artifact(
            path=str(b_file),
            computation=Computation(
                cmd=f"cat {a_file} > {b_file} && echo b >> {b_file}",
                deps=[a],
            ),
        )

        computed = materialize([b], force=True)

        assert len(computed) == 2
        assert a_file.exists()
        assert b_file.exists()
        content = b_file.read_text()
        assert "a" in content
        assert "b" in content

    def test_materialize_failure(self, temp_dir):
        """Test that failed computation raises error."""
        artifact = Artifact(
            path=str(temp_dir / "output.txt"),
            computation=Computation(cmd="exit 1"),
        )

        with pytest.raises(RuntimeError, match="Computation failed"):
            materialize([artifact], force=True)
