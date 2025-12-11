"""Tests for dvx repro - reproduce artifacts from .dvc files."""

import tempfile
from pathlib import Path

import pytest

from dvx.run.artifact import Artifact, Computation
from dvx.run.dvc_files import write_dvc_file
from dvx.run.repro import (
    ReproConfig,
    build_dag_from_dvc_files,
    matches_patterns,
    repro,
    repro_artifact,
    should_cache,
    should_force,
    status,
    topological_sort,
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestMatchesPatterns:
    """Tests for pattern matching."""

    def test_exact_match(self):
        """Test exact pattern match."""
        assert matches_patterns("output.txt", ["output.txt"])
        assert not matches_patterns("output.txt", ["other.txt"])

    def test_glob_star(self):
        """Test glob star pattern."""
        assert matches_patterns("output.txt", ["*.txt"])
        assert not matches_patterns("output.txt", ["*.csv"])

    def test_glob_double_star(self):
        """Test double star pattern."""
        assert matches_patterns("path/to/output.txt", ["*/to/*"])
        assert matches_patterns("s3/ctbk/normalized/202501.parquet", ["*/normalized/*"])

    def test_multiple_patterns(self):
        """Test multiple patterns."""
        assert matches_patterns("a.txt", ["*.csv", "*.txt"])
        assert not matches_patterns("a.txt", ["*.csv", "*.json"])

    def test_empty_patterns(self):
        """Test empty pattern list."""
        assert not matches_patterns("output.txt", [])


class TestShouldForceCached:
    """Tests for force/cache pattern matching."""

    def test_should_force_global(self):
        """Test global force flag."""
        config = ReproConfig(force=True)
        assert should_force("any/path.txt", config)

    def test_should_force_pattern(self):
        """Test force with pattern."""
        config = ReproConfig(force_upstream=["*/normalized/*"])
        assert should_force("s3/ctbk/normalized/202501.parquet", config)
        assert not should_force("s3/ctbk/raw/data.csv", config)

    def test_should_cache_pattern(self):
        """Test cache with pattern."""
        config = ReproConfig(cached=["*/raw/*"])
        assert should_cache("s3/ctbk/raw/data.csv", config)
        assert not should_cache("s3/ctbk/normalized/202501.parquet", config)


class TestBuildDagFromDvcFiles:
    """Tests for building DAG from .dvc files."""

    def test_single_artifact(self, temp_dir):
        """Test building DAG from single .dvc file."""
        # Create output and .dvc file
        output = temp_dir / "output.txt"
        output.write_text("hello")

        write_dvc_file(
            output_path=output,
            md5="abc123",
            size=5,
            cmd="echo hello > output.txt",
        )

        dag = build_dag_from_dvc_files([temp_dir / "output.txt.dvc"])

        assert len(dag) == 1
        assert str(output) in dag
        assert dag[str(output)].computation is not None
        assert dag[str(output)].computation.cmd == "echo hello > output.txt"

    def test_with_deps(self, temp_dir):
        """Test building DAG with dependencies."""
        # Create input
        input_file = temp_dir / "input.txt"
        input_file.write_text("input data")
        write_dvc_file(
            output_path=input_file,
            md5="input_hash",
            size=10,
        )

        # Create output with dep
        output = temp_dir / "output.txt"
        output.write_text("output data")
        write_dvc_file(
            output_path=output,
            md5="output_hash",
            size=11,
            cmd="process input.txt",
            deps={str(input_file): "input_hash"},
        )

        dag = build_dag_from_dvc_files([temp_dir / "output.txt.dvc"])

        # Should include both output and input
        assert len(dag) == 2
        assert str(output) in dag
        assert str(input_file) in dag

    def test_handles_missing_dep_dvc(self, temp_dir):
        """Test handling deps without .dvc files (external inputs)."""
        # Create output referencing non-existent .dvc dep
        output = temp_dir / "output.txt"
        output.write_text("output data")
        write_dvc_file(
            output_path=output,
            md5="output_hash",
            size=11,
            cmd="process external.csv",
            deps={"external.csv": "external_hash"},
        )

        dag = build_dag_from_dvc_files([temp_dir / "output.txt.dvc"])

        # Should only have output (external dep has no .dvc)
        assert len(dag) == 1
        assert str(output) in dag


class TestTopologicalSort:
    """Tests for topological sorting."""

    def test_single_artifact(self, temp_dir):
        """Test sorting single artifact."""
        artifact = Artifact(path="output.txt")
        artifacts = {"output.txt": artifact}

        sorted_list = topological_sort(artifacts)

        assert len(sorted_list) == 1
        assert sorted_list[0] == artifact

    def test_chain(self, temp_dir):
        """Test sorting chain: a -> b -> c."""
        a = Artifact(path="a.txt")
        b = Artifact(
            path="b.txt",
            computation=Computation(cmd="make b", deps=[a]),
        )
        c = Artifact(
            path="c.txt",
            computation=Computation(cmd="make c", deps=[b]),
        )

        artifacts = {"a.txt": a, "b.txt": b, "c.txt": c}
        sorted_list = topological_sort(artifacts)

        # Should be in order: a, b, c
        assert sorted_list.index(a) < sorted_list.index(b)
        assert sorted_list.index(b) < sorted_list.index(c)

    def test_diamond(self, temp_dir):
        """Test sorting diamond: a -> b, c -> d."""
        a = Artifact(path="a.txt")
        b = Artifact(
            path="b.txt",
            computation=Computation(cmd="make b", deps=[a]),
        )
        c = Artifact(
            path="c.txt",
            computation=Computation(cmd="make c", deps=[a]),
        )
        d = Artifact(
            path="d.txt",
            computation=Computation(cmd="make d", deps=[b, c]),
        )

        artifacts = {"a.txt": a, "b.txt": b, "c.txt": c, "d.txt": d}
        sorted_list = topological_sort(artifacts)

        # a should come before b, c, d
        assert sorted_list.index(a) < sorted_list.index(b)
        assert sorted_list.index(a) < sorted_list.index(c)
        assert sorted_list.index(a) < sorted_list.index(d)
        # b, c should come before d
        assert sorted_list.index(b) < sorted_list.index(d)
        assert sorted_list.index(c) < sorted_list.index(d)


class TestReproArtifact:
    """Tests for reproducing single artifact."""

    def test_leaf_node_skipped(self, temp_dir):
        """Test that leaf nodes are skipped."""
        artifact = Artifact(path=str(temp_dir / "leaf.txt"))
        config = ReproConfig()

        result = repro_artifact(artifact, config)

        assert result.success
        assert result.skipped
        assert "leaf node" in result.reason

    def test_cached_pattern_skipped(self, temp_dir):
        """Test that cached pattern artifacts are skipped."""
        artifact = Artifact(
            path="s3/ctbk/raw/data.csv",
            computation=Computation(cmd="download"),
        )
        config = ReproConfig(cached=["*/raw/*"])

        result = repro_artifact(artifact, config)

        assert result.success
        assert result.skipped
        assert "cached by pattern" in result.reason

    def test_fresh_artifact_skipped(self, temp_dir):
        """Test that fresh artifacts are skipped."""
        from dvx.run.hash import compute_md5

        output = temp_dir / "output.txt"
        output.write_text("hello world")
        md5 = compute_md5(output)

        # Write .dvc with matching hash
        write_dvc_file(output, md5=md5, size=11, cmd="echo hello world")

        artifact = Artifact(
            path=str(output),
            md5=md5,
            computation=Computation(cmd="echo hello world"),
        )
        config = ReproConfig()

        result = repro_artifact(artifact, config)

        assert result.success
        assert result.skipped
        assert "up-to-date" in result.reason

    def test_dry_run(self, temp_dir):
        """Test dry run mode."""
        artifact = Artifact(
            path=str(temp_dir / "output.txt"),
            computation=Computation(cmd="echo hello"),
        )
        config = ReproConfig(dry_run=True)

        result = repro_artifact(artifact, config)

        assert result.success
        assert not result.skipped
        assert "would run" in result.reason

    def test_execute_success(self, temp_dir):
        """Test successful execution."""
        output = temp_dir / "output.txt"
        artifact = Artifact(
            path=str(output),
            computation=Computation(cmd=f"echo hello > {output}"),
        )
        config = ReproConfig()

        result = repro_artifact(artifact, config)

        assert result.success
        assert not result.skipped
        assert output.exists()
        assert "hello" in output.read_text()

    def test_execute_failure(self, temp_dir):
        """Test failed execution."""
        artifact = Artifact(
            path=str(temp_dir / "output.txt"),
            computation=Computation(cmd="exit 1"),
        )
        config = ReproConfig()

        result = repro_artifact(artifact, config)

        assert not result.success
        assert "command failed" in result.reason


class TestRepro:
    """Tests for full repro workflow."""

    def test_single_target(self, temp_dir):
        """Test reproducing single target."""
        output = temp_dir / "output.txt"
        write_dvc_file(output, md5="", size=0, cmd=f"echo hello > {output}")

        results = repro([temp_dir / "output.txt.dvc"], ReproConfig())

        assert len(results) == 1
        assert results[0].success
        assert output.exists()

    def test_chain_execution_order(self, temp_dir):
        """Test that chain is executed in order."""
        a = temp_dir / "a.txt"
        b = temp_dir / "b.txt"

        # Create .dvc files
        write_dvc_file(a, md5="", size=0, cmd=f"echo a > {a}")
        write_dvc_file(
            b,
            md5="",
            size=0,
            cmd=f"cat {a} > {b} && echo b >> {b}",
            deps={str(a): ""},
        )

        results = repro([temp_dir / "b.txt.dvc"], ReproConfig())

        # Both should succeed
        assert len(results) == 2
        assert all(r.success for r in results)

        # a should be created before b
        assert a.exists()
        assert b.exists()
        content = b.read_text()
        assert "a" in content
        assert "b" in content

    def test_stops_on_failure(self, temp_dir):
        """Test that repro stops on first failure."""
        a = temp_dir / "a.txt"
        b = temp_dir / "b.txt"

        # a fails
        write_dvc_file(a, md5="", size=0, cmd="exit 1")
        write_dvc_file(b, md5="", size=0, cmd=f"echo b > {b}", deps={str(a): ""})

        results = repro([temp_dir / "b.txt.dvc"], ReproConfig())

        # Should have at least one failure
        assert any(not r.success for r in results)
        # b should not be created (stopped after a failed)
        assert not b.exists()


class TestStatus:
    """Tests for status checking."""

    def test_fresh_artifact(self, temp_dir):
        """Test status of fresh artifact."""
        from dvx.run.hash import compute_md5

        output = temp_dir / "output.txt"
        output.write_text("hello")
        md5 = compute_md5(output)

        write_dvc_file(output, md5=md5, size=5, cmd="echo hello")

        result = status([temp_dir / "output.txt.dvc"])

        assert str(output) in result
        fresh, reason = result[str(output)]
        assert fresh
        assert "up-to-date" in reason

    def test_stale_artifact(self, temp_dir):
        """Test status of stale artifact."""
        output = temp_dir / "output.txt"
        output.write_text("hello")

        write_dvc_file(output, md5="wrong_hash", size=5, cmd="echo hello")

        result = status([temp_dir / "output.txt.dvc"])

        assert str(output) in result
        fresh, reason = result[str(output)]
        assert not fresh
        assert "hash mismatch" in reason

    def test_leaf_artifact(self, temp_dir):
        """Test status of leaf artifact (no computation)."""
        output = temp_dir / "output.txt"
        output.write_text("hello")

        write_dvc_file(output, md5="abc", size=5)

        result = status([temp_dir / "output.txt.dvc"])

        assert str(output) in result
        fresh, reason = result[str(output)]
        assert fresh
        assert "leaf node" in reason
