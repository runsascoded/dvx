"""Tests for parallel executor with Artifacts."""

import tempfile
from pathlib import Path

import pytest

from dvx.run.artifact import Artifact, Computation
from dvx.run.dvc_files import write_dvc_file
from dvx.run.executor import (
    ExecutionConfig,
    ExecutionResult,
    ParallelExecutor,
    _group_into_levels,
    _topological_sort,
    run,
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestGroupIntoLevels:
    """Tests for level grouping."""

    def test_single_artifact(self):
        """Test single artifact."""
        a = Artifact(path="a.txt", computation=Computation(cmd="echo a"))
        levels = _group_into_levels([a])
        assert len(levels) == 1
        assert levels[0] == [a]

    def test_independent_artifacts(self):
        """Test independent artifacts go in same level."""
        a = Artifact(path="a.txt", computation=Computation(cmd="echo a"))
        b = Artifact(path="b.txt", computation=Computation(cmd="echo b"))
        levels = _group_into_levels([a, b])
        # Both should be in same level (no deps between them)
        assert len(levels) == 1
        assert set(levels[0]) == {a, b}

    def test_chain(self):
        """Test chain creates multiple levels."""
        a = Artifact(path="a.txt", computation=Computation(cmd="echo a"))
        b = Artifact(path="b.txt", computation=Computation(cmd="echo b", deps=[a]))
        c = Artifact(path="c.txt", computation=Computation(cmd="echo c", deps=[b]))

        # Sorted order should be [a, b, c]
        levels = _group_into_levels([a, b, c])

        assert len(levels) == 3
        assert levels[0] == [a]
        assert levels[1] == [b]
        assert levels[2] == [c]

    def test_diamond(self):
        """Test diamond pattern: a -> b,c -> d."""
        a = Artifact(path="a.txt", computation=Computation(cmd="echo a"))
        b = Artifact(path="b.txt", computation=Computation(cmd="echo b", deps=[a]))
        c = Artifact(path="c.txt", computation=Computation(cmd="echo c", deps=[a]))
        d = Artifact(path="d.txt", computation=Computation(cmd="echo d", deps=[b, c]))

        levels = _group_into_levels([a, b, c, d])

        # Level 1: a
        # Level 2: b, c (parallel)
        # Level 3: d
        assert len(levels) == 3
        assert levels[0] == [a]
        assert set(levels[1]) == {b, c}
        assert levels[2] == [d]

    def test_leaf_nodes(self):
        """Test leaf nodes (no computation) are in first level."""
        leaf = Artifact(path="leaf.txt")  # No computation
        computed = Artifact(
            path="out.txt",
            computation=Computation(cmd="echo", deps=[leaf]),
        )

        levels = _group_into_levels([leaf, computed])

        assert len(levels) == 2
        assert levels[0] == [leaf]
        assert levels[1] == [computed]


class TestTopologicalSort:
    """Tests for topological sort."""

    def test_chain(self):
        """Test chain is sorted correctly."""
        a = Artifact(path="a.txt")
        b = Artifact(path="b.txt", computation=Computation(cmd="", deps=[a]))
        c = Artifact(path="c.txt", computation=Computation(cmd="", deps=[b]))

        artifacts = {"a.txt": a, "b.txt": b, "c.txt": c}
        sorted_list = _topological_sort(artifacts)

        assert sorted_list.index(a) < sorted_list.index(b)
        assert sorted_list.index(b) < sorted_list.index(c)


class TestParallelExecutor:
    """Tests for ParallelExecutor."""

    def test_execute_simple(self, temp_dir):
        """Test executing single artifact."""
        output = temp_dir / "output.txt"
        artifact = Artifact(
            path=str(output),
            computation=Computation(cmd=f"echo hello > {output}"),
        )

        config = ExecutionConfig(force=True)
        executor = ParallelExecutor([artifact], config)
        results = executor.execute()

        assert len(results) == 1
        assert results[0].success
        assert not results[0].skipped
        assert output.exists()
        assert "hello" in output.read_text()

    def test_skips_fresh(self, temp_dir):
        """Test that fresh artifacts are skipped."""
        from dvx.run.hash import compute_md5

        output = temp_dir / "output.txt"
        output.write_text("existing content")
        md5 = compute_md5(output)

        # Write .dvc with matching hash
        write_dvc_file(output, md5=md5, size=output.stat().st_size)

        artifact = Artifact(
            path=str(output),
            md5=md5,
            computation=Computation(cmd="echo should not run"),
        )

        config = ExecutionConfig()
        executor = ParallelExecutor([artifact], config)
        results = executor.execute()

        assert len(results) == 1
        assert results[0].success
        assert results[0].skipped

    def test_parallel_independent(self, temp_dir):
        """Test independent artifacts run in parallel."""
        a = temp_dir / "a.txt"
        b = temp_dir / "b.txt"

        artifacts = [
            Artifact(path=str(a), computation=Computation(cmd=f"echo a > {a}")),
            Artifact(path=str(b), computation=Computation(cmd=f"echo b > {b}")),
        ]

        config = ExecutionConfig(max_workers=2, force=True)
        executor = ParallelExecutor(artifacts, config)
        results = executor.execute()

        assert len(results) == 2
        assert all(r.success for r in results)
        assert a.exists()
        assert b.exists()

    def test_chain_execution_order(self, temp_dir):
        """Test chain executes in correct order."""
        a = temp_dir / "a.txt"
        b = temp_dir / "b.txt"

        a_artifact = Artifact(
            path=str(a),
            computation=Computation(cmd=f"echo a > {a}"),
        )
        b_artifact = Artifact(
            path=str(b),
            computation=Computation(
                cmd=f"cat {a} > {b} && echo b >> {b}",
                deps=[a_artifact],
            ),
        )

        config = ExecutionConfig(force=True)
        executor = ParallelExecutor([a_artifact, b_artifact], config)
        results = executor.execute()

        assert len(results) == 2
        assert all(r.success for r in results)
        assert b.exists()
        content = b.read_text()
        assert "a" in content
        assert "b" in content

    def test_dry_run(self, temp_dir):
        """Test dry run doesn't execute."""
        output = temp_dir / "output.txt"
        artifact = Artifact(
            path=str(output),
            computation=Computation(cmd=f"echo hello > {output}"),
        )

        config = ExecutionConfig(dry_run=True)
        executor = ParallelExecutor([artifact], config)
        results = executor.execute()

        # Should return results but not execute
        assert len(results) == 1
        assert not output.exists()

    def test_failure_stops_execution(self, temp_dir):
        """Test that failure stops subsequent levels."""
        a = temp_dir / "a.txt"
        b = temp_dir / "b.txt"

        a_artifact = Artifact(
            path=str(a),
            computation=Computation(cmd="exit 1"),  # Fails
        )
        b_artifact = Artifact(
            path=str(b),
            computation=Computation(cmd=f"echo b > {b}", deps=[a_artifact]),
        )

        config = ExecutionConfig(force=True)
        executor = ParallelExecutor([a_artifact, b_artifact], config)
        results = executor.execute()

        # Should have failure for a, b shouldn't run
        assert any(not r.success for r in results)
        assert not b.exists()


class TestRun:
    """Tests for run() function."""

    def test_run_from_dvc_files(self, temp_dir):
        """Test running from .dvc files."""
        output = temp_dir / "output.txt"

        # Create .dvc file with computation
        write_dvc_file(
            output,
            md5="",
            size=0,
            cmd=f"echo hello > {output}",
        )

        config = ExecutionConfig(force=True)
        results = run([temp_dir / "output.txt.dvc"], config)

        assert len(results) == 1
        assert results[0].success
        assert output.exists()

    def test_run_with_deps(self, temp_dir):
        """Test running with dependencies."""
        a = temp_dir / "a.txt"
        b = temp_dir / "b.txt"

        # Create .dvc files
        write_dvc_file(a, md5="", size=0, cmd=f"echo a > {a}")
        write_dvc_file(
            b,
            md5="",
            size=0,
            cmd=f"cat {a} > {b}",
            deps={str(a): ""},
        )

        config = ExecutionConfig(force=True)
        results = run([temp_dir / "b.txt.dvc"], config)

        # Should include both a and b
        assert len(results) == 2
        assert all(r.success for r in results)
        assert a.exists()
        assert b.exists()
