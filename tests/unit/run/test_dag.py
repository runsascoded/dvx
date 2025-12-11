"""Tests for DAG builder and topological sort."""

import pytest

from dvx.run.dag import DAG
from dvx.run.stage import Stage


def test_simple_dag():
    """Test building DAG from simple stage dependencies."""
    stages = [
        Stage(name="a", cmd="cmd_a", deps=[], outs=["a.txt"]),
        Stage(name="b", cmd="cmd_b", deps=["a.txt"], outs=["b.txt"]),
        Stage(name="c", cmd="cmd_c", deps=["b.txt"], outs=["c.txt"]),
    ]

    dag = DAG(stages)

    # Check dependencies
    assert dag.get_dependencies("a") == set()
    assert dag.get_dependencies("b") == {"a"}
    assert dag.get_dependencies("c") == {"b"}

    # Check dependents
    assert dag.get_dependents("a") == {"b"}
    assert dag.get_dependents("b") == {"c"}
    assert dag.get_dependents("c") == set()


def test_parallel_dag():
    """Test DAG with independent parallel stages."""
    stages = [
        Stage(name="a", cmd="cmd_a", deps=[], outs=["a.txt"]),
        Stage(name="b", cmd="cmd_b", deps=[], outs=["b.txt"]),
        Stage(name="c", cmd="cmd_c", deps=["a.txt", "b.txt"], outs=["c.txt"]),
    ]

    dag = DAG(stages)

    # a and b are independent
    assert dag.get_dependencies("a") == set()
    assert dag.get_dependencies("b") == set()

    # c depends on both
    assert dag.get_dependencies("c") == {"a", "b"}


def test_topological_sort_linear():
    """Test topological sort with linear dependencies."""
    stages = [
        Stage(name="a", cmd="cmd_a", deps=[], outs=["a.txt"]),
        Stage(name="b", cmd="cmd_b", deps=["a.txt"], outs=["b.txt"]),
        Stage(name="c", cmd="cmd_c", deps=["b.txt"], outs=["c.txt"]),
    ]

    dag = DAG(stages)
    levels = dag.topological_sort()

    assert len(levels) == 3
    assert levels[0] == ["a"]
    assert levels[1] == ["b"]
    assert levels[2] == ["c"]


def test_topological_sort_parallel():
    """Test topological sort with parallel stages."""
    stages = [
        Stage(name="a", cmd="cmd_a", deps=[], outs=["a.txt"]),
        Stage(name="b", cmd="cmd_b", deps=[], outs=["b.txt"]),
        Stage(name="c", cmd="cmd_c", deps=[], outs=["c.txt"]),
        Stage(name="d", cmd="cmd_d", deps=["a.txt", "b.txt", "c.txt"], outs=["d.txt"]),
    ]

    dag = DAG(stages)
    levels = dag.topological_sort()

    assert len(levels) == 2
    # First level should have a, b, c (order within level doesn't matter)
    assert set(levels[0]) == {"a", "b", "c"}
    # Second level should have d
    assert levels[1] == ["d"]


def test_topological_sort_diamond():
    """Test topological sort with diamond dependency pattern."""
    stages = [
        Stage(name="a", cmd="cmd_a", deps=[], outs=["a.txt"]),
        Stage(name="b", cmd="cmd_b", deps=["a.txt"], outs=["b.txt"]),
        Stage(name="c", cmd="cmd_c", deps=["a.txt"], outs=["c.txt"]),
        Stage(name="d", cmd="cmd_d", deps=["b.txt", "c.txt"], outs=["d.txt"]),
    ]

    dag = DAG(stages)
    levels = dag.topological_sort()

    assert len(levels) == 3
    assert levels[0] == ["a"]
    assert set(levels[1]) == {"b", "c"}
    assert levels[2] == ["d"]


def test_cycle_detection_simple():
    """Test detection of simple cycle."""
    stages = [
        Stage(name="a", cmd="cmd_a", deps=["b.txt"], outs=["a.txt"]),
        Stage(name="b", cmd="cmd_b", deps=["a.txt"], outs=["b.txt"]),
    ]

    dag = DAG(stages)
    cycle = dag.check_cycles()

    assert cycle is not None
    assert len(cycle) >= 2
    # Should contain both a and b
    assert "a" in cycle
    assert "b" in cycle


def test_cycle_detection_self():
    """Test detection of self-cycle."""
    stages = [
        Stage(name="a", cmd="cmd_a", deps=["a.txt"], outs=["a.txt"]),
    ]

    dag = DAG(stages)
    cycle = dag.check_cycles()

    assert cycle is not None
    assert "a" in cycle


def test_topological_sort_with_cycle():
    """Test that topological sort raises error on cycle."""
    stages = [
        Stage(name="a", cmd="cmd_a", deps=["b.txt"], outs=["a.txt"]),
        Stage(name="b", cmd="cmd_b", deps=["a.txt"], outs=["b.txt"]),
    ]

    dag = DAG(stages)

    with pytest.raises(ValueError, match="Circular dependency"):
        dag.topological_sort()


def test_independent_stages():
    """Test completely independent stages."""
    stages = [
        Stage(name="a", cmd="cmd_a", deps=[], outs=["a.txt"]),
        Stage(name="b", cmd="cmd_b", deps=[], outs=["b.txt"]),
        Stage(name="c", cmd="cmd_c", deps=[], outs=["c.txt"]),
    ]

    dag = DAG(stages)
    levels = dag.topological_sort()

    # All stages should be in first level
    assert len(levels) == 1
    assert set(levels[0]) == {"a", "b", "c"}
