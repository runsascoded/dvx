"""Tests for parallel executor, including multi-output deduplication."""

import subprocess
from io import StringIO
from pathlib import Path

import pytest
import yaml

from dvx.run.artifact import Artifact, Computation
from dvx.run.executor import ExecutionConfig, ParallelExecutor


@pytest.fixture
def tmp_workdir(tmp_path, monkeypatch):
    """Change to temporary directory for tests."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_multi_output_deduplication(tmp_workdir):
    """Test that multiple outputs with same cmd only run the command once."""
    # Create a script that writes to two files and increments a counter
    counter_file = tmp_workdir / "counter.txt"
    counter_file.write_text("0")

    script = tmp_workdir / "multi_output.sh"
    script.write_text("""#!/bin/bash
count=$(cat counter.txt)
echo $((count + 1)) > counter.txt
echo "output1" > output1.txt
echo "output2" > output2.txt
""")
    script.chmod(0o755)

    cmd = f"bash {script}"

    # Create two artifacts with the same command
    artifact1 = Artifact(
        path=str(tmp_workdir / "output1.txt"),
        computation=Computation(cmd=cmd, deps=[]),
    )
    artifact2 = Artifact(
        path=str(tmp_workdir / "output2.txt"),
        computation=Computation(cmd=cmd, deps=[]),
    )

    # Execute both
    output = StringIO()
    config = ExecutionConfig(max_workers=2)
    executor = ParallelExecutor([artifact1, artifact2], config, output)
    results = executor.execute()

    # Check results
    assert len(results) == 2
    assert all(r.success for r in results)

    # Key assertion: command should have run only once
    assert counter_file.read_text().strip() == "1"

    # Both outputs should exist
    assert (tmp_workdir / "output1.txt").read_text().strip() == "output1"
    assert (tmp_workdir / "output2.txt").read_text().strip() == "output2"

    # Check logs show deduplication
    log_output = output.getvalue()
    assert "running..." in log_output
    assert "co-output ready" in log_output or "waiting" in log_output


def test_multi_output_different_deps(tmp_workdir):
    """Test that co-outputs can have different dependencies."""
    # Create input files
    input1 = tmp_workdir / "input1.txt"
    input2 = tmp_workdir / "input2.txt"
    input1.write_text("data1")
    input2.write_text("data2")

    script = tmp_workdir / "multi_output.sh"
    script.write_text("""#!/bin/bash
echo "output1" > output1.txt
echo "output2" > output2.txt
""")
    script.chmod(0o755)

    cmd = f"bash {script}"

    # Create leaf artifacts for inputs (no computation)
    leaf1 = Artifact(path=str(input1))
    leaf2 = Artifact(path=str(input2))

    # Create artifacts with same cmd but different deps
    artifact1 = Artifact(
        path=str(tmp_workdir / "output1.txt"),
        computation=Computation(
            cmd=cmd,
            deps=[leaf1],
        ),
    )
    artifact2 = Artifact(
        path=str(tmp_workdir / "output2.txt"),
        computation=Computation(
            cmd=cmd,
            deps=[leaf2],
        ),
    )

    # Execute with all artifacts including leaves
    output = StringIO()
    config = ExecutionConfig(provenance=True)
    executor = ParallelExecutor([leaf1, leaf2, artifact1, artifact2], config, output)
    results = executor.execute()

    # Filter to only computed results
    computed_results = [r for r in results if r.path.endswith(".txt") and "output" in r.path]
    assert all(r.success for r in computed_results)

    # Check that .dvc files have different deps
    dvc1 = yaml.safe_load((tmp_workdir / "output1.txt.dvc").read_text())
    dvc2 = yaml.safe_load((tmp_workdir / "output2.txt.dvc").read_text())

    deps1 = dvc1["meta"]["computation"]["deps"]
    deps2 = dvc2["meta"]["computation"]["deps"]

    # Each should have its own dep
    assert "input1.txt" in str(deps1)
    assert "input2.txt" in str(deps2)


def test_multi_output_partial_failure(tmp_workdir):
    """Test handling when command succeeds but doesn't produce all outputs."""
    script = tmp_workdir / "partial.sh"
    script.write_text("""#!/bin/bash
echo "output1" > output1.txt
# Intentionally not creating output2.txt
""")
    script.chmod(0o755)

    cmd = f"bash {script}"

    artifact1 = Artifact(
        path=str(tmp_workdir / "output1.txt"),
        computation=Computation(cmd=cmd, deps=[]),
    )
    artifact2 = Artifact(
        path=str(tmp_workdir / "output2.txt"),
        computation=Computation(cmd=cmd, deps=[]),
    )

    output = StringIO()
    executor = ParallelExecutor([artifact1, artifact2], ExecutionConfig(), output)
    results = executor.execute()

    # One should succeed, one should fail
    successes = [r for r in results if r.success]
    failures = [r for r in results if not r.success]

    assert len(successes) == 1
    assert len(failures) == 1
    assert "not produced" in failures[0].reason


def test_multi_output_command_failure(tmp_workdir):
    """Test handling when the shared command fails."""
    cmd = "exit 1"

    artifact1 = Artifact(
        path=str(tmp_workdir / "output1.txt"),
        computation=Computation(cmd=cmd, deps=[]),
    )
    artifact2 = Artifact(
        path=str(tmp_workdir / "output2.txt"),
        computation=Computation(cmd=cmd, deps=[]),
    )

    output = StringIO()
    config = ExecutionConfig(max_workers=2)
    executor = ParallelExecutor([artifact1, artifact2], config, output)
    results = executor.execute()

    # Both should fail
    assert all(not r.success for r in results)
    # At least one should mention "co-output" (the waiter)
    reasons = [r.reason for r in results]
    assert any("failed" in r for r in reasons)
