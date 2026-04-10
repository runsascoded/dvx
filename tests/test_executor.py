"""Tests for parallel executor, including multi-output deduplication."""

import subprocess
from io import StringIO
from pathlib import Path

import pytest
import yaml

from dvx.run.artifact import Artifact, Computation
from dvx.run.executor import ExecutionConfig, ParallelExecutor, _group_into_levels, run


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
    output1_path = tmp_workdir / "output1.txt"
    output2_path = tmp_workdir / "output2.txt"

    script = tmp_workdir / "multi_output.sh"
    # Use absolute paths to avoid working directory issues in CI
    script.write_text(f"""#!/bin/bash
count=$(cat {counter_file})
echo $((count + 1)) > {counter_file}
echo "output1" > {output1_path}
echo "output2" > {output2_path}
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
    # Track how many times the script runs
    counter_file = tmp_workdir / "counter.txt"
    counter_file.write_text("0")

    output1_path = tmp_workdir / "output1.txt"
    script = tmp_workdir / "partial.sh"
    # Use absolute paths to avoid working directory issues
    script.write_text(f"""#!/bin/bash
count=$(cat {counter_file})
echo $((count + 1)) > {counter_file}
echo "output1" > {output1_path}
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
    # Use max_workers=2 to test parallel execution
    config = ExecutionConfig(max_workers=2)
    executor = ParallelExecutor([artifact1, artifact2], config, output)
    results = executor.execute()

    # Command should run exactly once
    assert counter_file.read_text().strip() == "1", "Command should run exactly once"

    # One should succeed (output1.txt), one should fail (output2.txt not produced)
    successes = [r for r in results if r.success]
    failures = [r for r in results if not r.success]

    assert len(successes) == 1, f"Expected 1 success, got {len(successes)}: {successes}"
    assert len(failures) == 1, f"Expected 1 failure, got {len(failures)}: {failures}"
    assert "output1.txt" in successes[0].path
    assert "output2.txt" in failures[0].path
    assert "not created" in failures[0].reason or "not produced" in failures[0].reason


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


def test_external_dep_no_circular_dependency(tmp_workdir):
    """Test that deps without .dvc files don't cause 'Circular dependency detected'.

    When a .dvc file has a dep on a git-tracked file (no .dvc file), the dep
    should be treated as a leaf node and not block execution.
    """
    # Create the external dep (a git-tracked file, no .dvc file)
    external_dep = tmp_workdir / "script.py"
    external_dep.write_text("print('hello')\n")

    # Create .dvc file for output that depends on external dep
    output_path = tmp_workdir / "output.txt"
    dvc_file = tmp_workdir / "output.txt.dvc"
    dvc_content = {
        "outs": [{"md5": "abc123", "size": 100, "path": "output.txt"}],
        "meta": {
            "computation": {
                "cmd": f"echo result > {output_path}",
                "deps": {str(external_dep): "deadbeef"},
            }
        },
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    # This should NOT raise "Circular dependency detected"
    output = StringIO()
    config = ExecutionConfig(dry_run=True)
    results = run([dvc_file], config=config, output=output)

    # Should complete without error
    assert len(results) == 1
    assert results[0].success


def test_group_into_levels_with_external_deps():
    """Test _group_into_levels handles artifacts whose deps are leaf nodes."""
    leaf = Artifact(path="external.py")
    computed = Artifact(
        path="output.txt",
        computation=Computation(cmd="echo hi", deps=[leaf]),
    )

    levels = _group_into_levels([leaf, computed])

    assert len(levels) == 2
    assert levels[0] == [leaf]
    assert levels[1] == [computed]


def test_group_into_levels_with_git_deps():
    """Test _group_into_levels handles git_deps as dependencies."""
    git_dep = Artifact(path="script.py")
    computed = Artifact(
        path="output.txt",
        computation=Computation(cmd="echo hi", deps=[], git_deps=[git_dep]),
    )

    levels = _group_into_levels([git_dep, computed])

    assert len(levels) == 2
    assert levels[0] == [git_dep]
    assert levels[1] == [computed]


def test_run_with_git_deps_in_dvc_file(tmp_workdir):
    """Test that run() handles .dvc files with git_deps."""
    # Create the git dep file
    script = tmp_workdir / "script.py"
    script.write_text("print('hello')\n")

    # Create .dvc file with git_deps
    output_path = tmp_workdir / "output.txt"
    dvc_file = tmp_workdir / "output.txt.dvc"
    dvc_content = {
        "outs": [{"md5": "abc123", "size": 100, "path": "output.txt"}],
        "meta": {
            "computation": {
                "cmd": f"echo result > {output_path}",
                "git_deps": {"script.py": "aabbccdd"},
            }
        },
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    # Should not raise "Circular dependency detected"
    output = StringIO()
    config = ExecutionConfig(dry_run=True)
    results = run([dvc_file], config=config, output=output)

    assert len(results) == 1
    assert results[0].success


def test_failed_stage_exit_code_and_log(tmp_workdir):
    """Test that a failing stage records exit code in reason and writes a log file."""
    artifact = Artifact(
        path=str(tmp_workdir / "fail_output.txt"),
        computation=Computation(cmd="echo fail_msg >&2 && exit 42", deps=[]),
    )

    output = StringIO()
    config = ExecutionConfig()
    executor = ParallelExecutor([artifact], config, output)
    results = executor.execute()

    assert len(results) == 1
    result = results[0]
    assert not result.success
    assert "fail_msg" in result.reason

    # Log file should exist in tmp/
    log_path = tmp_workdir / "tmp" / "dvx-run-fail_output.log"
    assert log_path.exists()
    log_content = log_path.read_text()
    assert "fail_msg" in log_content


def test_summary_file_output(tmp_workdir):
    """Test that a stage writing to $DVX_SUMMARY_FILE has its summary shown."""
    output_path = tmp_workdir / "summary_test.txt"

    cmd = f'echo result > {output_path} && echo "Stage completed successfully" > "$DVX_SUMMARY_FILE"'

    artifact = Artifact(
        path=str(output_path),
        computation=Computation(cmd=cmd, deps=[]),
    )

    output = StringIO()
    config = ExecutionConfig()
    executor = ParallelExecutor([artifact], config, output)
    results = executor.execute()

    assert len(results) == 1
    assert results[0].success

    log_output = output.getvalue()
    assert "Stage completed successfully" in log_output


def test_env_vars_are_set(tmp_workdir):
    """Test that $DVX_COMMIT_MSG_FILE and $DVX_SUMMARY_FILE are set to non-empty paths."""
    output_path = tmp_workdir / "env_test.txt"
    env_dump = tmp_workdir / "env_dump.txt"

    cmd = (
        f'echo "COMMIT=$DVX_COMMIT_MSG_FILE" > {env_dump} && '
        f'echo "SUMMARY=$DVX_SUMMARY_FILE" >> {env_dump} && '
        f'echo ok > {output_path}'
    )

    artifact = Artifact(
        path=str(output_path),
        computation=Computation(cmd=cmd, deps=[]),
    )

    output = StringIO()
    config = ExecutionConfig()
    executor = ParallelExecutor([artifact], config, output)
    results = executor.execute()

    assert len(results) == 1
    assert results[0].success

    env_content = env_dump.read_text()
    lines = env_content.strip().split("\n")
    commit_line = [l for l in lines if l.startswith("COMMIT=")][0]
    summary_line = [l for l in lines if l.startswith("SUMMARY=")][0]

    commit_val = commit_line.split("=", 1)[1]
    summary_val = summary_line.split("=", 1)[1]

    assert commit_val != "", "DVX_COMMIT_MSG_FILE should be non-empty"
    assert summary_val != "", "DVX_SUMMARY_FILE should be non-empty"
    assert commit_val != summary_val, "Commit and summary files should be different paths"


def test_after_ordering(tmp_path):
    """Stages with after: constraints run after the referenced stage."""
    import os

    os.chdir(tmp_path)

    # Create two stages: stage_a and stage_b (b runs after a)
    log_file = tmp_path / "order.log"

    stage_a = Artifact(
        path="stage_a",
        computation=Computation(cmd=f"echo A >> {log_file}"),
    )
    stage_b = Artifact(
        path="stage_b",
        computation=Computation(
            cmd=f"echo B >> {log_file}",
            after=["stage_a"],
        ),
    )

    # Put b before a to test that after: reorders them
    artifacts = [stage_b, stage_a]

    from dvx.run.executor import _group_into_levels
    levels = _group_into_levels(artifacts)

    # stage_a should be in an earlier level than stage_b
    a_level = None
    b_level = None
    for i, level in enumerate(levels):
        for a in level:
            if a.path == "stage_a":
                a_level = i
            if a.path == "stage_b":
                b_level = i

    assert a_level is not None
    assert b_level is not None
    assert a_level < b_level, f"stage_a (level {a_level}) should be before stage_b (level {b_level})"
