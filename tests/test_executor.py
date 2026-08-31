"""Tests for parallel executor, including multi-output deduplication."""

import subprocess
import time
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


def test_cross_level_co_output_does_not_deadlock(tmp_workdir):
    """Co-outputs of one cmd that end up in different levels must not deadlock.

    When artifact B declares artifact A as a dep AND they share the same cmd,
    ``_group_into_levels`` places A in Level 1 and B in Level 2 despite
    them being co-outputs. Before the fix, Level 1's primary called
    ``_wait_for_co_outputs`` and blocked on B's ``_dvc_done_event`` — which
    could never fire because B's ``_execute_artifact`` doesn't run until
    Level 2 is submitted. Deadlock: primary parked on B's event, main
    thread parked in ``as_completed`` waiting for the primary. Repro
    matched hccs/path's ``path-data months`` outputs (see 2026-06-19+
    daily-update runs that hung at 6h GHA timeout).
    """
    script = tmp_workdir / "multi.sh"
    script.write_text(
        "#!/bin/bash\n"
        "echo l1 > a.txt\n"
        "echo l2 > b.txt\n"
    )
    script.chmod(0o755)
    cmd = f"bash {script}"

    # a.txt: no deps → Level 1. b.txt: dep on a.txt → Level 2. Same cmd.
    a = Artifact(path="a.txt", computation=Computation(cmd=cmd, deps=[]))
    b = Artifact(
        path="b.txt",
        computation=Computation(cmd=cmd, deps=[a]),
    )

    # Run in a thread so a deadlock surfaces as a timeout, not a hung suite.
    import threading
    executor = ParallelExecutor([a, b], ExecutionConfig(max_workers=2), StringIO())
    results: list = []
    errors: list = []

    def _run():
        try:
            results.extend(executor.execute())
        except Exception as e:  # pragma: no cover - surfaced via assertion
            errors.append(e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=15)
    assert not t.is_alive(), (
        "executor deadlocked — primary waited on a cross-level co-output "
        "whose _dvc_done_event was never set"
    )
    assert not errors, f"executor raised: {errors[0]!r}"
    assert len(results) == 2
    assert all(r.success for r in results), [r.reason for r in results]


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


def test_run_caches_output_blob(tmp_path):
    """dvx run copies output blobs to local cache so historical versions persist."""
    import os

    os.chdir(tmp_path)

    # Initialize a DVC repo (need .dvc dir for cache_blob)
    subprocess.run(["dvc", "init", "--no-scm"], cwd=tmp_path, capture_output=True, check=True)

    output = tmp_path / "result.txt"
    artifact = Artifact(
        path=str(output),
        computation=Computation(cmd=f"echo 'hello world' > {output}"),
    )

    config = ExecutionConfig(max_workers=1)
    output_log = StringIO()
    executor = ParallelExecutor([artifact], config, output_log)
    results = executor.execute()

    assert all(r.success for r in results)
    assert output.exists()

    # Verify the output blob was added to cache
    from dvx.run.hash import compute_md5
    md5 = compute_md5(output)
    cache_path = tmp_path / ".dvc" / "cache" / "files" / "md5" / md5[:2] / md5[2:]
    assert cache_path.exists(), f"Output blob should be cached at {cache_path}"
    assert cache_path.read_text() == output.read_text()


def test_run_prunes_at_fresh_artifact(tmp_workdir):
    """`dvx run` shouldn't walk past a fresh target into an unrelated upstream chain.

    Reproduces the spec scenario: A.txt -> A.pqt -> B.parquet -> C.dvc (side-effect).
    With every link's md5s already aligned, running C.dvc should skip everything
    cleanly even when A.txt has been deleted from disk.
    """
    from dvx.run.dvc_files import write_dvc_file
    from dvx.run.hash import compute_md5

    a_txt = tmp_workdir / "a.txt"
    a_txt.write_text("raw\n")
    a_md5 = compute_md5(a_txt)

    a_pqt = tmp_workdir / "a.pqt"
    a_pqt.write_text("processed-a\n")
    a_pqt_md5 = compute_md5(a_pqt)
    write_dvc_file(
        output_path=a_pqt,
        md5=a_pqt_md5,
        size=a_pqt.stat().st_size,
        cmd="false",  # would fail if executed
        deps={str(a_txt): a_md5},
    )

    b_pq = tmp_workdir / "b.parquet"
    b_pq.write_text("combined\n")
    b_md5 = compute_md5(b_pq)
    write_dvc_file(
        output_path=b_pq,
        md5=b_md5,
        size=b_pq.stat().st_size,
        cmd="false",
        deps={str(a_pqt): a_pqt_md5},
    )

    # Side-effect stage C with only B as a dep — no outs hash
    c_path = tmp_workdir / "sync"
    write_dvc_file(
        output_path=c_path,
        cmd="false",
        deps={str(b_pq): b_md5},
        side_effect=True,
    )

    # Now wipe the raw input — would normally cause A.pqt's freshness check
    # to fail (and its cmd to be (re-)run), even though C is fresh.
    a_txt.unlink()

    config = ExecutionConfig(max_workers=1, dry_run=True)
    output = StringIO()
    results = run([Path(str(c_path) + ".dvc")], config=config, output=output)

    paths = {r.path for r in results}
    assert paths == {str(c_path)}, f"expected only {c_path} in plan, got {paths}"
    assert all(r.success for r in results)
    assert all(r.skipped for r in results)


def test_run_no_prune_fresh_walks_full_chain(tmp_workdir):
    """--no-prune-fresh restores the old behavior of walking the full upstream chain."""
    from dvx.run.dvc_files import write_dvc_file
    from dvx.run.hash import compute_md5

    a_txt = tmp_workdir / "a.txt"
    a_txt.write_text("raw\n")
    a_md5 = compute_md5(a_txt)

    a_pqt = tmp_workdir / "a.pqt"
    a_pqt.write_text("processed-a\n")
    a_pqt_md5 = compute_md5(a_pqt)
    write_dvc_file(
        output_path=a_pqt,
        md5=a_pqt_md5,
        size=a_pqt.stat().st_size,
        cmd="true",
        deps={str(a_txt): a_md5},
    )

    b_pq = tmp_workdir / "b.parquet"
    b_pq.write_text("combined\n")
    b_md5 = compute_md5(b_pq)
    write_dvc_file(
        output_path=b_pq,
        md5=b_md5,
        size=b_pq.stat().st_size,
        cmd="true",
        deps={str(a_pqt): a_pqt_md5},
    )

    config = ExecutionConfig(max_workers=1, dry_run=True, prune_fresh=False)
    output = StringIO()
    results = run([Path(str(b_pq) + ".dvc")], config=config, output=output)

    paths = {r.path for r in results}
    assert paths == {str(a_pqt), str(b_pq)}


def test_run_cache_idempotent(tmp_path):
    """Caching is idempotent — re-running doesn't error if blob already cached."""
    import os

    os.chdir(tmp_path)
    subprocess.run(["dvc", "init", "--no-scm"], cwd=tmp_path, capture_output=True, check=True)

    output = tmp_path / "data.txt"
    artifact = Artifact(
        path=str(output),
        computation=Computation(cmd=f"echo 'data' > {output}"),
    )

    # Run twice
    for _ in range(2):
        config = ExecutionConfig(max_workers=1, force=True)
        executor = ParallelExecutor([artifact], config, StringIO())
        results = executor.execute()
        assert all(r.success for r in results)

    # Blob should still be cached
    from dvx.run.hash import compute_md5
    md5 = compute_md5(output)
    cache_path = tmp_path / ".dvc" / "cache" / "files" / "md5" / md5[:2] / md5[2:]
    assert cache_path.exists()


def test_fresh_consumer_does_not_shadow_an_explicit_target(tmp_workdir):
    """A pruned-fresh consumer must not erase its dep from the plan.

    ``prune_fresh`` registers a fresh artifact's deps as bare leaves ("already
    satisfied, don't walk further"). That's right only while nothing else needs
    the dep as a real node. When the dep is *itself* an explicit target and the
    consumer is popped first, the placeholder used to win: the later target hit
    ``output_str in artifacts`` and returned early, so the dep lost its
    computation, vanished from the plan, and — being a leaf — imposed no
    ordering, leaving the consumer in Level 1 alongside nothing.

    Silent in every direction: no warning, and invisible on a machine where the
    dep's file already exists. It surfaced as nj-crashes' "co-output dep edges
    are dropped" (``specs/done/co-output-dep-edge-loss.md``), but co-outputs are
    incidental — the trigger is target ordering, and ``cm.pqt`` simply sorts
    ahead of the ``crashes.parquet`` it consumes.
    """
    from dvx.run.dvc_files import write_dvc_file
    from dvx.run.hash import compute_md5

    a_pqt = tmp_workdir / "a.pqt"
    a_pqt.write_text("a\n")
    a_md5 = compute_md5(a_pqt)
    write_dvc_file(output_path=a_pqt, md5=a_md5, size=a_pqt.stat().st_size, cmd="true")

    b_pqt = tmp_workdir / "b.pqt"
    b_pqt.write_text("b\n")
    write_dvc_file(
        output_path=b_pqt,
        md5=compute_md5(b_pqt),
        size=b_pqt.stat().st_size,
        cmd="true",
        deps={str(a_pqt): a_md5},
    )

    # Consumer first — the order that triggers it.
    config = ExecutionConfig(max_workers=1, dry_run=True)
    output = StringIO()
    results = run(
        [Path(f"{b_pqt}.dvc"), Path(f"{a_pqt}.dvc")], config=config, output=output,
    )
    assert sorted(r.path for r in results) == [str(a_pqt), str(b_pqt)]

    # ...and the dep is ordered ahead of its consumer, not merely present.
    plan = [ln for ln in output.getvalue().split("\n") if ln.startswith("Execution plan")]
    assert plan == ["Execution plan: 2 levels, 2 computations"]


def test_warns_when_a_dep_resolves_to_nothing(tmp_workdir):
    """A bare cross-directory dep misresolves under the .dvc's own dir; say so.

    ``njdot/data/a.pqt`` declared in ``www/b.db.dvc`` resolves to
    ``www/njdot/data/a.pqt`` — nothing produces it, the edge is dropped, and
    the stage runs in Level 1 against a file that doesn't exist. The `/`-form
    is the fix; this warning is what makes the mistake visible at plan time
    instead of at runtime on a fresh checkout.
    """
    from dvx.run.dvc_files import write_dvc_file
    from dvx.run.hash import compute_md5

    (tmp_workdir / "njdot" / "data").mkdir(parents=True)
    (tmp_workdir / "www").mkdir()

    # Relative paths: `_resolve_dep_paths` is a no-op for an absolute .dvc dir.
    a_pqt = Path("njdot/data/a.pqt")
    a_pqt.write_text("a\n")
    a_md5 = compute_md5(a_pqt)
    write_dvc_file(output_path=a_pqt, md5=a_md5, size=a_pqt.stat().st_size, cmd="true")

    # Hand-authored: `write_dvc_file` always emits the `/`-form for a dep
    # outside the .dvc's dir, so the broken shape only arises from hand-edits
    # — which is exactly where nj-crashes' six came from.
    b_db = Path("www/b.db")
    Path("www/b.db.dvc").write_text(yaml.dump({
        "meta": {"computation": {
            "cmd": "true",
            "deps": {"njdot/data/a.pqt": a_md5},   # bare: misresolves under www/
            "side_effect": True,
        }},
    }))

    config = ExecutionConfig(max_workers=1, dry_run=True)
    output = StringIO()
    run([Path(f"{b_db}.dvc")], config=config, output=output)
    warnings = [ln for ln in output.getvalue().split("\n") if ln.startswith("⚠")]
    assert warnings == [
        "⚠ www/b.db: dep 'www/njdot/data/a.pqt' matches no .dvc file and "
        "no path on disk — no ordering edge"
    ]

    # Same file, one character different: the `/`-prefixed form binds.
    Path("www/b.db.dvc").write_text(yaml.dump({
        "meta": {"computation": {
            "cmd": "true",
            "deps": {"/njdot/data/a.pqt": a_md5},
            "side_effect": True,
        }},
    }))
    output = StringIO()
    run([Path(f"{b_db}.dvc")], config=config, output=output)
    assert [ln for ln in output.getvalue().split("\n") if ln.startswith("⚠")] == []


def test_side_effect_flag_survives_the_stages_own_run(tmp_workdir):
    """A driver stage's ``side_effect: true`` must survive dvx rerunning it.

    End-to-end guard for ``specs/co-output-side-effect-flag-durability.md``:
    the executor rewrites the driver ``.dvc`` after its cmd runs to refresh
    dep hashes. If that rewrite dropped the flag, the *next* run would treat
    the outs-less driver as a failed co-output
    (``✗ ...: co-output not produced``) and halt the level. The unit-level
    guard is ``test_rewrite_preserves_side_effect_flag``; this exercises the
    real executor write path end to end.
    """
    from dvx.run.dvc_files import read_dvc_file, write_dvc_file
    from dvx.run.hash import compute_md5

    dep = tmp_workdir / "input.txt"
    dep.write_text("v1\n")

    driver = tmp_workdir / "harmonize"
    write_dvc_file(
        output_path=driver,
        cmd="true",
        deps={str(dep): compute_md5(dep)},
        side_effect=True,
    )
    assert read_dvc_file(driver).side_effect is True

    # Change the dep so the stage is stale and actually reruns (and thus
    # rewrites its .dvc with the refreshed dep hash).
    dep.write_text("v2\n")

    results = run(
        [Path(str(driver) + ".dvc")],
        config=ExecutionConfig(max_workers=1, cache_push=False, commit="never"),
        output=StringIO(),
    )
    assert [(r.path, r.success, r.reason) for r in results] == [
        (str(driver), True, "completed")
    ]

    # The flag survived the rewrite, and the dep hash was refreshed.
    info = read_dvc_file(driver)
    assert info.side_effect is True
    assert info.deps == {str(dep): compute_md5(dep)}


def test_should_run_rechecks_freshness_when_concurrent_worker_satisfies_deps(
    tmp_workdir,
):
    """A dep-missing stage whose deps get satisfied by a concurrent worker
    must NOT rerun — the pre-pass re-checks freshness before bailing.

    Regression (specs/done/parallel-dep-materialization-recheck.md): several
    sibling stages sharing one tracked dep dir race in `_should_run`. Worker
    A's freshness check reports ``dep missing: data/inA.txt``; before A calls
    `_missing_dep_pull_targets`, worker B materializes the shared ``data/``
    dir, satisfying every dep on disk. A's `_missing_dep_pull_targets` then
    finds nothing to pull (all deps present) and returns ``[]`` — which used
    to break straight to a rerun, executing a cmd on a stage that was already
    fully materialized and violating the zero-cmd fresh-clone invariant.
    """
    artifact = Artifact(
        path="out0.txt",
        computation=Computation(cmd="cat data/in0.txt > out0.txt", deps=["data/in0.txt"]),
    )
    executor = ParallelExecutor([artifact], ExecutionConfig(max_workers=8), StringIO())

    # Scripted freshness: the first check (top of `_should_run`) sees the dep
    # absent; by the time the empty-targets branch re-checks, a concurrent
    # worker has materialized the shared dir, so we're fresh.
    freshness_returns = iter([
        (False, "dep missing: data/in0.txt"),
        (True, "up-to-date"),
    ])
    import dvx.run.executor as executor_mod

    orig_is_fresh = executor_mod.is_output_fresh

    def _scripted_is_output_fresh(path, *args, **kwargs):
        try:
            return next(freshness_returns)
        except StopIteration:  # pragma: no cover - guards over-calling
            return orig_is_fresh(path, *args, **kwargs)

    executor_mod.is_output_fresh = _scripted_is_output_fresh
    # A sibling already materialized every dep, so there is nothing to pull.
    executor._missing_dep_pull_targets = lambda path: []
    try:
        should_run, reason = executor._should_run(artifact)
    finally:
        executor_mod.is_output_fresh = orig_is_fresh

    assert (should_run, reason) == (False, "fetched (up-to-date)")


def test_should_run_reruns_when_deps_genuinely_unpullable(tmp_workdir):
    """The empty-targets recheck only spares a stage that is *actually* fresh.

    A stage whose dep is a raw, uncached file that is genuinely absent has no
    pull targets and stays stale on recheck — it must still fall through to a
    rerun (negative control for the concurrent-materialization recheck).
    """
    artifact = Artifact(
        path="out0.txt",
        computation=Computation(cmd="cat missing.txt > out0.txt", deps=["missing.txt"]),
    )
    executor = ParallelExecutor([artifact], ExecutionConfig(max_workers=8), StringIO())

    freshness_returns = iter([
        (False, "dep missing: missing.txt"),
        (False, "dep missing: missing.txt"),
    ])
    import dvx.run.executor as executor_mod

    orig_is_fresh = executor_mod.is_output_fresh

    def _scripted_is_output_fresh(path, *args, **kwargs):
        try:
            return next(freshness_returns)
        except StopIteration:  # pragma: no cover - guards over-calling
            return orig_is_fresh(path, *args, **kwargs)

    executor_mod.is_output_fresh = _scripted_is_output_fresh
    executor._missing_dep_pull_targets = lambda path: []
    try:
        should_run, reason = executor._should_run(artifact)
    finally:
        executor_mod.is_output_fresh = orig_is_fresh

    assert (should_run, reason) == (True, "dep missing: missing.txt")


def test_git_critical_section_is_serialized_under_parallelism(tmp_workdir, monkeypatch):
    """The per-stage `git add -u` → commit → push triple must be mutually
    exclusive across worker threads, or concurrent stages race the index and
    the shared branch (dropping the losers' commits). Drives
    ``_handle_stage_output`` from N threads with a widened git-subprocess
    window and asserts no two threads are ever inside it at once.
    """
    import threading

    import dvx.run.executor as executor_mod

    artifact = Artifact(
        path="out.txt",
        computation=Computation(cmd="echo hi", deps=[]),
    )
    config = ExecutionConfig(commit="always", push="each", max_workers=8)
    executor = ParallelExecutor([artifact], config, StringIO())
    # The cache-blob push is deliberately outside the lock; stub it out so the
    # test isolates the git critical section.
    monkeypatch.setattr(executor, "_push_cache_blobs", lambda *a, **k: None)

    active = 0
    max_active = 0
    tracker_lock = threading.Lock()

    def fake_run(cmd, *args, **kwargs):
        nonlocal active, max_active
        if cmd[:1] == ["git"]:
            with tracker_lock:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.01)  # widen the window a real race would exploit
            finally:
                with tracker_lock:
                    active -= 1
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(executor_mod.subprocess, "run", fake_run)

    def drive(i: int) -> None:
        commit_msg_path = tmp_workdir / f"msg-{i}.txt"
        commit_msg_path.write_text(f"commit {i}\n")
        summary_path = tmp_workdir / f"summary-{i}.txt"
        summary_path.write_text("")
        executor._handle_stage_output(
            path=f"out-{i}.txt",
            commit_msg_path=str(commit_msg_path),
            summary_path=str(summary_path),
        )

    threads = [threading.Thread(target=drive, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert max_active == 1
