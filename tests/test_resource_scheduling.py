"""Tests for resource-aware level scheduling.

`dvx run` groups the DAG into levels and runs every stage in a level
concurrently. Memory was invisible to that: a level mixing a couple of heavy
pandas/sqlite builds with many light ones would OOM the heavy one on a shared
box (nj-crashes' `crashes.db`, exit 137), and the only levers were blunt —
ceiling the whole job's memory, or cap `-j` globally. Stages can now declare
`meta.computation.resources.mem_gb`, and a budget gate serializes the heavy
ones while the light ones stay parallel.

Regression of `specs/done/resource-aware-scheduling.md`.
"""

import threading
from io import StringIO
from pathlib import Path

import yaml

from dvx.run.dvc_files import read_dvc_file, write_dvc_file
from dvx.run.executor import ExecutionConfig, _BudgetGate, run
from dvx.run.hash import compute_file_size, compute_md5

# ────────────────────────────────────────────────────────────────────────────
# Declaration: parse, round-trip, author-owned durability
# ────────────────────────────────────────────────────────────────────────────

def test_resources_parse_from_dvc(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "out.txt").write_text("x\n")
    Path("out.txt.dvc").write_text(yaml.dump({
        "outs": [{"path": "out.txt", "md5": "0" * 32, "size": 2, "hash": "md5"}],
        "meta": {"computation": {
            "cmd": "true",
            "resources": {"mem_gb": 40, "cpus": 4},
        }},
    }))
    info = read_dvc_file(Path("out.txt"))
    assert info.resources == {"mem_gb": 40.0, "cpus": 4.0}


def test_unknown_and_nonnumeric_resource_keys_are_dropped(tmp_path, monkeypatch):
    """Advisory hints: a key the scheduler doesn't model, or a non-number,
    must not fail the run — it's silently ignored."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "out.txt").write_text("x\n")
    Path("out.txt.dvc").write_text(yaml.dump({
        "outs": [{"path": "out.txt", "md5": "0" * 32, "size": 2, "hash": "md5"}],
        "meta": {"computation": {
            "cmd": "true",
            "resources": {"mem_gb": 8, "gpus": "lots", "disk_tb": None},
        }},
    }))
    info = read_dvc_file(Path("out.txt"))
    assert info.resources == {"mem_gb": 8.0}


def test_resources_round_trip_and_survive_rewrite(tmp_path, monkeypatch):
    """A hand-authored `resources` block survives the stage's own rewrite —
    the executor refreshes dep hashes without passing it, and (like
    `side_effect`) it must be preserved, not stripped."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "out.txt").write_text("x\n")
    md5 = compute_md5(Path("out.txt"))
    write_dvc_file(
        output_path=Path("out.txt"),
        md5=md5,
        size=compute_file_size(Path("out.txt")),
        cmd="build",
        deps={"in.txt": "1" * 32},
        resources={"mem_gb": 40.0, "cpus": 4.0},
    )
    assert read_dvc_file(Path("out.txt")).resources == {"mem_gb": 40.0, "cpus": 4.0}

    # A dep-hash-only rewrite that doesn't pass resources.
    write_dvc_file(
        output_path=Path("out.txt"),
        md5=md5,
        size=compute_file_size(Path("out.txt")),
        cmd="build",
        deps={"in.txt": "2" * 32},
    )
    info = read_dvc_file(Path("out.txt"))
    assert info.resources == {"mem_gb": 40.0, "cpus": 4.0}
    assert info.deps == {"in.txt": "2" * 32}


# ────────────────────────────────────────────────────────────────────────────
# The gate
# ────────────────────────────────────────────────────────────────────────────

def test_gate_admits_while_under_budget():
    gate = _BudgetGate(64)
    gate.acquire(40)  # 40/64
    done = threading.Event()

    def take():
        gate.acquire(10)  # 50/64 — fits
        done.set()

    t = threading.Thread(target=take)
    t.start()
    assert done.wait(timeout=2)
    t.join()


def test_gate_blocks_when_sum_exceeds_budget_then_releases():
    gate = _BudgetGate(64)
    gate.acquire(40)  # 40/64
    entered = threading.Event()

    def take():
        gate.acquire(40)  # 80 > 64 — must block until the first releases
        entered.set()

    t = threading.Thread(target=take)
    t.start()
    # Still blocked: nothing released yet.
    assert not entered.wait(timeout=0.3)
    gate.release(40)
    assert entered.wait(timeout=2)
    t.join()


def test_gate_runs_an_over_budget_stage_alone():
    """A stage heavier than the whole budget must still run (clamped), rather
    than block forever — but it holds the gate alone while it does."""
    gate = _BudgetGate(64)
    gate.acquire(100)  # > budget: clamped to 64, admitted since gate was idle
    blocked = threading.Event()
    proceeded = threading.Event()

    def take():
        blocked.set()
        gate.acquire(1)  # nothing fits alongside a full budget
        proceeded.set()

    t = threading.Thread(target=take)
    t.start()
    assert blocked.wait(timeout=2)
    assert not proceeded.wait(timeout=0.3)  # the over-budget stage runs alone
    gate.release(100)
    assert proceeded.wait(timeout=2)
    t.join()


# ────────────────────────────────────────────────────────────────────────────
# Integration through the executor
# ────────────────────────────────────────────────────────────────────────────

def _heavy_stage(repo: Path, name: str, log: Path, mem_gb: float) -> None:
    (repo / f"{name}.txt.dvc").write_text(yaml.dump({
        "outs": [{"path": f"{name}.txt"}],
        "meta": {"computation": {
            "cmd": f"printf 'start:{name}\\n' >> {log}; sleep 0.4; "
                   f"printf 'end:{name}\\n' >> {log}; printf done > {name}.txt",
            "resources": {"mem_gb": mem_gb},
        }},
    }))


def _parse_intervals(log_text: str) -> list[tuple[str, str]]:
    """[(event, name), ...] in the order they were logged."""
    events = []
    for line in log_text.splitlines():
        kind, name = line.split(":")
        events.append((kind, name))
    return events


def test_heavy_stages_serialize_under_a_budget(tmp_path, monkeypatch):
    """Two 40 GB stages in one level, budget 64 GB: they can't run together,
    so their [start,end) intervals are disjoint — one fully precedes the other,
    never two starts before an end."""
    monkeypatch.chdir(tmp_path)
    log = tmp_path / "sched.log"
    _heavy_stage(tmp_path, "a", log, 40)
    _heavy_stage(tmp_path, "b", log, 40)

    results = run(
        [Path("a.txt.dvc"), Path("b.txt.dvc")],
        ExecutionConfig(mem_budget_gb=64, cache_push=False, commit="never"),
        output=StringIO(),
    )
    assert sorted((r.path, r.success) for r in results) == [
        ("a.txt", True), ("b.txt", True),
    ]

    events = _parse_intervals(log.read_text())
    # Disjoint ⇒ the sequence alternates start,end,start,end.
    assert [e[0] for e in events] == ["start", "end", "start", "end"]
    # And the two names are different stages, in whichever order won the race.
    assert {events[0][1], events[2][1]} == {"a", "b"}
    assert events[0][1] == events[1][1] and events[2][1] == events[3][1]


def test_budget_disabled_when_nothing_labeled(tmp_path, monkeypatch):
    """No --mem and no `mem_gb` on any stage ⇒ the gate is off entirely —
    today's fixed fan-out, no behavior change."""
    from dvx.run.artifact import Artifact, Computation
    from dvx.run.executor import ParallelExecutor

    monkeypatch.chdir(tmp_path)
    arts = [
        Artifact(path="a.txt", computation=Computation(cmd="true")),
        Artifact(path="b.txt", computation=Computation(cmd="true")),
    ]
    ex = ParallelExecutor(arts, ExecutionConfig(), StringIO())
    assert ex._budget_gate is None


def test_explicit_mem_builds_a_gate_even_when_unlabeled(tmp_path, monkeypatch):
    from dvx.run.artifact import Artifact, Computation
    from dvx.run.executor import ParallelExecutor

    monkeypatch.chdir(tmp_path)
    arts = [Artifact(path="a.txt", computation=Computation(cmd="true"))]
    ex = ParallelExecutor(arts, ExecutionConfig(mem_budget_gb=32), StringIO())
    assert ex._budget_gate is not None
    # Unlabeled stage charges the default weight (0 by default).
    assert ex._mem_weight(arts[0]) == 0.0
