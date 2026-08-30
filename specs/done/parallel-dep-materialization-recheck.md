# Parallel dep-materialization: empty pull-targets must re-check freshness, not rerun

Sequel to [`parallel-pull-lock-contention.md`](parallel-pull-lock-contention.md). Once materialization went lock-free, `test_fresh_clone_parallel_wide_level_zero_cmds` still flaked (~1 in 10–20 local runs; bit CI once at `1a7d6ec77`): a single sibling of a wide level would print `⟳ outN.txt: running...` and execute a cmd, breaking the zero-cmd fresh-clone invariant.

## Root cause — a TOCTOU on the shared tracked-dep dir

The wide level is N sibling stages that each dep on one file inside the *same* DVC-tracked directory (`data/inK.txt`), and that dir's stage is not in the plan. On a fresh clone every worker's `_should_run` walks the materialization pre-pass:

1. `output missing` → materialize the stage's own output → re-check → `dep missing: data/inK.txt`.
2. `dep missing` → `_missing_dep_pull_targets` → `data.dvc` → materialize the whole shared dir → re-check → fresh.

The race is between step-2's freshness check and its `_missing_dep_pull_targets` call. `_missing_dep_pull_targets` skips any dep already on disk (`if dep.exists(): continue`). So if a *sibling* worker materializes the shared `data/` dir in that window, the losing worker sees every dep already present, collects **no** pull targets, and the old code did:

```python
targets = self._missing_dep_pull_targets(path)
if not targets:
    break            # ← straight to rerun, skipping the final freshness recheck
```

`break` fell out of the loop to `return True, reason` — rerunning a stage that was, by then, fully materialized. Captured interleaving (8-way), `out5` the loser:

```
out5.txt: recheck fresh=False reason='dep missing: data/in5.txt'
out5.txt: dep targets=[]          # data/in5.txt now on disk (a sibling materialized data/)
⟳ out5.txt: running               # every other stage: dep targets=['data.dvc'] → fetched
```

Empty targets is ambiguous: either the deps are genuinely unpullable (raw file, no `.dvc`, no parent dir, still absent) **or** a concurrent worker just satisfied them. The break conflated the two.

## Fix

In the `dep missing` branch, when `_missing_dep_pull_targets` returns nothing, re-check freshness before bailing. A concurrent materialization makes us fresh → return `fetched`; a genuinely-missing dep stays stale → fall through to the rerun as before. (`src/dvx/run/executor.py`, `_should_run`.)

The other break points are race-free: a stage's own output is produced by no one else (output-missing misses are real), and the placeholder / unexpected-reason breaks don't gate on shared state.

## Tests

- `tests/test_executor.py::test_should_run_rechecks_freshness_when_concurrent_worker_satisfies_deps` — deterministic: scripts `is_output_fresh` (dep-missing then up-to-date) and forces empty pull-targets; asserts `(False, "fetched (up-to-date)")`. Fails without the fix (`(True, "dep missing: data/in0.txt")`).
- `tests/test_executor.py::test_should_run_reruns_when_deps_genuinely_unpullable` — negative control: recheck still stale ⇒ still reruns.
- `tests/test_run_pull_deps.py::test_fresh_clone_parallel_wide_level_zero_cmds` — the original flaky integration test; 300 consecutive local passes with the fix (was ~1/12 failing).
