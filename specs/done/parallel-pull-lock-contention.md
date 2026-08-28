# Parallel auto-pull materialization contends on the DVC repo lock

nj-crashes Fargate re-test at `512dbf61a` (job `dvx-data_cells_raw_s2_l21_dvc_data_cells_cells-s2_db_dvc`, 2026-08-27). The `512dbf61a` fixes verify: the CLI parses (`--no-commit --push each`), and the materialization pre-pass fires. But under `-j 16` on a 28-stage level:

```
↓ njdot/data/2003/NewJersey2003Accidents.pqt: pull failed (Unable to acquire lock.
  Most likely another DVC process is running or was terminated abruptly. ...)
⟳ njdot/data/2003/NewJersey2003Accidents.pqt: running...
```

**10 stages won the lock and `fetched (up-to-date)`; 18 lost it, fell through to re-run, and crashed** on inputs that don't exist on a fresh clone. `_try_materialize_from_remote` treats any pull exception as "not materializable" (by design — but lock contention is retryable, not a miss), and each parallel worker opens its own `Repo()` → `repo.pull`, so the workers race for DVC's rwlock.

## Fix shape

Preferred: **batch materialization per level** — before executing a level, collect every stage classified materializable (output/dep missing, recorded closure consistent) and issue ONE `repo.pull(targets=[...])` for all of them, then execute the level. Kills the contention *and* the N× per-pull overhead (each pull pays repo open + remote index).

Cheaper alternative: a process-wide mutex (or retry-with-backoff on the lock error) around `_try_materialize_from_remote` — correct but serializes what could be one batched call.

Invariant test (extends the fresh-clone one): fresh clone + closure-in-remote + `-j 16` over a wide level ⇒ zero cmds executed. The `-j 1` version presumably passes today.

## Secondary: fall-through on failed pull should not run the cmd blind

Even once pulls are lock-safe, a *transient* pull failure (network, S3 5xx) currently downgrades to "re-run the cmd" — which for stages with undeclared/intermediate inputs (nj-crashes' zip→txt→pqt, where the txt isn't tracked) produces confusing downstream crashes rather than a clear "couldn't materialize X". Consider: failed materialization of a *consistent-closure* stage should be an error (or retried), not a silent re-run trigger.

## Resolution

Took a third path, per the dvx philosophy of reducing repo-wide locking
(vs DVC): **lock-free materialization**, not level-batched `repo.pull`
and not a mutex/backoff.

**Mechanism** (`src/dvx/cache.py`): `materialize_targets(dvc_paths)`
goes remote blob → local cache → workspace directly, never invoking a
`@locked` DVC operation:

- `_get_remote_odb()` — remote ODB handle cached per (root, remote),
  built once per process via a bare `DVCRepo()` (instantiation doesn't
  take the rwlock; only operations like `pull` do). Reuse also kills
  the N× repo-open + fs-construction overhead the spec's batching
  option targeted.
- `_fetch_blob(md5)` — download to a tmp sibling, `os.replace` into
  the content-addressed cache path. Atomic + idempotent: concurrent
  fetches of one blob (threads or separate processes) converge on
  identical bytes; last rename wins harmlessly. Same pattern as dvx's
  lock-free `add_to_cache`.
- `_checkout_file(md5, path)` — cache → workspace, also tmp+rename.
- Directory outputs fetch the `.dir` manifest + every inner blob, then
  build the tree.

`_try_materialize_from_remote` in the executor now calls this instead
of `Repo().pull(targets=...)`. No lock exists to contend on, so the
"10 won / 18 lost" split is structurally impossible — and this holds
across *processes* too (two `dvx run`s sharing a cache), which the
level-batching option wouldn't have covered.

**Secondary fixed too**, with an honest absent-vs-error split:

- Blob absent from the remote (`FileNotFoundError` / `odb.exists()`
  False) → return "not materializable" → stage falls through to rerun
  (legitimate rebuild trigger; the existing
  `test_pull_deps_falls_through_when_remote_missing_blob` behavior).
- Transport-level failure (network, 5xx, auth) → `MaterializeError`,
  which `_execute_artifact` turns into a **stage failure** with reason
  `materialization failed: …` — the cmd is never run blind.

**Tests** (both TFFP-verified red against the `repo.pull` code):

- `test_fresh_clone_parallel_wide_level_zero_cmds` — the spec's
  invariant: 8 sibling stages sharing a tracked-dir dep, fresh clone,
  `-j 8` ⇒ `Executed: 0`, all `fetched (up-to-date)`. Red pre-fix
  (lock contention reproduces locally with 8 threads).
- `test_transport_error_fails_stage_instead_of_rerun` — simulated 503
  on every fetch ⇒ stage fails `✗ … materialization failed (…)`,
  exit 1, cmd not executed.

## Verified fixed at `72d62b28e` — one residual gap (nj-crashes Fargate run 5)

Lock contention: gone. 34/35 stages `fetched (up-to-date)` / skipped, zero `Unable to acquire lock`, zero spurious re-runs. The remaining failure is a different, final gap:

**A genuinely-stale stage runs without its declared dep files materialized.** `njsp/data/njsp_njdot_residuals.parquet` is legitimately stale (recorded dep `crashes.parquet: 3f6e4628…` vs the upstream `.dvc`'s current `1bd4f002…`) → re-run is correct. But its input `njsp/data/crashes.parquet` is the output of an upstream stage that's *pruned as fresh* — nothing materializes it — and the cmd dies `FileNotFoundError: njsp/data/crashes.parquet`. The dep-materialization pre-pass covers *classification*; this needs the execution-time counterpart: before running a stale stage, pull each declared dep whose file is absent, at the md5 its own `.dvc` (the upstream out stamp) records. Invariant: a stage never executes with a declared dep absent when that dep's blob exists in the remote.

**Secondary observation — co-output stamp divergence.** `njsp_njdot_match.parquet` and `njsp_njdot_residuals.parquet` are co-outputs of one cmd (`njsp match_njdot`), yet match's dep stamps are current (daily CI re-runs re-stamp it) while residuals' lag by days — so one sibling reads fresh and the other stale off the same execution. If co-outputs' `.dvc`s aren't all re-stamped when the shared cmd runs, staleness classification splits within a cmd group. Worth checking whether the daily's targeted runs (`$DVX njsp/data/njsp_njdot_match.parquet.dvc`) update sibling co-output `.dvc`s.

### Run-5 resolution

**Primary fixed**: `_execute_artifact` now runs an execution-time dep
materialization for any stage about to execute (stale OR forced, when
`pull_deps` is on): every declared dep whose file is absent from the
workspace is pulled at the md5 *its own* `.dvc` records — the upstream
out stamp, not the stale downstream stamp — via the same
`_missing_dep_pull_targets` + lock-free `materialize_targets` path the
classification pre-pass uses. Deps absent from the remote fall through
(the cmd then fails on its own terms); transport errors fail the stage
as before. This also blesses the `-f <target> --cached <patterns>`
"rebuild this subtree against pulled inputs" pattern from the
first-smoke findings — `--cached`-skipped upstreams get their outputs
pulled for the stale downstream.

Test: `test_stale_stage_materializes_absent_deps_before_running` —
downstream with a corrupted (stale) dep stamp, fresh clone, upstream
in-plan but `--cached`-skipped ⇒ dep materialized at the upstream's
recorded md5, cmd succeeds. Red pre-fix (`FileNotFoundError` → exit 1).

**Secondary confirmed, deferred to its own spec**: targeted runs do
NOT re-stamp out-of-plan co-output `.dvc`s. Mechanism:
`_cmd_artifact_paths` (the cmd → co-output map) is built only from
artifacts in the plan, so `$DVX match.parquet.dvc` runs the shared cmd
and re-stamps match's `.dvc` while residuals' — never loaded — lags.
A fix needs out-of-plan co-output *discovery* (a recursive `.dvc` scan
building the cmd map repo-wide, then post-run stamping of unplanned
siblings) — separable scope with its own tradeoffs (scan cost, whether
unplanned siblings' outputs should also be verified/cached). Interim
workaround for nj-crashes' daily: target the whole cmd group
(`$DVX match.parquet.dvc residuals.parquet.dvc`) so both are in-plan.
