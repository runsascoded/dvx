# `run --commit --push each` git-pushes race under level-parallelism

## Problem

`dvx run --commit --push each` does, per stage (executor.py ~1163-1177):
`git add -u` → `git commit` → commit-gated `git push`. Under level-parallelism
(`-j>1`, the reproc default), stages in a level finish concurrently, so these
git ops run concurrently on one repo. Observed in the nj-crashes full-DAG
from-scratch reproc (160 targets, `-j` wide): the per-stage `git push`es to the
shared branch race —

```
 ! [remote rejected]  reproc-results/… -> reproc-results/…
   (cannot lock ref 'refs/heads/reproc-results/…':
    is at <A> but expected <B>)
```

repeatedly. dvx logs `⚠ push failed` and continues (non-fatal), but the losing
stages' commits never reach the remote — so with a container that's torn down
at the end, those regenerated `.dvc`s are lost. (Concurrent `git commit` on one
index is also unsafe in principle — `index.lock` contention — though the push
races dominated in practice.)

The serial daily pipeline (one `$DVX <target>` step at a time) never hits this;
it's specific to a single `dvx run` fanning the whole DAG out in parallel.

## Workarounds in use (nj-crashes)

`batch/entrypoint.sh` now runs `dvx run --no-commit` (dvx still *writes* updated
`.dvc` md5s to the worktree, just doesn't commit) and does one
`git add -u && commit && push` after the parallel run returns — race-free, one
commit. Works, but pushes the concern into every caller that wants parallel +
push-back.

## Proposals (dvx side)

1. **Serialize the git critical section.** A process-wide lock around the
   `git add -u`/`commit`/`push` triple so `--commit --push each` is safe under
   `-j`. Cheapest; preserves per-stage granularity. Push is I/O-bound, so
   serializing it barely dents wall-clock vs the compute.
2. **`--commit end` / `--push end` that truly batches git.** A mode where dvx
   accumulates and commits+pushes once at the end of the run (not per stage),
   for callers that don't need per-stage commits. (`--push end` today still
   commits per-stage; only the cache/push is deferred.)
3. At minimum: make a failed `git push` under `--push each` **fatal** (or
   surface a nonzero summary), so a caller can't mistake a run full of
   "⚠ push failed" for success.

(1) is the general fix; (2) matches the batch/parallel use case; (3) is a
safety floor regardless.

## Resolution

Implemented **proposal (1)** — serialize the git critical section — in
`src/dvx/run/executor.py`:

- A process-wide `self._git_lock = threading.Lock()` (alongside the existing
  `self._cmd_lock`).
- `_handle_stage_output` now runs the whole `git add -u` → `commit` → `push`
  triple under `with self._git_lock:`. The lock is held only for the git ops;
  the cache-blob push (`_push_cache_blobs`) stays outside it — it's
  concurrency-safe (atomic content-addressed writes) and I/O-bound, so it keeps
  running in parallel.

This closes the reported hole completely: the nj-crashes data loss came purely
from concurrent per-stage `git push`es racing the shared branch (`cannot lock
ref … is at <A> but expected <B>`), which dropped the losing stages' commits.
Serialized, each stage's commit fast-forwards the branch and its push
succeeds; the same lock also removes the `index.lock` contention on concurrent
`git commit`. Push is I/O-bound, so serializing it barely dents wall-clock vs
the (parallel) compute. The `--push end` path (post-level, single-threaded in
the main thread) was already race-free and is unchanged.

Test: `tests/test_executor.py::test_git_critical_section_is_serialized_under_parallelism`
drives `_handle_stage_output` from 8 threads with a widened git-subprocess
window and asserts at most one thread is ever inside the git region
(`max_active == 1`). TFFP-verified: with the lock neutralized the same test
sees all 8 threads concurrent (`max_active == 8`).

Full suite green after the change.

### Not done (deferred — available on demand)

- **(2) `--commit end` / `--push end` that truly batches git** into a single
  end-of-run commit+push. The nj-crashes caller already has a clean workaround
  (`dvx run --no-commit`, then one manual `git add -u && commit && push`), and
  (1) makes per-stage `--commit --push each` safe again, so this larger feature
  is deferred until a caller actually wants per-stage granularity dropped.
- **(3) Fatal / nonzero-summary on a failed `--push each` git push.** With (1)
  the race-driven push failures are gone; a *genuine* push failure (auth,
  network) is a different, non-race condition. Making it fatal is a BC-affecting
  behavior change (the `dvx batch` container runs `--commit never`, so it never
  git-pushes per stage; the daily serial pipeline uses `--commit --push each`
  and a transient blip shouldn't halt it). Left as a follow-up pending a
  decision on the exact form (fatal vs. surfaced-nonzero-count).
