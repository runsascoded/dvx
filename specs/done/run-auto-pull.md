# `dvx run` auto-pull / cache policy flags

## Problem

In CI (or any fresh checkout), `dvx run <targets>` recomputes every stage
from scratch even when the `.dvc` files' deps hashes are unchanged from
the cached run. Symptom from `hccs/path`'s daily cron:

- ~25min recomputing 2017–2026 yearly `.pqt`s + hourly + day-types + B&T
  + downstream JSONs/PNGs
- All outputs ALREADY exist in S3 (pushed by yesterday's run)
- All upstream PDFs are byte-identical to yesterday's
- Yet today's run re-derives every stage from PDFs

Root cause: `dvx run` checks deps' hashes against the `.dvc` file. On a
fresh runner, the deps don't exist locally for any non-leaf stage, so
DVX has to run upstream stages to produce them, which has the same
problem recursively → all the way back to the `import-url` PDFs, which
DO exist, and then everything gets re-derived.

The fix users currently apply: `dvx pull && dvx run …`. That works, but
the project-side workaround is bandaid for what feels like a CI-aware
default DVX should make easy.

## Conceptual model

A `dvx run <targets>` invocation walks the dep graph from each target
toward `import-url` / `git-deps` roots. For each stage:

- **Up-to-date** — deps' hashes match `.dvc`, output exists with
  matching hash → skip.
- **Stale** — deps' hashes don't match `.dvc` → must rerun the stage's
  `cmd` to regenerate the output.
- **Materializable** — deps' hashes match `.dvc` (i.e. would-be skip)
  but output doesn't exist locally. **This is the case the flag
  governs.**

Pre-fix, "materializable" stages were treated like stale stages: rerun.
Safe but expensive when the cache has the result.

## Fix (this PR's scope)

Single flag: **`--pull-deps`** (default-on) / **`-D/--no-pull-deps`**.

When a stage is "materializable" (would skip if the output were on
disk), the executor calls `repo.pull(targets=[<stage>.dvc])` to
materialize it from the remote, then re-evaluates freshness. If the
fetch succeeds and the stage is now fresh → skip. Otherwise → fall
through to rerun normally.

Lazy per-stage, not eager pre-pass: each materializable stage attempts
the fetch only when reached during the graph walk. Stages already known
stale (deps changed) never attempt the fetch.

Cost when fetch is unavailable / unhelpful: one `repo.pull` round-trip
per materializable stage. For freshly-pushed pipelines this is cheap;
for empty / lagging remotes the cost is the `repo.pull` overhead before
falling through to the rerun.

Implementation:

- `ExecutionConfig.pull_deps: bool = True` field.
- `--no-pull-deps` (-D) on `dvx run`.
- `_should_run` ("dvx/run/executor.py"): when `is_output_fresh` returns
  `False, "output missing"` AND the `.dvc` file has a recorded md5
  (skip placeholder stubs), call `_try_materialize_from_remote(path)`.
  On success, re-call `is_output_fresh` and short-circuit if now True.
- `_try_materialize_from_remote(path)`: opens a fresh `Repo`, calls
  `repo.pull(targets=[<path>.dvc])`. Non-fatal: any exception (no
  remote, missing blob, network error) is swallowed and the stage
  falls through to its normal rerun. Verbose logging only.

## Out of this PR's scope

- **`--err-on-unpulled` / `--cached-deps`** — proposed in the original
  spec discussion but punted; they target additional policy modes that
  no current user is blocking on.
- **Direct vs trans-dep heuristic** — for now ALL stages (including
  positional targets) get the pull pre-pass. Refinement possible later
  (e.g. only trans-deps get the pull, direct targets always rerun on
  miss) once we have a real use case.
- **Dry-run / status integration** — `dvx run --dry-run` still reports
  "would run" for materializable stages without attempting the fetch.
  The pull pre-pass only runs in the live execution path.
- **Concurrency** — when multiple stages in one level are
  materializable, each opens its own `Repo()` and `repo.pull`. DVC
  serializes these via its own lock; pulls in the same level run
  sequentially rather than in parallel. Acceptable for the MVP.

## Tests (`tests/test_run_pull_deps.py`, 4)

- `test_pull_deps_skips_rerun_when_remote_has_output` — push a stage,
  wipe local cache + workspace, re-run. Default `--pull-deps` shows
  `○ out.txt: fetched (up-to-date)` and `Executed: 0, Skipped: 1`.
  Workspace file is materialized from remote.
- `test_no_pull_deps_reruns_when_output_missing` — same setup, but
  `--no-pull-deps` falls through to rerunning (the prior behavior).
- `test_pull_deps_falls_through_when_remote_missing_blob` — run
  WITHOUT `--push` so the blob is only in local cache; wipe; re-run.
  Default `--pull-deps` tries the pull, fails (no remote blob), falls
  through to rerunning.
- `test_pull_deps_does_not_interfere_with_forced_rerun` — `--force`
  bypasses the pull check; forced stages always re-execute.

## Behavior change vs prior

`dvx run` (no flags) now attempts a remote pull for each
materializable stage. Most users (the `hccs/path` daily CI being the
prototypical case) want this — fresh checkouts skip the expensive
recompute when the cache has it. Users who explicitly want
reproducibility-from-source pass `--no-pull-deps`.

Default-on instead of opt-in because:

1. The default ("rerun anything missing") was a CI footgun.
2. Forcing the user to remember `--pull-deps` on every CI command
   means the footgun stays in place for new pipelines.
3. Graceful degradation: when the remote isn't configured or doesn't
   have the blob, the rerun fires anyway — no breakage, just a
   small per-stage overhead.
