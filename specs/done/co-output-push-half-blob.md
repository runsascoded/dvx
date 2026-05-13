# Co-output `dvx run --push each` only pushes N-1 of N blobs to cache

## Symptom

`hccs/crashes` daily CI (2026-05-12, run [25751208586]) runs
`update_pqts.dvc` whose cmd `njsp update_pqts --s3` produces two
co-outputs: `njsp/data/crashes.parquet` and
`www/public/njsp/crashes.db`.

DVX logs the completion as:

```
Level 1/1: 2 computation(s)
  ⟳ njsp/data/crashes.parquet: running...
  ◐ www/public/njsp/crashes.db: waiting (same cmd running)...
  ✓ njsp/data/crashes.parquet: completed (22.0s)
  ✓ www/public/njsp/crashes.db: co-output ready
    📝 committed: Update NJSP data
    📤 pushed
    📤 cache pushed (1 blob)
```

**2 outputs, 1 blob pushed.** Both `.dvc` files were updated to new
md5s and committed (one commit, both files in the diff). The git push
went through. But only `crashes.parquet`'s blob (`5e52b723…`) landed
in `s3://nj-crashes/.dvc/files/md5/`; `crashes.db`'s blob
(`19da58a2…`) was never uploaded.

The next day's CI (2026-05-13, run [25815746395]) fails on `dvx pull
www/public/njsp/crashes.db.dvc`:

```
WARNING: Some of the cache files do not exist neither locally nor on remote. Missing cache files:
md5: 19da58a23b1f999250ca1fdd45f00dfc
Error: Checkout failed for following targets:
www/public/njsp/crashes.db
```

[25751208586]: https://github.com/hudcostreets/nj-crashes/actions/runs/25751208586
[25815746395]: https://github.com/hudcostreets/nj-crashes/actions/runs/25815746395

## Why this is worse than the prior co-output bug

Unlike [co-output-concurrency-locked.md], which fires loudly (the run
fails immediately with `database is locked`), this one **silently
publishes a broken .dvc**: git is fast-forwarded with the new md5,
S3 isn't, and the failure surfaces 24h later in a downstream `dvx
pull`. By the time CI fails, the broken commit is on `main` and the
fix requires regenerating + re-pushing the blob.

[co-output-concurrency-locked.md]: ./co-output-concurrency-locked.md

## Root cause

Two compounding bugs in `ParallelExecutor._handle_stage_output`:

1. **Push manifest is single-stage**: the primary thread calls
   `self._push_cache_blobs([f"{path}.dvc"], indent="    ")` with only
   its own `.dvc` path. `repo.push(targets=[primary.dvc])` reads only
   that file, sees one blob to upload. Co-outputs sit in
   `.dvc/cache/` but never make it into the push manifest.

2. **No barrier between co-output `.dvc` writes and primary's commit
   + push**: the primary calls `_handle_stage_output` right after
   writing its own `.dvc`. The co-output thread is racing in
   `_handle_co_output` (compute md5 → cache blob → write `.dvc`).
   When the co-output's hash finishes before the primary's (typical
   for the smaller of two outputs), the `.dvc` is in place before
   `git add -u` runs, so the commit picks it up — that's the
   "lucky" case the spec's symptom captured. With reversed timing,
   the co-output's `.dvc` would also be missing from the commit,
   not just from the push manifest.

The first bug always fires for co-outputs. The second is timing-
dependent: in the spec's failure the commit happened to include
both `.dvc`s; the cache push didn't.

## Fix

In `src/dvx/run/executor.py`:

1. Pre-group artifact paths by cmd at `ParallelExecutor.__init__`:
   `self._cmd_artifact_paths: dict[str, list[str]]`.
2. Pre-allocate a per-artifact `threading.Event`:
   `self._dvc_done_events: dict[str, Event]`. The event is set when
   the artifact's `.dvc` has been written (or definitively not — the
   `_handle_co_output` `try/finally` always signals, so a failed
   co-output doesn't hang the primary).
3. Before `_handle_stage_output`, the primary signals its own event,
   then `_wait_for_co_outputs(cmd, path)` blocks on every other
   artifact in the cmd group. By the time `git add -u` runs, every
   co-output's `.dvc` is on disk and the commit captures the full
   md5 set.
4. The primary passes `co_paths` into `_handle_stage_output`. The
   push manifest is now `[primary.dvc, *co_paths.dvc]`, so
   `repo.push(targets=...)` sees every blob.

Caveat: requires `max_workers ≥ largest cmd-group size` — with `-j 1`
and a multi-output cmd, the primary's wait would deadlock the pool
(only one worker; the co-output's future can't run while the primary
is blocked). The default `max_workers=None` uses `min(32, cpu_count+4)`
so this is safe in practice; documented inline in `__init__`.

`--push end` was already correct: it pushes
`[r.path + ".dvc" for r in results if r.success and not r.skipped]`,
and `_handle_co_output` returns `success=True, skipped=False`, so
co-outputs were already in the end-of-run manifest. Only the per-cmd
`--push each` path needed the fix.

## Test

`tests/test_run_push_s3.py::test_push_each_uploads_all_co_output_blobs`:

- Two `.dvc` files share one cmd: `sleep 0.1 && echo aaa > a.txt &&
  echo bbb > b.txt`. The `sleep` widens the window so the second
  artifact reliably enters the co-output dedup path while the first
  is running.
- Pre-fix output: `📤 cache pushed (1 blob)`; assertion fails with
  `co-output blob(s) missing from remote: ['b.txt']`. Exact match
  for the spec's hccs/crashes symptom.
- Post-fix output: `📤 cache pushed (2 blobs)`; both blobs in remote.

The test also asserts `"co-output ready" in result.output` as a
sanity check — without it, if the threading happened to deduplicate
differently (e.g. both stages independently running the cmd), the
test would mask the bug by producing 2 blobs through 2 independent
runs rather than 1 cmd + 1 co-output.

## Workaround for affected repos

Once a broken commit is on `main`:

1. Regenerate the output locally (`dvx run <co-output.dvc>` — DVX
   should detect "data changed" and just `dvx commit` it without re-
   executing if the deps haven't drifted).
2. `dvx push <co-output.dvc>` to upload the missing blob.

If the deps have drifted (live data source), regen will produce a
new md5; commit the new `.dvc` and push.

## Related

- [co-output-concurrency-locked.md] — different co-output bug (race
  during scheduling, not push). Same family: anything that special-
  cases the "second co-output" path tends to drop steps that the
  primary path handles.
- [run-push-includes-s3.md] — the design doc for `dvx run --push`
  semantics.

[run-push-includes-s3.md]: ./run-push-includes-s3.md
