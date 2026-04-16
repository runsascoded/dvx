# `dvx run --push` should also push cache blobs to S3 (not just `git push`)

## Problem

`dvx run --commit --push each` (and `--push end`) currently only invokes
`git push` after committing the rewritten `.dvc` file — it never pushes
the corresponding cache blob to the configured remote (S3).

This is silently lossy:

1. Stage runs, produces a new output, computes md5, rewrites `.dvc` file.
2. Blob lands in **local** DVX cache (per `cache_blob` in commit
   `b20c64988`, 2026-04-11).
3. `--push each` commits + git-pushes the new `.dvc`.
4. **Cache blob is never uploaded to S3.**
5. Next CI run (or any other clone) checks out HEAD, runs
   `dvx pull <target>` → fails with `Checkout failed for following
   targets: ... cache files do not exist neither locally nor on remote`.

In the `nj-crashes` project this caused the daily GHA pipeline to fail
for 5+ days. Each run produced a new `crash-log.parquet` md5 + 3 new
`FAUQStats*.xml` md5s, all orphaned in S3. Workaround was per-stage
`dvx push <target>` calls in the workflow:

```yaml
- run: |
    $DVX njsp/data/refresh.dvc
    dvx push data/FAUQStats2024.xml data/FAUQStats2025.xml data/FAUQStats2026.xml
- run: |
    $DVX njsp/data/crash-log.parquet.dvc
    dvx push njsp/data/crash-log.parquet
```

This is verbose, easy to forget when adding new stages, and conceptually
backwards — the user said "push" and got half a push.

## Proposal

Make `--push each` and `--push end` also push the just-produced cache
blob(s) to the configured remote, in addition to `git push`.

### Semantics

For `--push each`:

1. Compute `git push` (current behavior) — committing the `.dvc` rewrite.
2. **New:** for each `outs` entry in the just-rewritten `.dvc`, push its
   blob to the remote cache (≈ `dvx push <target>` for that artifact).
3. Log both actions: `📤 git pushed` and `📤 cache pushed (N blobs)` (or
   combine into one line).

For `--push end`:

1. After all stages complete, `git push` once (current behavior).
2. **New:** push every blob produced by executed (non-skipped) stages to
   the remote cache.

### Failure handling

- If `git push` fails: same as today (log `⚠ push failed`, continue).
- If cache push fails: log `⚠ cache push failed: <error>`, **do not abort
  the run**. The `.dvc` is committed; user can manually `dvx push`
  afterward. (Aborting would conflate transient S3 issues with stage
  failures.)
- Both failures are non-fatal but visible.

### Backward compatibility

Adding S3 push to the existing `--push each|end` modes changes behavior
for existing users in a way that could surprise:

- Previously: `--push each` was effectively "commit + git push only";
  users who wanted S3 push had to invoke `dvx push` separately.
- New: `--push each` does both.

Most users will want this — it matches the natural reading of "push" in
a DVC-like system. For users who genuinely want git-only push, add a
new `--push git-only` (or `--no-cache-push`) flag.

Suggest making the new behavior the default and providing the opt-out
flag, since the silent-loss failure mode is severe.

## Test plan

Add `tests/test_run_push_s3.py` covering:

1. **`--push each` includes cache push** — fake remote cache; run a
   stage producing a new output; verify the blob lands in remote.
2. **`--push end` batches cache pushes** — two stages each producing
   outputs; verify both blobs in remote after run completes.
3. **Cache push failure doesn't abort run** — fake remote raises on
   write; verify run still completes, `.dvc` is committed, warning
   logged.
4. **Skipped stages don't trigger cache push** — stage is up-to-date;
   verify no cache push attempted.
5. **`--no-cache-push` opt-out** — flag bypasses cache push, only does
   git push (matches old behavior).

Also add an integration-style test that exercises the workaround
disappearing: a daily-pipeline-shaped fixture where stage A produces a
DVX output and stage B in the next run pulls it. Without the fix,
stage B fails. With the fix, stage B succeeds.

## Related

- `b20c64988` (2026-04-11): added local cache_blob — half of the fix
- The other half (this spec): also push to remote cache

## Out of scope

- Push parallelism / batching across stages (could be a follow-up)
- Push retry / backoff (S3 occasionally 503s; current `dvx push` uses
  DVC's underlying transport, retries are inherited)
- Pre-push verification (`dvx push --verify`) is already a thing for
  ad-hoc invocations; not needed in the per-stage path
