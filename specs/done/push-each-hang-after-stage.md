# `--push each`: post-stage `_push_cache_blobs` hangs indefinitely (Fargate run 7)

nj-crashes `dvx batch submit` at `471442216` (job `7c44b15f`, 2026-08-28). The good news first: **the materialization invariant now holds end-to-end** — 33 stages across levels 1–3 all `fetched (up-to-date)` in <2 min, and the one genuinely-stale stage (`njsp_njdot_residuals`, lagging co-output stamps) re-ran correctly with its inputs materialized (66.8 s ✓). That's the first run where dvx did exactly the right thing on every stage it classified.

Then it hung.

## Symptom

```
  ⟳ njsp/data/njsp_njdot_residuals.parquet: running...
  ✓ njsp/data/njsp_njdot_residuals.parquet: completed (66.8s)
    stderr: ... DVX commit: Match NJSP↔NJDOT (9527 pairs, 1471 residuals)
<nothing for 45 minutes; terminated by hand at 20:50Z>
```

Last log event 20:05:49Z. The next thing that should have logged is either `📤 cache pushed (N blobs)` / `⚠ cache push failed` from `_push_cache_blobs`, or `⟳ … running...` for a level-5 stage. Neither appeared. `_push_cache_blobs` is wrapped in a catch-all `except Exception` that logs — so this is a **hang, not an exception** — and it's the only code between the two missing lines. Expected wall for the whole job was ~5 min; it was killed at 46.

## Narrowed (crashes session, follow-up analysis)

Two of the three candidates below are **ruled out**; corrections to the first draft:

- **Not `git push`.** The job ran `--no-commit` (your fixed `run_command`), so `commit_msg is None` and the whole `git add`/`commit`/`push` branch is skipped. Only `_push_cache_blobs` sits in the log gap.
- **Not `push_dir_inner_blobs`.** Both targets (`njsp_njdot_residuals.parquet`, `njsp_njdot_match.parquet`) are plain-file outs; the function early-returns `(0, [])` when no `.dir` manifests are present.
- **Probably not the rwlock either** (my original leading theory): DVC's `@locked` raises `LockError` after its retries — that is exactly the "Unable to acquire lock" text run 4 produced — and `_push_cache_blobs`'s catch-all would have logged `⚠ cache push failed`. We saw no log line at all, which points at something with *no timeout*, not a lock.

So: **`Repo()` construction or `repo.push(targets=…)` itself**, hanging with no bound. Verified locally that the same call on the same two targets is instant (`repo open 0.5s`, `pushed: 0`, total 1.6s) — but locally there was **nothing to upload**. In the container `residuals` had just been regenerated, so this was the first actual blob *upload* of the whole batch effort (runs 1–6 never got a stage to execute successfully, so this code path had never run in Fargate). Suspicion is therefore on the upload transport (boto/S3 PUT with no timeout) or on `Repo()`'s scm layer touching git in a `--depth 1`, credential-less clone.

## Original candidates (in `_push_cache_blobs`, executor.py ~1047)

1. **`repo.push(targets=dvc_paths)` with a `Repo()` acquiring the rwlock** — the run-4 lock-contention fix moved *pulls* off `repo.pull`, but pushes still go through `Repo()` + `@locked repo.push`. If any other lock holder is alive in-process (a materializer thread? the pull-deps pass?), this blocks forever with no timeout and no log line. The symptom — silent, indefinite, immediately after the first *executed* stage — fits a lock wait exactly.
2. `push_dir_inner_blobs` existence-check pool (`jobs=min(32, n)`) deadlocking or hanging on S3 without a timeout — less likely (no `.dir` outs here: both co-outputs are plain parquets), but the same "no logging until done" shape.
3. boto/S3 retry storm on the ~10 MB `njsp_njdot_match.parquet` sibling — would still normally surface within minutes.

Note the co-output angle: `dvc_paths` = `[residuals.dvc, match.dvc]` (the `co_paths` extension). `match.parquet` was *not* re-produced (classified `up-to-date`, never re-run) but its `.dvc` is in the push list — pushing an already-remote blob should be a no-op existence check, but worth checking it doesn't take an odd path.

## Asks

1. **Log before the push, not only after**: `📤 pushing N blob(s) for <stage>…` at the top of `_push_cache_blobs`, so a hang is attributable from the log alone. Cheap, and it would have saved the guesswork above.
2. **No indefinite waits in the push path**: whatever the lock/network primitive is, a bounded wait + `⚠ cache push timed out (Ns)` → continue. The executor already treats push as non-fatal; a hang violates that contract worse than a failure would.
3. **Make push lock-free the way pull now is** (the user's standing preference: "we've generally tried to reduce repo-wide locking in dvx vs dvc"). `pull_hashes` proved `DVCRepo()` instantiation doesn't take the lock — only `@locked` ops do; the push path could use the same remote-odb primitives directly.

Repro shape for a test: `--push each` with a stage that executes while at least one other stage's materialization is in flight (or has left a lock), then assert the post-stage push completes within a bound. The nj-crashes cells targets reproduce it deterministically on Fargate; happy to re-run on request — each attempt is ~5 min and pennies.

## Resolution (dvx, 2026-08-28)

Implemented all three asks; the fix is one commit on `main`.

**Ask 3 — lock-free push** (`dvx.cache.push_targets`, mirroring `materialize_targets`):
`_push_cache_blobs` no longer calls `Repo().push()`. It now enumerates cache
objects locally (`collect_push_objects`: file blobs, `.dir` manifests, and every
inner blob a manifest names), then existence-checks and uploads each one through
the cached remote-ODB handle from `_get_remote_odb` — the same primitive the
lock-free materialization path uses. No `@locked` operation, no repo-wide rwlock,
and `Repo()` isn't even re-opened per stage.

This also subsumes `push_dir_inner_blobs` for the executor path: the old pairing
was `repo.push` (which short-circuits on a present `.dir` manifest) plus a gap-fill
pass behind it; enumerating every object up front makes the gap structurally
impossible. `push_dir_inner_blobs` stays for the `dvx push` CLI, which is
deliberately left on `repo.push` — it's a foreground, user-invoked command where
the lock is expected and `--all-branches/--all-tags/--all-commits` are DVC's.

**Ask 1 — log before the push**: `📤 pushing N object(s) (SIZE)...` is emitted
before any network call (enumeration is pure local I/O), so a stall is attributable
to the push from the log alone, with the in-flight byte count. The completion line
is unchanged: `📤 cache pushed (N blobs)`.

**Ask 2 — no indefinite waits**: the upload runs on a daemon thread with a
**stall** timeout — `--push-timeout` seconds with no single object settling
(default 600; `ExecutionConfig.push_timeout`). A stall logs
`⚠ cache push stalled (600s without progress) — continuing; re-run `dvx push` to flush`
and the run proceeds. Stall rather than total elapsed, so a legitimately long
upload isn't capped; daemon threads rather than a `ThreadPoolExecutor`, because
pool threads are joined at interpreter exit — that turned an abandoned push back
into a hang at exit (caught in testing).

### Tests

- `test_push_each_does_not_take_the_repo_lock` — patches both `dvx.repo.Repo.push`
  and `dvc.repo.Repo.push` to raise; blobs still reach the remote. Encodes
  lock-freedom structurally rather than by timing.
- `test_push_stall_times_out_instead_of_hanging` — `_push_blob` never returns;
  the run finishes with the stall warning. Verified to have teeth: the identical
  scenario with `--push-timeout 9999` hangs past 20 s.
- `test_cache_push_failure_is_non_fatal` — rewritten onto the new primitive;
  also asserts the pre-push line still fires when the push itself fails.
- The `test_run_push_s3.py` output parser gained `cache_objects` / `cache_size` /
  `cache_stalled` fields; the pre-push line's object count is now asserted in every
  push test (notably 6 for the dir co-output case: 2 manifests + 4 inner blobs).

### Caveat

An abandoned upload is abandoned mid-flight. On S3 a PUT is atomic (nothing
partial becomes visible), but a local-directory remote can be left with a partial
file that a later existence check counts as present. `dvx cache comm` /
`dvx push --verify` remain the way to audit that; it only arises after a stall
warning, which is itself a "something is wrong with the remote" signal.

### Note on cause

The narrowed analysis is right that a lock wait would have surfaced as
`LockError` → `⚠ cache push failed`, so the rwlock probably wasn't the proximate
cause — but this fix doesn't depend on knowing which unbounded call hung. The
transport is now bounded and instrumented per object, so if run 8 stalls again,
the log names how many objects were in flight and the run still completes.
