# `dvx push` for directory outputs only checks the `.dir` manifest, not inner blobs

## Symptom

After landing [`099855640`] (`cache_blob` handles directory outputs),
the manifest blob for `ymccmcs` finally reached S3, and a fresh
`dvx push www/public/data/njdot/ymccmcs.dvc` correctly reported
`17 file(s) pushed` for a directory whose `.dir` was new to the remote.

But on `hccs/crashes` the historical `ymccmc` manifest
(`252d4ed9215ec14d3e678f54898174d2.dir`) had been in S3 since
**2026-05-15** while 17 of its 21 inner blobs were missing — silently
broken state, the manifest claims the blobs but the blobs aren't there.

Running `dvx push www/public/data/njdot/ymccmc.dvc` reported:

```
Checking remote: 100%|██████████| 1/1 [00:00<00:00,  1.64file/s]
Nothing to push (all files already in remote).
Already in remote: 1 file(s) (3.3 MB)
```

`--verify` didn't catch it either:

```
$ dvx push --verify www/public/data/njdot/ymccmc.dvc
0 file(s) pushed.
```

Then `dvx pull` from a fresh clone failed with
`ERROR: failed to transfer '<inner-blob-md5>' - The specified key does not exist.`
for each of the 17 missing inner blobs — at *pull* time, not push time.

## Root cause

DVC's `repo.push(targets=[<dir>.dvc])` does a shallow existence check
on the single registered `outs.md5` value (the `.dir` manifest). If
the manifest exists in remote, push short-circuits and never recurses
into the manifest to verify inner blobs are present. Confirmed in a
local scratch repo: delete one inner blob from remote, re-push →
`0 file(s) pushed`, gap unfilled.

The crashes state — manifest present, inner blobs missing — was
created by the pre-`099855640` cache_blob bug: directory output → local
cache miss → push had nothing to upload locally either. Once one
"manifest-only" state landed in remote, subsequent `dvx run --push
each` saw "manifest in remote → done" and never attempted to upload
the inner blobs that, by then, were actually present in local cache.

Net effect: any directory output whose history straddles the
`099855640` fix can end up in this state, and the broken state is
invisible to push callers.

## Fix

`src/dvx/cache.py::push_dir_inner_blobs(dvc_paths, remote=None,
jobs=None) -> (uploaded, missing_locally)`:

1. For each `.dvc` with a directory output, parse its local `.dir`
   manifest from `<cache>/<h[:2]>/<h[2:]>.dir`.
2. Batch-check remote existence (`check_remote_cache_batch`) for the
   manifest hash + every inner blob hash. Parallel, dedup'd.
3. For each hash missing in remote: locate it in local cache,
   `remote_odb.fs.put(local, remote)`. Bulk transfer, fall back
   one-by-one on bulk failure.
4. Returns `(uploaded, missing_locally)`. `missing_locally` is hashes
   missing from BOTH remote and local — caller may want to warn (no
   way to fill them without a `dvx pull`).

Wired into:

- **`dvx push`** (`src/dvx/cli/transfer.py::push`): after `repo.push`
  returns, run `push_dir_inner_blobs` on the same `.dvc` targets (or
  every `.dvc` in the repo if no targets given). Emits
  `Backfilled N dir blob(s) missing from remote.` when any were
  filled; warns on stderr if any couldn't be filled locally.
- **`dvx run --push each|end`** (`src/dvx/run/executor.py::_push_cache_blobs`):
  same pattern, post-`repo.push`. The per-stage
  `📤 cache pushed (N blobs)` line now includes the gap-fill count.

`--dry-run` and `--verify` are deliberately left untouched for now —
the underlying `get_transfer_status` is shallow (single hash per .dvc)
but that affects pre-flight reporting, not the actual push. Could be
deepened in a follow-up if dry-run accuracy matters more than the
shallow-but-fast counts it produces today.

Cost: per-push, one `remote_odb.exists(...)` call per inner blob. S3
head-object is ~10-50ms parallelized across 32 workers; negligible
relative to the run time of the cmd that produced the dir output.

## Test

`tests/test_push_dir_gap_fill.py` (new):

- **`test_dvx_push_backfills_missing_inner_blob`**: push a dir with 3
  inner files; manually delete one inner blob from remote; re-push →
  `0 file(s) pushed.` then `Backfilled 1 dir blob(s) missing from
  remote.`; all 4 blobs back in remote.
- **`test_dvx_push_noop_when_remote_complete`**: when remote is whole,
  no `Backfilled` line emitted.
- **`test_dvx_push_warns_when_local_cache_also_missing`**: drop inner
  blob from BOTH remote and local cache; push emits the
  `⚠ N dir blob(s) missing from remote AND local cache` warning.
- **`test_dvx_run_push_each_fills_pre_existing_remote_gap`**: simulate
  the historical `hccs/crashes` state (manifest in remote, inner blobs
  not); re-run with a forced dep change so the stage executes again;
  `--push each`'s post-push gap-fill restores the inner blobs.

## Workaround for affected repos (historical)

Pre-fix workaround used on `hccs/crashes` `ymccmc.dvc` (2026-06-27):
parse local `.dir` manifest, `head-object` each inner md5, `aws s3 cp`
the missing ones directly from `.dvc/cache/files/md5/<prefix>/<rest>`
into S3. Applied; fresh-clone `dvx pull` then succeeded (46 file(s)
fetched, 44 added).

Post-fix this is automatic: `dvx push <dir>.dvc` or any `dvx run
--push each|end` heals existing gaps as a side effect.

## Related

- `specs/done/dir-co-output-push-missing.md` (`099855640`) — fixed the
  upstream cause (local cache wasn't populated for dir outputs). That
  fix prevents NEW manifests from landing in remote without inner
  blobs; this fix heals manifests that ALREADY landed in remote in the
  broken state, and adds belt-and-suspenders for any other path that
  might miss-cache a dir output.
- `specs/done/co-output-push-half-blob.md` — the file-output analog;
  the family started there.

[`099855640`]: https://github.com/runsascoded/dvx/commit/099855640
