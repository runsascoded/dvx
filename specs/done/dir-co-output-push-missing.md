# Directory co-output `.dir` blobs not pushed by `dvx run --push each`

## Symptom

`hccs/crashes` daily CI runs `$DVX www/public/data/njdot/ymccmc.dvc
www/public/data/njdot/ymccmcs.dvc` (one shared cmd: `env -u PYTHONPATH
njdot agg`, two **directory** outputs). The pipeline log reports:

```
⟳ www/public/data/njdot/ymccmcs: running...
✓ www/public/data/njdot/ymccmcs: completed (59.6s)        ← primary
✓ www/public/data/njdot/ymccmc: co-output ready           ← co-output
    📝 committed: Run ymccmcs
    📤 pushed
    📤 cache pushed (0 blob)
```

Git is fast-forwarded with both `.dvc` files at new md5s. But the
`.dir` manifest blobs (and the inner blobs they reference) are **not**
uploaded to S3.

Symptom surfaces 24h later when a downstream consumer (or a `dvx pull`
in the next CI run) tries to fetch the blob and hits `NoSuchKey`.

## Evidence

Cross-referencing `.dvc` md5 history vs `s3://nj-crashes/.dvc/files/md5/`
for the last 10 commits touching each .dvc on `hccs/crashes` `main`:

| Output | Type | History (10 most-recent md5s) → S3 |
|---|---|---|
| `cmymc.db.dvc` | single file | **10/10 in S3** |
| `aashto_supplemented_crashes.parquet.dvc` | single file | **10/10 in S3** |
| `ymccmc.dvc` | directory (.dir) | 9/10 in S3 — the stable manifest IS there, one outlier missing |
| `ymccmcs.dvc` | directory (.dir) | **0/10 in S3** — every recorded `.dir` md5 is missing |

The asymmetry was the giveaway: single-file outputs push reliably,
directory outputs systematically don't.

In the dispatched run that surfaced this ([28300331056]), the pull
stage emitted:

```
ERROR: failed to transfer '66ecbabcc02a656840a1f2ba748d3ba9' - The specified key does not exist.
…
Error: 18 files failed to download
```

Eighteen missing inner-file blobs from a directory whose manifest was
never pushed.

[28300331056]: https://github.com/hudcostreets/nj-crashes/actions/runs/28300331056

## Root cause

`dvx.cache.cache_blob` only supports files. Both `_run_computation`
(`src/dvx/run/executor.py:552`) and `_handle_co_output_inner`
(`src/dvx/run/executor.py:652`) call:

```python
md5 = compute_md5(out)        # returns bare manifest hash for dirs
cache_blob(out, md5)           # → _cache_file → shutil.copy2(...)
```

For a directory output, `shutil.copy2` raises `IsADirectoryError`, which
both call sites silently swallow (`⚠ … couldn't cache output: [Errno 21]
Is a directory: '<path>'`). The .dvc file is then written with
`outs[0].md5 = <hash>.dir` referencing a manifest that doesn't exist in
the local cache.

When `_handle_stage_output` later invokes
`repo.push(targets=[primary.dvc, co_paths.dvc])`, DVC reads each .dvc,
discovers a `.dir` hash, looks for the manifest in
`<root>/.dvc/cache/files/md5/<h[:2]>/<h[2:]>.dir`, finds nothing, and
silently skips. Pushed count: 0.

This is downstream of the prior `co-output-push-half-blob.md` fix —
that fix routed the co-output's .dvc into the push manifest correctly;
it just had no manifest blob to push.

(The hypotheses in the original spec — push recursion bug, or co-output
timing on directories — were both wrong. The bug was strictly local:
the cache step silently no-op'd for dirs and there was nothing for
push to upload.)

### How does `hccs/crashes` complete its daily runs at all?

In the affected production repo the local cache *does* get populated
for directory outputs — but not by `cache_blob`. Likely path: the
`njdot agg` cmd internally uses `dvc commit` / `dvx commit` (or DVC's
own ingestion does it on a follow-up step). For a plain
`dvx run --push each` with no extra dvc plumbing, the local cache is
also empty — easy to reproduce in a scratch repo with two co-output
dir stages (see test below).

Either way the push step is wrong: DVX's run path is responsible for
caching the artifacts it produces, and it wasn't doing so for dirs.

## Fix

Make `dvx.cache.cache_blob` directory-aware. When passed a dir:

1. Walk `dir_path.rglob("*")`, hash each inner file, cache each via
   `_cache_file` keyed by its own md5.
2. Build the DVC-format manifest (`{md5, relpath}` JSON, sorted, with
   `separators=(', ', ': ')`).
3. Verify the recomputed manifest hash matches the caller's `md5`
   (defensive — caller already ran `compute_md5`, but we re-hash here
   to catch caller/file drift).
4. Write the manifest atomically to
   `<cache>/<md5[:2]>/<md5[2:]>.dir`.

Implemented in `src/dvx/cache.py::cache_blob` + new
`_cache_directory` helper (extracted to keep the file-path call site
unchanged). `add_to_cache`'s inline dir-caching loop is unchanged —
it could be refactored to use the helper, but the duplication is
small and `add_to_cache` has additional concerns (size accounting,
.dvc preservation) that complicate sharing.

## Test

`tests/test_run_push_s3.py::test_push_each_uploads_dir_co_output_blobs`:

- Two `.dvc` files share one cmd that produces two directories, each
  with two inner files: `mkdir -p a && echo aaa > a/x.txt && echo aa2
  > a/y.txt && mkdir -p b && echo bbb > b/x.txt && echo bb2 > b/y.txt`.
  `sleep 0.1` widens the co-output race window.
- Pre-fix: `📤 cache pushed (0 blobs)`, both `.dir` manifests missing
  from remote, log warns `couldn't cache output: [Errno 21] Is a
  directory`. Assertion fails on `_remote_has_dir_blob(remote, md5_a)`.
- Post-fix: `📤 cache pushed (6 blobs)` — 2 manifests + 4 inner files
  — and all six end up in `remote/files/md5/`.

Race-tolerant stage assertion mirrors the existing
`test_push_each_uploads_all_co_output_blobs`.

## Workaround for affected `hccs/crashes` repo

If a broken commit is already on `main`:

```
cd ~/c/hccs/crashes
dvx push www/public/data/njdot/ymccmc.dvc
dvx push www/public/data/njdot/ymccmcs.dvc
```

(Local cache has the blobs from however the live `njdot agg` cmd
populates it.) Then re-enable any pre-pull change in
`.github/workflows/daily.yml` that was reverted ([1c31c9d73b4]).

[1c31c9d73b4]: https://github.com/hudcostreets/nj-crashes/commit/1c31c9d73b4

## Related

- `specs/done/co-output-push-half-blob.md` — the file-output analog;
  this spec is the directory-output completion of the same family.
- `src/dvx/cache.py::add_to_cache` — has its own directory-handling
  path that already worked correctly; `cache_blob` was the lagging
  sibling.
