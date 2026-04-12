# `dvx run` should add outputs to local cache

## Problem

After `dvx run` completes a stage that produces an output file, it updates the `.dvc` file with the new output hash but does NOT copy the output blob into the local DVX cache (`.dvc/cache/files/md5/...`).

Consequence: the output exists only in the working tree. If it gets overwritten by a subsequent stage (e.g. a daily pipeline overwriting `crash-log.parquet`), the previous hash's content is lost locally. Tools that look up historical versions by hash (e.g. `dxdc -R <commit>`, `dvx checkout`) fail with "cache missing".

## Reproduction

```bash
# Initial state
dvx run njsp/data/crash-log.parquet.dvc     # produces hash A
git commit -am "crash log A"

# Cache check
ls .dvc/cache/files/md5/${A:0:2}/${A:2}      # MISSING — bug

# Next day
dvx run njsp/data/crash-log.parquet.dvc     # produces hash B, overwrites file
git commit -am "crash log B"

# Try to look up A
dvx checkout -R HEAD^ njsp/data/crash-log.parquet
# Error: Cache missing for 'HEAD^': dd0a841755c4afe199c3ca3f3853ec11
```

## Compare: `dvx add`

`dvx add` (CLI) calls `add_to_cache()` in `dvx/cache.py`, which copies the file into the cache keyed by MD5. `dvx run` has no equivalent call in its executor path.

## Fix

After a non-side-effect stage produces its output successfully, call `add_to_cache()` on the output path before writing the `.dvc` file. Side-effect stages don't produce outputs, so no change there.

In `executor.py` around line 531-544, after computing `md5 = compute_md5(out)`:

```python
from dvx.cache import add_to_cache
try:
    add_to_cache(out, force=True)  # idempotent if blob already present
except Exception as e:
    self._log(f"  ⚠ {path}: couldn't cache output: {e}")
```

## Related

Also consider: should `dvx run --push` push cached blobs to the remote? Currently `--push` refers to git push, but the S3 blob push is ambiguous. Could be `--push-cache` or similar.
