# Fix dep freshness check for raw files (no .dvc)

## Bug

When a stage has a dep on a file that doesn't have its own `.dvc` file (or whose `.dvc` file is in a different directory), DVX falls through to "raw file exists, can't verify hash, assume ok" — marking the stage as fresh even when the recorded dep hash is obviously wrong (e.g. all-zeros).

## Repro

```yaml
# njsp/data/harmonize.dvc
meta:
  computation:
    cmd: njsp harmonize_muni_codes
    deps:
      crashes.parquet: 00000000000000000000000000000000
```

`crashes.parquet` exists at `njsp/data/crashes.parquet` but its `.dvc` file is at `njdot/data/crashes.parquet.dvc` (different directory). DVX can't find a `.dvc` for the dep, sees the raw file exists, and says "fresh."

```
$ dvx status njsp/data/harmonize.dvc
✓ njsp/data/harmonize.dvc    # WRONG — dep hash is all-zeros
```

## Expected

DVX should compute the actual MD5 of the raw file and compare against the recorded dep hash. If they don't match, the stage is stale.

```
$ dvx status njsp/data/harmonize.dvc
✗ njsp/data/harmonize.dvc (dep changed: crashes.parquet)
```

## Root cause

In `dvc_files.py` `is_output_fresh()`, the dep check logic:

```python
dep_info = read_dvc_file(dep)
if dep_info is None:
    if not dep.exists():
        return False, f"dep missing: {dep_path}"
    # Raw file exists but no .dvc - can't verify hash, assume ok
    continue  # <-- BUG: should compute hash and compare
```

## Fix

When a dep has no `.dvc` file but the file exists, compute its MD5 and compare against the recorded hash:

```python
if dep_info is None:
    if not dep.exists():
        return False, f"dep missing: {dep_path}"
    # Raw file — compute actual hash and compare
    actual_md5 = compute_md5(dep)
    if actual_md5 != recorded_md5:
        return False, f"dep changed: {dep_path}"
    continue
```

## Impact

This affects all stages with deps on files whose `.dvc` files are in different directories, or on files that are git-tracked (no `.dvc` at all). Common in projects where `.dvc` files aren't colocated with their outputs.

Also affects the "never-run" case: new `.dvc` files with zeroed dep hashes should always be stale, but currently appear fresh.
