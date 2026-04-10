# Fix targeted `dvx pull` for .dvc files

## Bug

`dvx pull njsp/data/crashes.parquet` fails with:

```
Error: 'njsp/data/crashes.parquet' does not exist as an output or a stage name in 'dvc.yaml'
```

DVX's `pull` delegates to DVC which looks for `dvc.yaml`. But DVX doesn't use `dvc.yaml` — outputs are defined in per-file `.dvc` files.

`dvx pull` with no args works (pulls everything), but targeted pull is broken.

## Expected

```bash
dvx pull njsp/data/crashes.parquet
# reads njsp/data/crashes.parquet.dvc, gets the MD5, pulls from S3 cache
```

Should accept:
- Output path: `dvx pull njsp/data/crashes.parquet`
- .dvc path: `dvx pull njsp/data/crashes.parquet.dvc`
- Directory: `dvx pull njsp/data/` (all .dvc files in that dir)

## Why this matters

In CI, pulling everything is slow and wasteful (6.6GB+ of NJDOT databases). The daily pipeline only needs `njsp/data/crashes.parquet` (~300MB) as a dep for downstream stages.

## Fix

`dvx pull <target>` should:
1. Find the `.dvc` file for the target
2. Read the MD5 hash from it
3. Pull that specific file from the DVC cache/remote (using DVC's cache API, not its pipeline API)
