# Fix side-effect stage path resolution in subdirectories

## Bug

Side-effect `.dvc` files in subdirectories fail with "command succeeded but output not created" even though side-effect handling code exists and `is_side_effect` returns `True`.

## Repro

```yaml
# njsp/data/refresh.dvc
meta:
  computation:
    cmd: njsp refresh_data --s3
    fetch:
      schedule: daily
```

```
$ dvx run njsp/data/refresh.dvc
  ⟳ refresh: running...
  ✗ refresh: command succeeded but output not created
```

## Root cause

The executor resolves the artifact path as `refresh` (stripped from `njsp/data/refresh.dvc`). When it calls `read_dvc_file(Path("refresh"))` at line 416 of `executor.py`, it looks for `refresh.dvc` in CWD — not `njsp/data/refresh.dvc`. This returns `None` (file not found), so `is_side_effect` is `False`, and it falls through to the "output not created" check.

The path `refresh` is the *output* path (what the `.dvc` file tracks), but for side-effect stages there is no output file — the `.dvc` file itself is the only artifact. The executor needs to use the original `.dvc` file path, not the derived output path.

## Fix

In `executor.py`, when resolving the `.dvc` file for a given artifact path, use the original `.dvc` file path from the artifact/computation graph — not `Path(path)` which is the output path. The computation graph already knows the `.dvc` file location since it discovered it during the recursive scan.

Alternatively, `read_dvc_file` could search for the `.dvc` file relative to the artifact path: given `refresh`, try `refresh.dvc`, `*/refresh.dvc`, etc. But passing the original path is cleaner.
