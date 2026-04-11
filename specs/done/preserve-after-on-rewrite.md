# Bug: `after:` field dropped when DVX rewrites `.dvc` files

## Problem

When DVX runs a stage and rewrites its `.dvc` file (to update dep hashes, `last_run`, etc.), the `after:` ordering field is silently dropped.

## Root cause

`write_dvc_file` in `src/dvx/run/dvc_files.py:484` accepts an `after` parameter, but the call sites in the executor (`src/dvx/run/executor.py:476` and `:530`) don't pass it:

```python
dvc_file = write_dvc_file(
    output_path=Path(path),
    cmd=cmd if self.config.provenance else None,
    deps=deps_hashes if self.config.provenance else None,
    git_deps=git_deps_hashes if self.config.provenance else None,
    fetch_schedule=fetch_schedule,
    fetch_last_run=fetch_last_run,
    # after= is missing!
)
```

The `after` info is available on `artifact.computation.after` (populated from the original `.dvc` file), but never passed through.

## Fix

At both `write_dvc_file` call sites in `executor.py`, pass `after=artifact.computation.after`.

## Repro

Any `.dvc` file with an `after:` block will lose it after `dvx run`:

```yaml
# before
meta:
  computation:
    cmd: some_command
    after:
      - other/stage.dvc

# after dvx run
meta:
  computation:
    cmd: some_command
    # after: gone
```
