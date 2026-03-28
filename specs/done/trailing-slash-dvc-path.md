# Bug: `dvx add dir/` creates `.dvc` file inside directory instead of beside it

## Problem

When running `dvx add archive/` (with a trailing slash), the `.dvc` file is created at `archive/.dvc` instead of the expected `archive.dvc`.

This happens because `cache.py:add_to_cache` line 383 uses the raw `target` string (which preserves the trailing slash from the CLI argument) instead of `target_path` (a `Path` object which normalizes it):

```python
# Line 323: target_path normalizes the slash
target_path = Path(target)      # Path("archive/") → Path("archive")

# Line 383: BUG — uses raw `target` string, not `target_path`
dvc_path = Path(str(target) + ".dvc")
# "archive/" + ".dvc" = "archive/.dvc"  ← WRONG
# "archive"  + ".dvc" = "archive.dvc"   ← CORRECT
```

DVC convention is `archive.dvc` beside the directory (not inside it). The file inside the directory also means `archive/` can't be gitignored normally, since `archive/.dvc` needs to be tracked by git.

## Fix

Line 383 of `src/dvx/cache.py`:

```python
# Before (bug):
dvc_path = Path(str(target) + ".dvc")

# After (fix):
dvc_path = Path(str(target_path) + ".dvc")
```

`target_path` is already defined on line 323 as `Path(target)`, which strips the trailing slash.

## How to verify

```bash
mkdir -p /tmp/dvx-test && cd /tmp/dvx-test
git init && dvx init
mkdir data && echo "hello" > data/file.txt

# Before fix:
dvx add data/
ls data/.dvc          # BUG: file is here
ls data.dvc           # expected location, does not exist

# After fix:
dvx add data/
ls data.dvc           # CORRECT: file is here
```

## Also check

`dvc_files.py:write_dvc_file` (line 282) uses `output_path` (a `Path` object) so it's not affected. But worth auditing other places where raw string `target` is concatenated with `.dvc` to ensure consistency.

## Found by

Discovered when `dvx add archive/` in the marin-bot Discord archiver project created `archive/.dvc` instead of `archive.dvc`.
