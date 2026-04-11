# Bug: Side-effect stages write stale dep hashes to `.dvc` after execution

## Problem

After `dvx run` executes a side-effect stage, it writes back the *old* dep hashes to the `.dvc` file instead of recomputing them from current file contents. This means the `.dvc` file never reflects that the stage ran against updated dependencies.

## Root Cause

`Computation.get_dep_hashes()` in `run/artifact.py:90-109` prefers `dep.md5` (the hash loaded from the `.dvc` file) over recomputing:

```python
def get_dep_hashes(self) -> dict[str, str]:
    hashes = {}
    for dep in self.deps:
        if isinstance(dep, Artifact):
            path = Path(dep.path)
            # If artifact has a known hash, use it  <-- BUG: always true when loaded from .dvc
            if dep.md5:
                hashes[str(path)] = dep.md5
            elif path.exists():
                hashes[str(path)] = compute_md5(path)
    ...
```

When an `Artifact` is loaded via `from_dvc()` (artifact.py:217), deps get `md5` set from the stored `.dvc` values:
```python
deps = [Artifact(path=dep_path, md5=dep_md5) for dep_path, dep_md5 in info.deps.items()]
```

So `get_dep_hashes()` always returns the stale stored hash, and `write_dvc_file()` writes it back unchanged.

## Impact

- Side-effect `.dvc` files never update their dep hashes after execution
- `dvx status` sees the stage as stale again on the next run (deps changed but `.dvc` still has old hashes)
- Commits after side-effect stages contain no actual diff (empty commits)

## Reproduction

```yaml
# slack_post.dvc before run
meta:
  computation:
    cmd: ./slack_post.sh
    deps:
      crash-log.parquet: 933fdbd9cfdeee194ed4860d78526661  # old hash
```

```bash
# crash-log.parquet has been updated, new hash is c2ab003abe682035c56d43ec28a5eb12
dvx run --commit njsp/data/slack_post.dvc
# Commits, but .dvc file still has 933fdbd9... — no diff in commit
```

## Fix

In `executor.py` around line 472-476, after a stage runs, recompute dep hashes from disk instead of using the loaded (stale) values. Either:

1. **Preferred**: Add a `recompute=True` parameter to `get_dep_hashes()` that forces `compute_md5(path)` even when `dep.md5` is set, and use it in the post-execution write path.

2. **Alternative**: Clear `dep.md5` on all deps before calling `get_dep_hashes()` after execution.

The same issue may affect `get_git_dep_hashes()` (line 111-134) — it also prefers `dep.md5` over `get_git_object_sha()`.

## Scope

This affects all stage types after execution (not just side-effects), but it's most visible for side-effects because there's no output hash change to create a diff in the commit. For normal stages, the output hash does change, masking the fact that dep hashes are also stale.
