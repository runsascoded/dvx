# Fix `dvx run` circular dependency error for external deps + git dep support

## Problem

`dvx run` reports "Circular dependency detected" when `.dvc` files have deps on files that aren't DVX-tracked (e.g. git-tracked notebooks, PDFs).

## Root Cause

In `src/dvx/run/executor.py:run()` (lines 511-516), when building the artifact graph from `.dvc` files, dependencies are only added to the graph if they have a corresponding `.dvc` file:

```python
if artifact.computation:
    for dep in artifact.computation.deps:
        dep_path = dep.path if isinstance(dep, Artifact) else str(dep)
        dvc_file = Path(str(dep_path) + ".dvc")
        if dvc_file.exists() and dep_path not in artifacts:
            pending.append(dvc_file)
```

External dependencies (git-tracked files like `.pdf`, `.ipynb`) are silently dropped from the graph. Then in `_group_into_levels()`, when checking if all deps are "done", these missing dep paths are never in the `done` set, so the artifact is permanently "not ready". Since nothing is ever ready, it raises "Circular dependency detected".

## Immediate Fix (unblock `dvx run`)

In `run()`, when a dep has no `.dvc` file, add it as a leaf `Artifact` (no computation):

```python
if artifact.computation:
    for dep in artifact.computation.deps:
        dep_path = dep.path if isinstance(dep, Artifact) else str(dep)
        if dep_path not in artifacts:
            dvc_file = Path(str(dep_path) + ".dvc")
            if dvc_file.exists():
                pending.append(dvc_file)
            else:
                # External dependency (no .dvc file) - add as leaf
                artifacts[dep_path] = Artifact(path=dep_path)
```

This way `_group_into_levels` sees leaf artifacts (computation=None) for external deps and immediately adds them to `done` at line 78.

## Design: `git_deps` for git-tracked dependencies

Currently deps use content MD5 hashes:
```yaml
deps:
  data/2025-PATH-Monthly-Ridership-Report.pdf: 22c8cb757209390928b525c6fc5b37f5
  monthly.ipynb: 48c56466c13cd724fe771ab7631fff57
```

But for git-tracked files, using git blob SHAs would be better:
- Freshness checks via `git ls-tree HEAD` (already cached in `_get_blob_cache()` in `dvc_files.py`)
- No need to read file content / compute MD5 for freshness
- Clear semantic distinction: DVX deps (have `.dvc` files, MD5) vs git deps (git-tracked, SHA-1)

### Proposed format

Add a separate `git_deps` key in the computation block:

```yaml
meta:
  computation:
    cmd: papermill monthly.ipynb out/monthly-2025.ipynb -p year 2025
    deps:
      data/2025.pqt: 278da34972b360e3a54aade6cdaf0384
    git_deps:
      data/2025-PATH-Monthly-Ridership-Report.pdf: <git-blob-sha>
      monthly.ipynb: <git-blob-sha>
```

- `deps`: DVX-tracked artifacts (have `.dvc` files). Hash is content MD5.
- `git_deps`: Git-tracked files. Hash is git blob SHA-1.

### Implementation

**`DVCFileInfo`** (`dvc_files.py`): add `git_deps: dict[str, str]` field, populate from `computation.get("git_deps") or {}`.

**`Computation`** (`artifact.py`): add `git_deps: list[Artifact | str | Path]` field.

**`Artifact.from_dvc`** (`artifact.py`): populate `git_deps` from `info.git_deps`.

**`is_output_fresh`** (`dvc_files.py`): for `git_deps`, compare recorded blob SHA against `get_git_blob_sha(dep_path, "HEAD")` (uses the already-cached `_get_blob_cache`).

**`run()`** (`executor.py`): when building graph, git_deps are always leaf nodes (no `.dvc` file). Add them as leaf Artifacts.

**`_group_into_levels`**: no change needed — git deps become leaf artifacts, immediately "done".

**`write_dvc_file`** (`dvc_files.py`): accept `git_deps` param, write into `meta.computation.git_deps`.

**`Computation.get_dep_hashes`**: for git_deps, use `get_git_blob_sha()` instead of `compute_md5()`.

### Script to populate git_deps

The `add-dvc-computations.py` script (or a new one) should:
1. For each `.dvc` file, determine which deps are git-tracked vs DVX-tracked
2. For git-tracked deps, compute `get_git_blob_sha(path, "HEAD")` and put in `git_deps`
3. For DVX-tracked deps, keep in `deps` with content MD5

## Reproduction

In the `hccs/path` repo:
```bash
dvx run -n -v data/2025.pqt.dvc
# Error: Circular dependency detected
```

The `data/2025.pqt.dvc` file has:
```yaml
meta:
  computation:
    cmd: papermill monthly.ipynb out/monthly-2025.ipynb -p year 2025
    deps:
      data/2025-PATH-Monthly-Ridership-Report.pdf: 22c8cb757209390928b525c6fc5b37f5
      monthly.ipynb: 48c56466c13cd724fe771ab7631fff57
```

Neither dep has a `.dvc` file (both are git-tracked).

## Testing

After the fix:
```bash
dvx run -n -v data/2025.pqt.dvc
# Should show: "would run" or "skip (up-to-date)" instead of circular dep error
```

Also test with multi-target:
```bash
dvx run -n -v data/*.dvc www/public/*.dvc
# Should show execution plan with levels
```
