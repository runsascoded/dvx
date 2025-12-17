# DVX Porting Plan

Porting features from `e/main` and `main` branches (fork approach) to `dvx-wrapper` (composition approach).

## Architecture

**Key Distinction**: This branch (`dvx-wrapper`) uses the **composition approach** - it depends on DVC as a library and wraps it. The `e/main` and `main` branches use the **fork approach** - they fork DVC and modify it from within.

The wrapper approach provides:
- Simpler maintenance (no need to rebase on DVC updates)
- Cleaner separation of DVX-specific features
- Easier installation (just `pip install dvx`)

## Current State (Final)

The `dvx-wrapper` branch now has feature parity with `e/main`:
- Basic DVC wrapper (`src/dvx/repo.py`) delegating to upstream `dvc` package
- Click CLI (`src/dvx/cli.py`) with all DVX-specific commands
- Cache introspection (`src/dvx/cache.py`)
- Run module (`src/dvx/run/`) with:
  - `hash.py` - DVC-compatible MD5 hashing
  - `status.py` - SQLite mtime cache for hash caching
  - `dvc_files.py` - .dvc file read/write with computation blocks, git blob comparison, directory manifest support
  - `artifact.py` - Artifact/Computation dataclasses, `delayed` decorator, `materialize()` with parallel execution
  - `executor.py` - Parallel execution engine
- Test suite (54 tests)

## Completed Features

### 1. Fast Git-Based Dependency Checking ✓
- `get_git_blob_sha()` - get git blob SHA for file at ref
- `has_file_changed_since()` - check if file changed via git blob comparison
- `have_deps_changed_since()` - check if any deps changed since commit
- `is_output_fresh()` uses git blob comparison when `code_ref` is available

### 2. Directory Manifest Support ✓
- `find_parent_dvc_dir()` - walk up tree to find parent .dvc-tracked directory
- `read_dir_manifest()` - read directory manifest JSON from cache
- `get_file_hash_from_dir()` - get hash of file inside tracked directory

### 3. Enhanced Artifact.from_dvc() ✓
- Supports files inside DVC-tracked directories
- Falls back to directory manifest lookup when no direct .dvc file

### 4. Materialize with update_dvc Parameter ✓
- `_run_one_artifact()` helper with `update_dvc` control
- `materialize()` with parallel execution and `update_dvc` parameter
- Proper ThreadPoolExecutor-based parallelism

### 5. Placeholder .dvc Files ✓
- `write_dvc_file()` accepts `md5=None` and `size=None`
- Omits these fields from YAML (doesn't write `null`)
- Enables two-phase prep/run workflow

### 6. Enhanced Status Command ✓
- `dvx status` with computation-aware freshness checking
- `-j/--jobs` for parallel execution
- `-v/--verbose` to show fresh files
- `--json` output mode
- Directory expansion (`dvx status data/` finds all .dvc files)
- Icons: ✓ (fresh), ✗ (stale), ? (missing), ! (error)

### 7. Content Diff Command ✓
- `dvx diff <path>` - actual content diff
- `-s/--summary` - file/hash changes (like old behavior)
- `-r/--refspec` and `-R/--ref` for commit ranges
- `-x/--exec-cmd` for preprocessing pipeline via `dffs`
- `-U/--unified`, `-w/--ignore-whitespace`, `-c/--color` options

### 8. Improved Diff (from latest e/main) ✓
- `CacheStatus` enum and `CacheResult` dataclass for better error handling
- Distinguishes "not tracked" from "cache missing" (suggests `dvc pull`)
- Support for diffing files inside DVC-tracked directories
- Directory diff support - compares manifest changes with file sizes
- `_find_parent_dvc_file()` to locate parent .dvc for files in tracked dirs
- `_diff_directory()` for directory manifest comparison

### 9. dffs as Required Dependency ✓
- Moved `dffs>=0.0.7` from optional to required in `pyproject.toml`

### 10. write_dvc_file with `hash: md5` field ✓
- Always writes `hash: md5` to use new-style DVC cache (not legacy md5-dos2unix)

### 11. Colored Directory Diff Output ✓
- Added files shown in green with `+` prefix
- Removed files shown in red with `-` prefix
- Modified files show both old (red) and new (green) entries
- File sizes from cache lookup included in output

## Verification

All functionality from `e/main` has been ported and verified:
- 54 tests passing
- All core CLI commands working
- Diff, status, run commands fully functional

## Dependencies

- `dffs>=0.0.7` (required, for content diff with preprocessing)
- `dvc>=3.50` (required, upstream DVC library)
- `click>=8.0` (required, CLI framework)
- `pyyaml` with CSafeLoader (already part of DVC deps)
