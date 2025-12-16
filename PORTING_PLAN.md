# DVX Porting Plan

Porting features from `e/main` and `main` branches (fork approach) to `dvx-wrapper` (composition approach).

## Current State (Updated)

The `dvx-wrapper` branch now has:
- Basic DVC wrapper (`src/dvx/repo.py`) delegating to upstream `dvc` package
- Click CLI (`src/dvx/cli.py`) with core commands + DVX enhancements
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
- `-x/--exec-cmd` for preprocessing pipeline (requires `dffs` package)
- `-U/--unified`, `-w/--ignore-whitespace`, `-c/--color` options

## Remaining / Future Work

### Additional Features (from EC2 work)

*To be added as Ryan provides more context from EC2 node work.*

### Testing

- Add unit tests for new directory manifest functions
- Add integration tests for enhanced status/diff commands
- Current: 54 tests passing

## Dependencies

- `dffs` package (optional, for content diff with preprocessing)
- `pyyaml` with CSafeLoader (already using)
