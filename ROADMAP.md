# DVX Roadmap

DVX is a fork/evolution of DVC focused on **self-documenting computational artifacts**. The core idea: each `.dvc` file should encode its own provenance—not just what the artifact *is*, but *how it was produced*.

## Status Overview

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Enhanced `.dvc` File Format | ✅ Complete |
| 2 | Staleness Detection | ✅ Complete |
| 3 | Selective Recomputation | ✅ Complete |
| 4 | Incremental State Tracking | ✅ Complete (SQLite mtime cache) |
| 5 | Python Library API | ✅ Complete |
| 6 | Advanced Features | Future |

---

## Core Philosophy

Traditional DVC treats `.dvc` files as passive storage descriptors:
```yaml
outs:
- md5: abc123...
  path: output.parquet
```

DVX evolves these into **computational records**:
```yaml
outs:
- md5: abc123...
  path: output.parquet
computation:
  cmd: "ctbk agg create -ge -ac 202506"
  code_ref: "a1b2c3d"  # git SHA, not timestamp
  deps:
  - path: s3/ctbk/consolidated/202506.dvc
    md5: def456...
```

## Design Principles

1. **Artifact-centric provenance**: Each `.dvc` file is self-contained—you can understand how any artifact was produced by reading its `.dvc` file alone, without tracing git history or reading source code.

2. **Git SHA over wall-clock time**: Staleness is determined by comparing git SHAs of code and input artifact hashes, never by timestamps. An artifact is "dirty" if its recorded `code_ref` or `deps` hashes don't match current state.

3. **Computation as a DAG node**: Each artifact's computation is a node in a larger workflow graph. DVX traverses this DAG to determine what needs recomputation.

4. **No dvc.yaml**: Unlike DVC, DVX does not use a separate `dvc.yaml` file. All pipeline information is embedded in the `.dvc` files themselves. This eliminates the split-brain problem of keeping two files in sync.

---

## Phase 1: Enhanced `.dvc` File Format ✅

### 1.1 Computation Block

`.dvc` files contain a `computation` section:

```yaml
outs:
- md5: abc123...
  path: output.parquet

computation:
  # What command produced this artifact
  cmd: "python train.py --model rf"

  # Git SHA of the repo when this was last computed
  # Used to detect if code has changed
  code_ref: "a1b2c3d4e5f6..."

  # Input dependencies with their hashes at computation time
  deps:
  - path: data/input.csv.dvc
    md5: 111222...
  - path: src/train.py
    md5: 333444...

  # Optional: explicit parameters (for inspection/debugging)
  params:
    model: "rf"
    split: 0.2
```

### 1.2 Backward Compatibility

- Existing `.dvc` files without `computation` block remain valid
- DVX treats them as "leaf nodes" (imported or legacy data)

---

## Phase 2: Staleness Detection ✅

### 2.1 Freshness Model

An artifact is **fresh** if:
1. Its `computation.code_ref` matches the current commit (or code files haven't changed since that commit)
2. All `computation.deps` hashes match current state of those dependencies
3. The output file's hash matches `outs.md5`

An artifact is **stale** if any of these conditions fail.

### 2.2 Commands

```bash
# Check if artifacts are stale
dvx status output.parquet.dvc

# Show what would need recomputation (including upstream)
dvx status --upstream output.parquet.dvc

# JSON output for programmatic use
dvx status --json output.parquet.dvc

# Recompute stale artifacts
dvx run output.parquet.dvc
```

### 2.3 DAG Traversal

Given a target artifact, DVX traverses `computation.deps` recursively to build the dependency graph. For recomputation:
1. Find all stale ancestors
2. Group into execution levels (artifacts in same level can run in parallel)
3. Execute level by level, updating `.dvc` files as each completes

---

## Phase 3: Selective Recomputation ✅

### 3.1 Pattern Matching for Upstream Control

Users can specify which upstream nodes to recompute vs. use cached:

```bash
# Recompute everything from scratch
dvx run --force output.dvc

# Recompute only if deps changed, trust cached intermediates
dvx run output.dvc

# Force recompute of specific upstream pattern
dvx run --force-upstream "*/normalized/*" output.dvc

# Use cached version of specific upstream even if stale
dvx run --cached "*/raw/*" output.dvc
```

### 3.2 Glob Patterns

Support flexible patterns for specifying which nodes to force/cache:
- `*/normalized/*` — any normalized artifacts
- `202506*` — anything for June 2025
- `*.parquet.dvc` — all parquet outputs

---

## Phase 4: Incremental State Tracking ✅

### 4.1 Problem

For large repos, checking freshness of all artifacts requires reading many files and computing hashes. This can be slow.

### 4.2 Solution: SQLite Mtime Cache

DVX maintains a cached view of file states:

```
.dvx/
  mtime_cache.db   # SQLite database for mtime-based caching
```

The cache maps `(path, mtime, size)` to MD5 hashes, avoiding recomputation when files haven't changed. Cache is automatically invalidated when file metadata changes.

---

## Phase 5: Python Library API ✅

### 5.1 Two-Phase Model: Prep + Run

Separate pipeline **definition** from **execution**:

```bash
# Phase 1: Generate .dvc files (lazy, no computation)
ctbk normalized prep 202501-202512
# Creates: s3/ctbk/normalized/202501.dvc, ..., 202512.dvc
# Each contains computation block but output hash is empty/placeholder

# Phase 2: Execute pending computations
dvx run s3/ctbk/normalized/*.dvc
# Runs computations, fills in output hashes, updates metadata
```

### 5.2 DVX as Library + Engine

DVX serves two roles:

1. **Library**: Python API for constructing lazy pipeline representations
   ```python
   from dvx.run import Artifact, Computation

   def normalized_month(ym: str) -> Artifact:
       return Artifact(
           path=f"s3/ctbk/normalized/{ym}",
           computation=Computation(
               cmd=f"ctbk normalized create {ym}",
               deps=[tripdata_zip(ym)],  # returns another Artifact
           )
       )

   # Generate .dvc files for a range
   for ym in month_range("202501", "202512"):
       normalized_month(ym).write_dvc()
   ```

2. **Engine**: CLI/runtime that executes `.dvc` computations
   ```bash
   dvx run -j 4 s3/ctbk/normalized/*.dvc
   ```

### 5.3 Lazy Pipeline Representation

Inspired by Dask's `Delayed` abstraction—build a computation graph without executing it:

```python
from dvx.run import delayed, materialize, Artifact, Computation

@delayed
def normalize(ym: str, src: Artifact) -> Artifact:
    return Artifact(
        path=f"normalized/{ym}",
        computation=Computation(cmd=f"ctbk norm {ym}", deps=[src])
    )

@delayed
def aggregate(ym: str, src: Artifact, group_by: str) -> Artifact:
    return Artifact(
        path=f"aggregated/{group_by}_{ym}.parquet",
        computation=Computation(
            cmd=f"ctbk agg -g {group_by} {ym}",
            deps=[src],
            params={"group_by": group_by}
        )
    )

# Build lazy graph
months = ["202501", "202502", "202503"]
raw = [tripdata_zip(ym) for ym in months]
normalized = [normalize(ym, r) for ym, r in zip(months, raw)]
aggregated = [aggregate(ym, n, "ge") for ym, n in zip(months, normalized)]

# Option A: Write .dvc files only (prep)
from dvx.run import write_all_dvc
write_all_dvc(aggregated)

# Option B: Write and execute (prep + run)
materialize(aggregated, parallel=4)
```

### 5.4 API Summary

```python
from dvx.run import (
    # Core types
    Artifact,        # Data artifact with optional computation
    Computation,     # Command + deps + params

    # Lazy construction
    delayed,         # Decorator for lazy functions
    write_all_dvc,   # Write .dvc files for artifact graph
    materialize,     # Write .dvc files and execute

    # Execution
    run,             # Execute from .dvc file paths
    ExecutionConfig, # Configuration for execution
    ExecutionResult, # Result of artifact execution
    ParallelExecutor,# Low-level executor class
)
```

---

## Phase 6: Advanced Features (Future)

### 6.1 Docker/Container Support

For maximum reproducibility, support container-based computation:

```yaml
computation:
  container:
    image: "sha256:abc123..."  # immutable image reference
    cmd: "python train.py"
  deps:
    - path: data/input.csv.dvc
      md5: ...
```

### 6.2 Remote Execution

Track whether computation was local or remote:

```yaml
computation:
  exec:
    remote: "github-actions"
    run_id: "12345"
    logs_url: "https://..."
```

---

## Phase 7: Performance Optimization (Future)

### 7.1 Rust-based Hashing

The hash computation for large files is CPU-bound. A Rust extension could provide:
- 2-5x faster MD5 computation via SIMD
- Memory-mapped file reading for large files
- Parallel chunk hashing within a single file

Potential implementation:
```rust
// dvx-hash crate
#[pyo3::pyfunction]
fn compute_md5(path: &str) -> PyResult<String> {
    // SIMD-accelerated MD5 with mmap
}
```

### 7.2 Native DAG Traversal

For very large DAGs (thousands of .dvc files), Rust-based graph traversal could reduce latency:
- Parallel file stat/parsing
- Lock-free concurrent hash map for caching
- Zero-copy YAML parsing

### 7.3 SQLite Optimization

The mtime cache could be optimized:
- WAL mode for concurrent reads (already supported)
- Bulk upserts for fsck operations
- In-memory caching layer

---

## CLI Reference

### dvx run

Execute artifact computations from .dvc files:

```bash
dvx run [OPTIONS] [TARGETS]...

Options:
  -n, --dry-run              Show execution plan without running
  -j, --jobs N               Number of parallel jobs (default: CPU count)
  -f, --force                Force re-run all computations
  --force-upstream PATTERN   Force re-run upstream artifacts matching pattern
  --cached PATTERN           Use cached value for artifacts matching pattern
  --no-provenance            Don't include provenance metadata in .dvc files
  -v, --verbose              Enable verbose output

Examples:
  dvx run                              # Run all *.dvc in current dir
  dvx run output.parquet.dvc           # Run specific artifact
  dvx run normalized/*.dvc             # Run with glob pattern
  dvx run --dry-run                    # Show what would execute
  dvx run --force                      # Force re-run everything
  dvx run -j 4                         # Parallel with 4 workers
```

### dvx status

Check freshness status of artifacts:

```bash
dvx status [OPTIONS] [TARGETS]...

Options:
  -j, --jobs N         Number of parallel workers (default: 4)
  -d, --with-deps      Check upstream dependencies as well
  --json               Output as JSON

Examples:
  dvx status                           # Check all *.dvc in current dir
  dvx status output.parquet.dvc        # Check specific artifact
  dvx status -d output.dvc             # Include upstream deps
  dvx status --json                    # Output as JSON
```

### dvx fsck

Verify artifact hashes and rebuild the hash cache:

```bash
dvx fsck [OPTIONS] [TARGETS]...

Options:
  -j, --jobs N         Number of parallel workers (default: 4)
  --clear-cache        Clear hash cache before verifying (forces full rehash)
  --json               Output as JSON

Examples:
  dvx fsck                             # Verify all **/*.dvc recursively
  dvx fsck s3/tripdata/*.dvc           # Verify specific artifacts
  dvx fsck --clear-cache               # Force full rehash
  dvx fsck -j 8                        # Parallel with 8 workers
```

---

## Module Structure

```
dvx/run/
  __init__.py      # Public API exports
  artifact.py      # Artifact, Computation, delayed, materialize
  executor.py      # ParallelExecutor, run(), ExecutionConfig
  dvc_files.py     # .dvc file read/write, freshness checking
  hash.py          # MD5 computation with mtime caching
  status.py        # SQLite mtime cache for artifact status

dvx/commands/
  run.py           # dvx run command implementation
  status.py        # dvx status command implementation
  fsck.py          # dvx fsck command implementation
```

---

## Relationship to DVC

| DVC Concept | DVX Equivalent |
|-------------|----------------|
| `dvc.yaml` stages | `computation` block in each `.dvc` file |
| `dvc.lock` | Embedded in `.dvc` files (deps with hashes) |
| `dvc repro` | `dvx run` with DAG traversal |
| Pipeline | Implicit from `computation.deps` graph |

The key difference: DVC separates pipeline definition (`dvc.yaml`) from artifact tracking (`.dvc` files). DVX unifies them—each artifact carries its own pipeline node definition. This eliminates the need to keep two files in sync and makes each `.dvc` file truly self-documenting.
