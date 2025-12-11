# DVX Roadmap

DVX is a fork/evolution of DVC focused on **self-documenting computational artifacts**. The core idea: each `.dvc` file should encode its own provenance—not just what the artifact *is*, but *how it was produced*.

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

3. **Computation as a DAG node**: Each artifact's computation is a node in a larger workflow graph. DVX should be able to traverse this DAG to determine what needs recomputation.

4. **Directory outputs for multi-file results**: When a computation produces multiple outputs, they go into a DVC-tracked directory. This simplifies the model (one computation → one output path).

---

## Phase 1: Enhanced `.dvc` File Format

### 1.1 Computation Block

Add a `computation` section to `.dvc` files:

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

### 1.2 Execution Metadata (Optional)

For debugging and audit trails:

```yaml
computation:
  # ... core fields above ...

  exec:
    timestamp: "2025-12-11T14:30:00Z"
    duration_seconds: 42.5
    executor: "github-actions"  # or "local", username, etc.
```

### 1.3 Backward Compatibility

- Existing `.dvc` files without `computation` block remain valid
- DVX treats them as "untracked provenance" (imported or legacy data)

---

## Phase 2: Staleness Detection

### 2.1 Freshness Model

An artifact is **fresh** if:
1. Its `computation.code_ref` matches the current commit (or code files haven't changed since that commit)
2. All `computation.deps` hashes match current state of those dependencies
3. The output file's hash matches `outs.md5`

An artifact is **stale** if any of these conditions fail.

### 2.2 Commands

```bash
# Check if an artifact is stale
dvx status output.parquet.dvc

# Show what would need recomputation
dvx status --upstream output.parquet.dvc

# Recompute stale artifacts
dvx repro output.parquet.dvc
```

### 2.3 DAG Traversal

Given a target artifact, DVX traverses `computation.deps` recursively to build the dependency graph. For recomputation:
1. Find all stale ancestors
2. Topologically sort them
3. Recompute in order, updating `.dvc` files as each completes

---

## Phase 3: Selective Recomputation

### 3.1 Pattern Matching for Upstream Control

Users should be able to specify which upstream nodes to recompute vs. use cached:

```bash
# Recompute everything from scratch
dvx repro --force output.dvc

# Recompute only if deps changed, trust cached intermediates
dvx repro output.dvc

# Force recompute of specific upstream pattern
dvx repro --force-upstream "*/normalized/*" output.dvc

# Use cached version of specific upstream even if stale
dvx repro --cached "*/raw/*" output.dvc
```

### 3.2 Glob/Regex Patterns

Support flexible patterns for specifying which nodes to force/cache:
- `*/normalized/*` — any normalized artifacts
- `202506*` — anything for June 2025
- `*.parquet.dvc` — all parquet outputs

---

## Phase 4: Incremental State Tracking (Future)

### 4.1 Problem

For large repos, checking freshness of all artifacts requires reading many files and computing hashes. This can be slow.

### 4.2 Solution: State Cache

Maintain a cached view of project state (similar to `watchman` or git's index):

```
.dvx/
  state.json   # cached hashes and freshness status
  index.db     # optional SQLite for faster queries
```

This cache is:
- Updated incrementally on file changes
- Invalidated by git operations (checkout, pull, merge)
- Optional—DVX works without it, just slower

### 4.3 Reactive Updates

Consider integration with filesystem watchers for real-time freshness tracking in development workflows.

---

## Phase 5: Templated/Parameterized Pipelines

### 5.1 The Problem

Many pipelines have repetitive structure across a dimension (e.g., YYYYMM):
- `normalized/202501.dvc`, `normalized/202502.dvc`, ... all have identical logic
- Only the month parameter differs
- Manually maintaining N identical `.dvc` files is error-prone

### 5.2 Two-Phase Model: Prep + Run

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

### 5.3 DVX as Library + Engine

DVX serves two roles:

1. **Library**: Python API for constructing lazy pipeline representations
   ```python
   from dvx import Artifact, Computation

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
   dvx run --parallel 4 s3/ctbk/normalized/*.dvc
   ```

### 5.4 Lazy Pipeline Representation

Inspired by Dask's `Delayed` abstraction—build a computation graph without executing it:

```python
from dvx import delayed, materialize

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
for a in aggregated:
    a.write_dvc()

# Option B: Write and execute (prep + run)
materialize(aggregated, parallel=4)
```

### 5.5 Why Not Just Dask?

Dask's `Delayed` is appealing but may bring baggage:
- Heavy dependencies (numpy, pandas optional but common)
- Designed for in-process execution, not shell commands
- Graph serialization format doesn't match `.dvc` YAML

**Options**:
1. **Dask-inspired, custom impl**: Borrow the API pattern, implement lightweight
2. **Dask interop**: Let users build graphs with Dask, export to DVX format
3. **Pure functional**: Simple Python functions returning Artifact objects, no decorator magic

Leaning toward (1)—a minimal `dvx.delayed` that feels like Dask but serializes to `.dvc` files.

### 5.6 CLI Pattern for User Projects

Each `ctbk` subcommand gets `prep` and `run` variants:

```bash
# Generate .dvc files for normalized stage
ctbk normalized prep 202501-202512

# Execute (delegates to dvx)
ctbk normalized run 202501-202512
# Equivalent to: dvx run s3/ctbk/normalized/2025*.dvc

# Or just: prep + run in one
ctbk normalized create 202501-202512  # existing behavior, but now uses dvx
```

### 5.7 Handling Complex Parameterization

Some stages have more than just YYYYMM:
- Tripdata: `{JC-,}` × YYYYMM (two zips per month)
- Aggregated: YYYYMM × group_by × aggregate_by

The lazy representation handles this naturally:

```python
def tripdata_zips(ym: str) -> list[Artifact]:
    return [
        Artifact(path=f"tripdata/{ym}-citibike-tripdata.zip", ...),
        Artifact(path=f"tripdata/JC-{ym}-citibike-tripdata.csv.zip", ...),
    ]

def aggregated_variants(ym: str, norm: Artifact) -> list[Artifact]:
    variants = []
    for group_by in ["ge", "gse", "ymrgtb"]:
        for agg_by in ["c", "cd"]:
            variants.append(aggregate(ym, norm, group_by, agg_by))
    return variants
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

This provides stronger reproducibility guarantees than `code_ref` alone.

### 6.2 Artifact Aliasing

For cases where an artifact needs to appear at multiple paths:

```yaml
# In /reports/monthly/202506.parquet.dvc
alias_of: s3/ctbk/aggregated/ge_c_202506.parquet.dvc
```

Uses OS-level linking where available; DVX manages the relationship.

### 6.3 Remote Execution

Track whether computation was local or remote:

```yaml
computation:
  exec:
    remote: "github-actions"
    run_id: "12345"
    logs_url: "https://..."
```

---

## Migration Path

### From DVC

1. Existing `.dvc` files work unchanged
2. `dvx add` creates enhanced `.dvc` files with computation block
3. `dvx migrate` can add computation blocks to existing files (requires re-running the computation or manual annotation)

### From This Repo (ctbk)

The ctbk pipeline currently uses DVC for:
- Importing raw data (`dvc import-url`)
- Tracking generated outputs (`dvc add`)

Migration would involve:
1. Enhancing `has_root_cli.py` to write computation blocks
2. Recording `code_ref` at computation time
3. Explicitly listing input `.dvc` files as deps

---

## Open Questions

1. **Granularity of code_ref**: Should we track the SHA of specific files, or just the repo HEAD? File-level is more precise but complex.

2. **Params handling**: Should params be in the `.dvc` file (inspectable) or referenced from `params.yaml` (DVC-style)?

3. **Lock file**: DVC uses `dvc.lock` for pipelines. Do we need something similar, or is per-artifact `.dvc` sufficient?

4. **Multi-repo deps**: What if a computation depends on artifacts in another repo?

---

## Relationship to DVC Concepts

| DVC Concept | DVX Equivalent |
|-------------|----------------|
| `dvc.yaml` stages | `computation` block in each `.dvc` file |
| `dvc.lock` | Embedded in `.dvc` files (deps with hashes) |
| `dvc repro` | `dvx repro` with DAG traversal |
| Pipeline | Implicit from `computation.deps` graph |

The key difference: DVC separates pipeline definition (`dvc.yaml`) from artifact tracking (`.dvc` files). DVX unifies them—each artifact carries its own pipeline node definition.

---

## Implementation Notes

### Files to Modify

- `dvc/dvcfile.py` — Schema for `.dvc` files
- `dvc/schema.py` — Validation for new fields
- `dvc/stage/` — Stage representation with computation info
- `dvc/commands/` — CLI for new operations

### New Modules

- `dvx/freshness.py` — Staleness detection logic
- `dvx/dag.py` — Dependency graph construction and traversal
- `dvx/state.py` — Optional state caching (Phase 4)
