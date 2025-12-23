# DVX - Minimal Data Version Control

DVX is a lightweight wrapper around [DVC] that provides core data versioning with several enhancements:

- **Parallel pipeline execution** with per-file provenance tracking
- **Decentralized workflow definitions** - each `.dvc` file contains its computation, deps, and outputs
- **Enhanced diff** with preprocessing pipelines and directory support
- **Cache introspection** commands for examining cached data
- **Performance optimizations** for large repos (batched git lookups, mtime caching)

## Why DVX?

### Decentralized Pipelines

DVC stores all pipeline stages in a single `dvc.yaml` file. DVX takes a different approach: each `.dvc` file contains its own computation metadata:

```yaml
# output.parquet.dvc
outs:
  - md5: abc123...
    size: 1048576
    hash: md5
    path: output.parquet
meta:
  computation:
    cmd: python process.py input.parquet output.parquet
    deps:
      - path: input.parquet
        md5: def456...
```

This means:
- **Parallel execution**: Independent artifacts run concurrently
- **Self-contained provenance**: Each output knows exactly how it was created
- **Git-friendly**: Changes to one artifact don't touch other files
- **No lock contention**: Multiple processes can add artifacts simultaneously

### Enhanced Diff

Diff DVC-tracked files between commits, optionally piping through preprocessing commands first. This is especially useful for binary formats like Parquet, gzipped files, or any format that benefits from transformation before diffing.

```bash
# Content diff of a file (HEAD vs worktree)
dvx diff data.csv

# Diff between commits
dvx diff -r HEAD^..HEAD data.csv

# Diff specific commit vs its parent
dvx diff -R abc123 data.csv

# Summary mode: show changed files with hashes
dvx diff -s
dvx diff -s -r HEAD~5..HEAD
```

#### Preprocessing Pipelines

The real power is piping files through commands before diffing:

```bash
# Compare line counts
dvx diff wc -l data.csv

# Compare Parquet schema (using parquet2json)
dvx diff parquet2json {} schema data.parquet

# Compare first row as pretty JSON
dvx diff 'parquet2json {} cat -l 1 | jq .' data.parquet

# Decompress and compare headers of gzipped CSVs
dvx diff 'gunzip -c {} | head -n1' data.csv.gz

# Compare sorted, deduplicated content
dvx diff 'sort {} | uniq' data.txt
```

#### Directory Diffs

When diffing DVC-tracked directories, DVX shows which files changed with their hashes:

```bash
$ dvx diff -R abc123 data/
test.parquet: c07bba3f... -> f46dd86f...
test.txt: e20b902b... -> 9306ec07...
```

### Cache Introspection

Examine cached data without checkout:

```bash
# Get cache path for a tracked file
dvx cache path data.parquet

# Get MD5 hash
dvx cache md5 data.parquet

# View cached file contents directly
dvx cat data.csv

# Works with files inside DVC-tracked directories
dvx cat data_dir/subset.parquet
```

## Installation

```bash
pip install dvx

# With S3 support
pip install dvx[s3]

# With all remote backends
pip install dvx[all]
```

## Usage

### Running Pipelines

```bash
# Run all .dvc computations (parallel by default)
dvx run

# Run specific target
dvx run output.dvc

# Use 4 parallel workers
dvx run -j 4

# Dry-run to see execution plan
dvx run --dry-run

# Force re-run (ignore freshness)
dvx run --force
```

### Tracking Data

```bash
# Initialize
dvx init

# Track files (parallel-safe, lock-free)
dvx add data/
dvx add model.pkl

# Track with dependencies (for provenance)
dvx add output.parquet --dep input.parquet --cmd "python process.py"

# Auto-add stale deps first (recursive)
dvx add -r output.parquet
```

### Status and Diff

```bash
# Check freshness (data vs deps)
dvx status
dvx status -v          # also show fresh files
dvx status --yaml      # detailed YAML output with hashes
dvx status -j4 data/   # parallel checking

# Content diff
dvx diff data.parquet
dvx diff -r HEAD^..HEAD results/
dvx diff -s            # summary mode (files + hashes)
```

### Push/Pull

```bash
# Configure remote
dvx remote add -d myremote s3://mybucket/dvc

# Push to remote
dvx push
dvx push --dry-run     # see what would be pushed

# Pull from remote
dvx pull
dvx pull --dry-run     # see what would be pulled

# Ref-specific operations
dvx pull -r HEAD~3     # pull data as of 3 commits ago
```

### Python API

```python
from dvx import Repo

# Initialize
repo = Repo.init()

# Or open existing
with Repo() as repo:
    repo.add("data/")
    repo.push()

    status = repo.status()
    diff = repo.diff("HEAD~1")
```

## Commands

| Command | Description |
|---------|-------------|
| `run` | Execute computations from .dvc files (parallel) |
| `add` | Track file(s) with optional provenance |
| `status` | Show freshness of tracked files (data & deps) |
| `diff` | Content diff with preprocessing support |
| `cache` | Inspect cache (path, md5, dir) |
| `cat` | View cached file contents |
| `push` | Upload data to remote storage |
| `pull` | Download data from remote storage |
| `fetch` | Download to cache (no checkout) |
| `checkout` | Restore data files from cache |
| `gc` | Garbage collect unused cache |
| `init` | Initialize a DVX repository |
| `remote` | Manage remotes |
| `config` | Configure settings |
| `remove` | Stop tracking file(s) |
| `move` | Move tracked file(s) |
| `import` | Import from another DVC repo |
| `import-url` | Import from a URL |
| `get` | Download without tracking |
| `get-url` | Download URL without tracking |
| `shell-integration` | Output shell aliases |

## What's Different from DVC

### Added in DVX
- `dvx run` - Parallel pipeline execution with per-file provenance
- `dvx cache path/md5` - Cache introspection
- `dvx cat` - View cached files directly
- `dvx diff` preprocessing - Pipe through commands before diffing
- `dvx status --yaml` - Detailed status with hashes
- Lock-free parallel `add` operations
- Git blob batching for faster status checks

### Not Included
DVX intentionally excludes DVC's heavier features:
- **Central pipelines** (`dvc.yaml`, `dvc repro`, `dvc dag`)
- **Experiments** (`dvc exp`, experiment tracking)
- **Metrics/Params/Plots** (`dvc metrics`, `dvc params`, `dvc plots`)

If you need these features, use DVC directly.

## Freshness Model

DVX tracks two types of freshness for each artifact:

1. **Data freshness**: Does the actual data match the hash in the `.dvc` file?
2. **Dep freshness**: Do recorded dependency hashes match the deps' `.dvc` files?

```bash
$ dvx status s3/output/
✗ s3/output/result.parquet.dvc (data changed (abc123... vs def456...))
✗ s3/output/summary.json.dvc (dep changed: s3/input/data.parquet)
✓ s3/output/metadata.json.dvc (up-to-date)
```

### Provenance Tracking

When adding outputs with dependencies:

- **Deps must be fresh**: `dvx add` errors if any dep's hash differs from its `.dvc` hash
- **Recursive add**: Use `dvx add -r` to auto-add stale deps first
- **Accurate recording**: Recorded dep hashes always match what was actually used

## Performance

DVX is optimized for large repos:

- **Mtime caching**: SQLite-backed cache skips unchanged files
- **Batched git lookups**: Single `git ls-tree` call for all blob SHAs
- **Lock-free adds**: Parallel-safe via atomic file writes
- **Parallel status**: Check many files concurrently with `-j/--jobs`
- **Parallel runs**: Independent computations execute concurrently

## Compatibility

- DVX uses `.dvc` files - fully compatible with DVC
- DVX repos are DVC repos - you can use `dvc` commands too
- DVC plugins (dvc-s3, dvc-gs, etc.) work with DVX

## License

Apache 2.0

[DVC]: https://dvc.org
