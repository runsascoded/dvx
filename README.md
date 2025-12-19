# DVX - Minimal Data Version Control

DVX is a lightweight wrapper around [DVC](https://dvc.org) that provides only the core data versioning functionality, without pipelines, experiments, metrics, params, or plots.

## Why DVX?

DVC is a powerful tool, but its feature set has grown significantly. If you only need to:
- Track large files with `.dvc` files
- Push/pull data to remote storage (S3, GCS, etc.)
- Version data alongside your code

...then DVX gives you exactly that, with a simpler interface and smaller surface area.

## Installation

```bash
pip install dvx

# With S3 support
pip install dvx[s3]

# With all remote backends
pip install dvx[all]
```

## Usage

### CLI

```bash
# Initialize
dvx init

# Track files (parallel-safe, lock-free)
dvx add data/
dvx add model.pkl

# Configure remote
dvx remote add -d myremote s3://mybucket/dvc

# Push to remote
dvx push

# Pull from remote
dvx pull

# Check status (shows data vs dep freshness)
dvx status
dvx status -v          # also show fresh files
dvx status --json      # JSON output
dvx status -j4 data/   # parallel checking
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

DVX exposes these DVC commands:

| Command | Description |
|---------|-------------|
| `init` | Initialize a DVX/DVC repository |
| `add` | Track file(s) with DVX |
| `push` | Upload data to remote storage |
| `pull` | Download data from remote storage |
| `fetch` | Download data to cache (no checkout) |
| `checkout` | Restore data files from cache |
| `status` | Show freshness of tracked files (data & deps) |
| `diff` | Show changes between revisions |
| `gc` | Garbage collect unused cache |
| `remove` | Stop tracking file(s) |
| `move` | Move tracked file(s) |
| `import` | Import from another DVC repo |
| `import-url` | Import from a URL |
| `get` | Download without tracking |
| `get-url` | Download URL without tracking |
| `config` | Configure settings (delegates to DVC) |
| `remote` | Manage remotes (delegates to DVC) |
| `cache` | Manage cache (delegates to DVC) |

## What's NOT included

DVX intentionally excludes:

- **Pipelines** (`dvc.yaml`, `dvc run`, `dvc repro`, `dvc dag`)
- **Experiments** (`dvc exp`, experiment tracking)
- **Metrics** (`dvc metrics`)
- **Params** (`dvc params`)
- **Plots** (`dvc plots`)
- **Stages** (`dvc stage`)

If you need these features, use DVC directly.

## Freshness Model

DVX tracks two types of freshness for each artifact:

1. **Data freshness**: Does the actual data match the hash in the `.dvc` file?
2. **Dep freshness**: Do recorded dependency hashes match the deps' `.dvc` files?

This mirrors git's model - each `.dvc` file declares what it expects, with no transitivity. If a dependency's data differs from its own `.dvc` file, that's a separate issue for that dependency.

```bash
$ dvx status s3/output/
✗ s3/output/result.parquet.dvc (data changed (abc123... vs def456...))
✗ s3/output/summary.json.dvc (dep changed: s3/input/data.parquet)
✓ s3/output/metadata.json.dvc (up-to-date)
```

## Performance

DVX is optimized for large repos:

- **Mtime caching**: Skips hash computation when file mtime unchanged (SQLite-backed)
- **Batched git lookups**: Uses `git ls-tree -r` for all blob SHAs in one call
- **Lock-free adds**: Parallel-safe cache operations via atomic file writes
- **Parallel status**: Check many files concurrently with `-j/--jobs`

## Compatibility

- DVX uses `.dvc` files - fully compatible with DVC
- DVX repos are DVC repos - you can use `dvc` commands too
- DVC plugins (dvc-s3, dvc-gs, etc.) work with DVX

## License

Apache 2.0
