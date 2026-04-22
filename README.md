# DVX - Minimal Data Version Control

DVX is a lightweight wrapper around [DVC] that provides core data versioning with several enhancements:

- **Parallel pipeline execution** with per-file provenance tracking
- **Decentralized workflow definitions** - each `.dvc` file contains its computation, deps, and outputs
- **Side-effect stages** for deploys, posts, and syncs without local file outputs
- **Fetch schedules** for periodic re-fetch of external data (daily/hourly/cron)
- **Enhanced diff** with preprocessing pipelines and directory support
- **Git-tracked imports** with URL provenance for small files
- **Per-stage commits and push** (git + cache blobs) with `dvx.stage` library and `.dvx/config.yml`
- **Transitive staleness** in `dvx status` (colored: `✗` red, `⚠` yellow, `✓` green)
- **Version-aware GC** with `--keep N` and `--older-than` retention policies
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

The real power is piping files through commands before diffing. Use `{}` as a placeholder for the file path (like `find -exec`), or omit it to append the path at the end:

```bash
# Compare Parquet schema (using parquet2json)
dvx diff -R abc123 'parquet2json {} schema' data.parquet

# Compare row counts
dvx diff -R abc123 'parquet2json {} rowcount' data.parquet

# Compare all rows as compact JSON
dvx diff -R abc123 'parquet2json {} cat | jq -c .' data.parquet

# Decompress and compare headers of gzipped CSVs
dvx diff 'gunzip -c {} | head -n1' data.csv.gz

# Compare sorted, deduplicated content
dvx diff 'sort {} | uniq' data.txt
```

#### Directory Diffs

When diffing DVC-tracked directories, DVX shows which files changed with their hashes and sizes:

```bash
$ dvx diff -R abc123 data/
- data/test.parquet  c07bba3f...  1592
+ data/test.parquet  f46dd86f...  1592
- data/test.txt  e20b902b...  20
+ data/test.txt  9306ec07...  35
```

#### Live Examples

Examples below use [ryan-williams/dvc-helpers@test], a small repo with DVC-tracked text and Parquet files:

```bash
git clone -b test https://github.com/ryan-williams/dvc-helpers.git && cd dvc-helpers
dvx pull -A  # fetch all cached data
```

**Text file update** — `seq 10` → `seq 15` ([`0455b50`]):

```bash
$ dvx diff -R 0455b50 test.txt
10a11,15
> 11
> 12
> 13
> 14
> 15
```

**Parquet schema change** — `INT64` → `INT32` ([`f29e52a`]):

```bash
$ dvx diff -R f29e52a 'parquet2json {} schema' test.parquet
2c2
<   OPTIONAL INT64 num;
---
>   OPTIONAL INT32 num;
```

**Parquet row count** — 5 → 8 rows:

```bash
$ dvx diff -R f29e52a 'parquet2json {} rowcount' test.parquet
1c1
< 5
---
> 8
```

**Parquet row data** — 3 rows appended:

```bash
$ dvx diff -R f29e52a 'parquet2json {} cat | jq -c .' test.parquet
5a6,8
> {"num":666,"str":"fff"}
> {"num":777,"str":"ggg"}
> {"num":888,"str":"hhh"}
```

**Directory diff** — files changed inside DVC-tracked directory ([`ae8638a`]):

```bash
$ dvx diff -R ae8638a data/
- data/test.parquet  c07bba3fae2b64207aa92f422506e4a2  1592
+ data/test.parquet  f46dd86f608b1dc00993056c9fc55e6e  1592
- data/test.txt  e20b902b49a98b1a05ed62804c757f94  20
+ data/test.txt  9306ec0709cc72558045559ada26573b  35
```

[ryan-williams/dvc-helpers@test]: https://github.com/ryan-williams/dvc-helpers/tree/test

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

# With cron schedule support
pip install dvx[cron]

# With all remote backends
pip install dvx[all]
```

## Usage

### Running Pipelines

```bash
# Run all .dvc computations (recursive discovery, parallel)
dvx run

# Run specific target
dvx run output.dvc

# Use 4 parallel workers
dvx run -j 4

# Dry-run to see execution plan
dvx run --dry-run

# Force re-run (ignore freshness)
dvx run --force

# Auto-commit after each stage
dvx run --commit

# Push strategies
dvx run --push each     # git-push + cache-push after each per-stage commit
dvx run --push end      # batch commits, single git-push + cache-push at finish
dvx run --push each -P  # -P/--no-cache-push: git-push only, skip blob upload
```

With `--push each|end`, DVX does both `git push` and the equivalent of
`dvx push <target>` (uploading the just-produced blob to the configured
remote). This prevents downstream runs / fresh clones from hitting
"cache files do not exist neither locally nor on remote". Use
`-P`/`--no-cache-push` to opt out.

Outputs are also copied into the local DVC cache on every `dvx run`, so
historical versions remain retrievable after subsequent runs overwrite
the workspace file.

Commands run with CWD set to the `.dvc` file's directory, so `./deploy.sh` in `www/deploy.dvc` runs from `www/`.

#### Per-Stage Commits

Stages can trigger commits by writing to `$DVX_COMMIT_MSG_FILE` (set by DVX before each cmd):

```bash
# In your stage script:
echo "Refresh data: 5 new records" > "$DVX_COMMIT_MSG_FILE"
```

Or using the Python library:

```python
from dvx.stage import stage

stage.commit("Refresh data: 5 new records")
stage.summary("5 new records found")
stage.push()  # request immediate git-push + cache-push for this stage
```

With `dvx run --commit`, stages that don't write a commit message get a default one (e.g. "Run refresh"). DVX also sets `$DVX_SUMMARY_FILE` and `$DVX_PUSH_FILE` env vars.

#### Configuration

Create `.dvx/config.yml` (or `dvx.yml`) for defaults and per-stage overrides:

```yaml
run:
  commit: auto       # auto | always | never
  push: end          # never | each | end
  stages:
    deploy.dvc:
      push: each     # push immediately after deploy
    import.dvc:
      commit: never  # don't commit for this stage
```

Priority: CLI flags > `$DVX_PUSH`/`$DVX_COMMIT` env vars > per-stage config > global config.

#### Error Output

When a stage fails, DVX shows the exit code, last 20 lines of stderr, and saves the full log:

```
  ✗ refresh: failed (exit code 1)

    stderr (last 20 lines):
      ConnectionError: Failed to fetch https://...

    Full output: tmp/dvx-run-refresh.log
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
# Check freshness (data vs deps). Output is grouped by status
# (Stale → Missing → Transitive → Error → Fresh) with colored headers.
dvx status                 # ✗ red, ? magenta, ⚠ yellow (transitive), ✓ green
dvx status -v              # also show fresh files
dvx status -G              # -G/--no-group: flatten, no per-status headers
dvx status --yaml          # detailed YAML output with hashes
dvx status -j4 data/       # parallel checking
dvx status --no-transitive # hide transitively stale stages

# Filter by status (comma-sep, prefix-matched: s/m/f/e/t)
dvx status -x m            # -x/--omit: hide missing (? paths)
dvx status -s s,t          # -s/--status: show only stale + transitive

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

# Pull specific targets (no dvc.yaml needed)
dvx pull data.parquet          # by output path
dvx pull data.parquet.dvc      # by .dvc path
dvx pull njsp/data/            # all .dvc files in directory

# Ref-specific operations
dvx pull -r HEAD~3     # pull data as of 3 commits ago
```

### Garbage Collection

```bash
# Keep only blobs referenced by HEAD
dvx gc -w

# Keep 5 most recent versions per artifact
dvx gc --keep 5

# Delete versions older than 30 days
dvx gc --older-than 30d

# Consider all local branches (not just HEAD)
dvx gc --keep 10 -a

# Dry-run: show what would be deleted
dvx gc --keep 3 --dry

# GC specific artifact
dvx gc --keep 5 data.parquet.dvc
```

Version-aware GC walks git history to find all versions of each `.dvc` file, then applies retention policies to determine which cached blobs to keep.

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
| `audit` | Blob classification, lineage, and cache analysis |
| `cache` | Inspect cache (path, md5, dir) |
| `cat` | View cached file contents |
| `push` | Upload data to remote storage |
| `pull` | Download data from remote storage |
| `fetch` | Download to cache (no checkout) |
| `checkout` | Restore data files from cache |
| `gc` | Garbage collect with `--keep N`, `--older-than`, version-aware retention |
| `init` | Initialize a DVX repository |
| `remote` | Manage remotes |
| `config` | Configure settings |
| `remove` | Stop tracking file(s) |
| `move` | Move tracked file(s) |
| `import` | Import from another DVC repo |
| `import-url` | Import from a URL (`--git` for git-tracked, `-A` for User-Agent) |
| `update` | Re-fetch imported data from source |
| `get` | Download without tracking |
| `get-url` | Download URL without tracking |
| `shell-integration` | Output shell aliases |

## What's Different from DVC

### Added in DVX
- `dvx run` - Parallel pipeline execution with per-file provenance
- Side-effect stages - Deploys/syncs modeled as `.dvc` files with no `outs`
- Fetch schedules - Periodic re-fetch with daily/hourly/weekly/cron staleness
- Directory dependencies - Git tree SHA tracking for `git_deps`
- `dvx import-url --git` - Git-tracked imports with URL provenance
- Per-stage commits - `$DVX_COMMIT_MSG_FILE` env var + `--commit` flag
- Per-stage push (`--push each|end`) - both `git push` and cache-blob push, opt out with `-P`
- `dvx run` caches outputs locally - historical versions stay retrievable
- Transitive staleness - `dvx status` shows `⚠` for indirectly stale stages
- `dvx status` grouping + `-s`/`-x` filters (prefix-matched status names)
- Version-aware GC - `dvx gc --keep N --older-than` with git history walk
- Colored status output - `✗` red, `⚠` yellow, `?` magenta, `✓` green
- Detailed error output - Exit code, stderr tail, log file on failure
- Stage output on success - `-v` shows inline, always saves to log file
- `dvx diff` preprocessing - Pipe through commands before diffing (with `{}` placeholder)
- `dvx audit` - Blob classification (input/generated/foreign), lineage, orphan detection
- `dvx cache path/md5` - Cache introspection
- `dvx cat` - View cached files directly
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

DVX tracks three types of freshness for each artifact:

1. **Data freshness**: Does the actual data match the hash in the `.dvc` file?
2. **Dep freshness**: Do recorded dependency hashes match the deps' `.dvc` files?
3. **Transitive freshness**: Are any upstream ancestors stale?

```bash
$ dvx status s3/output/
✗ s3/output/result.parquet.dvc (data changed (abc123... vs def456...))
✗ s3/output/summary.json.dvc (dep changed: s3/input/data.parquet)
⚠ s3/output/report.json.dvc (upstream stale: s3/output/summary.json.dvc)
✓ s3/output/metadata.json.dvc (up-to-date)
```

### Provenance Tracking

When adding outputs with dependencies:

- **Deps must be fresh**: `dvx add` errors if any dep's hash differs from its `.dvc` hash
- **Recursive add**: Use `dvx add -r` to auto-add stale deps first
- **Accurate recording**: Recorded dep hashes always match what was actually used

## Side-Effect Stages

Not all pipeline stages produce local file outputs. Deploys, database imports, Slack posts — these are side effects. DVX models them as `.dvc` files with `meta.computation` but no `outs`:

```yaml
# www-deploy.dvc
meta:
  computation:
    cmd: wrangler pages deploy www/dist --project-name my-app
    deps:
      www/dist/index.html: a1b2c3d4...
      www/dist/assets/app.js: e5f6a7b8...
```

- `dvx status` reports stale when dep hashes change
- `dvx run` executes the command and updates dep hashes
- No cache push/pull — the `.dvc` file itself is the receipt
- Side-effect is inferred from no `outs` + having a `cmd` (optionally explicit via `computation.side_effect: true`)

## Fetch Schedules

External data sources change on their own schedule. DVX can track periodic fetches with a `fetch.schedule`:

```yaml
# data/live-feed.xml.dvc
outs:
- md5: abc123...
  path: live-feed.xml
meta:
  computation:
    cmd: curl -o live-feed.xml https://api.example.com/feed
    fetch:
      schedule: daily        # or "hourly", "weekly", "0 15 * * *", "manual"
      last_run: 2026-04-07T15:10:00Z
```

- `dvx status` reports stale when `last_run + interval` has elapsed
- `dvx run` executes the fetch and updates `last_run`
- If fetched data is identical (same hash), downstream stages stay fresh
- `"manual"` schedule is never auto-stale — only runs on `dvx run --force`
- Cron expressions require the optional `croniter` package: `pip install dvx[cron]`

## Directory Dependencies

Stages can depend on entire directory trees using `git_deps`. DVX uses git tree SHAs, which change when any file in the directory changes:

```yaml
# bundle.js.dvc
outs:
- md5: def456...
  path: bundle.js
meta:
  computation:
    cmd: cd www && pnpm build
    git_deps:
      www/src: abc123tree...     # tree SHA — any file change invalidates
      www/package.json: def456blob...  # blob SHA — individual file
```

## Git-Tracked Imports

For small files from URLs (configs, metadata), use `--git` to track in Git instead of DVC cache:

```bash
# Import and commit to Git (not DVC cache)
dvx import-url --git https://example.com/config.json

# With custom User-Agent (persisted for updates)
dvx import-url --git -A "MyBot/1.0" https://api.example.com/data.json

# Update: re-checks ETag/Last-Modified, re-downloads if changed
dvx update config.json.dvc
```

The `.dvc` file stores URL provenance (ETag, Last-Modified, size, User-Agent) so `dvx update` knows how to re-fetch.

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

[DVC]: https://github.com/iterative/dvc
