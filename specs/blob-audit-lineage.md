# Blob Audit and Lineage Tracking

## Problem

DVX stores provenance per-artifact (`meta.computation` in `.dvc` files), but there's no way to query across artifacts: which blobs are used where, which are still necessary, which are orphaned, and what the full dependency graph looks like. DVC's `gc` operates at the hash level (keep referenced hashes, delete unreferenced), but doesn't reason about the *relationships* between blobs across commits.

## Use Cases

### 1. "What blobs does this commit need?"
Given a commit SHA, enumerate all `.dvc` files, their output hashes, and transitively all input hashes. Answer: "to fully reproduce this commit's state, you need these N blobs totaling X MB."

### 2. "Where is this blob used?"
Given a blob hash (or path), find all commits/branches/tags that reference it — either as a direct output or as a transitive dependency. Answer: "this blob is referenced by 3 commits on `main` and 1 tag."

### 3. "Which blobs are generated vs. input?"
Classify all blobs in the cache (or remote) by their provenance:
- **Input**: no computation, added directly via `dvx add` or `dvx import-url`
- **Generated**: has computation, output of `dvx run`
- **Foreign**: imported via `--no-download`, tracked by ETag but not cached locally
- **Orphaned**: in cache but not referenced by any `.dvc` file in any branch/tag/commit

### 4. "What's the minimal cache for this branch?"
Given a branch, compute the minimal set of blobs needed:
- All input blobs (not reproducible)
- Generated blobs only if their inputs are unavailable
- Total size of irreducible inputs

### 5. "Can I safely delete this remote blob?"
Before deleting a blob from S3, check:
- Is it an input blob? (If so, it's irreplaceable — don't delete unless another copy exists)
- Is it generated? (Safe to delete if inputs are available)
- Is it referenced by any commit? (If not, it's orphaned — safe to delete)

## Implementation

### Module structure

```
src/dvx/audit/
  __init__.py       # Exports: scan_workspace, audit_artifact, find_orphans
  model.py          # BlobKind, Reproducibility, BlobInfo, AuditSummary
  scan.py           # Scanning and classification logic
src/dvx/cli/audit.py  # Click command
```

### Data model (`model.py`)

- **`BlobKind`** enum: `INPUT`, `GENERATED`, `FOREIGN`, `ORPHANED`
- **`Reproducibility`** enum: `REPRODUCIBLE`, `NOT_REPRODUCIBLE`, `UNKNOWN`
- **`BlobInfo`** dataclass: path, md5, size, kind, reproducible, cmd, deps, git_deps, in_local_cache, in_remote_cache, is_dir, nfiles
- **`AuditSummary`** dataclass: blobs list + computed aggregates (counts/sizes by kind, cache stats)
  - `to_dict()` for JSON serialization — this is the data contract for the web UI

### Classification logic (`scan.py`)

`classify_blob(DVCFileInfo) → (BlobKind, Reproducibility)`:
- No md5 on outs → `FOREIGN`
- Has `cmd` → `GENERATED`
  - `meta.reproducible: false` → `NOT_REPRODUCIBLE`
  - `meta.reproducible: true` or absent → `REPRODUCIBLE` (positive default)
- No cmd → `INPUT`

Reuses from existing code:
- `find_dvc_files()` from `cache.py` — discovers `.dvc` files
- `read_dvc_file()` from `dvc_files.py` — parses `.dvc` YAML
- `check_local_cache()` from `cache.py` — checks local cache existence
- `read_dir_manifest()` from `dvc_files.py` — reads directory manifest entries for orphan detection

### Orphan detection (`find_orphans`)

1. Collect all md5 hashes referenced by `.dvc` files (output hashes + dep hashes + dir manifest entries)
2. Walk `.dvc/cache/files/md5/` to enumerate all cached blobs
3. Return blobs not in the referenced set, with their sizes

### CLI (`cli/audit.py`)

```
dvx audit                         # workspace summary
dvx audit <path>                  # per-artifact lineage
dvx audit -o/--orphans            # list unreferenced cache blobs
dvx audit -g/--graph              # DOT dependency graph (colored by kind)
dvx audit --json                  # machine-readable (any mode)
dvx audit -r/--remote <name>      # also check remote cache
dvx audit -j/--jobs N             # parallel workers for remote checks
```

### DVCFileInfo extension

Added `reproducible: bool | None = None` field to `DVCFileInfo` in `dvc_files.py`, read from `meta.reproducible` in `read_dvc_file()`.

### Output formats

**Summary** (no args):
```
Blobs in workspace:       34
  Input:                  18 (810 MB)
  Generated:              16 (80 MB, 14 reproducible)
  Foreign:                 0

Local cache:              30 of 34 (870 MB)
  Missing:                 4 (20 MB)
```

**Per-artifact** (`dvx audit <path>`):
```
Path:    www/public/taxes-2025-lots.geojson
MD5:     abc123...
Size:    22.8 MB
Type:    Generated (reproducible)
Command: python -m jc_taxes.geojson_yearly --year 2025 --agg lot

Dependencies (2 data + 2 code):
  [data] data/taxrecords_enriched.parquet  (def456...)
  [code] src/jc_taxes/geojson_yearly.py    (git: aabbcc)

Cache:   local=yes  remote=not checked
```

**Orphans** (`dvx audit --orphans`):
```
4 orphaned blob(s) (12 MB):
  a433cf78...  (8.2 MB)
  b782ee41...  (3.8 MB)
```

**JSON** (`dvx audit --json`): full `AuditSummary.to_dict()` serialized.

**Graph** (`dvx audit --graph`): Graphviz DOT with kind-based node coloring:
- Input = palegreen, Generated = lightblue (lighter if reproducible), Foreign = gray dashed

### Integration with `dvx dag`

The graph output reuses the same conceptual DAG structure as `dvx dag` but colors nodes by `BlobKind` rather than by position (root/leaf/middle). For full graph features (clustering, Mermaid, HTML), use `dvx dag`.

## ML Pipeline Considerations

Large-scale ML training pipelines amplify the audit problem. A single training run may produce:
- **Checkpoints**: dozens of multi-GB model snapshots at different training steps
- **Evaluation artifacts**: metrics, predictions, confusion matrices at each checkpoint
- **Intermediate data**: preprocessed datasets, tokenized corpora, embedding caches

The `meta.reproducible: false` opt-out is particularly important here — expensive training outputs should be explicitly marked non-reproducible to prevent accidental eviction.

## Future (not this PR)

- Cross-commit scanning (which commits reference which blobs)
- `dvx gc --evict-reproducible` (uses audit classification) — see `evictable-generated-blobs.md`
- Remote cache size analysis
- SQLite index for large repos
- UI extension: audit view tab in `ui/` (Vite + React + @xyflow/react)

## Open Questions

- How expensive is scanning `.dvc` files across all commits? For repos with thousands of commits and hundreds of `.dvc` files, this could be slow. The SQLite index amortizes this but adds maintenance burden.
- Should `dvx audit` also check dep *availability* (can this blob actually be regenerated right now)? This requires checking that all transitive inputs exist in cache or remote.
- Should lineage be queryable in the other direction? ("What outputs does this input produce?" — useful for impact analysis when an input changes.)
- For ML pipelines: should DVX integrate with experiment trackers (W&B, MLflow) to correlate blob lineage with training metrics? Or should it stay purely at the data layer and let users join the two?
- Should `dvx audit` support a `--cost` flag that estimates regeneration cost from historical run times logged in `meta.computation`?
