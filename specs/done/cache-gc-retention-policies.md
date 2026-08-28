# Cache GC with CLI-driven retention

## Problem

DVX cache accumulates historical versions of artifacts. No mechanism to prune old versions. For daily-regenerated artifacts, unbounded growth.

## Design: CLI-driven, not config-driven

Retention policies belong at invocation time, not in `.dvc` files. The decision to GC is context-dependent (which branches matter, which remote, how aggressive). Codified as `dvx gc` commands in CI workflows.

## P1: `dvx gc` CLI

```bash
dvx gc                             # delete blobs not referenced by HEAD
dvx gc --keep 5                    # keep 5 most recent versions per artifact
dvx gc --older-than 30d            # delete versions older than 30d
dvx gc --all-branches              # consider all local branches (not just HEAD)
dvx gc --dry-run                   # show what would be deleted
dvx gc crashes.parquet.dvc         # target specific artifact
dvx gc --remote myremote           # also delete from remote cache
```

### Core operations

1. **List cached versions per artifact**: walk git log for each `.dvc` file, extract `(hash, commit_date)` pairs. Each commit that changed the `.dvc` introduced a new hash.

2. **Determine "in use" hashes**: referenced by HEAD (default), any local branch (`--all-branches`), or specific refs.

3. **Apply retention filter**:
   - No flags: keep only hashes referenced by HEAD (DVC's current behavior)
   - `--keep N`: keep the N most recent distinct hashes per artifact
   - `--older-than <duration>`: keep hashes newer than duration
   - Filters combine: `--keep 5 --older-than 30d` keeps hashes matching either criterion

4. **Delete**: remove unreferenced blobs from local cache. With `--remote`, also from remote.

### Duration format

`30d`, `7d`, `24h`, `1w` — simple suffixes. No need for cron/ISO.

## P2 (sketch): DVX↔Git relation DB

### Motivation

Walking `git log -p -- *.dvc` for every GC is O(commits × artifacts). An incremental local DB caches the DVX↔Git mapping so subsequent operations are fast.

### Schema (SQLite, untracked)

```sql
-- Which blob hash was current for which .dvc file at which commit range
CREATE TABLE blob_refs (
    dvc_path    TEXT NOT NULL,    -- e.g. "njsp/data/crashes.parquet.dvc"
    md5         TEXT NOT NULL,    -- blob hash from outs[0].md5
    start_sha   TEXT NOT NULL,    -- first commit with this hash
    end_sha     TEXT,             -- last commit with this hash (NULL = still current)
    start_date  TEXT NOT NULL,    -- author date of start_sha (ISO 8601)
    end_date    TEXT,             -- author date of end_sha
    PRIMARY KEY (dvc_path, start_sha)
);

-- Tracks how far we've indexed each .dvc file
CREATE TABLE index_state (
    dvc_path    TEXT PRIMARY KEY,
    last_sha    TEXT NOT NULL,    -- most recent commit we've indexed
    last_date   TEXT NOT NULL
);
```

### Range compaction

Each row represents a *range* of commits where a blob was current, not one row per commit. When indexing new commits:

1. Read `index_state` for the `.dvc` file
2. Walk `git log` from `last_sha..HEAD`
3. For each commit that changed the `.dvc` file:
   - Close the previous range (`end_sha`, `end_date`)
   - Insert new range (`start_sha`, `end_sha=NULL`)
4. If the `.dvc` file didn't change between `last_sha` and HEAD, extend the current range's `end_sha` (or leave NULL for "still current")

### Monotonicity

Author/committer dates are not guaranteed monotonic (rebases, cherry-picks, clock skew). The DB stores dates for display/filtering but range membership is defined by commit graph topology (SHA ancestry), not dates. For `--older-than`, we use the *start_date* of the range — the date the hash was introduced.

### Pathological cases

- **Reverted hashes**: same md5 appears in multiple disjoint ranges. Each range is a separate row.
- **Non-monotonic dates**: conservative — if a range's start_date is newer than its end_date (rebase artifact), treat the whole range as "newer" for retention purposes.
- **Orphan branches**: `--all-branches` indexes all local branches independently. Cross-branch dedup happens at the blob level (same md5 = same cache entry).

### Incremental update cost

After initial index: O(new_commits × changed_dvc_files) per update. Typical daily CI: ~1-5 commits, ~2-3 changed `.dvc` files → milliseconds.

### Uses beyond GC

The relation DB enables:
- `dvx log crashes.parquet` — show version history with dates and sizes
- `dvx checkout --ref abc123 crashes.parquet` — checkout a specific historical version
- `dvx audit` — cross-reference cached blobs against git history
- Cache hit/miss analysis — which blobs are cached locally vs need remote fetch

## Implementation order

### P1 (this PR)
1. `dvx gc` command with `--keep`, `--older-than`, `--all-branches`, `--dry-run`
2. Version listing via `git log` (no DB yet — direct git queries)
3. Local cache deletion

### P2 (future)
1. SQLite relation DB with range-compacted `blob_refs`
2. Incremental indexing (`dvx index` or auto on `dvx gc`)
3. `dvx log` command
4. Migrate `dvx gc` to use DB instead of direct git queries

## Resolution

**P1 is complete.** The core `dvx gc` CLI (`--keep N`, `--older-than`,
`--all-branches`, `--dry`/`-n`, per-target GC, direct-git version
listing via `get_artifact_versions`, local cache deletion) had already
landed in earlier work (`src/dvx/gc.py` + the `gc` command in
`src/dvx/cli/main.py`). This session closed the remaining gaps:

- **`.dir`-manifest expansion in the keep-set** — without it, version-aware
  gc would delete the inner blobs of kept/referenced *directories*
  (verified pre-fix: `dvx gc --keep 1` planned deletion of a
  HEAD-referenced dir's inner blob). Kept manifests now expand
  (local cache → remote fetch); unexpandable kept manifests hard-error
  instead of risking unknown children.
- **`--safe`** (spec'd separately in `cache-comm-remote-audit.md`):
  delete only blobs a remote can restore; skip + report the rest.
  Composes with `--keep`/`--older-than` and `-w`/`-a`/`-A`.
- Cache keys preserve the `.dir` suffix throughout (manifest and
  same-hash file blob are distinct objects).

Deviations from the spec sketch: combined `--keep 5 --older-than 30d`
keeps hashes matching *either* criterion (as spec'd); `--all-commits`
for the retention path is approximated by `--all-branches` version
walking (each branch's full log is already walked — a commit outside
any branch can't retain versions).

**P2 (SQLite relation DB, `dvx log`, incremental indexing) remains
deferred** — direct git queries are fast enough at current repo scales
(nj-crashes: ~1,400 .dvc files, sub-second plans). Revisit when a repo's
`gc --keep` wall-time hurts, or when `dvx log` / `dvx checkout --ref`
consumers materialize.
