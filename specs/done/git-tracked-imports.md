# Git-tracked imports with URL provenance

## Context

Some projects import small files (< 1MB) from HTTP URLs that should be:
1. **Committed to Git** (small enough, useful for diffing/auditing)
2. **Tracked by DVX** for provenance: source URL, download date, hash

Current `dvx import-url` pushes files to the DVC remote (S3) and `.gitignore`s them. For small files this is overkill — Git is the better storage backend, and having the file directly in the repo makes diffs, CI, and collaboration simpler.

### Motivating use case

The [NJ crashes project][crashes] imports annual UCR (Uniform Crime Report) Excel files from NJSP:

```
crime/2018_Uniform_Crime_Report.xlsx          (~150KB)
crime/2019_Uniform_Crime_Report.xlsx          (~130KB)
...
crime/2024-AGENCY-GROUPS-OFFENSE-SUMMARY-REPORT.xlsx  (~200KB)
```

These are small, rarely-changing files downloaded from government URLs. They should be Git-tracked for easy diffing (e.g. verifying quarterly vs annual 2023 data), but we want DVX to record:
- **Source URL** (changes across years/site migrations, e.g. `nj.gov/njsp/ucr/` → `njsp.njoag.gov/crime-reports/`)
- **Download date** (when we fetched it)
- **Hash** (detect if upstream republishes/corrects data)

[crashes]: https://github.com/hudcostreets/crashes

## Implemented behavior

### `dvx import-url --git` / `-G`

A flag that bypasses DVC's cache/gitignore machinery:
1. Downloads the file to the specified path (via `urllib`)
2. Computes MD5, captures HTTP headers (ETag, Last-Modified, Content-Length)
3. Creates a `.dvc` file with URL provenance and `meta.git_tracked: true`
4. **Does NOT** add the file to `.gitignore`
5. **Does NOT** cache the file in DVC

```bash
dvx import-url --git \
  https://njsp.njoag.gov/wp/wp-content/uploads/2023%20Uniform%20Crime%20Report.xlsx \
  -o crime/2023_Uniform_Crime_Report.xlsx
```

Resulting `.dvc` file:

```yaml
deps:
- path: https://njsp.njoag.gov/wp/wp-content/uploads/2023%20Uniform%20Crime%20Report.xlsx
  checksum: '"etag-value"'
  size: 153600
  mtime: '2025-01-30T00:00:00+00:00'   # Last-Modified as ISO 8601
outs:
- md5: a1b2c3d4...
  size: 153600
  hash: md5
  path: 2023_Uniform_Crime_Report.xlsx
meta:
  git_tracked: true
  import:
    fetched: '2026-03-09'
```

`--no-download` is supported: creates `.dvc` from HEAD request metadata only (no md5 in outs).

### `dvx update` behavior

`dvx update crime/2023_Uniform_Crime_Report.xlsx.dvc`:
- Detects `meta.git_tracked: true` and uses `update_git_import()` instead of DVC
- HEAD request to check ETag/Last-Modified
- If changed: re-download, update `.dvc` hash
- If unchanged: no-op
- `--no-download` supported: updates metadata only

### `dvx push` / `dvx pull` behavior

Files with `meta.git_tracked: true` are **skipped** by `get_transfer_status()` — they're in Git, not DVC cache.

## Implementation

### New files

- **`src/dvx/git_import.py`**: Core logic
  - `git_import_url(url, out, no_download)` — download + create `.dvc`
  - `update_git_import(dvc_path, no_download)` — re-check and update
  - `is_git_tracked_import(dvc_path)` — check if `.dvc` is git-tracked
  - `_ensure_not_gitignored(path)` — remove from `.gitignore` if present

### Modified files

- **`src/dvx/cli/external.py`**: Added `-G`/`--git` flag to `import-url`, git-tracked handling in `update`
- **`src/dvx/cache.py`**: `get_transfer_status()` skips git-tracked imports
- **`src/dvx/run/dvc_files.py`**: Added `git_tracked: bool` field to `DVCFileInfo`, read from `meta.git_tracked`

### Design decisions

- **`meta.git_tracked` not `outs[].git_tracked`**: DVC allows arbitrary data in `meta` but rejects unknown keys in `outs`. Putting the flag in `meta` ensures DVC compatibility.
- **No auto-`git add`**: DVX doesn't generally auto-stage files. The user runs `git add` themselves.
- **`urllib` not DVC's downloader**: Keeps the git-tracked path completely independent of DVC's stage system, avoiding `.gitignore` and cache side effects.

## Alternatives considered

### Just use a manifest file

A simpler approach: a `crime/sources.yaml` or similar that lists URLs and expected hashes. No DVX integration, just a convention.

**Downside**: No automated freshness checking, no integration with `dvx status`/`dvx update` workflows, duplicates hash tracking that DVX already does.

### `dvx add --import-url`

Instead of a new flag on `import-url`, extend `dvx add` (which already handles git-tracked files) with an optional `--import-url` to record provenance. This might be a cleaner API since `dvx add` already means "track this file" and the URL is just metadata.

```bash
dvx add crime/2023_Uniform_Crime_Report.xlsx \
  --import-url https://njsp.njoag.gov/wp/wp-content/uploads/2023%20Uniform%20Crime%20Report.xlsx
```

This is arguably better since `import-url` currently implies "DVC-managed remote storage". Could be added later as an alias.

## Future work

- `dvx status` upstream freshness checks (ETag/Last-Modified comparison via `--cloud`)
- Batch import from manifest YAML
- `dvx add --import-url` alias

## Relationship to other specs

- [http-import-last-modified]: Captures `Last-Modified` in `.dvc` deps — prerequisite for meaningful freshness checks
- [blob-audit-lineage]: Audit trail for tracked files — `git_tracked` imports participate in lineage tracking

[http-import-last-modified]: ./http-import-last-modified.md
[blob-audit-lineage]: ../blob-audit-lineage.md
