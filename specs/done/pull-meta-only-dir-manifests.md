# `dvx pull --meta-only`: fetch dir `.dir` manifests without the payload

Origin: surfaced by the ctbk (`~/c/hccs/ctbk`) session's RAC→HCCS reproducibility/migration audit. Processing a new month (`202608`) failed in `cons create` because the `6b67d324` guard makes `cons` depend on **all** normalized directory manifests, but CI's "Pull pipeline inputs" only fetches tripdata + prev-month + aggregates. `read_dir_manifest` reads the **local cache only** (no remote fetch), so the ~158 tiny `.dir` blobs (~243 B each, ~40 KB total) were absent → "manifest missing". Not an HCCS data gap (the blobs are on HCCS R2); a pre-existing CI pull-step gap that would fail identically on RAC, masked because the daily runs are new-month no-ops. ctbk's preferred fix (approach 1, per the CLI-over-scripts rule) was a proper `dvx` feature rather than an inline `aws s3 cp` CI step. Greenlit in the dvx session.

## Problem

There was no way to hydrate a directory output's manifest blob without also pulling its (potentially huge) inner data. `dvx pull <dir>.dvc` fetches the `.dir` manifest **and** every inner file blob (verified: a 3-file dir → 4 blobs fetched). A manifest-derived dependency guard only needs the `.dir` blob itself — 243 B, not the ~190 GiB of normalized data behind it.

## Change

Add `-m/--meta-only` to `dvx pull`:

- Resolves targets (or all `.dvc` files when none given; honors `--glob`) to their directory-output manifest keys (`<md5>.dir`) and fetches **only those blobs** — never the inner file blobs or the payload.
- File (non-dir) outputs have no manifest, so they're skipped and counted in the report.
- `--dry-run` lists the manifest keys that would be fetched, transferring nothing.
- `-R/--ref` + `--meta-only` is rejected (fail loud) — worktree manifests only, for now.
- Partial-failure is fail-loud: if the remote is missing some manifests, the fetched/cached counts are reported and the command errors (nonzero exit).

CI usage (ctbk): `dvx pull -m --glob 's3/ctbk/normalized/*.dvc'` in "Pull pipeline inputs" satisfies the `cons` guard for ~40 KB instead of the full dataset.

## TFFP

`tests/test_transfer.py::TestPullMetaOnly` — a DVC repo tracking one directory (→ `.dir` manifest + 3 inner blobs) and one file, pushed to a local remote with the cache cleared:

- `pull -m data.dvc` → cache holds **exactly** the `.dir` manifest, **zero** inner blobs (the discriminating assertion; a normal `pull data.dvc` on the same fixture fetches all 3 inner blobs + manifest).
- Second `pull -m` → "0 manifest(s) fetched, 1 already cached."
- `pull -m -n` → lists the manifest key, cache stays empty.
- `pull -m solo.txt.dvc` (file target) → "No directory manifests to pull (1 non-dir out(s) skipped)."
- `pull -m -R HEAD` → rejected with the actionable error.

## Resolution

Implemented across:

- **`src/dvx/cache.py`** — new `collect_dir_manifest_hashes(dvc_files) -> (dir_hashes, n_nondir_outs)`: walks the `.dvc` files via `read_dvc_file`, collects `<md5>.dir` keys for `is_dir` outputs (deduped, first-seen order), counts file outputs skipped. Reuses the existing `pull_hashes` (which already handles `.dir`-suffixed keys via `oid_to_path`) and `check_local_cache` for the fetched/cached/failed accounting.
- **`src/dvx/cli/transfer.py`** — `-m/--meta-only` flag on `pull`, dispatching to a new `_pull_meta_only(...)` helper before the normal/ref pull paths. Honors `--glob`, `--dry-run`, `--remote`, `--jobs`; rejects `--ref`; reports `<fetched> manifest(s) fetched, <already> already cached (<n> non-dir out(s) skipped)` and errors on any unfetched manifest.

Full suite green.
