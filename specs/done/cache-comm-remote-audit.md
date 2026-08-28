# `dvx cache comm` — set accounting between local cache, remote(s), and referenced objects

## Motivation

Disk-space triage on a laptop (2026-08-27, `hccs/crashes`, 18 GB local cache) needed the answer to: **"which local cache objects are safe to delete because they're (1) present in a remote and (2) not referenced from HEAD?"** Nothing in dvx/dvc answers this today:

- `dvx gc -w` deletes everything not workspace-referenced **without checking remote presence** — in the crashes audit, 697 objects (0.12 GB) were not in the remote and would have been unrecoverable. It also gives no size accounting before acting.
- `dvc status -c` compares only the *current workspace* refs vs a remote — no cache-wide view, no sizes, no multi-remote support.
- The audit had to be done with an ad-hoc script: `aws s3 ls --recursive` → parse, walk `.dvc/cache/files/md5`, parse HEAD `.dvc` files + expand `.dir` manifests, then set-arithmetic. Result: 18.00 GB local = 13.83 GB HEAD-referenced + 4.05 GB deletable (in remote, not on HEAD) + 0.12 GB unpushed-and-unreferenced.

This is a recurring use case (every "disk full" event) and fits dvx's cache-introspection niche.

## CLI

New subcommand group member: `dvx cache comm` (mirrors `comm`/`comm-x` semantics: set membership across two or more *locations*).

```
dvx cache comm [OPTIONS] [LOCATION...]
```

A **location** is one of:
- `local` — the local cache (`.dvc/cache`)
- a remote name (`s3`, `hf`, …) — objects listed from that remote
- a **ref-set**: `HEAD`, `workspace`, a git rev, `--all-commits`/`-A`, `--all-branches`/`-a` — objects referenced by `.dvc` files at that rev(s), with `.dir` manifests expanded (fetching a missing `.dir` from a remote if needed — see Edge cases)

Default with no args: `local s3-or-first-remote HEAD` (i.e. the triage case).

### Output

A Venn-style summary table, one row per membership pattern, with object counts and total sizes:

```
local  s3     HEAD    objects       size
  ✓     ✓      ✓        1,425   13.83 GB   # healthy: cached, pushed, live
  ✓     ✓      –        1,845    4.05 GB   # deletable locally (recoverable)
  ✓     –      –          697    0.12 GB   # ⚠ unpushed garbage: push or lose
  ✓     –      ✓            0          –   # ⚠ live but unpushed
  –     ✓      ✓          373    0.01 GB   # evictable done right: live, fetchable
  –     ✓      –        2,207    9.1 GB    # remote-only history
  –     –      ✓            1          ?   # ✗ missing everywhere (broken)
```

Options:
- `-o/--only PATTERN` — emit just the object list for one membership pattern (e.g. `-o 'local,s3,!HEAD'`), one `md5 size` per line on stdout, pipe-able straight into deletion or `dvx fetch`. Summary goes to stderr.
- `-j/--json` — machine-readable full breakdown.
- `-H/--human` (default when tty): humanized sizes.
- Remote listings and ref-set expansions should be cached under `.dvc/tmp/comm/` keyed by remote + rev, since `aws s3 ls --recursive` on a big remote is the slow step; `-F/--fresh` refetches.

### `dvx gc --safe`

Flag on the existing `gc`: before deleting any local object, require membership in ≥1 remote (the default remote, or `--remote NAME`, or `--any-remote`). Objects failing the check are reported (count + size) and skipped — composing with all existing retention policies (`-w`, `--keep N`, `--older-than`). `--dry` shows the split. This turns the crashes triage into:

```
dvx gc -w --safe          # delete only what the default remote can restore
```

## Edge cases (all hit in the motivating audit)

- **`.dir` manifest missing locally**: a HEAD-referenced `.dir` object may be absent from local cache (and even from the remote — crashes' `njdot/map.dvc` hit both). Expansion order: local cache → each remote → fall back to hashing the checked-out workspace files (if the output is checked out) → else mark the subtree "unexpandable" and treat its unknown children conservatively (never deletable). Report the condition either way.
- **Legacy cache layout**: support both `.dvc/cache/files/md5/xx/…` and legacy `.dvc/cache/xx/…`, and the same two layouts on remotes.
- **Multi-remote**: `dvx cache comm local s3 hf HEAD` — each remote is its own column; "in any remote" patterns available via `--any-remote` grouping.
- **Workspace links**: deleting cache objects is safe for reflink/copy-linked workspaces; note in docs that hardlink/symlink cache-link modes leave workspace files intact on cache rm (hardlink) or dangling (symlink) — `--safe` gc should warn when `cache.type` includes `symlink`.

## Non-goals

- Modifying remotes (that's `gc -c`, unchanged).
- Content verification (hash re-check) — membership is by key presence, as with dvc's own status.

## Reference

The ad-hoc audit script this replaces (session scratchpad, 2026-08-27): list remote via `aws s3 ls --recursive`; local via cache walk; refs via `git ls-files '*.dvc'` + `git show HEAD:<f>` + yaml-parse `outs[].md5` + `.dir` JSON expansion; then set arithmetic. ~80 lines; every piece belongs in dvx.

## Resolution

**`dvx cache comm`** landed (`src/dvx/comm.py` + `src/dvx/cli/cache/comm.py`):

- Locations: `local`, remote names (with `remote:NAME` disambiguation for
  a remote literally named `local`/`HEAD`/`workspace`), and ref-sets
  (`workspace`, `HEAD`, any git rev, `--all-commits`, `--all-branches`).
  Default no-arg form is the triage case: `local <default-remote> HEAD`.
- Venn table one row per membership pattern with counts + sizes (best-known
  across locations; unknown-size objects flagged `?` / `+?`).
- `-o/--only 'local,s3,!HEAD'` → `key size` lines on stdout, summary on
  stderr (pipe-able). Every location must be mentioned (bare or `!`-prefixed)
  — unmentioned is an error, not a wildcard.
- `-j/--json` full breakdown incl. `unexpandable_dirs`.
- Remote listings cached under `.dvc/tmp/comm/remote-<name>.json`;
  `-F/--fresh` refetches. Listing handles dict-shaped (object stores) and
  generator-shaped (dvc's local wrapper) `find()` results.
- `.dir` expansion order per edge-case #1: local cache → fetch manifest
  from remote (lock-free `_fetch_blob`, lands it in local cache) →
  unexpandable (reported; children treated conservatively). Workspace-hash
  fallback not implemented (the remote-fetch path covered every observed
  case; add if it ever misses).
- Legacy cache layout (`.dvc/cache/xx/…`) read for `local`; remote legacy
  layout not special-cased (the odb's own `path` determines the prefix).

**`dvx gc --safe`** reworked to spec (was: all-or-nothing abort if ANY blob
was unpushed):

- Skip-and-report: blobs absent from the checked remote(s) are reported
  (count + size + first 10 keys) and skipped; the rest delete normally.
- Composes with `--keep` / `--older-than` (partition applied to the
  retention plan's deletable set) and with `-w`/`-a`/`-A` via a dvx-native
  path (DVC's delegated gc can't skip per-object). `-T --safe` errors
  (unsupported yet).
- `--any-remote`: membership in ANY configured remote suffices;
  default is the `--remote`/default remote.
- Deletion decisions always use **fresh** remote listings (a stale cached
  listing could claim a since-deleted remote object is recoverable —
  data loss). The comm *report* keeps using the cache.
- Warns when `cache.type` includes `symlink` (workspace links dangle on
  cache deletion).

**Bonus fix (latent data-loss bug)**: version-aware `gc` never expanded
`.dir` manifests in its keep-set, so `dvx gc --keep 1` computed the inner
blobs of a HEAD-referenced *directory* as deletable (verified pre-fix).
`compute_gc_plan` now expands kept manifests (local → remote fetch) and
hard-errors if a kept manifest is unexpandable rather than risk deleting
unknown children. Cache keys now preserve the `.dir` suffix throughout
(`list_cache_blobs` previously stripped it via `.stem`).

Tests: `tests/test_cache_comm.py` — 14 tests over a real repo + local
remote (object-set primitives, expansion incl. remote-fetch and
unexpandable, membership table, `-o`, `-j`, gc `.dir` retention
regression, `--safe` skip/delete split, `--safe` composed with `--keep`).
