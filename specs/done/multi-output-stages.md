# Multi-output stages

## Problem

dvx's data model assumes a single output per `.dvc` file. The `DVCFileInfo`
dataclass holds one `md5`, one `size`, and one `path`. `read_dvc_file()` at
`src/dvx/run/dvc_files.py:501` reads only `data["outs"][0]`. Downstream code
that iterates `info.outs` is absent — instead it treats `info.md5` and
`info.path` as scalars throughout (status, run, push/pull, diff).

DVC has supported multi-output stages since its inception: one `cmd` produces
N files, each tracked by an entry under `outs:` in a single `.dvc` file. This
is the natural representation when one script writes a coherent group of
artifacts. dvx loses this affordance.

## Repro

A `.dvc` with multiple outs, all on disk with matching md5s and present in
the cache (e.g. via `dvx add` of each path), still reports as `missing`:

```yaml
# www/public/njsp/csvs.dvc
outs:
- md5: 5cbf247232d2e1685cd346b547c3ce26
  size: 254855
  hash: md5
  path: ytd.parquet
- md5: 6088b46679b985e7857d57a0ac6a212c
  size: 98287
  hash: md5
  path: monthly.parquet
- md5: c3a27aafc06c32f4ab1d9677a01b2b4b
  size: 33991
  hash: md5
  path: month-year.parquet
# ... 2 more
meta:
  computation:
    cmd: njsp update_www_data -f
    deps:
      /njsp/data/crashes.parquet: 634b90a475d405d4cd969c9b0d2a28a0
```

```sh
$ ls -la www/public/njsp/*.parquet  # all present
$ for f in ytd monthly month-year ...; do md5 -q www/public/njsp/$f.parquet; done
# all match the .dvc

$ dvx status -y www/public/njsp/csvs.dvc
www/public/njsp/csvs.dvc:
  status: missing
  reason: output missing
  output_expected: 5cbf247232d2e1685cd346b547c3ce26   # only outs[0] reported

$ dvx run --force www/public/njsp/csvs.dvc
  ✗ www/public/njsp/csvs: command succeeded but output not created
```

The cmd succeeds and writes all 5 parquets. The post-run check at
`src/dvx/run/executor.py:520` does `out = Path(path); if not out.exists()`,
where `path` is the conceptual single output (the .dvc's stem, here
`www/public/njsp/csvs`) — which is a directory or doesn't exist depending on
how `path` was derived. Either way it's not one of the 5 real outputs.

## Motivating use case (`crashes/www/public/njsp/`)

One `njsp update_www_data -f` invocation produces 5 parquets that share
the same source data (`crashes.parquet`):

- `ytd.parquet` (250 KB)
- `monthly.parquet` (98 KB)
- `month-year.parquet` (34 KB)
- `crash-homicide.parquet` (7 KB)
- `year-type-county.parquet` (7 KB)

Currently they're git-tracked, churning ~400 KB/day = ~145 MB/year of git
history bloat. The natural fix is to DVX-track them via a single multi-out
`csvs.dvc`, push to the S3 remote, and drop them from git. The migration is
blocked by this limitation.

Workarounds I considered:

1. **One `.dvc` per output**, each with the same `cmd`. Each file works in
   isolation (verified by `dvx add` producing single-out `.dvc`s that report
   fresh). But running `dvx` on the group re-executes the cmd 5× — wasteful
   and the deduplication story is unclear.
2. **Move parquets into a subdir** and track as a single dir-out (like
   `ymccmc.dvc`). Requires moving files + updating client import paths;
   invasive.
3. **Stay git-tracked** and live with the bloat. What I did. ~145 MB/year.

## Implementation (MVP scope)

### Data model — `src/dvx/run/dvc_files.py`

- New `OutputInfo` dataclass: `path`, `md5`, `size`, `is_dir`, `nfiles`.
- `DVCFileInfo` gains `outs: list[OutputInfo]`. The existing scalar
  fields (`md5`, `size`, `path`, `is_dir`, `nfiles`) stay as plain
  fields populated from `outs[0]` — back-compat shim for callers that
  haven't migrated. New code paths that need to handle multi-out
  iterate `info.outs` directly.
- `read_dvc_file()` iterates every entry in `data["outs"]` into
  `OutputInfo`s, then populates the scalar shims from `outs[0]`.

### Freshness

- `is_output_fresh()` iterates `info.outs`. Reasons are
  `"output missing: <path>"` and `"data changed: <path> (...)"` for
  multi-out; single-out keeps the legacy `"output missing"` /
  `"data changed (...)"` shape so existing tests / log scrapers
  don't break. First failing out short-circuits the check.
- `get_freshness_details()` likewise iterates; `output_expected`
  reports the specific failing out's recorded md5.

### Executor — `src/dvx/run/executor.py`

- Post-run output verification now iterates `info.outs`. When any
  out is missing the failure message names which: `"command succeeded
  but output(s) not created: <name>"`.
- Hash + cache pass: every declared out is hashed, cached via
  `cache_blob`, and a fresh `OutputInfo` is built with the updated
  md5/size/is_dir/nfiles. The .dvc is rewritten with all N entries
  intact via `write_dvc_file(outs=...)`. Pre-fix the executor wrote
  back a single `outs[0]`, **destructively dropping the other N-1
  entries**.
- `_should_run`'s pull-deps trigger (added in the prior
  `run-auto-pull` fix) now matches both `"output missing"` and
  `"output missing: <path>"` and considers the stage materializable
  when any `outs[i].md5` is set.

### Write-back

- `write_dvc_file()` gains an `outs: list[OutputInfo] | None`
  parameter. When provided, takes precedence over scalar
  `md5/size/is_dir/nfiles` and emits one `outs[]` entry per
  `OutputInfo`.

### Cache / push gap-fill

- `push_dir_inner_blobs()` (added in the prior
  `dir-push-shallow-existence-check` fix) iterates every
  `data["outs"]` entry, filtering for `.dir` hashes. Multi-out
  stages with any directory output get correct gap-fill; file-only
  multi-out stages are a no-op for the gap-fill (DVC's native push
  handles them).

### Cleared (works via DVC native)

- `dvx push` / `dvx pull` / `dvx checkout` on a multi-out `.dvc`
  pass through `repo.push/pull/checkout(targets=[<dvc>])`, and DVC
  iterates `outs` natively. No DVX-side change required.

## Out of MVP scope

- **`dvx add` writing multi-out**. Users hand-write the initial
  `csvs.dvc` (with `outs: [...]` paths) and let `dvx run` populate
  the md5s. A `dvx add foo bar baz` form that bundles into one
  multi-out .dvc would need a CLI design ("how does the user signal
  these N paths share one cmd?") that's out of scope here.
- **`dvx diff <single-out-path>`** on a path that's only declared as
  one of N outs in a multi-out .dvc. Today `_get_output_info` reads
  only `outs[0]`; would need a file-inside-multi-out-dvc lookup
  similar to `_get_file_in_dir_hash`. Defer; multi-out .dvc users
  can `dvx diff <dvc-stem>` instead.
- **`get_transfer_status` for `dvx push --dry-run`**: only reports
  `outs[0]` as the per-stage "missing" / "cached" indicator.
  Cosmetic; actual push behavior is correct via the gap-fill +
  DVC's native push.
- **Per-out `dvx status -y` enumeration**: today reports the *first*
  failing out's expected/actual. Listing every failing out would
  require restructuring `FreshnessDetails`; defer.

## Tests — `tests/test_multi_output_stages.py` (11)

- Parser: `read_dvc_file` populates `outs` for single-out (1
  element) and multi-out (3+ elements), preserves `.dir` suffix
  handling.
- Freshness: all-present → fresh; missing-out names path;
  data-change names path; `get_freshness_details` likewise.
- Executor: full `dvx run` flow on 3-out stub creates all 3 outs,
  caches each, rewrites the .dvc with 3 entries (regression of the
  destructive `outs[0]`-only write-back); re-run is skipped;
  partial cmd output names which file is missing.
- Push: `dvx run --push each` on multi-out → all blobs land in
  remote.

## Migration notes (consumers)

Existing single-out `.dvc` files continue to work unchanged — the
change is purely additive. `crashes/www/public/njsp/csvs.dvc` can
be hand-written with `outs:` listing the 5 parquet paths (no md5);
the first `dvx run` populates the hashes + caches the blobs, and
`dvx push csvs.dvc` uploads everything. `git rm --cached` the 5
parquets after the first successful run.

## Family

- `specs/done/dir-co-output-push-missing.md` — sibling family
  (multi-`.dvc` co-outputs sharing one cmd). Different shape:
  multiple `.dvc` files vs. one `.dvc` with multiple outs.
- `specs/done/dir-push-shallow-existence-check.md` —
  `push_dir_inner_blobs` updated here to iterate all outs.
- `specs/done/run-auto-pull.md` — `_should_run`'s materializability
  check updated here to recognize multi-out "output missing" reasons.
