# Side-effect stages + cron-triggered inputs

## Problem

DVX currently models pure data transformations: `cmd` + `deps` → `outs`. But real pipelines include:
1. **Side-effect stages**: deploying artifacts (CF Pages, D1 import, Slack/Bsky posting) that don't produce local file outputs
2. **Periodic external data fetches**: inputs from live URLs that should be re-fetched on a schedule, not just when a dep hash changes

Both are needed to model a full CI/CD pipeline as a DVX DAG, replacing ad hoc GHA workflows with `dvx run`.

## 1. Side-effect stages

### Concept

A `.dvc` file can represent a side-effect that was executed "as of" specific input SHAs. The `.dvc` file itself is the record that the effect happened — it gets committed with updated dep hashes after successful execution, serving as a receipt.

```yaml
# www-deploy.dvc
meta:
  computation:
    cmd: wrangler pages deploy www/dist --project-name nj-crashes
    deps:
      www/dist/index.html: a1b2c3d4...
      www/dist/assets/index-xyz.js: e5f6a7b8...
```

**Implementation note:** `side_effect: true` is NOT an explicit field. Side-effect
is inferred from having `meta.computation.cmd` with no `outs`. The `DVCFileInfo.is_side_effect`
property encodes this: `md5 is None and cmd is not None`.

When `dvx status` checks this file:
- It compares dep hashes against current files
- If deps changed → stage is stale → `dvx run` re-executes the cmd
- After successful execution, DVX updates the dep hashes in the `.dvc` file and commits it
- There's no `outs` — the "output" is the side effect in the external system

### Side-effect semantics (inferred from no `outs` + `computation.cmd`)

- No `outs` field (or empty `outs`)
- `dvx status` reports stale if any dep hash changed since last recorded execution
- `dvx run` executes `cmd`, then updates dep hashes in the `.dvc` file
- `dvx push`/`pull` skip side-effect stages (nothing to push to remote storage)
- The `.dvc` file is git-tracked — committing it records "this effect was applied as of these dep hashes"

### Examples

```yaml
# d1-import.dvc
meta:
  computation:
    cmd: ./api/d1-import.sh njsp-crashes www/public/njsp/crashes.db
    deps:
      www/public/njsp/crashes.db: 3e90ac77...

# slack-post.dvc
meta:
  computation:
    cmd: njsp slack sync -r $REFRESH_SHA
    deps:
      njsp/data/crash-log.parquet: d6453b1a...

# www-deploy.dvc
meta:
  computation:
    cmd: cd www && pnpm build && wrangler pages deploy dist --project-name nj-crashes
    deps:
      www/package.json: ...
      www/pnpm-lock.yaml: ...
      www/src/: ...  # directory hash
      www/public/njsp/ytd.csv: ...
      www/public/njsp/monthly.csv: ...
```

### Idempotency

Side-effect stages should be idempotent — re-running with the same inputs produces the same external state. This is true for:
- CF Pages deploy (same dist → same deployment, CF deduplicates)
- D1 import (same .db → same D1 state)
- Slack posting (thrds sync is declarative — same desired state → no changes)

### `dvx run` behavior with side effects

When `dvx run` encounters a stale side-effect stage:
1. Execute `cmd`
2. If exit code 0: update dep hashes in `.dvc`, mark as fresh
3. If exit code non-zero: leave `.dvc` unchanged, report failure
4. The updated `.dvc` file is staged for commit (or auto-committed with `dvx run --commit`)

## 2. Cron-triggered inputs

### Concept

Some inputs come from external sources that change on their own schedule (live APIs, web scraping). DVX should model these as "fetch" stages with a cron-like staleness policy.

```yaml
# njsp/data/FAUQStats2026.xml.dvc
outs:
- md5: abc123...
  path: FAUQStats2026.xml
meta:
  computation:
    cmd: njsp refresh-data
    fetch:
      schedule: "daily"      # or cron: "0 15 * * *"
      last_run: 2026-04-07T15:10:00Z
```

### `fetch.schedule` semantics

- `dvx status` checks if `last_run` + schedule interval has elapsed
- If elapsed → stage is stale, even if the output hash hasn't changed
- `dvx run` executes the fetch cmd, updates `last_run` and output hash
- If the fetched data is identical (same hash) → deps downstream remain fresh
- If the fetched data changed → downstream stages become stale

### Schedule formats

- `"daily"`, `"hourly"`, `"weekly"` — simple intervals
- `"0 15 * * *"` — cron expression (matches existing GHA schedule)
- `"manual"` — never auto-stale, only runs on explicit `dvx run` or `dvx run --force`

### Interaction with CI

In CI, `dvx run` replaces the hand-written pipeline:
```bash
# Instead of:
njsp refresh-data
njsp harmonize_muni_codes
njsp update_projections
njsp update_www_data
cd www && pnpm build && wrangler pages deploy ...
./api/d1-import.sh ...
njsp slack sync ...

# Just:
dvx run  # executes all stale stages in dependency order
```

The cron schedule in the `.dvc` file replaces the GHA cron trigger. GHA becomes a thin `dvx run` wrapper that runs on a schedule.

## 3. Directory deps

Currently DVX deps are individual files with MD5 hashes. For stages like www build, the input is an entire directory tree (`www/src/`). Options:

- **Tree hash**: hash of all file hashes in the directory (like git tree hash)
- **Glob pattern**: `www/src/**/*.{ts,tsx,scss}` with combined hash
- **Git ref**: use the git tree SHA for the directory (fast, already computed)

Recommend git tree SHA — it's free (git already computes it) and captures exactly what's tracked.

## 4. Notebook → CLI migration (crashes-specific)

The crashes repo has several Jupyter notebooks that are effectively CLI scripts with visual outputs:
- `harmonize-muni-codes.ipynb` → `njsp harmonize_muni_codes` (already has CLI wrapper)
- `update-projections.ipynb` → `njsp update_projections` (already has CLI wrapper)

These can become regular DVX stages:
```yaml
# njsp/data/muni_codes.parquet.dvc
meta:
  computation:
    cmd: njsp harmonize_muni_codes
    deps:
      njsp/data/crashes.parquet: cfb770bf...
      www/public/njdot/cc2mc2mn.json: ...
```

The notebooks are vestigial — the CLI wrappers already exist. The notebook format was originally for observability (seeing intermediate DataFrames, plots), but in practice nobody inspects them post-hoc. If observability is needed, the CLI can emit structured logs or write debug artifacts as side outputs.

## Implementation plan

### Phase 1: Side-effect stages ✅
1. ~~Add `side_effect: true` support~~ → Inferred from no `outs` + `computation.cmd`
   - `DVCFileInfo.md5`/`.size` now optional (`str | None = None`)
   - `DVCFileInfo.is_side_effect` property
   - `read_dvc_file()` returns info for no-outs .dvc files with computation
   - `write_dvc_file()` omits `outs` when md5/size are None and cmd is set
2. `dvx status` / `is_output_fresh()` / `get_freshness_details()` check deps only for side-effects
3. `dvx run` executor updates dep hashes in `.dvc` after successful side-effect execution
4. `dvx push`/`pull` skip side-effect stages (no cache to push — TODO: verify)

### Phase 2: Cron/fetch stages
1. Add `fetch.schedule` and `fetch.last_run` to `.dvc` schema
2. `dvx status` checks schedule + last_run for staleness
3. `dvx run` updates `last_run` after fetch

### Phase 3: Directory deps
1. Add directory dep support (git tree SHA or glob hash)
2. Use for www build deps (`www/src/`, `www/public/`)

### Phase 4: Migrate crashes CI to `dvx run`
1. Model remaining stages as `.dvc` files
2. Replace `daily.yml` pipeline with `dvx run` + thin GHA wrapper
3. Remove notebook execution (use CLI wrappers directly)
