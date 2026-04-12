# Repo-root-absolute dep path shorthand on write

## Problem

When a `.dvc` file depends on artifacts outside its own directory, DVX writes
the dep path via `os.path.relpath()`, producing `../`-laden strings:

```yaml
# www/public/njsp/csvs.dvc
deps:
  ../../../njsp/data/crashes.parquet: e9799868d9114846a7052100454cde51
```

These `../../../` prefixes are hard to scan at a glance, change meaning based
on which `.dvc` file they live in, and get noisier the deeper the `.dvc` file
is nested.

A shorter form is already supported on **read** (`src/dvx/run/dvc_files.py`
lines 108-126): a leading `/` in a dep path is treated as repo-root-absolute,
so the above is equivalent to:

```yaml
deps:
  /njsp/data/crashes.parquet: e9799868d9114846a7052100454cde51
```

But `_relativize_dep_paths` (same file, L129-153) is purely `os.path.relpath`
based, so DVX writes back the `../` form and hand-edited `/`-shorthand paths
revert on the next `dvx run` that rewrites the file.

## Proposal

In `_relativize_dep_paths`, prefer the repo-root-absolute shorthand for any
dep whose relpath from `dvc_dir` would contain `..`.

### Write rule

For each `(dep_path, hash)`:

- If `dep_path` is inside `dvc_dir` (i.e. `relpath(dep_path, dvc_dir)` has no
  `..`): write as-is relative (current behavior).
  - e.g. `njsp/data/refresh.dvc` with dep `njsp/data/FAUQStats2024.xml`
    → writes as `FAUQStats2024.xml` (dvc-dir-relative, no `/` prefix).
- If `dep_path` is outside `dvc_dir` (relpath would start with `../`): write
  as `/<dep_path>` (repo-root-absolute).
  - e.g. `www/public/njsp/csvs.dvc` with dep `njsp/data/crashes.parquet`
    → writes as `/njsp/data/crashes.parquet` (not `../../../njsp/data/...`).

### Rationale

- Humans scanning `.dvc` files in deeply-nested directories don't have to
  count `../` segments to locate the dep.
- Moving a `.dvc` file doesn't invalidate dep paths that point outside its
  directory — `/path` is stable, `../../path` is not.
- No migration needed for older files: reads continue to accept both forms.
- The `/` prefix is visually distinct from the bare `foo/bar` form used for
  same-directory deps, making it easy to tell at a glance whether a dep
  lives inside or outside the stage's own dir.

### Edge cases

- `dvc_dir == "."` (stage file is at repo root): paths are already
  repo-root-relative; don't add `/` prefix. Leave as-is.
- `dvc_dir` absolute (unusual; current code preserves this): don't modify.
- Windows: `os.path.relpath` may raise `ValueError` across drives; current
  code preserves the raw path in that case. The new rule doesn't apply
  there (no meaningful "repo root").

## Opt-out

Add a `run.path_style` config key in `.dvx/config.yml` (or `dvx.yml`):

```yaml
run:
  path_style: repo_root  # default: prefer / shorthand for out-of-dir deps
  # path_style: relative   # old behavior: always os.path.relpath
```

Only needed if there's demand for the old behavior (e.g. to minimize diff
churn on initial rollout). Probably unnecessary.

## Migration

No forced migration. Over time, as DVX rewrites each `.dvc` file in a given
repo, deps get normalized to the new shorthand. Downstream projects that
prefer an immediate bulk conversion can do it with a short script (find
every `.dvc` file, re-resolve each dep to repo-root-relative via the existing
`_resolve_dep_paths`, then write back via the new `_relativize_dep_paths`).

## Test plan

- New `test_dvc_files` cases covering:
  - Dep inside `dvc_dir`: writes dvc-dir-relative (no leading `/`).
  - Dep outside `dvc_dir`: writes `/repo-root-relative`.
  - `dvc_dir == "."`: writes repo-root-relative (no leading `/`).
  - Roundtrip: resolve(relativize(deps)) == resolve(original_deps) for any
    supported write form.
- Existing tests in `tests/test_run_dvc_files.py` for the read path should
  keep passing unchanged.

## Out of scope

- Changing the resolve (read) path. The leading-`/` form is already accepted
  and that doesn't need to change.
- Converting `outs` paths — those live within `dvc_dir` by convention.
- Source project: `hccs/crashes` will adopt this shorthand via its own spec
  (see `specs/dvx-dep-path-shorthand.md` there) once the DVX change lands.
