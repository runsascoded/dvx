# `dvx run <nonexistent.dvc>` silently no-ops — should be an error

## Repro (observed 2026-08-26, nj-crashes CI)

```
$ dvx run --commit --push each cells-api/deploy.dvc   # file doesn't exist
Summary:
  Total: 0
$ echo $?
0
```

A `workflow_dispatch` was fired with `targets: cells-api/deploy.dvc` before the commit adding that file had been pushed (the push was rejected non-FF; the dispatch raced it). The CI run **succeeded** — green check, zero work done. The deploy it was supposed to perform silently didn't happen, and only a manual `gh run view --log` (showing `Total: 0`) revealed why.

`dvx status` on the same input behaves better — it reports the target as an error:

```
$ dvx status cells-api/deploy.dvc
Error (1):
  ! cells-api/deploy.dvc (dvc file not found or invalid)
Fresh: 0, Stale: 0, Error: 1
```

— but `dvx run` apparently drops unresolvable targets from the plan without reporting them, and exits 0.

## Why it matters

- **CI**: a typo'd target, a stale branch, or a race like the above yields a green run that did nothing. The whole point of running under CI is that failures surface; this failure mode is invisible.
- **Interactive**: `dvx run njdot/data/crashs.parquet.dvc` (typo) prints `Total: 0` and exits 0 — easy to read as "everything was fresh".

## Expected behavior

An explicitly-named target that doesn't resolve to a valid `.dvc` file should be a **hard error** (nonzero exit, per-target message), matching `dvx status`'s classification. Suggested output:

```
Error: target not found: cells-api/deploy.dvc
```

Notes:
- This is about *explicit* targets only. No-target `dvx run` (run everything stale) discovering zero stale stages is legitimately `Total: 0`, exit 0.
- Globs that match nothing arguably deserve the same treatment, but that's a separate decision (make-style tools differ here); explicit literal paths are the unambiguous case.
- `run --force` on a missing target should obviously also error, not silently skip.

## Pointers

- Target resolution presumably happens near the executor's plan-building (`src/dvx/run/executor.py`); `read_dvc_file` returning `None` for an invalid path is likely where the target gets dropped.
- `src/dvx/cli/status.py` already has the "dvc file not found or invalid" classification to mirror.

## Resolution

Validation moved up into the CLI layer (`src/dvx/cli/run_cmd.py`) rather
than the executor. When `dvx run` is invoked with explicit targets (i.e.
the `targets` positional came from the user, not the recursive-discovery
fallback), each target is now resolved via `Artifact.from_dvc(...)`
before the plan is built. Any that fail to resolve are collected and
raised as one `ClickException`:

```
Error: target not found: cells-api/deploy.dvc
Error: targets not found: a.dvc, b.dvc   # plural form for >1
```

Executor-side plan-building is unchanged — discovered deps that lack a
`.dvc` file still become bare leaves (an untracked upstream is a
legitimate state), so the validation is scoped strictly to the initial
explicit-target list.

Tests: `tests/test_cli.py` — `test_run_explicit_missing_target_errors`,
`test_run_multiple_explicit_missing_targets_errors`,
`test_run_explicit_missing_target_with_force_errors`,
`test_run_partial_missing_targets_errors`,
`test_run_no_targets_with_dvc_files_no_error` (guards the auto-discovery
path from regressing into the new check).
