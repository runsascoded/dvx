# `dvx pull --glob <pattern>` regressed in non-dry-run path

## Symptom

```
$ dvx pull --glob 'www/public/*.dvc'
Error: '/home/runner/work/path/path/www/public/*.dvc' does not exist
```

The `--glob` flag is silently ignored: the literal glob string is
passed to DVC's pull as a path target.

`--dry-run` works correctly (tested locally — same invocation
returns the expected 7-file expansion). The regression is in the
non-dry-run code path only.

Hit by `hccs/path`'s `Deploy www` workflow:
<https://github.com/hudcostreets/path/actions/runs/25475228994>

## Cause

The original CLI (`b15590e9c`-era) routed `dvx pull` (with targets)
straight to `repo.pull(targets=..., glob=glob)` — DVC's pull called
`glob_targets(targets, glob=glob)` and expanded properly.

`65f993aa8` ("Fix targeted `dvx pull` for .dvc files (bypass dvc.yaml)")
introduced a `_pull_targets` helper that manually resolved `.dvc` files
and called `pull_hashes` + checkout. **It did not thread `glob` through
— first regression here.**

`536816f1b` ("Simplify targeted pull to use DVC's .dvc file target
support") swapped `_pull_targets` for `_resolve_pull_targets` +
`repo.pull(targets=resolved)`. Still no `glob`. The bug persisted but
wasn't introduced here — `536816f1b` just refactored already-broken
code into a different broken shape.

`src/dvx/cli/transfer.py:242-244` (HEAD `0d5bab93f` before fix):

```python
if targets:
    # Targeted pull: resolve to .dvc file paths, pass to DVC
    dvc_targets = _resolve_pull_targets(list(targets))
```

The `glob` flag is captured by Click but never threaded through.

The dry-run branch above (line 220, `glob_pattern=glob`) still works
because it uses a different code path (`get_transfer_status`) that does
honor the flag — which explains why `--dry-run` succeeds and the real
pull doesn't, and how the bug evaded any manual smoke-test that ran
`--dry-run` first.

`dvx push --glob` is **not** affected — push routes directly through
DVC's `repo.push(targets=..., glob=glob)`, which expands via
`glob_targets()` (DVC `push.py:113`).

## Expected

`dvx pull --glob 'www/public/*.dvc'` should expand the glob (just like
the dry-run does) and pull each matched `.dvc` file's outputs.

## Fix — **DONE**

Approach (1): expand glob in `_resolve_pull_targets` when `glob=True`.
Threaded the flag in from the CLI; resolver pre-expands each pattern
via `glob.glob(target, recursive=True)` before the per-target loop.

Regression test: `tests/test_transfer.py::TestTargetedPull::test_pull_glob_expands_pattern`
sets up a 3-file repo, removes outputs + cache, runs
`dvx pull --glob 'small*.dvc'`, asserts only `small.txt` returns and
no "does not exist" error. Verified the test fails on the broken code
with the exact symptom string.

## Reflection — why we missed it

`536816f1b`'s commit message says "Added 4 tests: pull by output path,
by .dvc path, nonexistent target, already up-to-date." None exercised
`--glob`. Coupled with the dual-code-path setup (dry-run uses
`get_transfer_status` which honored `glob`; real pull went through the
broken resolver), any manual smoke-test of `dvx pull --glob ... -n`
would have looked correct.

Lesson: when refactoring/adding a feature with N flags, at least one
test per flag combo should hit the production code path, not just
dry-run.

## Workaround applied in `hccs/path` (now obsolete after this fix)

In `.github/workflows/www.yml`:

```yaml
# Before:
run: dvx pull --glob 'www/public/*.dvc'
# After:
run: dvx pull www/public/
```

Directory form goes through the dir-expansion branch of
`_resolve_pull_targets` (which calls `Path.glob('**/*.dvc')`), so it
works correctly. Loses precision (pulls anything under
`www/public/`, not just direct `.dvc` files) but for path's flat
layout the result is the same. After this fix lands and path bumps
its dvx pin, the original `--glob` form is fine again.
