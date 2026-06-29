# `dvx add` doesn't update sibling `.gitignore`

## Symptom

After `dvx add foo.txt`, the file remains tracked by Git (shows up in
`git status`) instead of being auto-ignored — diverges from `dvc add`
behavior, which has long auto-added each output path to a sibling
`.gitignore` so the cached blob doesn't end up in two stores.

Reproducer (same fixture for both tools):

```bash
mkdir tmp/dvx-vs-dvc && cd tmp/dvx-vs-dvc && git init -q && dvc init -q
echo data > foo.txt

# DVC: creates .gitignore with `/foo.txt`
dvc add foo.txt
cat .gitignore  # → /foo.txt

# DVX: does NOT create .gitignore
rm foo.txt.dvc .gitignore
dvx add foo.txt
ls .gitignore   # → No such file or directory
```

Observed in [hccs/crashes] after `dvx add` on
`njdot/data/{2023,2024,2025}/vehicles.parquet`,
`njdot/data/{2023,2024,2025}/persons.parquet` (commit `5bfc7273127`),
and the five `aashto_supplemented_*.parquet` files — each one a
DVX-tracked blob that kept showing up as `??` in `git status` until a
human added `/<name>` to the right `.gitignore` by hand.

[hccs/crashes]: https://github.com/hudcostreets/nj-crashes

## Root cause

DVX CLI add (`src/dvx/cli/main.py:71`) calls a lock-free
re-implementation, `add_to_cache` (`src/dvx/cache.py:292`), instead of
`repo.add` → DVC's `@scm_context`-decorated path.

DVC's path is what fires the gitignore update:

```
dvc.repo.add.add                # @scm_context
└─ stage.add_outs / Output.add  # output.py:1362
   └─ self.ignore()              # output.py:1408
      └─ repo.scm_context.ignore(path)
         └─ self.scm.ignore(path)   # → writes .gitignore + tracks it
```

`add_to_cache` skips all of that — it computes the md5, writes the
`.dvc` YAML directly, and `shutil`s the blob into `.dvc/cache/`. The
trade-off was deliberate (lock-free parallel add) but the gitignore
update was an unintended casualty.

`dvx repo.add` (`src/dvx/repo.py:74`) DOES delegate to
`self._repo.add(...)` which would inherit DVC's behavior — but no CLI
path exercises it; everything goes through `add_to_cache`.

## Why this matters

Beyond the cosmetic `git status` pollution:

- Easy to accidentally `git add` a DVX-tracked file (especially under
  globs like `git add njdot/data/`).
- New contributors hit this footgun first; the recovery is
  hand-editing `.gitignore` for every previously-DVX-add'd path.
- Diverges from DVC; users coming from DVC expect the auto-ignore.

## Fix

Inline helper `dvx.cache._ensure_gitignored(path)` mirrors
`git_import._ensure_not_gitignored` (the inverse). Called from
`add_to_cache` after the `.dvc` write + blob cache. Lock-free, no
DVCRepo open.

```python
def _ensure_gitignored(path: Path) -> None:
    gitignore = path.parent / ".gitignore"
    entry = f"/{path.name}"
    if gitignore.exists():
        content = gitignore.read_text()
        if any(line.strip() == entry for line in content.splitlines()):
            return
        suffix = "" if not content or content.endswith("\n") else "\n"
        gitignore.write_text(content + suffix + entry + "\n")
    else:
        gitignore.write_text(entry + "\n")
```

Decisions:

- **Did not route through DVC's `scm_context.ignore`.** That would
  require opening a `DVCRepo()` per add — works, but the lock-free
  path is the whole point of `add_to_cache`, and the gitignore
  manipulation is small enough to inline cleanly. Matches DVX's
  existing `_ensure_not_gitignored` style. If `scm.ignore`'s subtler
  cases (e.g. deep subdir with implicit parent .gitignore creation)
  bite us, swap to it later.
- **Directory output uses `/<name>` (no trailing slash)** — same
  shape as files, matches DVC's convention.
- **No `core.autostage` integration**: DVX doesn't touch git at add
  time, so deferring this. The `.gitignore` change shows up as a UC
  for the user to commit alongside the new `.dvc`.

## Tests

`tests/test_cache.py` (6 added):

- `test_add_to_cache_updates_gitignore` — bare-file case writes
  `/foo.txt`.
- `test_add_to_cache_subdir_writes_local_gitignore` —
  `data/foo.txt` writes to `data/.gitignore` (not repo root).
- `test_add_to_cache_appends_to_existing_gitignore` — existing
  entries preserved.
- `test_add_to_cache_idempotent_gitignore` — second `add(force=True)`
  doesn't duplicate.
- `test_add_to_cache_directory_gitignored_as_dir_entry` —
  `dvx add d/` writes `/d` (no trailing slash, file-style entry).
- `test_add_to_cache_gitignore_preserves_trailing_newline` —
  appending to a file without trailing newline inserts one before
  the new entry.

## Related

- DVC reference impl: `dvc/repo/scm_context.py::ScmContext.ignore`,
  `dvc/output.py::Output.ignore` (called at the end of `Output.add`).
- DVX's `git_import.py:_ensure_not_gitignored` is the inverse — it
  *removes* paths from `.gitignore` for git-imported files. So the
  team has the gitignore-manipulation primitives; the `add` path
  just doesn't use them.
