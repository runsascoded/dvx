# `.dvc` rewrites should round-trip: preserve comments (and path style)

## Repro (observed 2026-08-26, nj-crashes)

`njsp/data/harmonize.dvc` carried an 8-line YAML comment block above its `deps:` explaining *why* the deps point at raw XMLs rather than `crashes.parquet` (a DAG-deadlock postmortem — exactly the kind of context that belongs next to the deps it justifies). The next daily CI run legitimately re-ran the stage and updated two dep md5s; DVX rewrote the file and **the comment block vanished**:

```diff
     cmd: njsp harmonize_muni_codes
     deps:
-      # The NJSP side reads the raw XMLs (`load_sp_data` → `get_crashes_df`),
-      # NOT `crashes.parquet`. This used to dep on `crashes.parquet`, which
-      # invented a `update_pqts` → `harmonize` edge that doesn't exist in the
-      # code — [...5 more lines...]
       /data/FAUQStats2008.xml: 017b63c673518b66eff6dcabf5af4778
       ...
-      /data/FAUQStats2025.xml: 2776168cd1b100f7041dd14dcdc19701
+      /data/FAUQStats2025.xml: 6e96ac604e24fd6310deb4ce26befc21
```

(nj-crashes commit `2f908dd7d5` — "Harmonize county/muni codes", GHA-authored.)

The practical consequence: any explanation written into a `.dvc` has the lifetime of one md5 bump, so rationale gets exiled to commit messages or unrelated files. In nj-crashes the workaround so far is duplicating the rationale into `daily.yml` comments — the wrong file for it.

## Same root cause, already-known sibling

The repo-root path-style regression (documented in nj-crashes project memory): `/repo-root`-style dep paths (`/data/FAUQStats2008.xml`) get rewritten to `../../`-relative form on md5 updates. Both bugs are the same shape — **DVX parses the YAML into a dict and re-serializes, discarding everything the data model doesn't carry** (comments, key ordering nuances, path spelling).

Note the `/`-paths *survived* in the 2026-08-26 repro above, so either the path bug is partially fixed or is triggered by a different rewrite path than the comment loss. Worth confirming which write paths are affected while in here.

## Expected behavior

Rewrites that only bump values (md5s, sizes, `git_deps` hashes) should leave everything else byte-identical: comments, blank lines, key order, path spelling as authored.

## Suggested approach

- **ruamel.yaml round-trip mode** (`YAML(typ='rt')`) preserves comments/order/anchors and is the standard tool for this. Load the existing file, mutate values in place, dump.
- If ruamel is an unwanted dependency, a narrower fix covers the common case: when a rewrite only changes *values* of existing keys (the md5-bump path), apply targeted line edits (regex on `^(\s*<key>:\s*)\S+$`) instead of re-serializing. Full re-serialization stays for structural changes (adding/removing deps/outs), where comment placement is genuinely ambiguous.
- Test: golden `.dvc` fixture with comments above/inline within `deps:`, `/repo-root` paths, and a trailing comment; bump one dep md5 via the library; assert the output differs from the input *only* on that one line.

## Where

Whatever writes `.dvc` files on `run`/`commit` — likely `src/dvx/run/dvc_files.py` (writer counterpart to `read_dvc_file`) and/or the `repo.commit` path.

## Resolution

Landed the ruamel round-trip route (option 1 from the spec above) in
`src/dvx/run/dvc_files.py::write_dvc_file`:

- **New helpers**: `_yaml_rt()` returns a `ruamel.yaml.YAML(typ='rt')`
  instance with `.dvc`-friendly indent + a huge `width` so cmd strings
  don't get autowrapped. `_merge_preserving_comments(existing, new_data)`
  updates the round-tripped tree in place — matching `outs` entries by
  `path`, updating scalar fields; matching `deps`/`git_deps` by a
  spelling-invariant `_norm_dep_key` (`/data/foo` and `data/foo` match).
- **Write step**: if the target `.dvc` already exists, load via ruamel
  → merge → dump. Preserves comments, blank lines, key order, and path
  spelling (leading `/` shorthand and hand-authored key ordering both
  survive). Fresh writes (no existing file) go through ruamel too so
  the initial format matches the round-trip format — no first-rewrite
  churn.
- **Non-DVX-managed fields survive**: sibling entries under `meta:`
  (`meta.owner`, `meta.tags`, arbitrary custom fields) are untouched
  by the merge; only `meta.computation.{cmd,deps,git_deps,side_effect,
  fetch}` are synced with the caller's canonical form.
- **Malformed existing files**: caught at load; the writer falls back
  to a fresh round-trip dump so the visible state matches the caller's
  data rather than silently preserving broken input.

Path-spelling design note: on ambiguity (existing has `/data/foo`, caller
passes `data/foo`), the existing spelling wins — that's the observed
user preference (they hand-authored the `/`-prefixed form). Genuinely
new deps take the caller's spelling.

Tests: `tests/test_dvc_rewrite_fidelity.py` — 8 tests covering the
comment-block-above-`deps`, `/repo-root`-prefixed dep paths, key
ordering, obsolete-dep removal, new-dep append, fresh-file emission,
comment-above-`outs`, and non-DVX-managed `meta` fields.

`ruamel.yaml` is already a transitive dep of `dvx` via `dvc`; no
`pyproject.toml` change was needed.
