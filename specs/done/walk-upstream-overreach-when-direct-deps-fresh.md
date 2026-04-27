# `walk_upstream` overreach: don't recurse past fresh artifacts

## Resolution

Implemented in `src/dvx/run/artifact.py` and `src/dvx/run/executor.py`:

- `Artifact.walk_upstream(prune_fresh=True)` short-circuits at fresh
  artifacts — when a node passes `is_output_fresh` per its own `.dvc`,
  its upstream isn't visited.
- `materialize` exposes `prune_fresh: bool = True`. `write_all_dvc`
  passes `prune_fresh=False` (prep phase wants every `.dvc` written
  regardless of current freshness state).
- The CLI `dvx run` path uses `executor.run`'s own BFS (not
  `walk_upstream`), so the same pruning was applied there:
  `ExecutionConfig.prune_fresh` (default `True`) is exposed through
  `dvx run --no-prune-fresh` (`-U`). Pruning auto-disables when
  `--force-upstream <pattern>` is set, since those patterns need a
  full walk to match. `--force` does *not* disable pruning, per spec.
- Bonus dep-stripping fix: `Computation.get_dep_hashes(recompute=True)`
  and `get_git_dep_hashes(recompute=True)` now fall back to the
  recorded `md5` when the dep file isn't materialized locally (e.g.
  with `--cached` or pruned upstream). Previously they silently dropped
  the dep, which caused the rewritten `.dvc` to lose its entire `deps:`
  section.

Tests added:
- `tests/test_run_artifact.py::test_walk_upstream_prunes_at_fresh`
- `tests/test_run_artifact.py::test_walk_upstream_prune_skips_missing_raw_dep`
- `tests/test_run_artifact.py::test_get_dep_hashes_recompute_falls_back_to_recorded`
- `tests/test_executor.py::test_run_prunes_at_fresh_artifact`
- `tests/test_executor.py::test_run_no_prune_fresh_walks_full_chain`

## Problem

`dvx run <stage>` walks the *entire* upstream chain unconditionally,
even when the target stage's direct deps are already fresh w.r.t. its
recorded md5s. This forces rebuilds of deep upstream stages that have
no bearing on the current target.

## Concrete case (from `nj-crashes`)

Pipeline shape:

```
Accidents.txt (raw)
   ↓
Accidents.pqt (per-year)
   ↓
crashes.parquet (combined)
   ↓
www/public/njdot/map/  (export_map_data + export_map_v2 + outlines)
   ↓
www/public/njdot/map_sync.dvc  (aws s3 sync map → S3, side-effect)
```

`map_sync.dvc` is a side-effect stage; its only dep is `map`. The
recorded `map: <md5>` matches the current `map.dvc` outs md5. By
`is_output_fresh`'s own per-stage logic (`dvc_files.py:583-662`,
explicitly documented as "no transitivity"), `map_sync` is fresh.

But `dvx run www/public/njdot/map_sync.dvc` walks all the way up to
the per-year `Accidents.pqt`s and fails because the `Accidents.txt`
sources aren't kept on disk on EC2 (gzipped, on-demand decompress
only). User has to add `--cached '*Accidents.pqt'` *and* `--force` to
get past it — and even then, hits the cmd-cwd bug ([separate spec][cwd])
when DVX tries to "re-run" `crashes.parquet` (it's a no-op since outs
are fresh, but the planning step still walks down to the next level
and tries to rerun `map.dvc`'s cmd, which fails because cwd is wrong).

[cwd]: ./cmd-cwd-vs-project-root.md

## Root cause

`materialize` (`artifact.py:440-472`) calls `walk_upstream` which
unconditionally recurses to leaves:

```python
def walk_upstream(self) -> list[Artifact]:
    """Recursively collect all upstream Artifacts.

    Returns artifacts in dependency order (leaves first).
    """
    visited = set()
    result = []

    def visit(artifact: Artifact):
        if artifact.path in visited:
            return
        visited.add(artifact.path)
        for upstream in artifact.get_upstream():
            visit(upstream)   # ← always recurses
        result.append(artifact)

    visit(self)
    return result
```

Then in `materialize`:

```python
for artifact in artifacts:
    for a in artifact.walk_upstream():
        if a.path not in seen:
            seen.add(a.path)
            all_artifacts.append(a)

# Filter to only computable artifacts
computable = [a for a in all_artifacts if a.computation]
```

`_run_one_artifact` does check `is_output_fresh` before running the
cmd, so a freshly-built upstream artifact is no-op'd. But the *walk*
still descended through it — so the walk-output includes the
artifacts further up, which then get put through their own
`is_output_fresh` checks. Those upstream checks fail because *their*
direct deps are missing from disk (`Accidents.txt`), and DVX bails.

## Why this contradicts the documented model

`is_output_fresh`'s docstring is unambiguous:

> Note: Dep checking compares our recorded dep hash against the dep's
> .dvc expected hash, NOT against the dep's actual data. This mirrors
> git's model — each .dvc file declares what it expects, with no
> transitivity.

The freshness *check* is per-stage and non-transitive. But the
materialization *walk* is fully transitive. That's the inconsistency:
freshness says "I don't care what's behind my direct dep — just that
its `.dvc` file's recorded outs md5 matches what I recorded", but the
runner pulls the whole chain into the work set anyway.

## Recommended fix

Short-circuit `walk_upstream` (or `materialize`'s collection loop)
when an artifact passes `is_output_fresh`. Any further-upstream stages
are irrelevant to the target — their state can't affect a downstream
that's already fresh.

```python
# artifact.py walk_upstream — proposed
def walk_upstream(self, prune_fresh: bool = True) -> list[Artifact]:
    visited = set()
    result = []

    def visit(artifact: Artifact):
        if artifact.path in visited:
            return
        visited.add(artifact.path)

        # Don't recurse past artifacts whose own .dvc file says they're fresh.
        # Their upstream state can't affect freshness of anything downstream.
        if prune_fresh and artifact.computation:
            from dvx.run.dvc_files import is_output_fresh
            fresh, _ = is_output_fresh(Path(artifact.path), check_deps=True)
            if fresh:
                result.append(artifact)
                return

        for upstream in artifact.get_upstream():
            visit(upstream)
        result.append(artifact)

    visit(self)
    return result
```

`materialize` (and `write_dvc_files`) call this with default
`prune_fresh=True`. The CLI can expose a `--no-prune-fresh` (or
similar) for the rare case where the user really wants to walk all
the way up — e.g. validating the entire chain is rebuildable from
sources.

`--force` should *not* automatically disable prune_fresh; it should
just force recomputation of the targets the user named (and any
genuinely-needed upstream). If the user wants "force-rebuild
everything", they can name the leaf stage explicitly.

## Side effects worth thinking about

- **`dvx status`**: currently reports transitively-stale stages
  ("⚠ www/public/njdot/map_sync.dvc (upstream stale: …)"). That
  reporting can stay — it's useful — but the *run planner* should
  prune at fresh artifacts. They're orthogonal: status surveys the
  whole graph; run only schedules the work that actually matters.
- **`--cached <pattern>`** flag: still useful, but should mostly
  become unnecessary for the side-effect-stage case once this lands.
  Keep it for the "I want to rebuild a dep's `.pqt` but its source
  `.txt` md5 has drifted and I trust the cached version" use case.
- **`-r`/`--recursive` (or whatever flag exists)**: if there's an
  existing flag for "rebuild everything from leaves", that's the
  knob to flip when the user wants the un-pruned walk. Don't make
  pruning the surprise.

## Test cases

Reproducer (the situation that hit `nj-crashes` today):

1. Pipeline with ≥3 levels: `A.txt → A.pqt → B.parquet → C.dvc (side-effect)`
2. Build the chain bottom-up so all `.dvc` files have correct md5s.
3. Delete `A.txt` from disk (or modify it so `A.pqt`'s recorded dep
   no longer matches).
4. `dvx run C.dvc` should succeed without touching `A.pqt` or `B.parquet`,
   because `C`'s direct dep (`B`) is fresh-recorded.
5. Currently: walks up to `A.pqt`, fails on missing `A.txt`.

A second test for the `--force C.dvc` case: should rerun `C`'s cmd,
not `B`'s.

## Related upstream-DVX issues hit at the same time

- [`cmd-cwd-vs-project-root.md`][cwd] — when `dvx run` does decide to
  re-execute upstream stages (incorrectly, per this spec), it then
  hits the cwd bug because the cmds in `nj-crashes` use project-root
  paths but DVX runs them from the `.dvc` file's parent dir.
- [`dir-suffix-asymmetry-in-dep-md5.md`](./dir-suffix-asymmetry-in-dep-md5.md)
  — orthogonal but in the same file (`dvc_files.py:660-662`).

## Bonus side-effect: deps stripped from `.dvc` on no-op rebuild

When `dvx run --force --cached '*.pqt' map_sync.dvc` traversed the
upstream chain in `nj-crashes`, it "rebuilt" `crashes.parquet.dvc` in
2.0s (the cmd no-op'd because outs were already fresh) but **wrote a
new `crashes.parquet.dvc` with the entire `deps:` section stripped**:

```diff
   computation:
     cmd: njdot compute pqt -t crashes
-    deps:
-      2001/NewJersey2001Accidents.pqt: 9e5d8dc8...
-      2002/NewJersey2002Accidents.pqt: e8e4efc4...
-      ...23 rows...
```

I'd guess: `materialize` rewrites the `.dvc` from in-memory state,
and `--cached` populates the artifact without reading the upstream
deps' md5s, so the in-memory deps map is empty → rewrite drops them.

This is at minimum a footgun: a "no-op" run silently destroys
provenance metadata that the user has to restore from git. At worst
it could ship to a clean checkout where `git checkout` doesn't help.

**Suggested fix**: when `update_dvc=True` after a no-op run (artifact
was already fresh), don't rewrite the `.dvc` at all. The on-disk
contents are already correct; rewriting is at best a no-op and at
worst (this case) destructive.
