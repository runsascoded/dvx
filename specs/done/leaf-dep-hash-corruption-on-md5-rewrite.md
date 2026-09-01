# `dvx run` corrupts leaf dep-hashes (and regresses `/`-paths) on md5-rewrite

Handoff from the nj-crashes session (`~/c/hccs/crashes`, session `7dbec304`). This is the "path-regression bug" that thread kept referencing — but the path regression is the *cosmetic* half; the load-bearing half is a **leaf dep-hash corruption** that makes committed `.dvc`s record a hash matching no blob, i.e. a silent, permanent provenance lie. It's a correctness bug, worth a TFFP fix, not just infra hygiene.

## Symptom

A full-DAG reproc (`dvx run --no-commit -f`, level-parallel, in `batch/entrypoint.sh`) regenerated three stages' `.dvc`s and wrote **wrong `meta.computation.deps` for their *leaf* (cmd-less source) deps**: both the path *and* the recorded md5 changed, and the new md5 corresponds to no file that has ever existed in either cache remote.

## Evidence (nj-crashes repo, all verified)

Stage `njdot/data/aashto_supplemented_occupants.parquet.dvc`, one dep line, before vs after the reproc:

```
# parent 7a1cc66d753 (pre-reproc)      — correct
      /njdot/data/2023/persons.parquet: 72625f5a685ceb7489789e886f4f3171
# re-baseline 6f864c14dd4 (reproc out) — corrupt
      2023/persons.parquet:             d25274c313b4bae22b9186741901116f
```

Two independent facts pin it as corruption, not a legitimate content change:

1. **`72625f5a` is the real, current content** of `njdot/data/2023/persons.parquet` — it is that leaf's own `.dvc` out-hash, it is the file's on-disk md5, and the blob exists in **both** the prod (`.dvc`) and reproc (`.dvc-reproc`) remotes.
2. **`d25274c3` is a phantom** — `head_object` finds it in **neither** remote's `files/md5/…`. No file with that hash was ever committed. So the reproc recorded a dep-hash for a file that does not exist.

We also proved the stage's *output* is canonical: pulling the committed deps and re-running the cmd reproduces the S3 output blob **byte-identically**, on both arm64 and x86. So the computation was correct; only the recorded `.dvc` deps are wrong.

### The sharp diagnostic: leaf deps corrupt, computed-output deps fine

In the *same* deps block, the *computed-output* dep updated **correctly**:

```
# 2023/crashes.parquet (a computed stage output, not a leaf)
  parent:      /njdot/data/2023/crashes.parquet: 23abc6794e13d6d8b035dfe866de86b9
  re-baseline:  2023/crashes.parquet:             affeb3aa3e962c81702877d29dbd7606   # correct: matches the reproc's rebuilt crashes.parquet
```

Every corrupted dep is a **leaf** (`persons.parquet`, `vehicles.parquet` — cmd-less source `.dvc`s, all three years, across occupants/pedestrians/vehicles supplements). Every *correct* dep is a **computed stage output** (`crashes.parquet`, `cc2mc2mn.json`). That split is the whole ballgame:

**Hypothesis:** on md5-rewrite, dvx recomputes a *computed-output* dep's hash by copying the producer stage's recorded `outs` md5 (authoritative, always right), but recomputes a *leaf* dep's hash by **re-hashing the file from disk** — and at that moment, under the parallel reproc, the resolved path pointed at the wrong/absent bytes (see the path regression below), yielding a phantom hash. Worth checking whether the two dep kinds go through different hashing code on rewrite.

### Path regression (the cosmetic half, but maybe the cause)

Every dep path also regressed from repo-root `/`-prefixed to **bare**: `/njdot/data/2023/persons.parquet` → `2023/persons.parquet`. Under dvx's own resolution rule (leading `/` = repo-root; bare = relative to the `.dvc`'s dir) the bare form *happens* to resolve correctly here (the `.dvc` lives in `njdot/data/`), so resolution didn't visibly break — but if the rewrite re-resolves the regressed path against a different base at hashing time, that would explain the phantom leaf hash. (nj-crashes' CLAUDE.md separately records this bug as regressing `/`-paths to `../../`; we observed regression to *bare*. Two observed forms — the normalizer may pick different relative bases in different contexts. Both are wrong: the `/`-prefixed form is the intended one and should be preserved verbatim.)

## Repro target (against dvx)

Construct a stage whose `.dvc` has a `/`-prefixed **leaf** dep (a cmd-less source `.dvc`) plus a `/`-prefixed **computed-output** dep, then drive the md5-rewrite path (`dvx run` that rewrites the stage's `.dvc` md5s without a full recompute — ideally the `--no-commit` worktree-rewrite path, under `-j`/level-parallelism if that's implicated). Assert the rewritten deps block preserves, verbatim:

- each dep's `/`-prefixed **path** (no regression to bare / `../../`), and
- each **leaf** dep's md5 == that leaf's own `.dvc` out-hash (no re-hash-from-disk drift).

TFFP: the test should fail today (leaf path bare-ified and/or leaf hash != leaf out-hash) and pass after the fix.

## Impact / why it matters

A corrupt leaf dep-hash is silent: `dvx status` then reports the stage "dep changed" forever (the recorded hash never matches the real leaf), so it's perpetually stale and re-runs needlessly — or, worse, a consumer trusts the phantom hash as provenance. The nj-crashes reproc shipped this in 3 stages; caught only because the first post-re-baseline daily flagged the phantom "dep changed". Hand-repaired in nj-crashes `24c82c2f5e4` (restore `/`-paths + canonical leaf hashes), but that's a workaround — the rewrite should never emit a hash for a file that doesn't exist.

## Cross-refs

- nj-crashes commits: `7a1cc66d753` (before) vs `6f864c14dd4` (reproc output) — `git show <sha>:njdot/data/aashto_supplemented_occupants.parquet.dvc`; fix `24c82c2f5e4`.
- Related dvx path guidance already in nj-crashes CLAUDE.md ("prefer `/repo-root` shorthand; dvx regresses `/`-paths on md5 updates; fix by hand").

## Resolution

Root cause was an **asymmetry between the two code paths that read a dep's hash**:

- The **rewrite** (`Computation.get_dep_hashes(recompute=True)`, `src/dvx/run/artifact.py`) recorded `compute_md5(<worktree file>)` for every dep.
- The **freshness check** (`is_output_fresh`, `src/dvx/run/dvc_files.py:1066-1086`) compares the recorded dep-hash against the dep's own **`.dvc` `outs` md5** (`read_dvc_file(dep).md5`) — *not* the worktree bytes — for any dep that has a `.dvc`, and only hashes the file from disk for a raw (untracked) dep.

So whenever a dep's worktree copy transiently differed from its committed `.dvc` out-hash — a partial or racing materialization during the level-parallel `--force` reproc — the rewrite recorded that transient (phantom) md5. `is_output_fresh` then compared it against the dep's stable `.dvc` out-hash, never matched, and reported `dep changed` forever. The phantom `d25274c3` matched no blob because it was the md5 of a worktree file that only briefly existed in that state.

This also explains the leaf-vs-computed-output split the spec noted: a computed-output dep's worktree copy was the freshly-rebuilt canonical output (its disk md5 *happened* to equal its new `.dvc` out-hash), so the disk re-hash accidentally agreed; a leaf dep had no such guarantee.

### Fix

`get_dep_hashes` now mirrors `is_output_fresh`'s dep source of truth: for a dep that has its own `.dvc`, record that `.dvc`'s `outs` md5 (the authoritative value freshness compares against); fall back to a disk hash only for a raw dep (no `.dvc`), then to the recorded `Artifact.md5`. Recorded dep-hash and freshness-expected dep-hash are now the *same value by construction* — a rewrite can never emit a hash that its own freshness check will reject, and never a hash for a file that doesn't exist. Applied uniformly (not just `recompute=True`), so `prep` gets the same guarantee.

### The path half (`/njdot/data/…` → bare) is not a bug

The dep-path spelling change is the **intended April-2026 convention**, not corruption: `_relativize_dep_paths` writes an in-dir dep (one under the `.dvc`'s own directory) in bare `.dvc`-relative form, and an out-of-dir dep in `/`-repo-root form. The AASHTO supplements' `.dvc`s live in `njdot/data/`, so `njdot/data/2023/persons.parquet` is in-dir → bare `2023/persons.parquet` is correct; the parent commit's `/njdot/data/…` was the older pre-convention spelling. Both forms resolve to the identical repo-root path (`_resolve_dep_paths`), and freshness matches regardless of spelling — so with the hash fix the stage is fresh either way. Rewrite fidelity preserves an existing spelling only when the normalized forms match (`_norm_dep_key` strips `/`); it can't map a bare in-dir key back to the `/`-form without the `.dvc`-dir context, which is why an old `/`-spelled in-dir dep migrates to bare. Left as-is; changing it would be a convention reversal, not a fix.

### Tests (`tests/test_run_artifact.py`)

- `test_get_dep_hashes_records_dep_dvc_outhash_not_disk_rehash` — worktree bytes deliberately differ from the leaf's `.dvc` out-hash; the rewrite records the `.dvc` out-hash. TFFP-verified (pre-fix records the disk md5).
- `test_rewritten_stage_stays_fresh_against_a_tracked_dep` — end-to-end: a rewritten stage is `is_output_fresh` → `(True, "up-to-date")`; pre-fix it is `(False, "dep changed: …")`, the exact nj-crashes symptom.
- `test_get_dep_hashes_raw_dep_still_hashes_from_disk` — regression guard: a raw dep (no `.dvc`) is still hashed from disk.

Full suite green (383 passed, 4 skipped).

### Downstream (nj-crashes)

The hand-repair in `24c82c2f5e4` stays valid (it restored canonical hashes). Going forward a reproc won't re-emit phantom leaf hashes; the residual "dep changed" churn from worktree/`.dvc` divergence is gone. The `/`-vs-bare spelling in a repaired `.dvc` is cosmetic — if a future reproc rewrites it to bare, that's the current convention and freshness is unaffected.
