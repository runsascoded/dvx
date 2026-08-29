# Co-output `.dvc` rewrite drops `side_effect: true`

> From the `hccs/crashes` reproc audit. `/read crashes` for context.

## Symptom

`njsp harmonize_muni_codes` is one cmd with four real co-outputs
(`data/county-city-codes.parquet`, `njdot/data/muni_codes.parquet`,
`njsp/data/muni_codes.parquet`, `www/public/njdot/cc2mc2mn.json`) plus a
fifth, outs-less **driver** `.dvc` — `njsp/data/harmonize.dvc` — carrying
`meta.computation.side_effect: true` so the co-output wait doesn't expect an
artifact from it.

Every reproc round, at the harmonize level:

```
✗ njsp/data/harmonize: co-output not produced
```

which fails the whole level (dvx halts at the first level with any failure),
so Levels 4–8 never run.

## Root cause

The flag is correct and dvx *honors* it —
`DVCFileInfo.is_side_effect()` returns the explicit field when set
(`src/dvx/run/dvc_files.py:506`). The problem is durability: **after the cmd
runs, dvx rewrites the co-output `.dvc` files and does not preserve
`side_effect: true`.** Proof from the crashes repo — a daily-cron commit that
merely re-ran the stage:

```
commit ad34b63f208  "Harmonize county/muni codes"  (GitHub Actions)
--- njsp/data/harmonize.dvc
@@ -6,7 +6,6 @@ meta:
     # ... comment describing the flag survives ...
-    side_effect: true
     deps:
       ... (FAUQStats dep hashes updated, as expected) ...
```

The rewrite legitimately refreshes the recorded dep hashes, but it drops the
hand-authored `side_effect: true` (the surrounding comment survives, which is
what makes the loss easy to miss). So the flag cannot survive the stage's own
execution — the daily strips it, and the next audit fails again. It's a
write-side regression of a field the read side already respects.

## Ask

When dvx regenerates a co-output `.dvc`, preserve the author-set
`meta.computation` intent — at minimum `side_effect`, and ideally treat the
whole `meta.computation` block as author-owned except the fields dvx computes
(`deps`/`git_deps` hashes, `outs` md5s). A round-trip test: a `.dvc` with
`side_effect: true` and a comment, run through the co-output rewrite, should
come back with both still present.

## Crashes-side status

Restored the flag in `njsp/data/harmonize.dvc` so the baked-into-image copy is
correct and round 9 parses it before running (the in-flight produced-check
reads the pre-run `DVCFileInfo`, so the post-run rewrite dropping it doesn't
affect that same run). But it'll be stripped again by the next real
`harmonize_muni_codes` commit until the dvx fix lands.

## Resolution

Two complementary changes, both in `src/dvx/run/`:

1. **`dvc_files.py` — `_merge_preserving_comments`.** `side_effect` was in the `managed` tuple with `cmd`, so a rewrite whose `new_comp` omitted it (the executor never passes it) stripped it. Split the two: `cmd` stays DVX-managed (re-supplied every provenance rewrite, so add/update/remove syncs it), while `side_effect` becomes author-owned — a new value updates it, its *absence* preserves it rather than deleting it. This is the core fix and covers any caller of `write_dvc_file`.

2. **`executor.py` — side-effect branch.** Re-assert an *explicit* author flag by passing `side_effect=info.side_effect` (the pre-run `DVCFileInfo`'s explicit field, `None` when side-effect status was only inferred from "no outs + cmd"). This makes the flag durable even on a fresh write, where there's no existing file for #1's merge to preserve from. Inferred side-effect status is left unstamped — it re-derives on read.

Tests:

- `tests/test_dvc_rewrite_fidelity.py::test_rewrite_preserves_side_effect_flag` — the direct repro: a hand-authored `side_effect: true` + comment, run through a `write_dvc_file` dep-hash rewrite that doesn't pass the flag, comes back with both intact (the round-trip test this spec requested). Failed before #1, passes after.
- `tests/test_executor.py::test_side_effect_flag_survives_the_stages_own_run` — end-to-end: a driver stage with the flag, made stale, run through the real executor; asserts the flag survives and the dep hash refreshed.

### Not done

- **`cmd` stays managed, not author-owned.** The spec's ideal ("treat the whole `meta.computation` block as author-owned except the fields dvx computes") would extend to `cmd`, but `cmd` *is* a field dvx re-supplies every provenance run, and a `--no-provenance` rewrite already leaves it alone (the whole `computation` block is skipped when no cmd/deps are emitted). No caller drops `cmd` the way the co-output path dropped `side_effect`, so the narrower fix is the correct-scoped one.
