# `dvx run --remote` (and `dvx batch submit --remote`)

Requested by the nj-crashes session, 2026-08-28, blocking the full-DAG reproc in `crashes/specs/batch-reproc.md`.

## Motivation

The reproc's whole point is to regenerate all ~1,386 derived targets from pinned raw leaves and check the results against what prod is serving. Two hard requirements follow:

1. **It must not write into the prod remote.** `--push each` currently targets the default remote (`s3://nj-crashes/.dvc`), so a `--force` reproc would spray regenerated blobs into the cache prod pulls from — including any that are *wrong* (a non-deterministic stage, an undeclared-dep bug). Reproc is an audit; audits don't mutate the thing they audit.
2. **The comparison wants two populated locations.** `dvx cache comm remote:s3 remote:reproc --only 'reproc,!s3'` is exactly the concordance report — every object the reproc produced that prod's cache doesn't have, i.e. the set of stages whose output changed. That only works if the reproc's blobs land somewhere separate and durable.

`--push never` satisfies (1) but not (2): blobs die with the container, and there's nothing left to diff.

## Ask

`dvx run --remote <name>` — route `--push` (and materialization *reads*, see below) at a named remote instead of the default. Plumb through `ExecutionConfig.remote`, and add the passthrough to `dvx batch submit --remote`.

The primitives already take it: `push_targets`/`materialize_targets` accept `remote=`, `_get_remote_odb(name=...)` is the seam, and `repo.push(remote=...)` exists for the CLI path. This is mostly threading an option, not new machinery.

**Read/write asymmetry to decide.** For our use case the ideal is *read from prod, write to scratch*: the reproc should materialize the raw leaves from the prod remote (they're pinned and identical) but push regenerated outputs to `reproc`. A single `--remote` gives read+write to one location, which forces us to pre-seed the scratch remote (fine but wasteful — 37 GB) or accept that the reproc's own force-rebuilt upstreams supply everything (true for `--force`, since nothing is fetched anyway). Suggest starting with the simple single `--remote`, since `--force` makes reads mostly moot; a later `--push-remote` / `--read-remote` split can refine it if a partial (non-forced) reproc ever wants both.

## Why not just do it container-side

`dvc remote default reproc` before the run works and needs nothing from dvx — but it's a mutation of tracked config inside the image, it silently affects any other dvx invocation in that container, and it can't express "read prod, write scratch" later. A flag is the honest interface, and `dvx batch submit` needs it as a passthrough regardless.

## Acceptance

- `dvx run --remote reproc --push each -f <targets>` pushes to `reproc`, leaves the default remote untouched (verify with `dvx cache comm`).
- `dvx batch submit --remote reproc ...` passes it through to the container's `dvx run`.
- Unit test on `run_command`: the flag appears in the built command; a round-trip through `dvx run`'s parser accepts it (the `--commit never` lesson from `batch-run-command-cli-mismatch.md`).

---

## Resolution

Implemented 2026-08-28. `-r`/`--remote <name>` on `dvx run`, `-r`/`--remote` on `dvx batch submit` (passthrough), matching the flag name/letter `dvx push`/`dvx pull` already use.

**Single `--remote`, read+write, as suggested.** `ExecutionConfig.remote` feeds both seams the spec identified, and they were the only two: `materialize_targets(targets, remote=…)` in `_try_materialize_from_remote` (read) and `push_targets(keys, remote=…)` in `_push_cache_blobs` (write). Nothing else in `run/` touches the network — `cache_blob` is a local-cache write. `_get_remote_odb` caches per `(root, remote)`, so pointing a run at a non-default remote costs one extra ODB handle, not a per-stage repo open.

No read/write split. The asymmetry argument holds, but `--force` makes reads moot for the reproc, and a `--read-remote`/`--push-remote` pair is cheap to add later on the same two seams — this doesn't paint us into a corner.

**A non-existent remote name degrades rather than erroring**, which is worth knowing before firing a 1,386-stage job: materialization catches the ODB-construction failure and falls through to a rerun (a legitimate path — "not in remote"), while the push logs `⚠ cache push failed: …` per stage and continues, since push is non-fatal by contract. So a typo'd `--remote` yields a full rebuild that pushes nothing, not a fast failure. Smoke one stage before submitting the full DAG.

### Tests

- `test_run_remote_pushes_to_named_remote_only` — the acceptance criterion: blob lands in `scratch`, and the default remote is asserted *empty* (not merely missing that one blob).
- `test_run_remote_materializes_from_named_remote` — read side, with the control half: after wiping the output and local cache, `--remote scratch` materializes and skips (`executed=0, skipped=1`); the identical state *without* `--remote` finds nothing in the empty default remote and reruns (`executed=1, skipped=0`). The two summaries are what distinguish "read routed" from "rebuilt to the same bytes".
- `test_run_command_remote` + `test_run_command_remote_parses_with_real_run_cli` — the built argv, and its round-trip through `dvx run`'s real click parser (the `--commit never` lesson from `batch-run-command-cli-mismatch.md`).
- `test_batch_submit_passes_remote_through` — end-to-end through the CLI to the submitted container command.

Full suite green. README's push section documents the flag and the `dvx cache comm` concordance pattern it exists for.
