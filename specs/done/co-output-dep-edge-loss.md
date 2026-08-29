# Co-output deps produce no ordering edge

## Symptom

A stage that correctly declares a dep on artifact `B` is scheduled in the same
level as (or earlier than) the stage that *produces* `B`, then fails at runtime
because `B` doesn't exist yet.

Happens only when `B` is a **co-output**: one of several `.dvc` files sharing a
single `cmd`, where `B` is not the co-output group's representative node.

## Repro (nj-crashes)

`njdot compute pqt -t crashes` has two `.dvc`s:

    njdot/data/crashes.parquet.dvc            <- B
    njdot/data/crash_pk_mappings.parquet.dvc  <- representative

Two consumers declare a dep on `B`, in both accepted path forms:

    www/public/njdot/crashes.db.dvc:  /njdot/data/crashes.parquet   (root-absolute)
    njdot/data/cm.pqt.dvc:            njdot/data/crashes.parquet    (dvc-dir-prefix
                                       back-compat form, `dvc_files.py:125`)

`dvx run --dry-run -v` places:

    Level 1: njdot/data/cm.pqt, www/public/njdot/crashes.db
    Level 2: njdot/data/crash_pk_mappings.parquet

and `njdot/data/crashes.parquet` **never appears in any level** — 158 targets
collapse to 157 computations, `B` being the one absorbed.

On a machine where `B` already exists this is invisible. On a fresh checkout
(AWS Batch, `--force`) both consumers die:

    FileNotFoundError: [Errno 2] No such file or directory: '/app/njdot/data/crashes.parquet'

Control: four sibling stages (`drivers/occupants/pedestrians/vehicles.db`)
depend on *non*-co-output parquets. After an unrelated dep-path fix they moved
L1 -> L4 correctly, so the leveling machinery is fine in the ordinary case —
it's specifically the co-output collapse that drops the edge.

## Diagnosis (unverified — dvx-side)

Co-output `.dvc`s are collapsed into a single execution node keyed by one
representative. The dep graph appears to be built over *node* identities, so an
edge naming a non-representative co-output path resolves to no node and is
dropped silently. The fix presumably belongs where the collapse happens:
every path in a co-output group should alias to the group's node.

## Why it matters

Silent. A declared dep that doesn't bind is indistinguishable from no dep — no
warning at parse, plan, or run time. The stage just runs too early, and only on
a machine where the file isn't already lying around.

Related: the same silence bites for dep paths that mis-resolve (a bare
cross-directory path resolving under the `.dvc`'s own dir). Both would be
caught by one check: **at plan time, warn when a declared dep resolves to
neither a known artifact nor an existing file.** That single diagnostic is
probably worth more than either individual fix.

## Workaround attempted — does not work

Adding a second dep on the group's representative
(`/njdot/data/crash_pk_mappings.parquet`) to both consumers did **not** fix the
ordering: `cm.pqt` and `crashes.db` stayed in Level 1. Worse, the
representative then disappeared from the plan too — computations went 152 ->
151, and neither `crashes.parquet` nor `crash_pk_mappings.parquet` appeared in
any level. So a dep *on* a co-output seems to remove that node from the plan
rather than create an edge to it, which may be the underlying defect rather
than a separate one.

The change has been reverted; nj-crashes carries no workaround. The two
consumers will keep failing under `dvx run --force` on a fresh checkout until
this is fixed. Options on our side if it drags: exclude both from the reproc
target set, or merge them into the producing stage as further co-outputs.

---

## Resolution

Implemented 2026-08-29. **The diagnosis in this spec is wrong in an instructive way: co-outputs have nothing to do with it.** The trigger is `prune_fresh` interacting with target order, and the co-output pair in the repro is a coincidence.

### What actually happens

`run()` walks targets breadth-first, keyed by output path. When an artifact is fresh per its own `.dvc`, it prunes — registering each of its deps as a bare leaf (`Artifact(path=…)`, no computation), meaning "already satisfied, don't walk further". That's correct only while nothing else needs the dep as a real node. The loop's first line was:

```python
if output_str in artifacts:
    continue
```

So once a pruned consumer had planted a placeholder at `njdot/data/crashes.parquet`, the *explicit target* `njdot/data/crashes.parquet.dvc` — popped later from the same queue — hit that early return and was never loaded. It kept the placeholder's identity: a leaf, no computation. Leaves are filtered out of the levels (158 → 157), and being in `done` from the start they impose no ordering, so every consumer stayed in Level 1.

This explains all three observations, including the two the spec found inexplicable:

- **Why `cm.pqt`, and not the four `.db` siblings.** Pure sort order. `njdot/data/cm.pqt` sorts *ahead* of `njdot/data/crashes.parquet`, so it prunes first and plants the placeholder. `njdot/data/drivers.parquet` sorts ahead of `www/public/njdot/drivers.db`, so the parquet is loaded properly first and the later prune is a no-op — hence L1 → L4 worked for them.
- **Why the workaround made it worse.** Adding a dep on `crash_pk_mappings.parquet` gave the fresh consumers a *second* dep to prune, so the representative got a placeholder too and also vanished (152 → 151). Exactly as observed. A dep *on* a co-output doesn't "remove that node" — a dep *from a fresh consumer* removes any node not already loaded.
- **Why it's invisible locally in the ordinary case.** Pruning requires the consumer to be fresh, which requires its output to exist. It bites hardest on the machine that has everything — the opposite of the usual "works on my box" polarity.

### The fix

Placeholders are now provisional. `run()` tracks which paths were registered by pruning; a later explicit target, or a dep of a stage we *didn't* prune, upgrades the placeholder to a real artifact and resumes traversal from it. A placeholder nothing else claims stays a leaf, so pruning keeps doing its job.

Verified in `~/c/hccs/crashes` at `094ed51f100`: `crashes.parquet` is back in the plan (Level 1) with both consumers correctly in Level 2, and the full reproc target list now plans **158 targets → 158 computations**, up from 157.

### Plan-time diagnostic

Also implemented, and this spec is right that it's worth more than either individual fix. `⚠ <artifact>: dep '<resolved>' matches no .dvc file and no path on disk — no ordering edge`, emitted before the execution plan (so `--dry-run` shows it).

Deliberately narrow: a dep *with* a `.dvc` is fine even when its file is absent — that's a pruned-fresh leaf, or something an earlier level builds. Only "no `.dvc` **and** no file" is unambiguously broken, which keeps it silent on a fresh checkout where most outputs don't exist yet.

Replayed against `094ed51f100^` (the state before nj-crashes' hand-fix), it flags **exactly the six** misresolved deps that were found by hand, and nothing else:

```
data/county-city-codes.parquet  -> data/www/public/Municipal_Boundaries_of_NJ.geojson
njdot/data/muni_codes.parquet   -> njdot/data/www/public/Municipal_Boundaries_of_NJ.geojson
www/public/njdot/{drivers,occupants,pedestrians,vehicles}.db
                                -> www/public/njdot/njdot/data/<tbl>.parquet
```

Worth noting for anyone hand-editing `.dvc`s: `write_dvc_file` always emits the `/`-prefixed form for a dep outside the `.dvc`'s own directory, so dvx never *creates* this shape. It only arises from hand-authored files — which is where all six came from.

### `dvx batch submit --watch`

Filed alongside this as "the second time watching has misled me tonight". I could not reproduce a `--watch` that returns 0 while the job is still RUNNING — `_watch` returns only on `SUCCEEDED`/`FAILED`, so if that happened the exit code likely came from elsewhere in the pipeline (a wrapper script, a `tee`). **If it recurs, the invocation and its full exit path would pin it.** Two adjacent defects were real and are fixed:

- **The log tail was dropped.** `get_log_events` caps a page at 1 MB / 10k events; the loop drained one page per 15s poll, including the *final* poll. On a chatty run the watch fell steadily behind and then returned without printing what the container said last — the run summary above all. That matches "the summary never printed" exactly, and may be the whole of the reported symptom. Now pages until the forward token stops advancing.
- **A retried attempt's paging token was carried across streams.** Spot reclaims are routine on the Spot queue, and tokens are scoped to the stream that issued them. The new stream is now read from its head, and the restart is announced (`new attempt, log stream: …`) so a silent retry reads as a retry.
- **A missing log stream was swallowed forever**, rendering a wrong log group as a job that simply prints nothing — which is what cost an evening of building out-of-band pollers. Now warns once, naming the group.

### Tests

- `test_fresh_consumer_does_not_shadow_an_explicit_target` — two stages, consumer passed first; asserts both are in the plan *and* that the level count reflects a real ordering edge.
- `test_warns_when_a_dep_resolves_to_nothing` — the hand-authored bare form warns; the same file with a leading `/` warns about nothing.
- `test_watch_drains_the_log_tail_before_returning`, `test_watch_resets_paging_token_on_a_new_attempt`, `test_watch_warns_once_when_the_log_stream_is_missing`.

### Not addressed

`www/public/census/population.parquet` isn't reproducible from a clean checkout (its `census/data/raw/*.json` cache is untracked and `build.py` never fetches). That's an nj-crashes data-tracking decision, not a dvx defect.
