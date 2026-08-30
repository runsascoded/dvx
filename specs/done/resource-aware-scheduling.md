# Resource-aware level scheduling

> From the `hccs/crashes` full-DAG reproc. `/read crashes` for context.

## Problem

`dvx run` groups the DAG into levels and runs every stage in a level
concurrently (bounded by `-j` / cpu count). Memory is invisible to that
scheduler. In the nj-crashes reproc, one level held ~9 stages, several of them
heavy pandas/sqlite builds; run together on a 64 GB Fargate task they blew the
memory ceiling and the OS OOM-killed the largest (`crashes.db`, exit 137),
failing the level. Everything else in the level was fine.

The only levers available were both blunt:

- **Raise the whole job's memory** to the platform ceiling (64→120 GB here), so
  every stage — including the 50 tiny ones — pays for the peak of the heaviest.
- **Cap `-j`** globally, serializing the *whole* level to protect against its
  one fat stage, throwing away parallelism on the light ones.

Neither expresses the actual shape: "these 2 stages want ~40 GB each, the other
7 are cheap." A scheduler that knew that could run the 7 light ones fully
parallel and the 2 heavy ones one-at-a-time, on a right-sized box.

## Sketch

1. **Declare** per-stage resource hints in the `.dvc`, e.g.

   ```yaml
   meta:
     computation:
       cmd: njdot compute db -t crashes
       resources:
         mem_gb: 40      # peak RSS estimate
         cpus: 4
   ```

   Optional and advisory; absent ⇒ today's behavior. A stage that OOMs is a
   natural place to record what it actually needed (dvx already watches exit
   codes; 137 could even prompt "consider `resources.mem_gb`").

2. **Pack** each level under a budget instead of a fixed fan-out. Given a host
   budget (`--mem GB`, `--cpus N`, or introspected), schedule a level as a
   bin-packing / greedy-by-descending-mem fill: start stages while their summed
   `mem_gb` fits the budget, queue the rest. Unlabeled stages take a default
   weight. This degrades to the current all-at-once behavior when nothing is
   labeled and the budget is generous.

3. **Batch tie-in (bigger).** Once stages carry resources, the batch executor
   can stop sizing one job for the whole DAG's peak. Options on a spectrum:
   - keep single-job, but let dvx right-size *that* job to the DAG's max
     concurrent-mem under its own packing (cheaper than "ceiling to be safe");
   - or, Phase-2 multi-job, map heavy stages to a bigger memory class and the
     wide light fan-out to many small tasks — the resource labels are exactly
     the input a placement policy needs.

## Why it's worth it

The failure mode is generic: any level with a mix of light and heavy stages on
a shared box. The current workaround (ceiling the whole job) scales cost with
the single heaviest stage across the *entire* run, not per level. Even a crude
greedy packer keyed off one `mem_gb` hint would have run the nj-crashes level
correctly on the original 64 GB box. And the hints double as documentation —
`crashes.db wants ~40 GB` is worth having written down wherever the pipeline is
described.

## Non-goals / cautions

- Not a cgroup enforcer. `mem_gb` is a *scheduling hint*, not a hard limit; a
  stage that lies still OOMs, just as today. Enforcement (per-stage cgroup caps)
  is a separable, later concern.
- Estimates rot. Peak RSS shifts with data volume across years; treat the hint
  as a floor to pack against, and let the OOM-then-annotate loop keep it honest
  rather than promising precision.

## Resolution

**Phase-1 shipped** — declare + budget-packed level scheduling. Phase-2 (batch multi-job placement) is a separate, larger effort left for later.

### Declaration

`meta.computation.resources.{mem_gb, cpus}` parses into `DVCFileInfo.resources` and `Computation.resources` (`_parse_resources` in `dvc_files.py`). Advisory: unknown keys and non-numeric values are dropped rather than erroring, so a hint the scheduler doesn't model yet never fails a run. It's **author-owned** — like `side_effect`, it survives the stage's own rewrite (the merge preserves it when the executor's dep-hash refresh doesn't pass it), so a stage can't strip its own `mem_gb`.

### Scheduling

The mechanism is deliberately *not* a re-architecture of level admission. A `_BudgetGate` (a `Condition` + running total) wraps **only the primary cmd's `subprocess.run`**:

- Co-outputs run no cmd (they verify + write their `.dvc`), so they never acquire — which is what keeps the gate from ever deadlocking against the primary's wait for its co-outputs. This was the crux: the existing invariant "`max_workers` ≥ largest cmd-group" stays intact, and the gate is purely additive on top of it.
- `acquire(w)` blocks while `running > 0 and running + w > budget`; a stage heavier than the whole budget is clamped to it (runs *alone*), and because the wait only holds while something else runs, an idle gate always admits the next stage. So forward progress is guaranteed — no configuration can hang the pool.

The budget: `--mem GB` wins; else a run with any `mem_gb`-labeled stage introspects total RAM (`_total_ram_gb`, `os.sysconf` — Linux + macOS, the reproc's platforms); else the gate is off (today's fixed fan-out). An unlabeled stage charges `--mem-default` (0 by default), so the feature is opt-in per stage — nothing blocks unless it's annotated.

The nj-crashes level that OOM'd (`crashes.db` + 8 siblings on 64 GB) would, with `crashes.db` labeled `mem_gb: 40`, run it while the light unlabeled siblings pack around it — correct on the original 64 GB box, no global `-j` cap.

Tests (`tests/test_resource_scheduling.py`): parse + unknown-key drop; round-trip + author-owned durability; the gate's three guarantees as deterministic thread tests (admit-under-budget, block-then-release, over-budget-runs-alone); an end-to-end serialization test (two 40 GB stages under a 64 GB budget log disjoint `[start,end)` intervals); and the off-when-unlabeled degradation.

### Not done (Phase-2 and beyond)

- **`cpus` is parsed and round-tripped but not yet a scheduling axis.** `-j` already caps concurrency by count; memory was the failure mode. A cpu budget can layer on the same gate later.
- **Batch job-sizing tie-in.** The labels are now the input a placement policy needs, but dvx still submits one Batch job sized for the DAG's peak. Right-sizing that job to the packed max-concurrent-mem, or mapping heavy stages to a bigger memory class, is the Phase-2 work.
- **Not a cgroup enforcer** (unchanged from the spec's non-goals): `mem_gb` is a scheduling weight, not a hard limit; a stage that under-declares still OOMs.
