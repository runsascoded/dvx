# A Python DAG-construction SDK for dvx (and the IDL question)

Status: design note / proposal (2026-10-23). Not yet implemented. Origin: dvx-session discussion while standing up the AWS/GCP Batch executors (`batch-backends`). As we schedule/execute DAGs on arbitrary engines, we want to (a) specify resources per node, (b) factor reused values so one value applies in many places, and (c) manipulate configs in Python before submitting to an executor. This note argues for the smallest thing that buys that, and records two framing decisions (IaC-vs-SDK layering; IDL/codegen) so they're reviewable rather than relitigated.

## Motivation

The authoring surface for dvx pipelines is YAML `.dvc` files, one per output, with `meta.computation` (cmd, deps, resources, …). That's the right **source of truth** — local, git-committed, human-readable. It is the wrong **authoring interface** once pipelines get large or parametric:

- **No DRY.** YAML can't factor a reused value, compute a path, or loop over partitions. Anchors/aliases exist but don't compose and nobody hand-maintains them across 158 files.
- **No programmatic manipulation.** "Generate a stage per partition", "re-target this subtree", "inject a per-env resource override", "filter the DAG to one branch" — all want code, not text.

The present, concrete use case is **reproc stage generation**: ctbk emits ~158 near-identical normalized-month stages; crashes does the same shape. That is a Python `for` loop over partitions with shared image/path/resource vars — valuable *today*, independent of any new executor.

Note: per-node resources are **already supported** on the `batch-backends` branch (`meta.computation.resources.{cpus,mem_gb}`, consumed by resource-aware scheduling and remote dispatch). So the gap this SDK fills is **authoring/manipulation**, not dvx's support for the fields. The corollary: resources (and future placement hints) can be carried as **opaque metadata** the executor reads only what it understands — so the SDK delivers value before every backend learns every field.

## Decision 1 — two layers, kept separate

- **Infrastructure (Pulumi/CDK/IaC):** the compute environment, Batch queues, IAM roles, registries, buckets. Long-lived desired-state, reconciled. This is where `dvx batch bootstrap` operates and where the "all cloud resources should be IaC" rule applies.
- **Workload (this SDK + dvx):** the per-run stage graph — data dependencies, per-stage resources, placement, content-addressed resume. Ephemeral per run.

Pulumi/CDK are **not** the tool for the workload layer: they have no notion of "run B after A produces artifact X; skip if X is fresh." They sit *underneath* the DAG, not at it. Conflating the two is the category error to avoid.

## Decision 2 — a library, not an eDSL; a schema, not an IDL

**Library, not eDSL.** "One value in many places" is just a Python variable once config is Python objects; "manipulate the DAG" is just iterating a list. That's a typed library of composable objects, not an embedded DSL with new syntax/operators. An eDSL (`a >> b`, decorators) is Airflow's path and drags orchestration-runtime concerns into the authoring layer — the main thing *not* to copy. Keep the SDK a pure config-construction + transform layer; runtime stays in the executor.

**Schema, not IDL/RPC.** A protobuf/gRPC-style IDL earns its keep at a cross-language *network boundary*; dvx's design is serverless ("the `dvx run` process is the only driver; there is no server") and Python-only today, so the RPC half has nothing to attach to. Generated classes are dumb data holders anyway — the ergonomic API is hand-written per language regardless, so codegen saves less than it appears, while a binary wire format fights the git-committed YAML SoT. Instead:

- **Pydantic is the Python SoT** for the model → `model_json_schema()` gives a **JSON Schema** export for free.
- **JSON Schema is the language-neutral contract** for the `.dvc`/submission format: it maps 1:1 onto the YAML/JSON `.dvc` already is, validates and documents the format, and lets a future Go/TS client generate types — with zero RPC/binary baggage, and without foreclosing a real IDL later.

Revisit a true IDL only when dvx grows an actual submission **service**, or a **second first-class authoring language**. Until then, YAGNI; the JSON-Schema discipline keeps the door open at ~10% of the cost.

## Sketch of the model

Pydantic objects that compose in Python and *lower* to artifacts dvx already understands (the `.dvc` graph + per-stage resource/placement metadata). Roughly:

```python
from dvx.sdk import Dag, Stage, Resources

big = Resources(cpus=16, mem_gb=64)              # defined once
img = "us-central1-docker.pkg.dev/proj/app:REV"  # reused everywhere

dag = Dag()
prev = None
for month in months:                              # the reproc pattern, in code
    s = dag.stage(
        name=f"norm/{month}",
        cmd=f"app normalize {month}",
        deps=[f"raw/{month}"] + ([prev.out] if prev else []),
        out=f"normalized/{month}",
        resources=big,                             # one value, many nodes
        executor="gcp:heavy",                      # opaque placement hint
        image=img,
    )
    prev = s

dag.validate()                                     # pre-submission checks
dag.to_dvc()                                        # emit/update .dvc files  (authoring)
# or
dag.submit(backend="gcp:heavy", dry_run=True)       # hand to the executor seam
```

- `to_dvc()` is the authoring/DRY win: generate or rewrite the `.dvc` graph from code, committed as usual. Works **today**, no executor needed.
- `submit(backend=…)` is the seam onto the executors `batch-backends` added; `dry_run` first.
- `resources` / `executor` / `image` ride as metadata; a backend consumes what it understands and ignores the rest.

## Decision 3 — three artifacts: provenance (`.dvc`) vs. authoring (SDK) vs. execution manifest ("run lock")

Placement/resources is **run-config, not provenance**, and must never participate in freshness/content-addressing — otherwise bumping `cpus: 16 → 32` spuriously invalidates byte-identical outputs. That splits the world three ways:

| Artifact | Role | Content-addressed? | Lives |
| --- | --- | --- | --- |
| `.dvc` (committed) | provenance: cmd, deps+hashes, outs+hashes | **yes** | next to the artifact, in git |
| SDK (Python) | authoring: build / DRY / manipulate the DAG | n/a | code |
| execution manifest ("run lock") | rendered run-config + job identity | no | coordination store (`results_url`), keyed by input-hash |

The **run lock** is a file shaped like a `.dvc` node but with *all execution config rendered in*: resolved image REV, resources, backend, queue, env, secret **versions**, exact command, input content-hashes, and the resulting job ID(s). Upstream comparison: DVC's `dvc.lock` is a *provenance* lock (resolved cmd + dep/out hashes + params) for the central `dvc.yaml` model — a role dvx already subsumes into per-`.dvc` `meta.computation`. DVC has **no execution/placement lock** (no remote-executor concept to render one from), so this is net-new and deliberately *separate* from the `.dvc`, not extra fields on it.

It is what makes **reattachment** and **audit** first-class: `batch-backends` already reattaches via content-addressing + `input_key` and writes result sidecars to `results_url`; the manifest extends that (record the fully-resolved spec + job ID beside the result). For the reproc audits (crashes/ctbk), "what exact config ran partition X, where" is precisely the artifact they reconstruct by hand today.

Lean: the manifest lives in the **coordination store, not git next to the `.dvc`** (committing image-REVs/job-IDs churns the repo and re-mixes run-config into provenance); a committed *audit* variant can be opt-in per project. This is a **v2 concern** — v1 remains author → `.dvc` → `submit`.

## Prior art — borrow ergonomics, don't adopt

**Flyte** and **Dagster** are the reference designs for typed-Python DAGs with per-task resource requests, reused config, content-addressed caching, and remote execution — Flyte is nearly a superset of dvx's ambitions. Read both before writing much: borrow their resource/config-composition ergonomics, and note where their authoring/runtime coupling went wrong. Do **not** adopt either wholesale — both would subsume dvx's locality-first, git-committed, per-`.dvc` provenance model, which is the thing that makes dvx dvx.

## v1 scope (smallest useful thing)

1. Pydantic model: `Stage`, `Resources`, `Dag`, with composition + `validate()`.
2. `Dag.to_dvc()` — emit/rewrite the `.dvc` graph (round-trips the existing format; reuses `write_dvc`/`read_dvc_file`). This alone unblocks reproc stage generation.
3. JSON Schema export of the model, checked into the repo as the published contract.
4. `Dag.submit(backend, dry_run=…)` onto the existing executor seam — thin.

**Non-goals for v1:** a runtime/scheduler of its own; an eDSL syntax; protobuf/gRPC; a non-Python client (JSON Schema leaves the door open — we don't walk through it yet).

## Decided

- **`.dvc` stays the SoT.** The SDK is a constructor/manipulator over the `.dvc` graph, not a replacement in-memory model — so git review of generated stages is preserved.
- **Placement/run-config does not go in the `.dvc`.** It renders into the execution manifest (Decision 3); the `.dvc` stays provenance-only and placement-free. Resources that `batch-backends` reads from `meta.computation.resources` are tolerable there only while they never feed the dep-hash; new placement fields land in the manifest, not provenance.
- **The SDK produces the fields `batch-backends` already consumes** (resource-aware scheduling, remote dispatch) — it does not re-model them.

## Open questions

- Exact on-disk shape + location of the execution manifest (coordination-store layout; keying by input-hash; the opt-in committed audit variant).
- Whether `resources` should eventually migrate out of `meta.computation` into the manifest too, or stay (it's scheduling input, reasonably stable per-stage) — defer until the manifest exists.
