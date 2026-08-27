# `dvx batch`: run a repo's DAG on AWS Batch (Fargate Spot)

Requested by the nj-crashes session (2026-08-26). Goal: reproc every DVX target in a repo from scratch — max parallelization, min wall-time — on ephemeral cloud compute, with no standing infra. The pattern is a direct port of `pyrmts-engine batch` (`~/c/pyrmts/specs/done/engine-batch-packaging.md` + its findings 1–7), which ctbk validated end-to-end in July; steal its decisions and scars wholesale.

## Why DVX is unusually well-suited to this

pyrmts-engine had to *build* the properties that make Spot viable; DVX has them natively:

- **Resume-from-manifest is free.** pyrmts' top finding was that Spot reclaims kill multi-hour builds unless completed work is skippable on re-run. DVX stages are content-addressed: with `--push each`, every completed stage is durable in the S3 remote before the next starts, and a re-submitted job's `dvx run` skips (or auto-pulls) everything already pushed. A reclaim costs one in-flight stage, not the run.
- **The DAG and its levels already exist** (`_group_into_levels`), with intra-level parallelism via `-j`.
- **Zero-output guards exist**: missing-target errors (`0ec2b1635`), dep freshness, `EmptySourceError`-style failure surfacing is the executor's normal behavior.

So the deliverable is thin: packaging + submit tooling, not an engine.

## Deliverables

### 1. Base container image

`Dockerfile` in dvx: python + `uv` + dvx + awscli, entrypoint `dvx`. Apps derive (`FROM dvx-batch:<rev>` + `COPY . && uv sync`) — pyrmts option (b), for the same reason (cold-start `uv sync` of a fat app would dominate short runs; derived images are cheap).

The app image must contain the repo's *git worktree* (dvx reads `.dvc` files + `git_deps` from the checkout), plus its venv. `git clone --depth 1` at build time or `COPY` from the build context both work; document that `git_deps` hashing needs the git objects for HEAD (full-depth not required).

### 2. `dvx batch` subcommands (port of `pyrmts_engine/batch.py`)

- `dvx batch push <ecr-ref>` — ECR login via boto3, `docker build` (default `--platform linux/arm64`), push, create repo if missing.
- `dvx batch bootstrap -i <ecr-ref> [-a ARM64] [-e K=V ...]` — idempotent CE (Fargate **Spot**), queue, job def, log group, logs-only IAM role. Plain boto3, no CDK. Defaults: 16 vCPU / 64 GiB / 100 GiB ephemeral, spot-retry ×2, `PYTHONFAULTHANDLER=1` baked into the job-def env (pyrmts finding 6 — it's free and turns mute 139s into diagnoses).
- `dvx batch submit [targets...] [--force] [-j N] [--watch] [--on-demand]` — `submit-job` wrapping `dvx run --commit never --push each [-f] [targets]`; `--watch` tails CloudWatch. `--on-demand` submits to a second (on-demand) CE/queue for "final" runs (pyrmts finding 7's knob).

### 3. Execution modes

**Phase 1 — single fat job (build this).** One Fargate task runs `dvx run -j <vcpus>` over the whole DAG. DVX's level-parallelism gives the fanout; the S3 remote gives durability. For repos like nj-crashes (~1,400 targets, the wide levels being ~25 years × ~5 tables of embarrassingly-parallel per-year conversions, each minutes), a 16-vCPU Graviton box should clear the whole DAG in low single-digit hours for ~$1–3 on Spot. This is the 90% solution and needs nothing DVX-side beyond what exists.

**Phase 2 — level fanout across jobs (defer until Phase 1's wall-time disappoints).** A driver walks the DAG levels and submits one Batch job per stage-group with `dependsOn` edges mirroring the DVX edges; each job runs `dvx run <target>` (deps auto-pull from the remote, output pushes back — the remote is the coordination substrate, no shared disk needed). Costs to respect before building: Fargate task startup is ~30–60 s, so per-*stage* jobs only win when stage cost ≫ startup — batch cheap stages into per-level jobs, fan out only the heavy EP families. The `dependsOn` graph also caps at 20 parents/job (Batch limit), so wide joins need a synthetic barrier job.

### 4. Git-side effects OFF by default

`--commit never` in the container: the job must not need push access to the git repo. The `.dvc` md5 updates land in the remote cache (that's what makes resume work); reconciling `.dvc` files in git happens back on a dev machine or in CI via `dvx run` (instant — everything fresh, auto-pull) or `dvx commit`. This also sidesteps credentials: the container gets S3 keys only.

## Inherited decisions (don't relitigate)

- **ECR over GHCR** (IAM-native, same-region, free pulls).
- **ARM64/Graviton Spot** as default (runtime-verifiable locally on Apple Silicon; ~20% cheaper; pyarrow-on-amd64-under-Rosetta can't be smoke-tested).
- **No teardown command** — idle CE/queue/role/logs cost nothing.
- **Size to footprint, not RSS** (Fargate has no swap; allocator retention counts fully — pyrmts finding 5). DVX subprocesses die per-stage so retention is less of a risk than pyrmts' long-lived process, but polars/pandas stages within one `dvx run` job still each get a fresh process — good.
- Config/creds via env; if both `AWS_*` and `R2_*` matter, remember pyrmts' quirk: inject only the set you mean.

## Non-goals

- No distributed single-stage execution (a stage runs on one node; stages that need more should shard themselves into more stages).
- No scheduling/cron (Batch jobs are submitted by a human or an app's CI).
- No GPU.

## Resolution — Phase 1 landed

**Files added**:

- `src/dvx/batch.py` — pure spec builders (`compute_environment_spec`,
  `job_definition_spec`, `push_commands`, `run_command`, `submit_overrides`,
  `ECR_LIFECYCLE_POLICY`) + boto3 orchestration (`push_image`, `bootstrap`,
  `submit`, `_watch`). Direct port of pyrmts_engine's `batch.py`; the
  adaptations are DVX-specific defaults (ARM64 / 16 vCPU / 64 GiB / 100
  GiB ephemeral) and the simpler `run_command` builder that wraps
  ``dvx run --commit never --push each -v [--force] [-j N] [targets]``.
- `src/dvx/cli/batch_cmd.py` — Click group with three subcommands:
  `push`, `bootstrap`, `submit`. `submit` derives a Batch-safe job name
  from the target list if `-n` is not passed.
- `Dockerfile` at repo root — Python 3.13-slim base, installs `dvx[s3]`
  from PyPI, entrypoint `dvx`. App images derive with `FROM dvx:<rev>` +
  `COPY . && uv sync` per the design note above.
- `tests/test_batch.py` — 23 tests exercising the pure builders + the
  CLI surface (subcommand + option-set assertions). boto3 orchestration
  itself is not integration-tested (no live AWS in this repo's CI); the
  boto3 calls are thin wrappers over the tested builders.

**pyproject.toml**: added `batch = ["boto3"]` optional-deps group.
Runtime users install via `pip install dvx[batch]`; without it,
`dvx batch --help` still works (boto3 is imported lazily inside the
orchestration functions).

**Phase 2 (level fanout across jobs) deferred** — not built, not
scaffolded. The single-fat-job pattern from `submit` handles the 90%
case identified in the spec (DVX's own level-parallelism inside one
container). If wall-time disappoints, revisit.

**Container hardening**: `PYTHONFAULTHANDLER=1` baked into
`job_definition_spec`'s default env — a mute SIGSEGV surfaces a
traceback rather than an unexplained exit 139 (finding 6 from
pyrmts-engine).

**ARM64 default**: matches the DVX-spec preference for Graviton
(cheaper, runtime-verifiable on Apple Silicon). The `push` subcommand
defaults `--platform linux/arm64` accordingly; pair with `bootstrap
--arch X86_64` if you'd rather deploy amd64 images.
