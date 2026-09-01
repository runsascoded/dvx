# `dvx.batch`: per-project resource prefix (kill the shared-`dvx` collision)

Status: done (2026-09-01). Surfaced by the nj-crashes reproducibility-audit session, which found its x86 audit bootstrap stepping on ctbk's Batch infra (the shared `dvx` job def was pointing at `ctbk-reproc` mid-run).

## Problem

`dvx.batch` hardcoded `PREFIX = "dvx"` for **every** AWS resource it manages — the IAM execution role (`dvx-batch-execution`), its inline secrets policy (`dvx-batch-secrets`), the CloudWatch log group (`/dvx/batch`), the Fargate compute environments (`dvx-spot` / `dvx-od`), the job queues (`dvx` / `dvx-od`), and the job definition (`dvx`). Only the ECR repo is per-image. So every project that imports `dvx.batch` (ctbk, nj-crashes, …) shares one global resource set:

- **Race / TOCTOU.** `bootstrap()` re-registers job def `dvx`; `submit()` targets `jobDefinition=dvx` (latest revision). The pair isn't atomic — if another project bootstraps in between, a job runs the wrong image. Single-tenant-serial was an unenforced assumption.
- **Cross-project config/cred leak.** Submit scripts bake AWS creds + project env (`NJC_S3`, …) into the shared job def's `environment`. A project submitting against `dvx`-latest without re-bootstrapping runs with *another project's* creds and config. ("Next user re-bootstraps" was load-bearing and unchecked.)

## Fix (this change)

Namespace every managed resource by a **project prefix** (default `"dvx"`, so existing behavior is byte-for-byte unchanged):

- Name derivations are pure helpers — `log_group(prefix)` → `/<prefix>/batch`, `execution_role(prefix)` → `<prefix>-batch-execution`, `secrets_policy(prefix)` → `<prefix>-batch-secrets`, `spot_ce(prefix)` → `<prefix>-spot`, `od_name(prefix)` → `<prefix>-od` (also the on-demand queue; the spot queue is the bare `<prefix>`, and the job def name is the bare `<prefix>`).
- The default-prefix module constants (`LOG_GROUP`, `EXECUTION_ROLE`, `SECRETS_POLICY`) are kept, now defined via the helpers, for back-compat with importers.
- `bootstrap(prefix=...)` addresses role / secrets-policy / log-group / CE / queue / job-def by the prefix. `submit(prefix=...)` targets the prefixed job def + log group; `queue` still selects spot (`<prefix>`, the default) vs on-demand (`<prefix>-od`). `compute_environment_spec(prefix=...)` and the `_watch`/`_drain` log-group are threaded through.
- CLI: `-P/--prefix` (default `dvx`) on both `dvx batch bootstrap` and `dvx batch submit`. `submit --prefix` must match the `bootstrap --prefix` that created the resources; `-O/--on-demand` maps the queue to `<prefix>-od`.

This is the dvx-side root-cause fix for the collision/race. It is **not** a full IaC migration — provisioning is still imperative `boto3` (create-if-missing), and creds still ride as job-def env. Moving the durable resources to Pulumi and swapping baked creds for a task role are separate, project-side follow-ups (tracked on the nj-crashes side); this change makes per-project isolation possible today without them, by giving each project its own resource namespace.

## Tests (`tests/test_batch.py`)

- `test_name_helpers_default_and_custom_prefix`, `test_default_name_constants_match_helpers` — the pure derivations, default + `nj-crashes`.
- `test_compute_environment_spec_custom_prefix` — CE name namespaced (spot + on-demand).
- `test_bootstrap_namespaces_all_resources_by_prefix` — mocked `_clients`: role, log group, CE, queue, and job-def name all addressed under `nj-crashes`, and the job def's `awslogs-group` is `/nj-crashes/batch`.
- `test_bootstrap_secrets_policy_namespaced_by_prefix` — the inline secrets policy is put on the prefixed role under the prefixed policy name.
- `test_submit_targets_prefixed_job_def_and_queue`, `test_submit_on_demand_queue_override` — `submit` uses the prefixed job def; queue defaults to the spot `<prefix>` and honors an explicit `<prefix>-od`.
- `test_batch_bootstrap_passes_prefix_through`, `test_batch_submit_passes_prefix_and_queue_through` — the CLI `-P/--prefix` reaches the functions; `-O` → `<prefix>-od`.

Full suite green (380 passed, 4 skipped).

## Downstream

nj-crashes / ctbk should `bootstrap --prefix <project>` and `submit --prefix <project>` so their Batch infra no longer collides. The default (`dvx`) is unchanged for anything not yet migrated.
