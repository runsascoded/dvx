# `dvx batch`: task role, explicit networking, and arbitrary submit commands

Requested by ctbk (2026-09-26, `ctbk/specs/batch-pipeline.md`). ctbk moved its trips-DAG Batch stack to a Pulumi-managed HCCS account and had to route around three `dvx.batch` gaps. Each is small; together they'd let projects use `dvx batch bootstrap/submit` directly instead of re-implementing the job definition in IaC.

## 1. Task role (`jobRoleArn`)

`job_definition_spec` sets only `executionRoleArn` (image pull, logs, secrets). A job whose *own code* calls AWS (ctbk's `norm` reads Citi Bike's `s3://tripdata`) needs a task role. Today the options are baking static keys (which the secrets work was meant to end) or skipping dvx for the job def.

- `job_definition_spec(job_role_arn=None)` → `containerProperties.jobRoleArn`.
- `bootstrap(job_role_policy: dict | None)`: create-if-missing `<prefix>-batch-job` with the given inline policy, or `--job-role-arn` to use an existing role.
- CLI: `bootstrap --job-role-arn ARN` (and/or `--job-role-policy FILE`).

## 2. Networking (non-default VPC)

`bootstrap` discovers `default-for-az` subnets and the `default` SG. Accounts without a default VPC (or that deliberately run Batch in a managed VPC, like HCCS's pyrmts-engine subnets) can't use it.

- `bootstrap(subnets=[…], security_group_ids=[…])`, CLI `--subnet` / `--sg` (repeatable), falling back to today's discovery.

## 3. Arbitrary container commands on `submit`

`dvx batch submit` always sends `run_command(...)`. ctbk's entrypoint also has a `script '<sh>'` mode (for steps that aren't `dvx run`-able), so it calls `dvx.batch.submit(command=[...])` from its own CLI. A passthrough would make that a one-liner:

- `dvx batch submit --command -- script 'ctbk station-harmonize create -f'`, or a `-c/--cmd` JSON/argv form, mutually exclusive with targets and run flags.

## Non-goals

- IaC ownership: projects that manage these resources in Pulumi keep doing so. These changes just stop dvx's imperative path from being the only one that can't express them.

## Tests

Pure-builder tests in the style of `tests/test_batch.py`:
- `job_definition_spec(job_role_arn=…)` emits `jobRoleArn`;
- `compute_environment_spec` honors explicit subnets and SGs;
- the CLI passes `--job-role-arn` / `--subnet` / `--sg` / `--command` through.
