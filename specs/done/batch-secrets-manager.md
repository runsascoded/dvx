# Batch: Secrets Manager / SSM `secrets` for container credentials

## Problem

`dvx.batch.register_job_definition` (via `bootstrap`) and `submit_overrides`
inject credentials only through the container's plaintext `environment` array:

```python
"environment": [{"name": k, "value": v} for k, v in sorted(env.items())]
```

So AWS access keys (job-def env) and — for git push-back — a GitHub RW token
(per-job env override) sit in cleartext, readable by anyone with
`batch:DescribeJobDefinition` / `batch:DescribeJobs`. AWS Batch natively
supports a `secrets` array on `containerProperties` that references Secrets
Manager or SSM Parameter Store ARNs; the value is fetched by the execution
role at container start and injected as an env var, never stored in the
job-def/job. dvx doesn't expose it.

## Motivating use case

Fargate reproc push-back (nj-crashes `batch/entrypoint.sh`): the container
runs `dvx run --commit --push each`, which `git push`es each regenerated
`.dvc`. That needs a repo-scoped `Contents:RW` GitHub token in the container.
Today it can only arrive as a plaintext env override.

## Proposal

1. `bootstrap(..., secrets: dict[str, str] | None = None)` and
   `submit(..., secrets=...)` — map `ENV_VAR -> secret-ARN` (Secrets Manager
   full ARN, or SSM parameter ARN; optionally `arn:...:secret:NAME:json-key::`
   for a single JSON field).
2. Emit `containerProperties.secrets` / `containerOverrides.secrets`:
   `[{"name": k, "valueFrom": arn} for k, arn in secrets.items()]`.
3. The execution role needs `secretsmanager:GetSecretValue` (and/or
   `ssm:GetParameters`) + `kms:Decrypt` on the CMK — `bootstrap` should attach
   an inline policy scoped to the referenced ARNs when it manages the role,
   else document the requirement.
4. Precedence: a name present in both `environment` and `secrets` is a config
   error — raise.

## Migration

nj-crashes `tmp/reproc-submit-c.py` would pass
`secrets={'FARGATE_GITHUB_RW_TOKEN': '<arn>'}` instead of a plaintext
`environment` entry; the AWS keys likewise move once an instance/task role or
secret is set up.

## Resolution

Implemented as specified. `src/dvx/batch.py`:

- **Pure builders** (the tested surface): `_secrets_container_list` (name-sorted `[{name, valueFrom}]`), `_reject_env_secret_overlap` (raises on a name in both env and secrets — point 4), and `execution_role_secrets_policy` (point 3). The policy classifies each ARN by its service field: Secrets Manager → `secretsmanager:GetSecretValue` on the *base* secret ARN (first 7 colon fields, so a `:json-key:stage:id` `valueFrom` selector is stripped), SSM → `ssm:GetParameters` on the ARN, anything else → `ValueError` (refuse to over-grant). A `kms:Decrypt` statement on `*` is always appended (the CMK ARN isn't known at bootstrap time; scope it manually for a customer-managed key).
- `job_definition_spec(..., secrets=...)` emits `containerProperties.secrets` (point 2); no key when empty.
- `submit_overrides(..., secrets=...)` emits the per-job `containerOverrides.secrets`.
- `bootstrap(..., secrets=...)` `put_role_policy`s the inline `dvx-batch-secrets` policy (idempotent — always overwrites to match the current secret set) and bakes the secrets into the job-def. `submit(..., secrets=...)` injects them as an ephemeral per-job override and makes **no** IAM changes — its ARNs must already be covered by a prior `bootstrap(secrets=...)`.

CLI (`src/dvx/cli/batch_cmd.py`): `-s/--secret NAME=ARN` (repeatable) on both `bootstrap` and `submit`, parsed by `_parse_secrets`.

**Migration for nj-crashes** (per the spec's Migration section): store the token in Secrets Manager, then either
- bake it in once: `dvx batch bootstrap -i <img> -s FARGATE_GITHUB_RW_TOKEN=<arn>` (grants the role + puts it in the job-def; no per-submit flag needed), or
- keep it per-job: `bootstrap -s ...` once to grant the role, then `dvx batch submit -s FARGATE_GITHUB_RW_TOKEN=<arn> ...`.

Either way the token stops riding in plaintext container env. AWS keys can move the same way once a secret/parameter holds them.

**Tests** (`tests/test_batch.py`, 12 new): job-def/submit-override secrets arrays + no-key-when-absent, env↔secret overlap raises (both builders), the IAM policy for SM (json-key suffix stripped), SSM, mixed, and unknown-ARN-raises, CLI `--secret` passthrough + parse error, and two mocked-`_clients` bootstrap tests asserting the scoped `put_role_policy` call (and that no-secrets touches neither the policy nor the job-def).
