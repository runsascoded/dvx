# Containerized DAG execution on AWS and GCP Batch

Install `dvx[batch,s3]` for AWS or `dvx[batch-gcp,gs]` for GCP. Cloud SDKs load only when needed. Both providers use the same recorded dependency hashes and verified cache materialization; compute placement belongs in run configuration, rather than artifact provenance.

There are three useful execution shapes:

| Shape | Command | Appropriate when |
| --- | --- | --- |
| Whole DAG in one container | `dvx batch submit --watch target.dvc` | Stages are small or share substantial local intermediates. |
| Independent target family | `dvx batch submit --each a.dvc b.dvc` | Target closures are independent and wider than one machine. |
| One driver, distributed DAG | `dvx run --executor gcp:heavy target.dvc` | Expensive stages need separate machines, resource sizes, or local/remote placement. |

The driver keeps dependency ordering and admits a command group as soon as its own dependencies finish. An unrelated slow branch does not hold up its consumers. Co-outputs run as one command group, with their sidecars reconciled before downstream work starts. `-j` limits local command groups; `--remote-jobs` independently limits cloud jobs. Local memory budgets apply to local commands; remote resource requests use the stage's `resources.cpus` and `resources.mem_gb` when supplied.

## AWS

Existing `from dvx.batch import submit, bootstrap, run_command` imports remain available. The implementation now lives in `dvx.batch.aws`.

```bash
dvx batch bootstrap -i ACCOUNT.dkr.ecr.REGION.amazonaws.com/app:REV -P app \
  --job-role-arn arn:aws:iam::ACCOUNT:role/app-task \
  --subnet subnet-a --subnet subnet-b --sg sg-app
dvx batch submit -P app --cached 'data/large/*' -f target.dvc --watch
dvx batch submit -P app --command '["script", "app maintenance"]'
```

Explicit networking requires both subnet and security-group lists. Otherwise discovery uses the default VPC. The execution role pulls images and injects secret references; the task role grants the application's AWS data permissions. `--job-role-policy policy.json` creates/updates a separate managed task role instead of selecting an existing ARN. Bootstrap can create infrastructure; applications with IaC-managed infrastructure should continue using that ownership model.

AWS does not accept secrets in `containerOverrides`. `submit(secrets=...)` and the new backend adapter register a separate definition containing the merged secret references and submit its exact ARN. They do not change the shared definition or expose secret values. `submit_overrides(secrets=...)` now raises with directions to use `submit`.

For driver-managed stages, configure an AWS profile:

```yaml
batch:
  profiles:
    app:
      image: ACCOUNT.dkr.ecr.REGION.amazonaws.com/app:REV
      job_definition: app:12
      queue: app
      on_demand_queue: app-od
      results_url: s3://app-cache/coordination
```

Then use `dvx run --executor aws:app target.dvc`. The profile name is also the resource prefix when no explicit queue/definition is configured. Arrays support up to 10,000 tasks; AWS has no equivalent to the GCP per-array `--parallelism` override, so that option is rejected.

## GCP

```yaml
batch:
  backend: gcp
  profiles:
    default:
      project: my-project
      region: us-central1
      image: us-central1-docker.pkg.dev/my-project/app/image:REV
      service_account: app@my-project.iam.gserviceaccount.com
      results_url: gs://app-cache/coordination
      cache_url: gs://app-cache/cache
      vcpus: 16
      memory_mib: 65536
      boot_disk_gb: 100
      scratch: {type: local-ssd, gb: 375}
      spot: true
      max_retries: 2
      max_run: 4h
      labels: {app: myapp, component: reproc}
      secrets: {TOKEN: app-token}
      env: {PYTHONFAULTHANDLER: "1"}
```

`cache_url` is for bootstrap's storage-IAM audit; actual pipeline transfers use the configured DVC remote. Configure that remote with `dvx remote add -d cache gs://app-cache/cache`. Secret references can be bare names (expanded to the profile project's latest version) or full Secret Manager version paths. A name cannot appear as both a secret and a plaintext environment variable.

```bash
dvx batch bootstrap -b gcp --check
dvx batch push -b gcp -f Dockerfile.gcp us-central1-docker.pkg.dev/my-project/app/base:REV
dvx batch submit -b gcp --dry-run target.dvc
dvx batch submit -b gcp --watch target.dvc
dvx batch submit -b gcp --each --parallelism 8 a.dvc b.dvc
dvx batch watch -b gcp projects/my-project/locations/us-central1/jobs/JOB
dvx batch cancel -b gcp projects/my-project/locations/us-central1/jobs/JOB
```

`-b` works on the `batch` group or individual cloud-facing subcommands. AWS-specific options raise under GCP and vice versa. `--dry-run` prints the GCP SDK's normalized job JSON without SDK client construction or cloud writes. The existing `-n/--job-name` alias remains; use the long `--dry-run` spelling.

GCP bootstrap checks API enablement, the service account, caller `actAs`, project roles, referenced secret versions/access, Artifact Registry access, cache-bucket access, and the selected network. IAM findings are conservative: indirect group grants and conditional bindings can require human review. Bootstrap does not mutate IAM. Its only possible creation is the Artifact Registry repository; `--check` or `--no-create` prevents that too.

`Dockerfile.gcp` installs this source checkout with GCP and GCS extras. Build its application derivative with the application's checkout and dependencies, including Git objects for `git_deps`. `batch push` stamps the image with `org.opencontainers.image.revision`. GCP defaults to amd64; AWS retains arm64. `--no-build` assumes the existing image already has the correct revision label.

Scratch disks mount at `/scratch`. With `DVX_SCRATCH` set, `dvx run` prepares a worktree on that volume before applying sidecar overlays, retaining access to the image's installed `.venv`; checked-out inputs and generated outputs therefore live there too. DVX honors `DVC_CACHE_DIR=/scratch/dvc-cache` for both normal DVC operations and its own lock-free cache paths. Optional GCS fuse mounts are application side inputs; they do not establish tracked dependency freshness. Cache traffic continues through DVC and MD5 verification.

## Distributed stages and recovery

```bash
dvx run --executor gcp:default --remote-jobs 16 target.dvc
dvx run --executor-for 'heavy/*=gcp:default' --executor-for 'deploy/*=local' target.dvc
```

Per-stage `run.stages.<path>.executor` configuration is also supported. The first matching `--executor-for` rule wins over that configuration and the default `--executor`.

The driver validates the image's revision label against Git HEAD, pins the matching registry digest, and refuses dirty tracked code. The pushed/pulled image must be present locally for Docker inspection. AWS submits an exact job-definition ARN, creating an isolated digest-pinned variant when the configured definition uses a mutable tag from the same image repository. `--allow-image-mismatch` deliberately overrides image verification. Sidecar changes are overlaid onto the task image before planning, so a downstream task receives newly regenerated dependency hashes rather than the image's older metadata.

A task pushes its full output closure before publishing a result manifest. The driver validates the input key, declared sidecar set, command, and output paths before applying metadata. Remote outputs remain remote until a local consumer needs their payloads. The task's summary and commit-message protocol is forwarded to the driver; Git writes stay with the driver.

Job handles and input keys live in `.dvx/batch-jobs/`; treat that directory as local state and exclude it in local/global Git configuration. After interruption, running the same inputs attaches to the recorded job. Changed inputs submit a new job and leave the earlier one alone with a warning. Failed jobs require `--retry-failed` before resubmission. Keep the records to avoid duplicate submissions; content-addressed outputs do not make arbitrary side effects idempotent.

Ctrl-C stops watching and offers cancellation of active cloud jobs. Declining or interrupting again detaches. Jobs can be watched or cancelled separately. Local subprocess cancellation retains its existing behavior.

Array task lists use environment JSON when small and a stored coordination manifest when larger. A `results_url` is required for the latter. DVX warns about shared stale upstream computations before array submission; compute those once first to avoid duplicate work.

GCP log reads are shared across active jobs in bounded UID batches, with boundary-entry deduplication and backoff. Terminal jobs drain delayed logs before returning. AWS array logs are labelled by task index. Application failures are not automatically retried; allowlisted infrastructure/preemption failures are eligible for retry, and completed stages resume through their recorded cache state.

## Operational limits

Offline SDK-schema tests and local task clones verify the control plane without credentials. Live Spot preemption, cloud provisioning, IAM propagation, image startup, and billing labels still need a small smoke run in each target account. Driver state and the cache are not a global submission lock: separate drivers can submit duplicate work. Stages should declare their inputs, avoid hidden local-file assumptions, and be idempotent when external systems can observe retries. Cross-cloud storage works through the installed DVC backend and credentials, with the provider's ordinary egress costs.

Whole-DAG retries start from the sidecars baked into the image. Changed and forced stages can therefore rerun despite earlier attempts having uploaded blobs; cache durability alone does not preserve regenerated provenance. Driver-managed stages persist reconciled sidecars and result manifests as they finish, which gives a stronger recovery boundary. Neither mode promises exactly-once effects in external systems.
