# `dvx batch` on GCP Batch: a backend interface, and remote stages inside `dvx run`

Requested from the disk-tree session (2026-10-16). Its deployments run their pipelines on GCP Batch: one project, one region, a service account per deployment, images in Artifact Registry, secrets from Secret Manager injected as env, cost labels on every job, `gcsfuse` mounts and local-SSD staging. Those pipelines are hand-rolled shell (a JSON job spec in a heredoc, `gcloud batch jobs submit`, Cloud Logging filters pasted from memory). A dvx-native GCP executor would let such a pipeline be a DVX DAG whose heavy stages run on Batch, with DVX's content addressing as the resume and coordination substrate, just as [`batch-executor.md`] does for AWS.

Status (2026-10-16): implementation and offline verification complete on `batch-backends`, based on the adaptive-materialization review branch. Live cloud provisioning/preemption and container startup remain rollout checks; no cloud resources or jobs were created during implementation.

Read first: [`batch-executor.md`] (phase 1: single fat job; phase 2 deferred: per-stage jobs), [`batch-per-project-prefix.md`], [`batch-secrets-manager.md`], [`batch-run-command-cli-mismatch.md`], the pending [`batch-job-role-and-networking.md`] and [`parallel-materialize.md`], and [`resource-aware-scheduling.md`].

## Goals

1. **One executor interface, two backends.** `dvx batch push|bootstrap|submit|watch|cancel` take `-b/--backend aws|gcp` (default from config, else `aws`, so today's behavior and imports don't change).
2. **GCP phase 1 parity**: a single fat `dvx run` job on Batch (Spot by default), watched with logs streamed, with the same `run_command` contract as AWS (`--no-commit --push each -v`).
3. **Array fan-out**: one Batch job of N tasks for an embarrassingly parallel family of targets.
4. **Remote stages inside `dvx run`** (AWS phase 2, built once for both backends): `dvx run --executor gcp` runs selected stages as Batch jobs. Each launches as soon as its own deps are done, not at a level barrier. The `dvx run` process is the only driver; there is no server.

## 1. Backend interface

`src/dvx/batch.py` becomes a package. The AWS code moves; it is not rewritten.

| New module | Contents |
| --- | --- |
| `dvx/batch/__init__.py` | Re-exports every public name `dvx.batch` has today (`submit`, `bootstrap`, `job_definition_spec`, `run_command`, `submit_overrides`, `RECLAIM_ONLY_RETRY`, the name helpers, `LOG_GROUP`, …), because ctbk and nj-crashes import them (`dvx.batch.submit(command=…)`). It also exposes `get_backend(name) -> Backend`. |
| `dvx/batch/base.py` | `Backend` protocol, `JobRequest`, `JobHandle`, `JobState`, the shared `watch()` loop, poll backoff, and `derive_job_name`, which moves from `cli/batch_cmd.py::_derive_job_name`. |
| `dvx/batch/command.py` | `run_command` (unchanged) plus the new `task_command` (§3). Both backends use them. |
| `dvx/batch/secrets.py` | `_reject_env_secret_overlap` (shared). The AWS-only `_secrets_container_list` and `execution_role_secrets_policy` stay in `aws.py`. |
| `dvx/batch/aws.py` | Everything else in today's `batch.py`: name helpers, `compute_environment_spec`, `job_definition_spec`, `submit_overrides`, `push_commands`, `ECR_LIFECYCLE_POLICY`, `_clients`, `_ensure_repo`, `push_image`, `bootstrap`, `submit`, `_drain`, `_watch`, `_wait`. `AwsBackend` wraps them. |
| `dvx/batch/gcp.py` | New: pure GCP spec builders plus a thin orchestration layer over `google-cloud-batch` / `google-cloud-logging`. Wrapped by `GcpBackend`. |

```python
@dataclass(frozen=True)
class JobRequest:
    command: list[str]                 # args after the image's `dvx` entrypoint
    name: str                          # backend sanitizes (GCP: ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$)
    profile: str                       # config profile (§2.1); AWS: the prefix
    vcpus: float | None = None
    memory_mib: int | None = None
    env: dict[str, str] = {}
    secrets: dict[str, str] = {}       # NAME -> backend ref (ARN | Secret Manager version)
    labels: dict[str, str] = {}        # GCP labels; AWS tags (best-effort, see open questions)
    task_count: int = 1                # >1 => array job (§3)
    on_demand: bool = False            # AWS: <prefix>-od queue; GCP: STANDARD provisioning
    max_run_seconds: int | None = None

class Backend(Protocol):
    def push(self, image: str, *, context: str, dockerfile: str | None, platform: str | None, build: bool) -> None: ...
    def bootstrap(self, profile: Profile, *, check_only: bool) -> list[Finding]: ...
    def submit(self, req: JobRequest) -> JobHandle: ...
    def status(self, h: JobHandle) -> JobState: ...      # normalized: QUEUED|RUNNING|SUCCEEDED|FAILED (+ per-task exit codes, attempt)
    def logs(self, h: JobHandle, cursor: LogCursor | None) -> tuple[list[LogLine], LogCursor]: ...
    def cancel(self, h: JobHandle) -> None: ...
```

`watch()` lives in `base.py` and is generic. It polls `status` with exponential backoff (5 s doubling to a 60 s cap, reset when the state or attempt changes), drains `logs` on every poll, and returns the exit code. It inherits both lessons of today's `_watch`: a new attempt resets the log cursor and is announced, and a missing log source produces one warning instead of looking like a quiet job. AWS's `_watch` becomes `AwsBackend.status` + `AwsBackend.logs`. Today's `_drain` paging loop is the AWS `logs`. AWS keeps its 15 s fixed poll unless the shared backoff proves fine there too.

The CLI stays one Click group (`cli/batch_cmd.py`) with a `-b/--backend` group option. Each subcommand's AWS-only options (`-a/--arch` Fargate enum, `-g/--ephemeral`) raise a usage error under `-b gcp`, and the GCP-only options do the same under `aws`. Nothing is silently ignored.

## 2. GCP mapping

### 2.1 Profiles instead of a job definition

GCP Batch has no compute environment, queue or job definition: every job carries its whole spec. So the AWS job definition's role of persisting image, roles, env and secrets between `bootstrap` and `submit` moves to a named profile in `.dvx/config.yml` (`dvx.config` already loads this file). A `batch:` section sits beside `run:`:

```yaml
batch:
  backend: gcp
  profiles:
    default:
      project: my-project
      region: us-central1
      image: us-central1-docker.pkg.dev/my-project/dvx/app:<rev>
      service_account: dvx-app-job@my-project.iam.gserviceaccount.com
      machine_type: n2-highmem-16      # optional; omitted => Batch picks from cpu/memory
      vcpus: 16
      memory_mib: 131072
      boot_disk_gb: 100
      scratch: {type: local-ssd, gb: 375}   # or pd-ssd; mounted at /scratch (§2.5)
      spot: true
      max_retries: 2
      max_run: 4h
      labels: {app: myapp, component: reproc}
      env: {PYTHONFAULTHANDLER: "1"}
      secrets: {GH_TOKEN: gh-rw-token}      # bare name => projects/<project>/secrets/<name>/versions/latest
      volumes: []                           # opt-in gcsfuse mounts (§2.5)
```

Every submit builds the full spec from the profile plus CLI overrides. The AWS TOCTOU between `bootstrap` re-registering a job definition and `submit` using it ([`batch-per-project-prefix.md`]) therefore cannot exist here. The profile name plays the role of the AWS `prefix`: it is the namespace for job IDs and the `dvx-profile` label. A profile holds no secret values, only references.

### 2.2 Job spec (`gcp.job_spec(profile, req) -> dict`)

The builder is pure and emits a snake_case dict that `batch_v1.Job(spec)` (proto-plus) accepts directly:

- `task_groups[0]`: `task_count` = `req.task_count`, and `parallelism` (defaults to `task_count`; the CLI's `--parallelism` caps it). `task_spec`:
  - `runnables`: one `container` with `image_uri`, `entrypoint: "dvx"`, `commands: req.command`, and `volumes` binding the host mounts to the same container paths.
  - `compute_resource`: `cpu_milli = vcpus*1000`, `memory_mib`, `boot_disk_mib`.
  - `max_retry_count`, `lifecycle_policies` (§2.4), `max_run_duration` (`"14400s"` form).
  - `environment`: `variables` (profile env, then request env) and `secret_variables`, with env/secret name overlap rejected by the shared `_reject_env_secret_overlap`.
  - `volumes` (§2.5).
- `allocation_policy`:
  - `instances[0].policy`: `machine_type` (only if set), `provisioning_model: SPOT|STANDARD` (`--on-demand` gives STANDARD), `boot_disk`, and the scratch `disks`.
  - `service_account.email`, `location.allowed_locations: ["regions/<region>"]`, and `labels`.
- `labels` on the job and on `allocation_policy`. The allocation-policy copy is what reaches the VMs, and so the billing export; job labels alone don't. Values are sanitized to GCP's label grammar (lowercase, `[a-z0-9_-]`, ≤ 63 chars). dvx always adds `dvx-profile`, `dvx-run` (a run ID shared by every job one `dvx run` launches), and for per-stage jobs `dvx-stage` (sanitized stage path, truncated, plus a short hash suffix when truncated).
- `logs_policy.destination: CLOUD_LOGGING`.

Job ID: `<profile>-<slug>-<yyyymmdd-hhmmss>-<4 hex>`, sanitized and ≤ 63 chars. It is unique within project and region, as GCP requires. A re-submit always gets a new ID.

`dvx batch submit -b gcp -n/--dry-run` prints `batch_v1.Job.to_json(job)` and submits nothing, like cw's `DRY=1`.

### 2.3 Secrets

`secret_variables` maps `NAME` to `projects/<p>/secrets/<s>/versions/<v>`. A bare `<s>` expands to that path with `versions/latest`; a full path is accepted verbatim; anything else raises. `submit -s NAME=ref` adds per-job secrets, the same flag as on AWS. Unlike AWS, `submit` needs no IAM of its own: the job's service account must hold `roles/secretmanager.secretAccessor` on each secret, and that grant belongs to IaC (§5). `bootstrap --check` verifies it.

### 2.4 Status, retries, preemption

- **Status**: `get_job` gives `status.state` (QUEUED/SCHEDULED/RUNNING map to `QUEUED|RUNNING`; SUCCEEDED; FAILED; DELETION_IN_PROGRESS maps to FAILED("cancelled")), plus `task_groups[*].counts` for array progress. Per-task exit codes come from `list_tasks`: the latest `status_events[*].task_execution.exit_code`. `JobState.exit_code` is task 0's code for a one-task job, and for an array job the first nonzero code (by task index).
- **Retry only infrastructure loss** (GCP's analog of `RECLAIM_ONLY_RETRY`). Set `max_retry_count = profile.max_retries`, plus `lifecycle_policies`:
  - `RETRY_TASK` on the Batch-reserved exit codes for preemption and VM loss: 50001 (preempted), 50002 (VM reporting timeout), 50003 (rebooted), 50006 (VM recreated).
  - Application failures and other non-allowlisted codes fail without retry under the allowlist's documented semantics.

Implementation correction: use only the `RETRY_TASK` allowlist. [GCP retry semantics] specify that unspecified exit codes take the opposite action, and the allowlist therefore fails application errors without a second policy. Offline protobuf tests assert the exact policy. Live preemption remains a rollout check.

  The constant is `PREEMPTION_ONLY_RETRY`. Without the FAIL_TASK rule, `max_retry_count` retries every failure: the nj-crashes "burned a 122-stage level twice" failure. A deterministic stage error must cost one run.
- **Resume after preemption is DVX's**, as on AWS: `--push each` makes each completed stage durable in the remote, so a retried task re-pulls (or skips) everything it already finished.

### 2.5 Storage on the VM: scratch disk, `gcsfuse`, and why DVX doesn't use fuse for its cache

- **Scratch**: the profile's `scratch` disk (`new_disk` `local-ssd` / `pd-ssd`, `device_name: scratch`) is mounted at `/mnt/disks/scratch` and bound to `/scratch` in the container. dvx sets `DVX_SCRATCH=/scratch` and points the DVC cache there (`DVC_CACHE_DIR`, or `dvx run --cache-dir` if one is added), so materialized deps and outputs never fill the boot disk. This is the cw job's `/stage` pattern.
- **`gcsfuse`** volumes (`{gcs: {remote_path}, mount_path, mount_options}`) are **opt-in per profile** and only for app-level side inputs: a stage whose cmd reads a raw bucket path that DVX doesn't track, like cw's `/gcs/<bucket>`. DVX's own deps and outputs always go through the DVC remote (§2.6). Remote pulls are md5-verified, content-addressed and parallel ([`parallel-materialize.md`]). A fuse mount of the cache bucket would bypass verification, behave badly with many small objects, and tie correctness to mount semantics. A fuse mount is not an input DVX knows about, so it does not count toward freshness. The spec says so in the profile docs.

### 2.6 Remotes and cache

- A `gs://` DVC remote (`dvx[gs]`, via `dvc-gs`) is the cache for GCP jobs. Credentials are the VM service account's ADC from the metadata server, so no keys are injected. The SA needs `roles/storage.objectUser` on the remote's bucket or prefix (IaC).
- A task materializes deps exactly as the AWS path does: `dvx run`'s classify→pull loop ([`batch-run-command-cli-mismatch.md`] §3) pulls absent deps by recorded md5. `--push each` pushes each output's blobs before the next stage starts, independent of commit mode.
- **Cross-cloud remotes work but cost egress.** A GCP job with an `s3://` or `r2://` remote (keys from `secret_variables`) is supported. An AWS job with a `gs://` remote is symmetric. Neither path special-cases this.
- A new base image variant, `Dockerfile.gcp` (or a build arg), installs `dvx[gs,batch-gcp]` and defaults to `--platform linux/amd64` (§7 Q2). App images derive from it as they do on AWS.

### 2.7 Logs

`logs_policy: CLOUD_LOGGING` writes task stdout/stderr to `projects/<p>/logs/batch_task_logs`, with labels `job_uid` and `task_id`. `GcpBackend.logs` uses `google.cloud.logging.Client.list_entries`:

- **Filter** (built by `gcp.log_filter(project, job_uid, since=None)`, pure): `logName="projects/<p>/logs/batch_task_logs" AND labels.job_uid="<uid>"`, plus `timestamp>="<cursor ts>"`.
- **Cursor**: the last timestamp plus the set of `insert_id`s seen at that timestamp. Entries are deduplicated by `insert_id`, because the inclusive `>=` re-reads boundary entries.
- **Array jobs** prefix each line with `[<task index>]`, parsed from `labels.task_id`.
- **Ingestion lag is the GCP analog of `_drain`'s final-page lesson.** Cloud Logging lags seconds to minutes behind the container. After a terminal state, `watch` keeps draining until two consecutive polls ≥ 15 s apart return nothing new, with a 3 min cap. Only then does it return, so the run summary isn't dropped.
- `batch_agent_logs` (provisioning/preemption events) are not streamed by default. `-A/--agent-logs` adds them. A preemption is announced from `status_events` either way.
- **Read-rate limits**: Logging reads are limited per minute per project, so the shared backoff matters. Phase 4 (§4) watches many jobs with one `list_entries` call per poll. Its filter is `labels.job_uid=("a" OR "b" …)` in chunks, routed to stages by `job_uid`. There is not one call per job.

### 2.8 Cancel

`cancel_job` where the client exposes it, else `delete_job`, which terminates running tasks. `dvx batch cancel <job>` does this. So does Ctrl-C in `watch` or `dvx run --executor`, after one confirmation (a second Ctrl-C detaches and leaves the jobs running; §4.4).

### 2.9 Push

`GcpBackend.push`: if the Artifact Registry repo is missing, create it (`google-cloud-artifact-registry`) with a cleanup policy mirroring `ECR_LIFECYCLE_POLICY` (keep the 4 most recent tagged versions; delete untagged after 7 days). Then `docker login -u oauth2accesstoken --password-stdin` with an ADC access token, then the existing `push_commands`. This is the one imperative resource creation on GCP, kept for parity with `_ensure_repo` (§5). `push` also stamps the image with `--label org.opencontainers.image.revision=$(git rev-parse HEAD)`, for §4.3. The `--label` goes into `push_commands`, so both backends get it.

## 3. Array fan-out (`submit --each`)

`dvx batch submit -b gcp --each <targets…>` (AWS: an array job) submits **one** job with `task_count = len(targets)`. Each task runs one target. For a family of independent stages (nj-crashes' per-year conversions, a scan per bucket) this beats one fat job when the family is wider than one VM, and beats N jobs on submit overhead and quota.

- The task list travels as env: `DVX_TASK_TARGETS` (a JSON list of target-lists). Its size is checked against Batch's env limit; when too large, it is written to `<remote>/dvx-jobs/<run-id>/tasks.json` and `DVX_TASK_TARGETS_URL` is set instead.
- The container command is `task_command(...)`: `dvx batch task-run --no-commit --push each -v`. `dvx batch task-run` reads `BATCH_TASK_INDEX` (GCP) or `AWS_BATCH_JOB_ARRAY_INDEX` (AWS) and execs `dvx run <flags> <targets[i]>`. It is backend-agnostic and pure in its parsing (`select_task_targets(env) -> list[str]`).
- Exit status: every task must succeed. `watch` reports per-task failures (`[7] exit 1: data/2019/…`), and the job's exit code is the first failure's.
- Overlapping upstreams across tasks are safe but wasteful: two tasks needing the same unmaterialized upstream both pull it, and if it's stale, both run it. Precondition: `--each` targets' stale closures should be disjoint. dvx warns when two targets share a stale non-leaf upstream (computed locally with the existing `_should_run` dry-run) and suggests running that upstream first.

## 4. Remote stages in `dvx run` (`--executor`)

This is AWS phase 2 from [`batch-executor.md`], built once for both backends. The `dvx run` process stays the scheduler. Selected stages run as single-task Batch jobs instead of local subprocesses, and the remote remains the only shared state.

### 4.1 Selecting stages

- `dvx run --executor gcp[:profile]` sends every stage with a `cmd` remote.
- `--executor-for 'PATTERN=gcp:heavy'` (repeatable, first match wins, `local` allowed) routes selectively.
- Config gets the same per-stage key, `run.stages.<path>.executor: gcp:heavy`, which extends `StageConfig`. It goes in config, not in `.dvc` `meta`, because placement is a property of where you run, not of the artifact: the same `.dvc` runs locally on a laptop and remotely in CI.
- Sizing: a remote stage's `meta.computation.resources` (`mem_gb`, `cpus`; [`resource-aware-scheduling.md`]) overrides the profile's `memory_mib` / `vcpus`. With no `machine_type`, Batch then picks a right-sized VM per stage, which is the per-stage right-sizing that resource-aware scheduling could only approximate inside one box.

### 4.2 Ready-set scheduling replaces level barriers

`ParallelExecutor.execute` today runs `_group_into_levels` and then a barrier per level: one slow stage in level k holds every level-k+1 stage, even those that don't depend on it. With per-stage Batch jobs (minutes of VM startup, Spot variance) that barrier is the dominant wall-time cost. The level loop becomes a ready-set loop. **Local runs switch too**, since the change is strictly better for them (see Q4 for the escape hatch):

- Keep a dependency count per artifact. Submit every ready artifact to the pool. On each completion, decrement dependents and submit the newly ready ones. On the first failure, stop submitting new work, let in-flight work finish (today's "break after a failed level" behavior), and report.
- Two pools: the local pool (`-j`, plus the `_BudgetGate`) and a remote pool with `--remote-jobs N` (default 32) concurrent Batch jobs. It is bounded for quota and Logging read limits; a remote stage's thread only waits on `status`.
- The level-shaped bookkeeping has to be rewritten, not deleted. `_scheduled_paths` exists so that `_wait_for_co_outputs` only waits on same-level co-outputs. Under a ready-set, co-outputs of one `cmd` are scheduled as a unit: they share deps by construction, so they become ready together, and the primary's wait set is its cmd group. `_execute_level`'s single-artifact fast path goes away. The co-output and deadlock tests in [`co-output-wait-deadlock-on-skip.md`] and [`co-output-concurrency-locked.md`] must pass unchanged. They are the regression harness for this change.
- `--dry-run` still prints levels: they remain a correct description of the DAG's shape, just not of the schedule.

### 4.3 What a remote stage does

1. **Driver** (`dvx run`):
   - Runs `_should_run` locally as today. Fresh or materializable stages never launch a job.
   - For a stage that must run, it builds `run_command((stage,), commit="never", push="each", force=…)` with `--no-prune-fresh` off and `--no-pull-deps` off, so the task pulls its deps by recorded md5. It sets `DVX_RESULT_URL=<remote>/dvx-jobs/<run-id>/<stage-hash>/result.json` and submits.
2. **Task**: runs `dvx run <stage>`, pushes the output blobs, then writes a **result manifest** to `DVX_RESULT_URL`: the stage's regenerated `.dvc` (and its co-outputs'), plus duration, exit code and the `DVX_SUMMARY_FILE` / `DVX_COMMIT_MSG_FILE` contents.

   The `.dvc` md5 updates otherwise die with the container. AWS phase 1 sidesteps this by deferring reconciliation to a later local `dvx run`. A driver that continues the DAG needs the new md5s *now*, to classify downstream freshness. This hook (`--result-url`, or the env var) lives in `dvx run` itself and works on any remote scheme.
3. **Driver**, when the job succeeds:
   - Fetches the manifest and writes the `.dvc` files into the worktree.
   - Runs the existing `_handle_stage_output` (commit per the run's strategy; the cache push is already done remotely, so `--push each` skips it here via a "pushed remotely" flag).
   - Signals dvc-done.

   Downstream local stages then pull the output on demand: the existing execution-time dep materialization. Downstream remote stages never need it locally at all.
4. **Code version.** The image bakes a worktree at some commit, and the driver's `.dvc` view must match it.
   - The driver reads the image's `org.opencontainers.image.revision` label (§2.9). It refuses to launch remote stages when that differs from `HEAD`, or when tracked files outside `*.dvc` are dirty. `--allow-image-mismatch` overrides.
   - `.dvc` files are passed over the image's copy as a small overlay: the stage's own `.dvc`, plus its dep `.dvc`s with the md5s the driver just reconciled. The overlay is uploaded beside the result URL, and the task applies it before `dvx run`. Without it, a stage downstream of a just-completed remote stage would see stale dep md5s baked into the image.

### 4.4 No server; re-attach after the driver dies

The driver can be a laptop, a CI runner, or itself a small Batch job (a phase 1 fat job with `--executor`, though nesting is not a goal). If it dies, its in-flight jobs keep running. The re-attach mechanism:

- Each launch writes `.dvx/batch-jobs/<stage-hash>.json` with `{backend, job name, job uid, input key, result url, submitted}`. The input key is the sha256 of the stage's cmd plus its dep md5s, exactly what freshness is computed from.
- A later `dvx run` that reaches the same stage with the **same input key** attaches instead of resubmitting: a RUNNING job is watched, a SUCCEEDED job's manifest is fetched, and a FAILED job is reported, then resubmitted only with `--retry-failed`.
- A different input key means the record is stale. The old job is left alone (dvx warns if it's still running) and a new one is submitted.
- Records are local state, ignored by the global excludes like `tmp/`. Their loss only costs a duplicate submit, which is idempotent because outputs are content-addressed.

### 4.5 What stays out

- No `dependsOn` job graphs pushed to the cloud. GCP Batch has no job-dependency field anyway, and AWS's 20-parent cap would force synthetic barriers. The driver owns the DAG.
- Fan-in inside one job: a stage is one task. Families use `--each` (§3) from the CLI. In `dvx run`, the driver may coalesce ready same-profile stages into one array job when many become ready together (§7 Q5); this is deferred.

## 5. Bootstrap: imperative vs IaC

Same line as [`batch-job-role-and-networking.md`]: projects that manage infra in Pulumi keep doing so, and dvx must neither require nor fight it.

| Resource | Owner | `dvx batch bootstrap -b gcp` |
| --- | --- | --- |
| APIs (batch, compute, logging, artifactregistry, secretmanager) | IaC / once by hand | check |
| Job service account | IaC | check exists |
| SA roles: `batch.agentReporter`, `logging.logWriter` (project); `artifactregistry.reader` (repo); `storage.objectUser` (remote bucket); `secretmanager.secretAccessor` (each profile secret) | IaC | check by reading IAM policies; print the missing bindings as `gcloud` one-liners and as a Pulumi snippet |
| Caller's `iam.serviceAccountUser` on the SA | IaC | check via `testIamPermissions` on the SA |
| Artifact Registry repo + cleanup policy | dvx (parity with ECR) or IaC | create if missing (skipped with `--no-create`) |
| Secrets (values) | human / IaC | check each referenced secret has an enabled version |
| VPC / subnet | default network, or profile `network`/`subnetwork` | check |

So `bootstrap -b gcp` is essentially `--check` plus the AR repo, and it returns findings (`Finding(resource, ok, fix)`) instead of mutating IAM. There's no CE or queue to create, and the job spec is rebuilt on every submit. Whether `--apply` should ever grant IAM imperatively is Q3.

Dependencies: a new extra, `batch-gcp = ["google-cloud-batch", "google-cloud-logging", "google-cloud-artifact-registry", "google-cloud-secret-manager", "google-auth"]`, imported lazily as boto3 is. The existing `batch` extra stays boto3, for compatibility with `pip install dvx[batch]`. No `gcloud` subprocesses anywhere.

## 6. Tests

These follow `tests/test_batch.py`'s style: pure builders, exact-equality assertions on whole dicts and lists, and a fake client for orchestration. No live GCP. No substring `in` assertions.

- **Refactor safety**: every existing `tests/test_batch.py` test passes with only import-path changes, and `from dvx.batch import submit, bootstrap, job_definition_spec, run_command, RECLAIM_ONLY_RETRY` still works (one test pins the re-export list).
- `test_gcp_job_spec_defaults` asserts `gcp.job_spec(profile, req) == {...}`, the full dict, for a minimal profile. `test_gcp_job_spec_full_profile` does the same with machine type, scratch disk, gcsfuse volume, secrets, labels and STANDARD provisioning.
- `test_gcp_job_spec_constructs_proto`: `batch_v1.Job(gcp.job_spec(...))` round-trips through `Job.to_dict` to the same values. This is the GCP twin of `test_run_command_parses_with_real_run_cli`: it catches misspelled field names offline.
- `test_preemption_only_retry` asserts the exact `lifecycle_policies` list and `max_retry_count`.
- `test_secret_ref_expansion`: bare name, full path, and invalid (raises). `test_gcp_env_secret_overlap_raises`.
- `test_label_sanitization`: a table of input → output pairs, including truncation plus hash suffix. `test_job_id_shape` uses an injected clock and RNG and asserts the exact ID.
- `test_log_filter`: exact strings for the single-job, cursor and multi-job (chunked OR) forms.
- `test_log_cursor_dedups_boundary_entries` uses a fake `list_entries` returning overlapping pages and asserts the exact printed lines.
- `test_watch_drains_after_terminal_until_quiet` uses a fake backend whose last log lines arrive two polls after SUCCEEDED, and asserts they're printed. `test_watch_reports_preemption_and_new_attempt`.
- `test_status_exit_code_from_tasks` covers an array job with mixed task exit codes, giving the exact `JobState`.
- `test_select_task_targets`: `BATCH_TASK_INDEX` / `AWS_BATCH_JOB_ARRAY_INDEX`, inline JSON and URL forms, and an out-of-range index (raises).
- **Scheduler**: `test_ready_set_does_not_wait_on_unrelated_slow_stage` uses a DAG A→C, B, with B slow. It asserts C starts before B finishes, from an exact event log (`[("start","A"),("start","B"),("done","A"),("start","C"),…]`, made deterministic with events, not sleeps). Plus the whole existing co-output suite unchanged.
- **Remote stage round trip**: a `FakeBackend` whose "job" runs `dvx run` in a temp clone against a `file://` remote and writes the result manifest. After `dvx run --executor fake`, assert the worktree's `.dvc` equals the one a local run produces, byte for byte, and a downstream local stage runs against the pulled output.
- `test_reattach_same_input_key` and `test_reattach_stale_input_key_resubmits` cover the two record cases with a fake backend.
- `test_image_revision_mismatch_refuses`.
- **CLI**: option sets per backend (exact `set` equality via the existing `_parse_options`), backend-specific flags rejected under the other backend, and `submit -b gcp -n` printing the exact JSON.

## 7. Phases

1. **Refactor**: the `dvx/batch/` package, `Backend` protocol, `AwsBackend`, and shared `watch`. No behavior change.
2. **GCP single job**: profiles, `job_spec`, secrets, retries, logs, `push`, `bootstrap --check`, `submit`/`watch`/`cancel`, and the GCP base image. This is the first useful milestone: a disk-tree-style pipeline whose whole DAG runs in one Batch job.
3. **Array fan-out**: `--each`, `task-run`.
4. **`dvx run --executor`**: ready-set scheduler (lands first, local only, behind the existing test suite), then remote stages, the result manifest, the overlay and re-attach.

## Non-goals

- A long-running coordinator, queue service or database. The `dvx run` process plus the remote is the whole control plane.
- GPUs, TPUs, multi-node tasks, MPI.
- Cloud Run Jobs, GKE, Vertex, or other GCP compute. Batch only.
- Cross-cloud scheduling within one run (some stages on AWS, some on GCP). It isn't forbidden by the design, but neither tested nor promised.
- IaC generation. dvx prints the bindings it found missing; it doesn't own a Pulumi program.
- Cron and scheduling. Jobs are submitted by a human, CI, or an external scheduler, as with [`batch-executor.md`].

## Open questions

1. **Retry semantics**: does `FAIL_TASK` on 1–255 actually take precedence over `max_retry_count` for application failures, and does 50001 reliably mark Spot preemption on current Batch? This needs one empirical run with a deliberately failing task and one with a forced preemption before `PREEMPTION_ONLY_RETRY` is trusted.
2. **Default architecture on GCP**: amd64 (broadest machine-family availability, and the motivating jobs are x86 highmem), or Arm (C4A/T2A, cheaper, and matches the AWS ARM64 default and Apple-Silicon smoke-testing)? This proposes amd64 for GCP, configurable per profile.
3. **Imperative IAM**: should `bootstrap -b gcp --apply` grant the missing bindings, as AWS `bootstrap` puts its secrets policy, or stay check-only? This proposes check-only: GCP IAM is project-wide blast radius, and the motivating deployments already manage the SA in Pulumi.
4. **Ready-set for local runs**: switch unconditionally, or keep `--levels` as an escape hatch for a release? Log output changes: the `Level k/n` headers go away, and something may parse them.
5. **Coalescing ready remote stages into array jobs** (fewer VM startups) versus one job per stage (simpler re-attach and logs). Defer until per-stage startup shows up in measurements.
6. **Result-manifest location**: `<remote>/dvx-jobs/…` puts small mutable objects in the content-addressed remote's bucket. A separate `batch.results_url` per profile is cleaner, but is one more thing to configure and grant. Which one?
7. **Overlay scope** (§4.3 step 4): is a `.dvc`-only overlay enough, or do stages whose `git_deps` changed since the image build need the image rebuilt (the proposal: yes, refuse)?
8. **AWS tags**: `JobRequest.labels` maps naturally to GCP labels. AWS Batch `tags` on `submit_job` don't propagate to Fargate billing without `propagateTags`. Wire that up in phase 1, or leave AWS labels as a no-op with a warning?

[`batch-executor.md`]: batch-executor.md
[`batch-per-project-prefix.md`]: batch-per-project-prefix.md
[`batch-secrets-manager.md`]: batch-secrets-manager.md
[`batch-run-command-cli-mismatch.md`]: batch-run-command-cli-mismatch.md
[`batch-job-role-and-networking.md`]: batch-job-role-and-networking.md
[`parallel-materialize.md`]: parallel-materialize.md
[`resource-aware-scheduling.md`]: resource-aware-scheduling.md
[`co-output-wait-deadlock-on-skip.md`]: co-output-wait-deadlock-on-skip.md
[`co-output-concurrency-locked.md`]: co-output-concurrency-locked.md

## Implementation report

- Preserved the legacy public AWS API in the new `dvx.batch` package. Added backend-neutral requests, handles, states, log cursors, named profiles, and a generic watcher. GCP SDKs remain optional and lazy; CI installs both SDK extras for offline contract tests across the Python matrix.
- GCP supports whole-DAG submission, arrays, job watching/cancellation, secrets, Spot/default or standard provisioning, CPU/memory sizing, boot/scratch disks, optional GCS mounts, billing labels on jobs and VMs, Artifact Registry push, and conservative IAM/network/bootstrap checks. IAM is never modified by GCP bootstrap; registry creation is disabled by `--check`/`--no-create`.
- Arrays work on both providers. Small target lists are inline; large ones use a stored coordination manifest. Task-index parsing is shared. Metadata-only planning warns about shared stale upstream work. AWS array exit/log reporting is task-indexed. Unsupported backend-specific options raise rather than being silently ignored. Existing `-n/--job-name` remains; dry-run uses `--dry-run`.
- Replaced the local level barrier with dependency-ready command-group admission, preserving command deduplication, co-output completion, directory-member producer ordering, local memory budgets, failure summaries, and stopping admission after a failure. `--remote-jobs` bounds a separate remote pool. Dry-run still prints the level-shaped plan and placement without fetching or launching.
- `--executor`, first-match `--executor-for`, and per-stage config route computations. Tasks receive a sidecar overlay, explicitly compute locally to prevent recursive submission from baked placement config, verify/push output closure, and publish a result keyed to their inputs. The driver validates sidecar membership, commands and output paths before applying metadata and forwarding summaries/commit messages. Local consumers restore remote outputs on demand; remote consumers only need updated sidecars.
- Per-stage records support reattachment for the same input key, resubmission for changed inputs, and explicit `--retry-failed` for failed jobs. Watch cancellation preserves handles. Separate drivers can still submit duplicate work; no global lock or exactly-once guarantee was added.
- Image verification now pins a registry digest after checking the local image's revision against clean tracked code. AWS creates a private digest-pinned definition variant when required and submits its exact ARN, avoiding mutable-tag/latest-definition races. `--allow-image-mismatch` is an explicit override.
- Corrected AWS secret handling uncovered by real botocore schema validation and the [AWS container overrides] reference: secret references belong in a pinned definition variant, not `containerOverrides`. Also implemented the adjacent AWS task-role/networking and parallel materialization specs.
- Corrected scratch semantics: moving only the cache leaves payloads on the image disk. `DVX_SCRATCH` now prepares a scratch worktree, retains access to the image environment, applies overlays there, and locates both DVC and DVX cache operations on scratch. An actual local CLI test checks workspace/output/cache placement and image-worktree preservation.
- GCP log queries are shared across active jobs in bounded UID batches with inclusive-cursor deduplication and backoff. Terminal jobs drain logs for two quiet polls, bounded to three minutes. AWS retains its legacy watcher and also implements the common backend interface.
- Decisions on the original open questions: GCP defaults to amd64; IAM remains check-only; ready-set scheduling is the default locally too; ready-stage array coalescing remains deferred; profiles can set a separate `results_url` or use the DVC remote; overlays contain only sidecars and code mismatches require a rebuilt image or explicit override; AWS labels propagate as tags.
- Verification: full suite passed 505 tests with 4 opt-in S3 tests skipped on Python 3.13 (`ulimit -n 4096` for the existing SQLite stress fixture). A subsequent focused 87-test API/control-plane run covers the final task-local placement guard. The scratch addition is covered by the remote/backend/CLI suite. Tests construct real GCP protobufs, validate AWS requests with botocore, exercise delayed logs and shared reads, and execute tasks in fresh local clones against a file remote. The package wheel builds. New backend code/tests pass Ruff and `git diff --check`.
- Live limits: Docker's daemon was unavailable, so the image was not built or started. No provider provisioning, IAM mutation, real job submission, Spot preemption, or billing-label smoke test was attempted. Whole-DAG retries inherit image metadata and may rerun changed/forced stages; distributed stages have the stronger result-reconciliation boundary. These runtime checks should happen in each owning project's environment before production rollout.

[GCP retry semantics]: https://docs.cloud.google.com/batch/docs/automate-task-retries
[AWS container overrides]: https://docs.aws.amazon.com/batch/latest/APIReference/API_ContainerOverrides.html
