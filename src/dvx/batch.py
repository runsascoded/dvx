"""AWS Batch packaging: one-time idempotent ``bootstrap`` + per-run
``submit`` (with optional log tailing). See ``specs/done/batch-executor.md``.

Direct port of the pyrmts_engine ``batch.py`` module (which ctbk validated
end-to-end in July 2026); design points inherited verbatim:

- **Fargate Spot** compute environment — no AMI / instance-role / capacity
  management. Spot reclaim resilience comes from DVX's own resume-from-remote
  semantics: with ``--push each``, every completed stage is durable in the S3
  remote before the next starts, so a re-submitted job's ``dvx run`` skips
  everything already pushed.
- Plain boto3 (lazy import; install ``dvx[batch]``), no CDK / TF. Region and
  creds from the standard env / config chain.
- IAM surface: Fargate *execution* role only (image pull + logs). S3 creds
  ride as plain env on the job definition / submit overrides — no AWS data
  permissions involved. (An S3 job role can be added later.)
- ``bootstrap`` is create-if-missing for role / logs / ECR / CE / queue;
  the job definition is re-registered each run (revisions are harmless
  and comparing specs is fussier than it's worth).

Pure spec-builder functions below are the tested surface; the boto3 calls
are thin wrappers around them.
"""
from __future__ import annotations

import sys
import time
from functools import partial

err = partial(print, file=sys.stderr)

PREFIX = "dvx"
LOG_GROUP = f"/{PREFIX}/batch"
EXECUTION_ROLE = f"{PREFIX}-batch-execution"
ECS_TRUST_POLICY = (
    '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", '
    '"Principal": {"Service": "ecs-tasks.amazonaws.com"}, '
    '"Action": "sts:AssumeRole"}]}'
)
ECS_EXECUTION_POLICY_ARN = (
    "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
)


# ── pure builders ───────────────────────────────────────────────────────────

def compute_environment_spec(
    *,
    name: str | None = None,
    spot: bool = True,
    max_vcpus: int = 16,
    subnets: list[str],
    security_group_ids: list[str],
) -> dict:
    return {
        "computeEnvironmentName": name or (f"{PREFIX}-spot" if spot else f"{PREFIX}-od"),
        "type": "MANAGED",
        "state": "ENABLED",
        "computeResources": {
            "type": "FARGATE_SPOT" if spot else "FARGATE",
            "maxvCpus": max_vcpus,
            "subnets": subnets,
            "securityGroupIds": security_group_ids,
        },
    }


def job_definition_spec(
    *,
    name: str = PREFIX,
    image: str,
    arch: str = "ARM64",
    vcpus: int = 16,
    memory_mib: int = 65536,
    ephemeral_gib: int = 100,
    execution_role_arn: str,
    log_group: str = LOG_GROUP,
    environment: dict[str, str] | None = None,
) -> dict:
    """Fargate task job definition.

    Defaults per ``specs/done/batch-executor.md``: 16 vCPU / 64 GiB / 100 GiB
    ephemeral, ARM64 (Graviton, ~20% cheaper than X86_64 and runtime-verifiable
    on Apple Silicon locally). ``PYTHONFAULTHANDLER=1`` is set into the
    environment by default so a mute SIGSEGV (exit 139) surfaces a traceback.
    """
    env = {"PYTHONFAULTHANDLER": "1"}
    if environment:
        env.update(environment)
    return {
        "jobDefinitionName": name,
        "type": "container",
        "platformCapabilities": ["FARGATE"],
        "containerProperties": {
            "image": image,
            "runtimePlatform": {
                "operatingSystemFamily": "LINUX",
                "cpuArchitecture": arch,
            },
            "resourceRequirements": [
                {"type": "VCPU", "value": str(vcpus)},
                {"type": "MEMORY", "value": str(memory_mib)},
            ],
            "ephemeralStorage": {"sizeInGiB": ephemeral_gib},
            "executionRoleArn": execution_role_arn,
            "networkConfiguration": {"assignPublicIp": "ENABLED"},
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {"awslogs-group": log_group},
            },
            "environment": [
                {"name": k, "value": v} for k, v in sorted(env.items())
            ],
        },
        "retryStrategy": {"attempts": 2},  # spot reclaim gets one retry
    }


def push_commands(
    image: str,
    *,
    dockerfile: str | None = None,
    context: str = ".",
    platform: str | None = "linux/arm64",
    build: bool = True,
) -> list[list[str]]:
    """Docker argvs for ``push`` (login is separate — it needs the ECR token).

    Default platform ``linux/arm64`` matches the default ARM64 job definition;
    override with ``--platform linux/amd64`` when pairing with ``bootstrap
    --arch X86_64``.
    """
    cmds = []
    if build:
        # No provenance/SBOM attestations: they turn the push into an OCI
        # image index, which Lambda / some ECR consumers reject.
        cmd = ["docker", "build", "-t", image, "--provenance=false", "--sbom=false"]
        if platform is not None:
            cmd += ["--platform", platform]
        if dockerfile is not None:
            cmd += ["-f", dockerfile]
        cmds.append(cmd + [context])
    cmds.append(["docker", "push", image])
    return cmds


def run_command(
    targets: tuple[str, ...] = (),
    *,
    force: bool = False,
    jobs: int | None = None,
    commit: str = "never",
    push: str = "each",
    remote: str | None = None,
    verbose: bool = True,
) -> list[str]:
    """Container command for the base image (whose entrypoint is ``dvx``).

    The default is ``dvx run --no-commit --push each -v`` — i.e. no git
    writes (the container has no push access to the git repo), every completed
    stage's cache blobs flushed to the remote before the next starts (that's
    what makes a Spot reclaim survivable), and verbose progress on stderr
    for CloudWatch.

    ``commit`` maps to the CLI's tri-state flag: ``"never"`` → ``--no-commit``,
    ``"always"`` → ``--commit``, ``"auto"`` → flag omitted.

    ``remote`` names the DVC remote the run reads deps from and pushes
    outputs to — a reproc audit points it at a scratch remote so it can't
    write into the one prod serves from.
    """
    cmd = ["run"]
    if commit == "never":
        cmd.append("--no-commit")
    elif commit == "always":
        cmd.append("--commit")
    elif commit != "auto":
        raise ValueError(f"invalid commit mode: {commit!r} (expected never|auto|always)")
    cmd += ["--push", push]
    if remote is not None:
        cmd += ["--remote", remote]
    if force:
        cmd.append("--force")
    if jobs is not None:
        cmd += ["-j", str(jobs)]
    if verbose:
        cmd.append("-v")
    cmd.extend(targets)
    return cmd


def submit_overrides(
    command: list[str],
    *,
    vcpus: int | None = None,
    memory_mib: int | None = None,
    environment: dict[str, str] | None = None,
) -> dict:
    overrides: dict = {"command": command}
    rr = []
    if vcpus is not None:
        rr.append({"type": "VCPU", "value": str(vcpus)})
    if memory_mib is not None:
        rr.append({"type": "MEMORY", "value": str(memory_mib)})
    if rr:
        overrides["resourceRequirements"] = rr
    if environment:
        overrides["environment"] = [
            {"name": k, "value": v} for k, v in sorted(environment.items())
        ]
    return overrides


ECR_LIFECYCLE_POLICY = {
    "rules": [
        {
            "rulePriority": 1,
            "description": "expire untagged (superseded buildx manifests) after 7 days",
            "selection": {
                "tagStatus": "untagged",
                "countType": "sinceImagePushed",
                "countUnit": "days",
                "countNumber": 7,
            },
            "action": {"type": "expire"},
        },
        {
            "rulePriority": 2,
            "description": "keep the 4 most recent tags",
            "selection": {
                "tagStatus": "tagged",
                "tagPatternList": ["*"],
                "countType": "imageCountMoreThan",
                "countNumber": 4,
            },
            "action": {"type": "expire"},
        },
    ],
}


# ── boto3 orchestration ─────────────────────────────────────────────────────

def _clients():
    import boto3
    return {name: boto3.client(name) for name in ("iam", "logs", "ecr", "ec2", "batch")}


def _ensure_repo(ecr, image: str) -> None:
    """Create ECR repo if missing, install a keep-4-tags + expire-untagged-7d
    lifecycle policy on creation (self-maintaining pruning from day one; leaves
    existing repos' policies alone in case they've been customized)."""
    repo_name = image.split("/")[-1].split(":")[0]
    try:
        ecr.describe_repositories(repositoryNames=[repo_name])
        err(f"ecr repo {repo_name}: exists")
    except ecr.exceptions.RepositoryNotFoundException:
        ecr.create_repository(repositoryName=repo_name)
        import json
        ecr.put_lifecycle_policy(
            repositoryName=repo_name,
            lifecyclePolicyText=json.dumps(ECR_LIFECYCLE_POLICY),
        )
        err(f"ecr repo {repo_name}: created (lifecycle: keep 4 tags, expire untagged >7d)")


def push_image(
    image: str,
    *,
    dockerfile: str | None = None,
    context: str = ".",
    platform: str | None = "linux/arm64",
    build: bool = True,
) -> None:
    """ECR-login docker, then build (unless disabled) + push ``image``.
    Creates the ECR repo if missing, so push can run before ``bootstrap``."""
    import base64
    from subprocess import run
    import boto3
    ecr = boto3.client("ecr")
    _ensure_repo(ecr, image)
    auth = ecr.get_authorization_token()["authorizationData"][0]
    password = base64.b64decode(auth["authorizationToken"]).decode().split(":", 1)[1]
    registry = auth["proxyEndpoint"]
    run(
        ["docker", "login", "--username", "AWS", "--password-stdin", registry],
        input=password.encode(), check=True,
    )
    for cmd in push_commands(
        image, dockerfile=dockerfile, context=context, platform=platform, build=build,
    ):
        err(f"+ {' '.join(cmd)}")
        run(cmd, check=True)


def bootstrap(
    *,
    image: str,
    arch: str = "ARM64",
    max_vcpus: int = 16,
    vcpus: int = 16,
    memory_mib: int = 65536,
    ephemeral_gib: int = 100,
    on_demand: bool = False,
    environment: dict[str, str] | None = None,
) -> None:
    """Create-if-missing IAM role, log group, ECR repo, Fargate-Spot compute
    environment + queue, and (re-)register the job definition. Idempotent."""
    c = _clients()

    # Execution role (image pull + logs).
    try:
        role = c["iam"].get_role(RoleName=EXECUTION_ROLE)["Role"]
        err(f"role {EXECUTION_ROLE}: exists")
    except c["iam"].exceptions.NoSuchEntityException:
        role = c["iam"].create_role(
            RoleName=EXECUTION_ROLE,
            AssumeRolePolicyDocument=ECS_TRUST_POLICY,
        )["Role"]
        c["iam"].attach_role_policy(
            RoleName=EXECUTION_ROLE, PolicyArn=ECS_EXECUTION_POLICY_ARN,
        )
        err(f"role {EXECUTION_ROLE}: created")

    # Log group.
    groups = c["logs"].describe_log_groups(logGroupNamePrefix=LOG_GROUP)["logGroups"]
    if not any(g["logGroupName"] == LOG_GROUP for g in groups):
        c["logs"].create_log_group(logGroupName=LOG_GROUP)
        err(f"log group {LOG_GROUP}: created")
    else:
        err(f"log group {LOG_GROUP}: exists")

    # ECR repo.
    _ensure_repo(c["ecr"], image)

    # Default-VPC networking.
    subnets = [
        s["SubnetId"]
        for s in c["ec2"].describe_subnets(
            Filters=[{"Name": "default-for-az", "Values": ["true"]}],
        )["Subnets"]
    ]
    if not subnets:
        raise SystemExit(
            "bootstrap: no default-VPC subnets found; pass a configured VPC (unsupported yet)"
        )
    sgs = [
        g["GroupId"]
        for g in c["ec2"].describe_security_groups(
            Filters=[{"Name": "group-name", "Values": ["default"]}],
        )["SecurityGroups"]
    ][:1]

    # Compute environments + queues: the spot pair always; an on-demand pair
    # (queue `<prefix>-od`) when requested — ~3.3× compute cost but immune
    # to Spot reclaims, for "final" runs.
    pairs = [(True, PREFIX)] + ([(False, f"{PREFIX}-od")] if on_demand else [])
    for spot, queue_name in pairs:
        spec = compute_environment_spec(
            spot=spot,
            max_vcpus=max_vcpus,
            subnets=subnets,
            security_group_ids=sgs,
        )
        ce_name = spec["computeEnvironmentName"]
        existing = c["batch"].describe_compute_environments(
            computeEnvironments=[ce_name],
        )["computeEnvironments"]
        if not existing:
            c["batch"].create_compute_environment(**spec)
            err(f"compute environment {ce_name}: created")
        else:
            err(f"compute environment {ce_name}: exists ({existing[0]['status']})")
        _wait(
            lambda: c["batch"].describe_compute_environments(
                computeEnvironments=[ce_name],
            )["computeEnvironments"][0]["status"] == "VALID",
            "compute environment VALID",
        )

        queues = c["batch"].describe_job_queues(jobQueues=[queue_name])["jobQueues"]
        if not queues:
            c["batch"].create_job_queue(
                jobQueueName=queue_name,
                state="ENABLED",
                priority=1,
                computeEnvironmentOrder=[
                    {"order": 1, "computeEnvironment": ce_name},
                ],
            )
            err(f"job queue {queue_name}: created")
        else:
            err(f"job queue {queue_name}: exists")

    # Job definition — always (re-)registered; revisions are harmless.
    c["batch"].register_job_definition(**job_definition_spec(
        image=image,
        arch=arch,
        vcpus=vcpus,
        memory_mib=memory_mib,
        ephemeral_gib=ephemeral_gib,
        execution_role_arn=role["Arn"],
        environment=environment,
    ))
    err(f"job definition {PREFIX}: registered")


def submit(
    *,
    command: list[str],
    job_name: str,
    queue: str = PREFIX,
    vcpus: int | None = None,
    memory_mib: int | None = None,
    environment: dict[str, str] | None = None,
    watch: bool = False,
) -> int:
    """Submit a job; with ``watch``, tail its log stream and return the
    container's exit code (also non-zero on FAILED without one)."""
    c = _clients()
    job = c["batch"].submit_job(
        jobName=job_name,
        jobQueue=queue,
        jobDefinition=PREFIX,
        containerOverrides=submit_overrides(
            command, vcpus=vcpus, memory_mib=memory_mib, environment=environment,
        ),
    )
    job_id = job["jobId"]
    err(f"submitted {job_id} ({job_name})")
    if not watch:
        print(job_id)
        return 0
    return _watch(c, job_id)


def _watch(c: dict, job_id: str) -> int:
    last_status = None
    next_token = None
    stream = None
    while True:
        desc = c["batch"].describe_jobs(jobs=[job_id])["jobs"][0]
        status = desc["status"]
        if status != last_status:
            err(f"  status: {status}")
            last_status = status
        stream = desc.get("container", {}).get("logStreamName") or stream
        if stream is not None:
            kwargs = {
                "logGroupName": LOG_GROUP,
                "logStreamName": stream,
                "startFromHead": True,
            }
            if next_token is not None:
                kwargs["nextToken"] = next_token
            try:
                resp = c["logs"].get_log_events(**kwargs)
            except c["logs"].exceptions.ResourceNotFoundException:
                resp = None
            if resp is not None:
                for e in resp["events"]:
                    print(e["message"])
                next_token = resp["nextForwardToken"]
        if status in ("SUCCEEDED", "FAILED"):
            exit_code = desc.get("container", {}).get("exitCode")
            reason = desc.get("statusReason")
            if reason:
                err(f"  reason: {reason}")
            return exit_code if exit_code is not None else (0 if status == "SUCCEEDED" else 1)
        time.sleep(15)


def _wait(pred, what: str, timeout: float = 300, interval: float = 5) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return
        time.sleep(interval)
    raise TimeoutError(f"bootstrap: timed out waiting for {what}")
