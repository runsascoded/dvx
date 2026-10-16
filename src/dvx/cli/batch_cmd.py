"""``dvx batch`` — package + submit the DAG to AWS Batch (Fargate Spot).

Thin CLI over ``dvx.batch``. Three subcommands:

- ``dvx batch push <ecr-ref>`` — build + ECR-login + docker push
- ``dvx batch bootstrap -i <ecr-ref>`` — one-time idempotent role / logs /
  ECR / compute-env / queue / job-definition creation
- ``dvx batch submit [targets...]`` — submit a ``dvx run`` job, optionally
  tailing CloudWatch logs

See ``specs/done/batch-executor.md``.
"""
from __future__ import annotations

import click

from dvx.batch.base import derive_job_name as _derive_job_name


def _parse_pairs(pairs: tuple[str, ...], *, hint: str) -> dict[str, str]:
    """Parse ``NAME=VALUE`` tokens into a dict; raises on malformed input."""
    out: dict[str, str] = {}
    for p in pairs:
        if "=" not in p:
            raise click.BadParameter(
                f"expected NAME=VALUE, got {p!r}", param_hint=hint,
            )
        k, v = p.split("=", 1)
        out[k] = v
    return out


def _parse_env(pairs: tuple[str, ...]) -> dict[str, str]:
    """Parse ``-e/--env NAME=VALUE`` tokens (plaintext container env)."""
    return _parse_pairs(pairs, hint="-e/--env")


def _parse_secrets(pairs: tuple[str, ...]) -> dict[str, str]:
    """Parse ``-s/--secret NAME=ARN`` tokens (Secrets Manager / SSM refs)."""
    return _parse_pairs(pairs, hint="-s/--secret")


@click.group()
@click.option("-b", "--backend", type=click.Choice(["aws", "gcp"]), default=None)
@click.pass_context
def batch(ctx: click.Context, backend: str | None) -> None:
    """Package and execute DAGs on AWS or GCP Batch."""
    from dvx.batch.config import load_batch_config
    ctx.ensure_object(dict)
    ctx.obj["backend"] = backend or load_batch_config().get("backend", "aws")


def _selected_backend(backend: str | None) -> str:
    selected = backend or "aws"
    # The top-level dvx command can have its own context object.
    ctx = click.get_current_context()
    while ctx:
        if isinstance(ctx.obj, dict) and "backend" in ctx.obj:
            selected = backend or ctx.obj["backend"]
            break
        ctx = ctx.parent
    if selected not in {"aws", "gcp"}:
        raise click.UsageError(f"Unknown Batch backend: {selected!r}")
    return selected


def _reject_explicit(options: tuple[str, ...], backend: str) -> None:
    from click.core import ParameterSource
    ctx = click.get_current_context()
    for option in options:
        if ctx.get_parameter_source(option) == ParameterSource.COMMANDLINE:
            raise click.UsageError(f"--{option.replace('_', '-')} is not supported by {backend}")


@batch.command("push")
@click.option("-b", "--backend", type=click.Choice(["aws", "gcp"]))
@click.option("-B", "--no-build", is_flag=True, help="Skip docker build (image tag already exists locally).")
@click.option("-c", "--context", default=".", show_default=True, help="Docker build context.")
@click.option("-f", "--dockerfile", help="Dockerfile path (default: <context>/Dockerfile).")
@click.option("-p", "--platform", default="linux/arm64", show_default=True, help="Target platform (pair with `bootstrap --arch` — arm64/ARM64 or amd64/X86_64).")
@click.argument("image")
def batch_push(backend: str | None, no_build: bool, context: str, dockerfile: str | None, platform: str, image: str) -> None:
    """ECR-login docker, build (unless -B), and push IMAGE.

    IMAGE is a full ECR ref, e.g.
    ``<acct>.dkr.ecr.<region>.amazonaws.com/dvx:<rev>``. The ECR repo is
    created if missing, so this can run before ``bootstrap``.
    """
    selected = _selected_backend(backend)
    if selected == "gcp":
        from click.core import ParameterSource

        from dvx.batch.gcp import push_image
        if click.get_current_context().get_parameter_source("platform") != ParameterSource.COMMANDLINE:
            platform = "linux/amd64"
    else:
        from dvx.batch import push_image
    push_image(
        image,
        dockerfile=dockerfile,
        context=context,
        platform=platform,
        build=not no_build,
    )


@batch.command()
@click.option("-b", "--backend", type=click.Choice(["aws", "gcp"]))
@click.option("--profile", default="default", help="Named Batch execution profile.")
@click.option("--check", is_flag=True, help="Read-only GCP infrastructure audit.")
@click.option("--no-create", is_flag=True, help="Do not create the Artifact Registry repository.")
@click.option("-a", "--arch", type=click.Choice(["X86_64", "ARM64"]), default="ARM64", show_default=True, help="Fargate CPU architecture (ARM64 = Graviton, ~20% cheaper).")
@click.option("-e", "--env", "envs", multiple=True, metavar="NAME=VALUE", help="Job-definition env var (repeatable; e.g. S3 creds).")
@click.option("-g", "--ephemeral", type=int, default=100, show_default=True, help="Ephemeral storage GiB (scratch for the DAG's intermediates).")
@click.option("-i", "--image", help="Container image ref (required for AWS).")
@click.option("--job-role-arn", help="Existing task role for the application's AWS calls.")
@click.option("--job-role-policy", type=click.Path(exists=True, dir_okay=False), help="JSON policy for a managed task role.")
@click.option("-M", "--max-vcpus", type=int, default=16, show_default=True, help="Compute-environment max vCPUs.")
@click.option("-m", "--memory", type=int, default=65536, show_default=True, help="Job-definition memory MiB.")
@click.option("-o", "--on-demand", is_flag=True, help="Also create an on-demand (non-Spot) CE + queue `<prefix>-od` (submit -O targets it).")
@click.option("-P", "--prefix", default="dvx", show_default=True, help="Project namespace for every managed resource (role/log-group/CE/queue/job-def); use a per-project value so projects sharing `dvx.batch` don't collide.")
@click.option("-s", "--secret", "secrets", multiple=True, metavar="NAME=ARN", help="Secret env var from a Secrets Manager / SSM ARN (repeatable); baked into the job-def `secrets` + granted to the execution role.")
@click.option("--subnet", multiple=True, help="Explicit subnet ID (repeatable; requires --sg).")
@click.option("--sg", multiple=True, help="Explicit security group ID (repeatable; requires --subnet).")
@click.option("-v", "--vcpus", type=int, default=16, show_default=True, help="Job-definition vCPUs.")
def bootstrap(
    backend: str | None,
    profile: str,
    check: bool,
    no_create: bool,
    arch: str,
    envs: tuple[str, ...],
    ephemeral: int,
    image: str,
    job_role_arn: str | None,
    job_role_policy: str | None,
    max_vcpus: int,
    memory: int,
    on_demand: bool,
    prefix: str,
    secrets: tuple[str, ...],
    subnet: tuple[str, ...],
    sg: tuple[str, ...],
    vcpus: int,
) -> None:
    """Idempotently create the role, log group, ECR repo, Fargate-Spot
    compute environment, queue, and job definition."""
    import json
    from pathlib import Path

    from dvx.batch import bootstrap as _bootstrap
    if _selected_backend(backend) == "gcp":
        _reject_explicit(("arch", "ephemeral", "job_role_arn", "job_role_policy", "subnet", "sg", "prefix", "max_vcpus", "on_demand", "memory", "vcpus"), "gcp")
        from dvx.batch.config import load_profile
        from dvx.batch.gcp import bootstrap as gcp_bootstrap
        configured = load_profile(profile)
        if image:
            configured["image"] = image
        if envs or secrets:
            raise click.UsageError("Configure GCP bootstrap env and secrets in the profile")
        findings = gcp_bootstrap(configured, check_only=check or no_create)
        for finding in findings:
            click.echo(f"{'OK' if finding.ok else 'MISSING'} {finding.resource}" + (f": {finding.fix}" if not finding.ok else ""))
        if not all(f.ok for f in findings):
            raise click.ClickException("GCP bootstrap checks failed")
        return
    _reject_explicit(("profile", "check", "no_create"), "aws")
    if not image:
        raise click.UsageError("AWS bootstrap requires --image")
    extra = {}
    if job_role_arn:
        extra["job_role_arn"] = job_role_arn
    if job_role_policy:
        extra["job_role_policy"] = json.loads(Path(job_role_policy).read_text())
    if subnet or sg:
        if not subnet or not sg:
            raise click.UsageError("Pass both --subnet and --sg")
        extra.update(subnets=list(subnet), security_group_ids=list(sg))
    _bootstrap(
        image=image,
        prefix=prefix,
        arch=arch,
        max_vcpus=max_vcpus,
        vcpus=vcpus,
        memory_mib=memory,
        ephemeral_gib=ephemeral,
        on_demand=on_demand,
        environment=_parse_env(envs),
        secrets=_parse_secrets(secrets),
        **extra,
    )


@batch.command("submit")
@click.option("-b", "--backend", type=click.Choice(["aws", "gcp"]))
@click.option("--profile", default="default", help="Named GCP Batch profile.")
@click.option("--dry-run", is_flag=True, help="Print the job specification without submitting.")
@click.option("--each", is_flag=True, help="Run one target per array task.")
@click.option("--parallelism", type=click.IntRange(min=1), help="Maximum concurrent GCP array tasks.")
@click.option("--max-run", help="Maximum task duration, e.g. 4h.")
@click.option("--cached", multiple=True, help="Read matching stages from cache even when forcing.")
@click.option("--force-upstream", multiple=True, help="Force matching upstream stages.")
@click.option("--fallback-remote", multiple=True, help="Ordered read fallback (repeatable).")
@click.option("-c", "--command", "command_json", help="Container argv as a JSON list; excludes targets and run flags.")
@click.option("-C", "--commit", default="never", show_default=True, help="`dvx run --commit` override (default `never`: no git writes from the container).")
@click.option("-e", "--env", "envs", multiple=True, metavar="NAME=VALUE", help="Extra container env var (repeatable).")
@click.option("-f", "--force", is_flag=True, help="`dvx run --force` (re-run everything, skip freshness).")
@click.option("-j", "--jobs", type=int, help="`dvx run -j` (parallel workers; default: the job's vCPUs).")
@click.option("-M", "--memory", type=int, help="Override job memory MiB.")
@click.option("-n", "--job-name", help="Batch job name (default: `dvx-<slug>` derived from targets or the repo).")
@click.option("-O", "--on-demand", is_flag=True, help="Submit to the on-demand queue (needs `bootstrap -o`); no Spot reclaims.")
@click.option("-P", "--prefix", default="dvx", show_default=True, help="Project namespace to target (must match the `bootstrap --prefix` that created the job-def/queue/log-group).")
@click.option("-p", "--push", default="each", show_default=True, help="`dvx run --push` mode (`each`, `end`, or `never`).")
@click.option("-r", "--remote", help="`dvx run --remote` (named DVC remote for cache reads and pushes; default: the repo's default remote).")
@click.option("-s", "--secret", "secrets", multiple=True, metavar="NAME=ARN", help="Per-job secret env var from a Secrets Manager / SSM ARN (repeatable); the execution role must already permit the ARN (grant at `bootstrap --secret`).")
@click.option("-V", "--vcpus", type=int, help="Override job vCPUs.")
@click.option("-w", "--watch", is_flag=True, help="Tail the job's log stream; exit with its status.")
@click.argument("targets", nargs=-1)
def batch_submit(
    backend: str | None,
    profile: str,
    dry_run: bool,
    each: bool,
    parallelism: int | None,
    max_run: str | None,
    cached: tuple[str, ...],
    force_upstream: tuple[str, ...],
    fallback_remote: tuple[str, ...],
    command_json: str | None,
    commit: str,
    envs: tuple[str, ...],
    force: bool,
    jobs: int | None,
    memory: int | None,
    job_name: str | None,
    on_demand: bool,
    prefix: str,
    push: str,
    remote: str | None,
    secrets: tuple[str, ...],
    vcpus: int | None,
    watch: bool,
    targets: tuple[str, ...],
) -> None:
    """Submit a `dvx run` job to the bootstrapped queue.

    Wraps the container's ``dvx run --commit <mode> --push <mode> [--force]
    [-j N] [targets]``. The default ``--commit never`` means the container
    never writes back to git — only the S3 remote cache is updated, and
    reconciling ``.dvc`` files in git happens on a dev machine or in CI via
    ``dvx run`` (instant — auto-pull materializes everything).
    """
    import json

    from dvx.batch import od_name, run_command
    from dvx.batch import submit as _submit
    if command_json:
        if targets or force or jobs or cached or force_upstream or fallback_remote or remote or commit != "never" or push != "each":
            raise click.UsageError("--command excludes targets and run flags")
        try:
            command = json.loads(command_json)
        except ValueError as exc:
            raise click.BadParameter("expected a JSON argv list", param_hint="--command") from exc
        if not isinstance(command, list) or not command or not all(isinstance(s, str) and s for s in command):
            raise click.BadParameter("expected a nonempty JSON string argv list", param_hint="--command")
    else:
        command = run_command(
            targets, force=force, jobs=jobs, commit=commit, push=push,
            remote=remote, verbose=True, cached=cached,
            force_upstream=force_upstream, fallback_remotes=fallback_remote,
        )
    selected = _selected_backend(backend)
    if selected == "gcp" or each or dry_run or max_run:
        from dvx.batch import get_backend
        from dvx.batch.base import JobRequest
        from dvx.batch.base import watch as watch_job
        from dvx.batch.command import array_environment, task_command
        from dvx.batch.config import load_profile
        from dvx.batch.gcp import duration_seconds
        configured = load_profile(profile) if selected == "gcp" else {}
        if selected == "gcp":
            _reject_explicit(("prefix",), "gcp")
        environment = _parse_env(envs)
        if each:
            if command_json or not targets:
                raise click.UsageError("--each requires targets and excludes --command")
            from dvx.batch.command import shared_stale_upstreams
            for path, consumers in shared_stale_upstreams(targets).items():
                click.echo(f"Warning: shared stale upstream {path} for {', '.join(consumers)}; run it before --each to avoid duplicate work", err=True)
            environment.update(array_environment([[target] for target in targets], configured.get("results_url"), dry_run=dry_run))
            command = task_command(
                force=force, jobs=jobs, commit=commit, push=push, remote=remote,
                cached=cached, force_upstream=force_upstream, fallback_remotes=fallback_remote,
            )
        req = JobRequest(
            command, job_name or _derive_job_name(targets),
            profile=profile if selected == "gcp" else prefix,
            vcpus=vcpus, memory_mib=memory, env=environment, secrets=_parse_secrets(secrets),
            task_count=len(targets) if each else 1, parallelism=parallelism,
            on_demand=on_demand, max_run_seconds=duration_seconds(max_run) if max_run else None,
        )
        provider = get_backend(selected, profile=configured)
        if dry_run:
            if selected == "gcp":
                from google.cloud.batch_v1 import Job

                from dvx.batch.gcp import job_spec
                click.echo(Job.to_json(Job(job_spec(configured, req))))
            else:
                from dataclasses import asdict
                click.echo(json.dumps(asdict(req), sort_keys=True))
            return
        handle = provider.submit(req)
        click.echo(handle.name)
        if watch:
            raise SystemExit(watch_job(provider, handle))
        return
    _reject_explicit(("profile", "parallelism"), "aws")
    queue = od_name(prefix) if on_demand else prefix
    name = job_name or _derive_job_name(targets)
    code = _submit(
        command=command,
        job_name=name,
        prefix=prefix,
        queue=queue,
        vcpus=vcpus,
        memory_mib=memory,
        environment=_parse_env(envs),
        secrets=_parse_secrets(secrets),
        watch=watch,
    )
    if code != 0:
        raise SystemExit(code)


# Export the click group under the conventional name for main.py's importer.
cmd = batch


@batch.command("task-run", context_settings={"ignore_unknown_options": True})
@click.argument("run_args", nargs=-1, type=click.UNPROCESSED)
def task_run(run_args: tuple[str, ...]) -> None:
    """Execute this array task's targets with the supplied run flags."""
    import os

    from dvx.batch.command import select_task_targets
    targets = select_task_targets(dict(os.environ))
    os.execvp("dvx", ["dvx", "run", *run_args, *targets])


@batch.command("watch")
@click.option("-b", "--backend", type=click.Choice(["aws", "gcp"]))
@click.option("-P", "--profile", default="default")
@click.option("-A", "--agent-logs", is_flag=True)
@click.argument("job")
def batch_watch(backend: str | None, profile: str, agent_logs: bool, job: str) -> None:
    """Attach to an existing job and stream its logs to completion."""
    from dvx.batch import get_backend
    from dvx.batch.base import JobHandle, watch
    from dvx.batch.config import load_profile
    selected = _selected_backend(backend)
    configured = load_profile(profile) if selected == "gcp" else {}
    if agent_logs and selected == "aws":
        raise click.UsageError("--agent-logs is GCP-only")
    provider = get_backend(selected, profile=configured, **({"agent_logs": agent_logs} if selected == "gcp" else {}))
    try:
        code = watch(provider, JobHandle(selected, job, profile=profile))
    except KeyboardInterrupt:
        if click.confirm("Cancel the job?", default=False):
            provider.cancel(JobHandle(selected, job, profile=profile))
        raise click.Abort() from None
    raise SystemExit(code)


@batch.command("cancel")
@click.option("-b", "--backend", type=click.Choice(["aws", "gcp"]))
@click.option("-P", "--profile", default="default")
@click.argument("job")
def batch_cancel(backend: str | None, profile: str, job: str) -> None:
    """Terminate an existing Batch job."""
    from dvx.batch import get_backend
    from dvx.batch.base import JobHandle
    from dvx.batch.config import load_profile
    selected = _selected_backend(backend)
    provider = get_backend(selected, profile=load_profile(profile) if selected == "gcp" else {})
    provider.cancel(JobHandle(selected, job, profile=profile))
