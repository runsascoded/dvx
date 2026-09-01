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
def batch() -> None:
    """AWS Batch (Fargate Spot) packaging — see specs/done/batch-executor.md."""


@batch.command("push")
@click.option("-B", "--no-build", is_flag=True, help="Skip docker build (image tag already exists locally).")
@click.option("-c", "--context", default=".", show_default=True, help="Docker build context.")
@click.option("-f", "--dockerfile", help="Dockerfile path (default: <context>/Dockerfile).")
@click.option("-p", "--platform", default="linux/arm64", show_default=True, help="Target platform (pair with `bootstrap --arch` — arm64/ARM64 or amd64/X86_64).")
@click.argument("image")
def batch_push(no_build: bool, context: str, dockerfile: str | None, platform: str, image: str) -> None:
    """ECR-login docker, build (unless -B), and push IMAGE.

    IMAGE is a full ECR ref, e.g.
    ``<acct>.dkr.ecr.<region>.amazonaws.com/dvx:<rev>``. The ECR repo is
    created if missing, so this can run before ``bootstrap``.
    """
    from dvx.batch import push_image
    push_image(
        image,
        dockerfile=dockerfile,
        context=context,
        platform=platform,
        build=not no_build,
    )


@batch.command()
@click.option("-a", "--arch", type=click.Choice(["X86_64", "ARM64"]), default="ARM64", show_default=True, help="Fargate CPU architecture (ARM64 = Graviton, ~20% cheaper).")
@click.option("-e", "--env", "envs", multiple=True, metavar="NAME=VALUE", help="Job-definition env var (repeatable; e.g. S3 creds).")
@click.option("-g", "--ephemeral", type=int, default=100, show_default=True, help="Ephemeral storage GiB (scratch for the DAG's intermediates).")
@click.option("-i", "--image", required=True, help="Container image ref (ECR); repo is created if missing.")
@click.option("-M", "--max-vcpus", type=int, default=16, show_default=True, help="Compute-environment max vCPUs.")
@click.option("-m", "--memory", type=int, default=65536, show_default=True, help="Job-definition memory MiB.")
@click.option("-o", "--on-demand", is_flag=True, help="Also create an on-demand (non-Spot) CE + queue `<prefix>-od` (submit -O targets it).")
@click.option("-P", "--prefix", default="dvx", show_default=True, help="Project namespace for every managed resource (role/log-group/CE/queue/job-def); use a per-project value so projects sharing `dvx.batch` don't collide.")
@click.option("-s", "--secret", "secrets", multiple=True, metavar="NAME=ARN", help="Secret env var from a Secrets Manager / SSM ARN (repeatable); baked into the job-def `secrets` + granted to the execution role.")
@click.option("-v", "--vcpus", type=int, default=16, show_default=True, help="Job-definition vCPUs.")
def bootstrap(
    arch: str,
    envs: tuple[str, ...],
    ephemeral: int,
    image: str,
    max_vcpus: int,
    memory: int,
    on_demand: bool,
    prefix: str,
    secrets: tuple[str, ...],
    vcpus: int,
) -> None:
    """Idempotently create the role, log group, ECR repo, Fargate-Spot
    compute environment, queue, and job definition."""
    from dvx.batch import bootstrap as _bootstrap
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
    )


@batch.command("submit")
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
    from dvx.batch import od_name, run_command
    from dvx.batch import submit as _submit
    queue = od_name(prefix) if on_demand else prefix
    name = job_name or _derive_job_name(targets)
    code = _submit(
        command=run_command(
            targets=targets,
            force=force,
            jobs=jobs,
            commit=commit,
            push=push,
            remote=remote,
            verbose=True,
        ),
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


def _derive_job_name(targets: tuple[str, ...]) -> str:
    """Build a Batch-safe job name from the target list (or a fallback).

    Batch job names allow ``[A-Za-z0-9_-]``, max 128 chars. Non-conforming
    chars in targets become ``_``; empty falls back to ``dvx-run``.
    """
    import re
    if not targets:
        return "dvx-run"
    joined = "_".join(targets)
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "_", joined)
    return f"dvx-{sanitized[:120]}"


# Export the click group under the conventional name for main.py's importer.
cmd = batch
