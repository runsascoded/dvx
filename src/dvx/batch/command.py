"""Container command construction shared by Batch backends."""

from typing import Any


def run_command(
    targets: tuple[str, ...] = (),
    *,
    force: bool = False,
    jobs: int | None = None,
    commit: str = "never",
    push: str = "each",
    remote: str | None = None,
    verbose: bool = True,
    cached: tuple[str, ...] = (),
    force_upstream: tuple[str, ...] = (),
    fallback_remotes: tuple[str, ...] = (),
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
    for pattern in cached:
        cmd += ["--cached", pattern]
    for pattern in force_upstream:
        cmd += ["--force-upstream", pattern]
    for fallback in fallback_remotes:
        cmd += ["--fallback-remote", fallback]
    if verbose:
        cmd.append("-v")
    cmd.extend(targets)
    return cmd


def task_command(**kwargs: Any) -> list[str]:
    return ["batch", "task-run", *run_command(**kwargs)[1:]]


def array_environment(
    targets: list[list[str]],
    results_url: str | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, str]:
    import json
    from hashlib import sha256

    raw = json.dumps(targets, separators=(",", ":"))
    if len(raw.encode()) <= 16000:
        return {"DVX_TASK_TARGETS": raw}
    if not results_url:
        raise ValueError("Large task lists require batch profile results_url")
    from .storage import write_json_url

    url = f"{results_url.rstrip('/')}/dvx-jobs/tasks/{sha256(raw.encode()).hexdigest()}.json"
    if not dry_run:
        write_json_url(url, targets)
    return {"DVX_TASK_TARGETS_URL": url}


def select_task_targets(env: dict[str, str]) -> list[str]:
    import json

    indices = [env[key] for key in ("BATCH_TASK_INDEX", "AWS_BATCH_JOB_ARRAY_INDEX") if key in env]
    if not indices or len(set(indices)) != 1:
        raise ValueError("Exactly one consistent Batch task index is required")
    if ("DVX_TASK_TARGETS" in env) == ("DVX_TASK_TARGETS_URL" in env):
        raise ValueError("Set exactly one of DVX_TASK_TARGETS and DVX_TASK_TARGETS_URL")
    if "DVX_TASK_TARGETS" in env:
        targets = json.loads(env["DVX_TASK_TARGETS"])
    else:
        from .storage import read_json_url

        targets = read_json_url(env["DVX_TASK_TARGETS_URL"])
    if (
        not isinstance(targets, list)
        or not targets
        or not all(
            isinstance(row, list) and row and all(isinstance(s, str) and s for s in row)
            for row in targets
        )
    ):
        raise ValueError("Task targets must be a nonempty list of nonempty target lists")
    index = int(indices[0])
    if not 0 <= index < len(targets):
        raise ValueError(f"Task index {index} is outside 0..{len(targets) - 1}")
    return targets[index]


def shared_stale_upstreams(targets: tuple[str, ...]) -> dict[str, list[str]]:
    """Metadata-only warning for duplicate upstream work in an array."""
    from io import StringIO
    from pathlib import Path

    from dvx.run.executor import ExecutionConfig, run

    owners = {}
    target_paths = {target.removesuffix(".dvc") for target in targets}
    for target in targets:
        results = run(
            [Path(target)], ExecutionConfig(dry_run=True, commit="never"), output=StringIO()
        )
        for result in results:
            if result.success and not result.skipped and result.path not in target_paths:
                owners.setdefault(result.path, []).append(target)
    return {path: named for path, named in sorted(owners.items()) if len(named) > 1}
