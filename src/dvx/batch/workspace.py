"""Run task worktrees on the scratch volume alongside their cache."""

import os
import shutil
from pathlib import Path
from uuid import uuid4


def prepare_workspace(targets: tuple[str, ...]) -> tuple[str, ...]:
    """Relocate the image worktree once; keep installed image environments."""
    scratch = os.environ.get("DVX_SCRATCH")
    if not scratch or os.environ.get("DVX_WORKSPACE_READY"):
        return targets
    from dvc.repo import Repo

    source = Path(Repo.find_root()).resolve()
    current = Path.cwd().resolve()
    scratch_root = Path(scratch).resolve()
    if scratch_root == source or scratch_root.is_relative_to(source):
        raise ValueError("DVX_SCRATCH must be outside the image worktree")
    scratch_root.mkdir(parents=True, exist_ok=True)
    destination = scratch_root / f"dvx-worktree-{uuid4().hex}"

    def ignore(directory: str, names: list[str]) -> list[str]:
        relative = Path(directory).relative_to(source)
        ignored = {".venv", "tmp", "wt", ".pytest_cache"} if relative == Path(".") else set()
        if relative == Path(".dvc"):
            ignored.update({"cache", "tmp", "dvx.db", "dvx.db-shm", "dvx.db-wal"})
        if relative == Path(".dvx"):
            ignored.add("batch-jobs")
        return sorted(set(names) & ignored)

    try:
        shutil.copytree(source, destination, symlinks=True, ignore=ignore)
    except BaseException:
        shutil.rmtree(destination, ignore_errors=True)
        raise
    image_environment = source / ".venv"
    if image_environment.exists():
        (destination / ".venv").symlink_to(image_environment, target_is_directory=True)
    os.chdir(destination / current.relative_to(source))
    os.environ["DVX_WORKSPACE_READY"] = str(destination)
    os.environ.setdefault("DVC_CACHE_DIR", str(scratch_root / "dvc-cache"))
    rewritten = []
    for target in targets:
        path = Path(target)
        rewritten.append(
            str(destination / path.relative_to(source))
            if path.is_absolute() and path.is_relative_to(source)
            else target
        )
    return tuple(rewritten)
