"""DVX cache utilities for inspecting DVC-tracked files."""

import os
from typing import Any


def _load_dvc_file(target: str, rev: str | None = None) -> dict[str, Any]:
    """Load and parse a .dvc file, optionally from a git revision.

    Args:
        target: Path to .dvc file or tracked file (adds .dvc if needed)
        rev: Git revision (e.g., HEAD, branch name, commit hash)

    Returns:
        Parsed .dvc file contents as dict
    """
    import yaml

    # Normalize path - add .dvc if not present
    if not target.endswith(".dvc"):
        target = target + ".dvc"

    if rev:
        # Read from git revision
        import subprocess

        result = subprocess.run(
            ["git", "show", f"{rev}:{target}"],
            capture_output=True,
            text=True,
            check=True,
        )
        return yaml.safe_load(result.stdout)
    else:
        # Read from filesystem
        with open(target) as f:
            return yaml.safe_load(f)


def _get_output_info(target: str, rev: str | None = None) -> tuple[str, int | None, bool]:
    """Get hash info from a .dvc file.

    Args:
        target: Path to .dvc file or tracked file
        rev: Git revision

    Returns:
        Tuple of (hash_value, size, is_dir)
    """
    dvc_data = _load_dvc_file(target, rev)
    outs = dvc_data.get("outs", [])
    if not outs:
        raise ValueError(f"No outputs found in {target}")

    out = outs[0]
    md5 = out.get("md5")
    if not md5:
        raise ValueError(f"No hash found in {target}")

    size = out.get("size")
    # Directory hashes end with .dir
    is_dir = md5.endswith(".dir")

    return md5, size, is_dir


def get_hash(target: str, rev: str | None = None) -> str:
    """Get the MD5 hash for a DVC-tracked file.

    Args:
        target: Path to .dvc file or tracked file (adds .dvc if needed)
        rev: Git revision (e.g., HEAD, branch name, commit hash)

    Returns:
        MD5 hash string

    Examples:
        >>> get_hash("data.txt.dvc")
        "d8e8fca2dc0f896fd7cb4cb0031ba249"
        >>> get_hash("data.txt", rev="HEAD~1")
        "abc123..."
    """
    md5, _, _ = _get_output_info(target, rev)
    return md5


def get_cache_path(
    target: str,
    rev: str | None = None,
    remote: str | None = None,
    absolute: bool = False,
) -> str:
    """Get the cache path for a DVC-tracked file.

    Args:
        target: Path to .dvc file or tracked file (adds .dvc if needed)
        rev: Git revision (e.g., HEAD, branch name, commit hash)
        remote: If specified, return remote storage URL instead of local path
        absolute: If True, return absolute path (default is relative)

    Returns:
        Local cache path or remote URL

    Examples:
        >>> get_cache_path("data.txt.dvc")
        ".dvc/cache/files/md5/d8/e8fca2dc0f896fd7cb4cb0031ba249"
        >>> get_cache_path("data.txt", remote="myremote")
        "s3://bucket/cache/files/md5/d8/e8fca2dc0f896fd7cb4cb0031ba249"
    """
    md5, _, is_dir = _get_output_info(target, rev)

    if remote:
        # Get remote URL using DVC's API
        from dvc.repo import Repo as DVCRepo

        with DVCRepo() as repo:
            remote_odb = repo.cloud.get_remote_odb(name=remote)
            # For directories, the hash already ends with .dir
            path = remote_odb.oid_to_path(md5)
            url = remote_odb.fs.unstrip_protocol(path)
            return url
    else:
        # Construct local cache path
        # DVC 3.x cache structure: .dvc/cache/files/md5/XX/XXXXX...
        # For directories: .dvc/cache/files/md5/XX/XXXXX....dir
        from dvc.repo import Repo as DVCRepo

        try:
            root = DVCRepo.find_root()
        except Exception:
            root = "."

        cache_dir = os.path.join(root, ".dvc", "cache", "files", "md5")
        # Hash prefix is first 2 chars, rest is the file
        hash_value = md5.rstrip(".dir") if is_dir else md5
        cache_path = os.path.join(cache_dir, hash_value[:2], hash_value[2:])
        if is_dir:
            cache_path += ".dir"

        if absolute:
            cache_path = os.path.abspath(cache_path)
        else:
            cache_path = os.path.relpath(cache_path)

        return cache_path
