"""DVX cache utilities for inspecting DVC-tracked files."""

import os
import re
from typing import Any


def is_md5_hash(value: str) -> bool:
    """Check if a string looks like an MD5 hash.

    Args:
        value: String to check

    Returns:
        True if value is 32 hex chars (optionally with .dir suffix)
    """
    # MD5 hash: 32 hex characters, optionally followed by .dir
    return bool(re.match(r"^[a-f0-9]{32}(\.dir)?$", value.lower()))


def get_cache_path_from_hash(
    md5: str,
    remote: str | None = None,
    absolute: bool = False,
) -> str:
    """Get the cache path for a given MD5 hash.

    Args:
        md5: MD5 hash (32 hex chars, optionally with .dir suffix)
        remote: If specified, return remote storage URL instead of local path
        absolute: If True, return absolute path (default is relative)

    Returns:
        Local cache path or remote URL

    Examples:
        >>> get_cache_path_from_hash("d8e8fca2dc0f896fd7cb4cb0031ba249")
        ".dvc/cache/files/md5/d8/e8fca2dc0f896fd7cb4cb0031ba249"
    """
    is_dir = md5.endswith(".dir")

    if remote:
        from dvc.repo import Repo as DVCRepo

        with DVCRepo() as repo:
            remote_odb = repo.cloud.get_remote_odb(name=remote)
            path = remote_odb.oid_to_path(md5)
            url = remote_odb.fs.unstrip_protocol(path)
            return url
    else:
        from dvc.repo import Repo as DVCRepo

        try:
            root = DVCRepo.find_root()
        except Exception:
            root = "."

        cache_dir = os.path.join(root, ".dvc", "cache", "files", "md5")
        hash_value = md5[:-4] if is_dir else md5  # Strip .dir suffix
        cache_path = os.path.join(cache_dir, hash_value[:2], hash_value[2:])
        if is_dir:
            cache_path += ".dir"

        if absolute:
            cache_path = os.path.abspath(cache_path)
        else:
            cache_path = os.path.relpath(cache_path)

        return cache_path


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


def _get_file_in_dir_hash(target: str, rev: str | None = None) -> str | None:
    """Get the hash of a file inside a DVC-tracked directory.

    Args:
        target: Path to a file inside a tracked directory
        rev: Git revision (e.g., HEAD, branch name, commit hash)

    Returns:
        MD5 hash string if found, None otherwise
    """
    import subprocess
    from pathlib import Path

    from dvx.run.dvc_files import find_parent_dvc_dir, read_dir_manifest

    target_path = Path(target)

    # For revision lookups, we need to find the parent .dvc file in git
    if rev:
        # Walk up the path looking for a .dvc file in the git revision
        parts = target_path.parts
        for i in range(len(parts) - 1, 0, -1):
            parent_path = Path(*parts[:i])
            parent_dvc = str(parent_path) + ".dvc"
            try:
                result = subprocess.run(
                    ["git", "show", f"{rev}:{parent_dvc}"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                # Found a .dvc file - parse it
                import yaml
                dvc_data = yaml.safe_load(result.stdout)
                relpath = str(Path(*parts[i:]))
                break
            except subprocess.CalledProcessError:
                continue
        else:
            return None
    else:
        # Current filesystem lookup
        result = find_parent_dvc_dir(target_path)
        if result is None:
            return None

        parent_dir, relpath = result

        # Get the parent directory's .dvc file hash
        parent_dvc = str(parent_dir) + ".dvc"
        try:
            dvc_data = _load_dvc_file(parent_dvc, rev)
        except (FileNotFoundError, Exception):
            return None

    outs = dvc_data.get("outs", [])
    if not outs:
        return None

    dir_hash = outs[0].get("md5")
    if not dir_hash or not dir_hash.endswith(".dir"):
        return None

    # Read the directory manifest from cache
    manifest = read_dir_manifest(dir_hash)
    return manifest.get(relpath)


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
        target: Path to .dvc file or tracked file (adds .dvc if needed),
                a file inside a DVC-tracked directory, or an MD5 hash
        rev: Git revision (e.g., HEAD, branch name, commit hash)

    Returns:
        MD5 hash string

    Examples:
        >>> get_hash("data.txt.dvc")
        "d8e8fca2dc0f896fd7cb4cb0031ba249"
        >>> get_hash("data.txt", rev="HEAD~1")
        "abc123..."
        >>> get_hash("tracked_dir/file.txt")  # file inside tracked dir
        "def456..."
        >>> get_hash("d8e8fca2dc0f896fd7cb4cb0031ba249")  # already a hash
        "d8e8fca2dc0f896fd7cb4cb0031ba249"
    """
    # If target is already a hash, return it
    if is_md5_hash(target):
        return target

    # First try direct .dvc file lookup
    import subprocess
    try:
        md5, _, _ = _get_output_info(target, rev)
        return md5
    except (FileNotFoundError, ValueError, subprocess.CalledProcessError):
        pass

    # Try file-inside-directory lookup
    md5 = _get_file_in_dir_hash(target, rev)
    if md5:
        return md5

    raise FileNotFoundError(f"No .dvc file found for {target}")


def get_cache_path(
    target: str,
    rev: str | None = None,
    remote: str | None = None,
    absolute: bool = False,
) -> str:
    """Get the cache path for a DVC-tracked file or hash.

    Args:
        target: Path to .dvc file or tracked file (adds .dvc if needed),
                a file inside a DVC-tracked directory, or an MD5 hash
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
        >>> get_cache_path("tracked_dir/file.txt")  # file inside tracked dir
        ".dvc/cache/files/md5/de/f456..."
        >>> get_cache_path("d8e8fca2dc0f896fd7cb4cb0031ba249")  # direct hash
        ".dvc/cache/files/md5/d8/e8fca2dc0f896fd7cb4cb0031ba249"
    """
    # Check if target is a direct MD5 hash
    if is_md5_hash(target):
        return get_cache_path_from_hash(target, remote=remote, absolute=absolute)

    # Try direct .dvc file lookup first
    import subprocess
    md5 = None
    is_dir = False
    try:
        md5, _, is_dir = _get_output_info(target, rev)
    except (FileNotFoundError, ValueError, subprocess.CalledProcessError):
        pass

    # If not found, try file-inside-directory lookup
    if md5 is None:
        md5 = _get_file_in_dir_hash(target, rev)
        if md5 is None:
            raise FileNotFoundError(f"No .dvc file found for {target}")
        is_dir = False  # Files inside directories are never directories themselves

    return get_cache_path_from_hash(md5, remote=remote, absolute=absolute)


def add_to_cache(
    target: str,
    force: bool = False,
) -> tuple[str, int, bool]:
    """Add a file or directory to DVC cache without global locking.

    This is a lock-free alternative to `dvc add` that can be safely called
    in parallel for independent files.

    Args:
        target: Path to file or directory to add
        force: Overwrite existing cache entry if present

    Returns:
        Tuple of (md5_hash, size, is_dir)
    """
    import hashlib
    import json
    import shutil
    import tempfile
    from pathlib import Path

    import yaml

    from dvc.repo import Repo as DVCRepo

    target_path = Path(target)
    if not target_path.exists():
        raise FileNotFoundError(f"{target} not found")

    # Get cache directory
    try:
        root = DVCRepo.find_root()
    except Exception:
        root = "."
    cache_dir = Path(root) / ".dvc" / "cache" / "files" / "md5"
    cache_dir.mkdir(parents=True, exist_ok=True)

    is_dir = target_path.is_dir()

    if is_dir:
        # For directories, compute manifest and hash
        entries = []
        total_size = 0
        for subfile in sorted(target_path.rglob("*")):
            if subfile.is_file():
                rel_path = subfile.relative_to(target_path)
                rel_path_str = str(rel_path).replace("\\", "/")
                file_hash = _hash_single_file(subfile)
                file_size = subfile.stat().st_size
                entries.append({
                    "md5": file_hash,
                    "relpath": rel_path_str,
                })
                total_size += file_size
                # Also cache individual files
                _cache_file(subfile, file_hash, cache_dir, force)

        # Sort entries by relpath (DVC convention)
        entries.sort(key=lambda e: e["relpath"])

        # Hash the manifest
        json_str = json.dumps(entries, separators=(", ", ": "))
        manifest_hash = hashlib.md5(json_str.encode()).hexdigest()  # noqa: S324
        dir_hash = manifest_hash + ".dir"

        # Write manifest to cache
        manifest_cache_path = cache_dir / manifest_hash[:2] / (manifest_hash[2:] + ".dir")
        manifest_cache_path.parent.mkdir(parents=True, exist_ok=True)
        if force or not manifest_cache_path.exists():
            with tempfile.NamedTemporaryFile(
                mode="w", dir=manifest_cache_path.parent, delete=False, suffix=".tmp"
            ) as tmp:
                json.dump(entries, tmp, separators=(", ", ": "))
                tmp_path = tmp.name
            os.replace(tmp_path, manifest_cache_path)

        md5 = dir_hash
        size = total_size
    else:
        # For files, compute hash and cache
        md5 = _hash_single_file(target_path)
        size = target_path.stat().st_size
        _cache_file(target_path, md5, cache_dir, force)

    # Write .dvc file
    dvc_path = Path(str(target) + ".dvc")
    dvc_content = {
        "hash": "md5",
        "outs": [{
            "md5": md5,
            "size": size,
            "path": target_path.name,
        }],
    }
    # Atomic write
    with tempfile.NamedTemporaryFile(
        mode="w", dir=dvc_path.parent, delete=False, suffix=".tmp"
    ) as tmp:
        yaml.dump(dvc_content, tmp, default_flow_style=False, sort_keys=False)
        tmp_path = tmp.name
    os.replace(tmp_path, dvc_path)

    return md5, size, is_dir


def _hash_single_file(file_path) -> str:
    """Compute MD5 hash of a single file."""
    import hashlib
    md5 = hashlib.md5()  # noqa: S324
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            md5.update(chunk)
    return md5.hexdigest()


def _cache_file(file_path, file_hash: str, cache_dir, force: bool = False):
    """Copy a file to DVC cache atomically."""
    import shutil
    import tempfile
    from pathlib import Path

    cache_path = cache_dir / file_hash[:2] / file_hash[2:]
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if not force and cache_path.exists():
        return  # Already cached

    # Atomic copy: write to temp file, then rename
    with tempfile.NamedTemporaryFile(
        dir=cache_path.parent, delete=False, suffix=".tmp"
    ) as tmp:
        tmp_path = Path(tmp.name)

    shutil.copy2(file_path, tmp_path)
    os.replace(tmp_path, cache_path)
