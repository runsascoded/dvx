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
    recursive: bool = False,
) -> tuple[str, int, bool]:
    """Add a file or directory to DVC cache without global locking.

    This is a lock-free alternative to `dvc add` that can be safely called
    in parallel for independent files.

    Args:
        target: Path to file or directory to add
        force: Overwrite existing cache entry if present
        recursive: If True, auto-add stale deps first; if False, error on stale deps

    Returns:
        Tuple of (md5_hash, size, is_dir)

    Raises:
        ValueError: If deps are stale and recursive=False
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

    # Write .dvc file, preserving existing metadata
    dvc_path = Path(str(target) + ".dvc")

    # Load existing .dvc file to preserve meta section
    existing_meta = None
    if dvc_path.exists():
        try:
            with open(dvc_path) as f:
                existing = yaml.safe_load(f)
                if existing and "meta" in existing:
                    existing_meta = existing["meta"]
        except Exception:
            pass  # If we can't read it, start fresh

    dvc_content = {
        "outs": [{
            "md5": md5,
            "size": size,
            "hash": "md5",
            "path": target_path.name,
        }],
    }

    # Preserve existing meta section, but update dep hashes to current values
    # The output was just regenerated, so deps should reflect what was actually used
    if existing_meta:
        dvc_content["meta"] = existing_meta
        # Update dep hashes from current .dvc files, validating they're fresh
        if "computation" in existing_meta and "deps" in existing_meta["computation"]:
            deps = existing_meta["computation"]["deps"]
            updated_deps = {}
            stale_deps = []

            for dep_path in deps.keys():
                dep_file = Path(dep_path)
                dep_dvc = Path(dep_path + ".dvc")

                # Get the .dvc hash
                dvc_hash = None
                if dep_dvc.exists():
                    try:
                        with open(dep_dvc) as f:
                            dep_data = yaml.safe_load(f)
                            if dep_data and "outs" in dep_data:
                                dvc_hash = dep_data["outs"][0].get("md5")
                    except Exception:
                        pass

                # Get the actual file hash
                file_hash = None
                if dep_file.exists():
                    file_hash = _hash_single_file(dep_file)

                # Check freshness: file hash must match .dvc hash
                if dvc_hash and file_hash and dvc_hash != file_hash:
                    stale_deps.append((dep_path, dvc_hash, file_hash))
                elif dvc_hash:
                    updated_deps[dep_path] = dvc_hash
                else:
                    # Fall back to existing hash if we can't get current
                    updated_deps[dep_path] = deps[dep_path]

            # Handle stale deps
            if stale_deps:
                if recursive:
                    # Auto-add stale deps first (depth-first)
                    for dep_path, _dvc_hash, _file_hash in stale_deps:
                        add_to_cache(dep_path, force=force, recursive=recursive)
                    # Re-read the now-updated .dvc hashes
                    for dep_path, _dvc_hash, file_hash in stale_deps:
                        updated_deps[dep_path] = file_hash
                else:
                    stale_msg = "\n".join(
                        f"  {p}: .dvc={dh[:8]}... file={fh[:8]}..."
                        for p, dh, fh in stale_deps
                    )
                    raise ValueError(
                        f"Cannot add {target}: {len(stale_deps)} stale dep(s):\n{stale_msg}\n"
                        f"Run `dvx add` on deps first, or use --recursive"
                    )

            existing_meta["computation"]["deps"] = updated_deps

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


# =============================================================================
# Dry-run / transfer status utilities
# =============================================================================


def _format_size(size: int | None) -> str:
    """Format size in human-readable form."""
    if size is None:
        return "?"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{size} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def find_dvc_files(
    targets: list[str] | None = None,
    glob_pattern: bool = False,
) -> list[str]:
    """Find .dvc files to process.

    Args:
        targets: Specific targets (files/dirs). If None, find all .dvc files.
        glob_pattern: If True, treat targets as glob patterns.

    Returns:
        List of .dvc file paths.
    """
    import glob
    from pathlib import Path

    from dvc.repo import Repo as DVCRepo

    try:
        root = DVCRepo.find_root()
    except Exception:
        root = "."

    if targets:
        dvc_files = []
        for target in targets:
            if glob_pattern:
                matches = glob.glob(target, recursive=True)
                for match in matches:
                    if match.endswith(".dvc"):
                        dvc_files.append(match)
                    elif os.path.exists(match + ".dvc"):
                        dvc_files.append(match + ".dvc")
            else:
                if target.endswith(".dvc"):
                    dvc_files.append(target)
                elif os.path.exists(target + ".dvc"):
                    dvc_files.append(target + ".dvc")
                elif os.path.isdir(target):
                    # Find all .dvc files in directory (exclude .dvc subdir)
                    for f in Path(target).rglob("*.dvc"):
                        if f.is_file() and ".dvc/" not in str(f):
                            dvc_files.append(str(f))
        return dvc_files
    else:
        # Find all .dvc files in repo (exclude .dvc directory and cache)
        return [
            str(f) for f in Path(root).rglob("*.dvc")
            if f.is_file() and ".dvc/" not in str(f)
        ]


def find_dvc_files_at_ref(ref: str, targets: list[str] | None = None) -> list[str]:
    """Find .dvc files at a specific git ref.

    Args:
        ref: Git ref (commit, branch, tag)
        targets: Specific paths to check (optional)

    Returns:
        List of .dvc file paths
    """
    import subprocess

    # Get list of all files at the ref
    result = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", ref],
        capture_output=True,
        text=True,
        check=True,
    )
    all_files = result.stdout.strip().split("\n")
    dvc_files = [f for f in all_files if f.endswith(".dvc") and not f.startswith(".dvc/")]

    if targets:
        # Filter to matching targets
        filtered = []
        for target in targets:
            target = target.rstrip("/")
            if target.endswith(".dvc"):
                if target in dvc_files:
                    filtered.append(target)
            else:
                # Check for exact match or directory prefix
                dvc_target = target + ".dvc"
                if dvc_target in dvc_files:
                    filtered.append(dvc_target)
                else:
                    # Check for files under this directory
                    prefix = target + "/"
                    filtered.extend(f for f in dvc_files if f.startswith(prefix))
        return filtered
    return dvc_files


def get_output_info_at_ref(dvc_file: str, ref: str) -> tuple[str, int | None, bool]:
    """Get output info from a .dvc file at a specific git ref.

    Args:
        dvc_file: Path to .dvc file
        ref: Git ref

    Returns:
        Tuple of (md5, size, is_dir)
    """
    import subprocess

    import yaml

    result = subprocess.run(
        ["git", "show", f"{ref}:{dvc_file}"],
        capture_output=True,
        text=True,
        check=True,
    )
    data = yaml.safe_load(result.stdout)
    outs = data.get("outs", [])
    if not outs:
        raise ValueError(f"No outputs in {dvc_file}")
    out = outs[0]
    md5 = out.get("md5", "")
    size = out.get("size")
    is_dir = md5.endswith(".dir")
    return md5, size, is_dir


def get_transfer_status_at_ref(
    ref: str,
    targets: list[str] | None = None,
    remote: str | None = None,
) -> dict:
    """Get status of what would be transferred for a specific git ref.

    Args:
        ref: Git ref to check
        targets: Specific targets to check
        remote: Remote name

    Returns:
        Dict with transfer status info (same format as get_transfer_status)
    """
    dvc_files = find_dvc_files_at_ref(ref, targets)

    missing = []
    cached = []
    errors = []
    total_missing_size = 0
    total_cached_size = 0

    for dvc_file in dvc_files:
        try:
            md5, size, _is_dir = get_output_info_at_ref(dvc_file, ref)
            data_path = dvc_file[:-4] if dvc_file.endswith(".dvc") else dvc_file

            if check_local_cache(md5):
                cached.append((data_path, md5, size))
                if size:
                    total_cached_size += size
            else:
                missing.append((data_path, md5, size))
                if size:
                    total_missing_size += size
        except Exception as e:
            errors.append((dvc_file, str(e)))

    return {
        "missing": missing,
        "cached": cached,
        "errors": errors,
        "total_missing_size": total_missing_size,
        "total_cached_size": total_cached_size,
    }


def pull_hashes(
    hashes: list[str],
    remote: str | None = None,
    jobs: int | None = None,
) -> int:
    """Pull specific hashes from remote to local cache.

    Args:
        hashes: List of MD5 hashes to pull
        remote: Remote name (uses default if None)
        jobs: Number of parallel jobs

    Returns:
        Number of files fetched
    """
    from dvc.repo import Repo as DVCRepo

    if not hashes:
        return 0

    with DVCRepo() as repo:
        remote_odb = repo.cloud.get_remote_odb(name=remote)
        local_odb = repo.odb.local

        # Filter to hashes not already in local cache
        to_fetch = [h for h in hashes if not check_local_cache(h)]

        if not to_fetch:
            return 0

        # Use DVC's transfer mechanism
        from dvc.fs import Callback

        callback = Callback.as_tqdm_callback(desc="Fetching", unit="file")
        try:
            transferred = remote_odb.fs.get(
                [remote_odb.oid_to_path(h) for h in to_fetch],
                [local_odb.oid_to_path(h) for h in to_fetch],
                callback=callback,
            )
            return len(to_fetch)
        except Exception:
            # Fallback to one-by-one transfer
            count = 0
            for h in to_fetch:
                try:
                    src = remote_odb.oid_to_path(h)
                    dst = local_odb.oid_to_path(h)
                    remote_odb.fs.get(src, dst)
                    count += 1
                except Exception:
                    pass
            return count


def check_local_cache(md5: str) -> bool:
    """Check if a hash exists in local cache.

    Args:
        md5: MD5 hash to check

    Returns:
        True if hash exists in local cache
    """
    from pathlib import Path

    from dvc.repo import Repo as DVCRepo

    try:
        root = DVCRepo.find_root()
    except Exception:
        root = "."

    is_dir = md5.endswith(".dir")
    hash_value = md5[:-4] if is_dir else md5
    cache_path = Path(root) / ".dvc" / "cache" / "files" / "md5" / hash_value[:2] / hash_value[2:]
    if is_dir:
        cache_path = cache_path.with_suffix(".dir")

    return cache_path.exists()


def check_remote_cache(md5: str, remote: str | None = None) -> bool:
    """Check if a hash exists in remote cache.

    Args:
        md5: MD5 hash to check
        remote: Remote name (uses default if None)

    Returns:
        True if hash exists in remote cache
    """
    from dvc.repo import Repo as DVCRepo

    try:
        with DVCRepo() as repo:
            remote_odb = repo.cloud.get_remote_odb(name=remote)
            return remote_odb.exists(md5)
    except Exception:
        return False


def check_remote_cache_batch(
    hashes: list[str],
    remote: str | None = None,
    jobs: int | None = None,
    progress: bool = True,
) -> dict[str, bool]:
    """Check if multiple hashes exist in remote cache (parallel).

    Args:
        hashes: List of MD5 hashes to check
        remote: Remote name (uses default if None)
        jobs: Number of parallel workers (default: min(32, len(hashes)))
        progress: Show progress bar

    Returns:
        Dict mapping hash -> exists (True/False)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from dvc.repo import Repo as DVCRepo

    if not hashes:
        return {}

    # Deduplicate
    unique_hashes = list(set(hashes))
    results = {}

    # Open repo once and reuse
    try:
        with DVCRepo() as repo:
            remote_odb = repo.cloud.get_remote_odb(name=remote)

            def check_one(md5: str) -> tuple[str, bool]:
                try:
                    return md5, remote_odb.exists(md5)
                except Exception:
                    return md5, False

            max_workers = jobs or min(32, len(unique_hashes))

            if progress:
                try:
                    from tqdm import tqdm
                    pbar = tqdm(total=len(unique_hashes), desc="Checking remote", unit="file")
                except ImportError:
                    pbar = None
            else:
                pbar = None

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(check_one, h): h for h in unique_hashes}
                for future in as_completed(futures):
                    md5, exists = future.result()
                    results[md5] = exists
                    if pbar:
                        pbar.update(1)

            if pbar:
                pbar.close()

    except Exception:
        # Fallback to sequential if parallel fails
        for h in unique_hashes:
            results[h] = check_remote_cache(h, remote)

    return results


def get_transfer_status(
    targets: list[str] | None = None,
    remote: str | None = None,
    direction: str = "pull",
    glob_pattern: bool = False,
    jobs: int | None = None,
    progress: bool = True,
) -> dict:
    """Get status of what would be transferred.

    Args:
        targets: Specific targets to check
        remote: Remote name
        direction: "pull" (check local cache) or "push" (check remote cache)
        glob_pattern: If True, treat targets as glob patterns
        jobs: Number of parallel workers for remote checks
        progress: Show progress bar for remote checks

    Returns:
        Dict with transfer status info:
        {
            "missing": [(path, md5, size), ...],  # Would be transferred
            "cached": [(path, md5, size), ...],   # Already in cache
            "errors": [(path, error), ...],       # Failed to check
            "total_missing_size": int,
            "total_cached_size": int,
        }
    """
    dvc_files = find_dvc_files(targets, glob_pattern)

    # First pass: gather all file info
    file_info = []  # [(dvc_file, data_path, md5, size), ...]
    errors = []

    for dvc_file in dvc_files:
        try:
            md5, size, _is_dir = _get_output_info(dvc_file)
            data_path = dvc_file[:-4] if dvc_file.endswith(".dvc") else dvc_file
            file_info.append((dvc_file, data_path, md5, size))
        except Exception as e:
            errors.append((dvc_file, str(e)))

    if not file_info:
        return {
            "missing": [],
            "cached": [],
            "errors": errors,
            "total_missing_size": 0,
            "total_cached_size": 0,
        }

    # Second pass: check cache (parallel for remote)
    missing = []
    cached = []
    total_missing_size = 0
    total_cached_size = 0

    if direction == "push":
        # Batch check remote (parallel)
        all_hashes = [md5 for _, _, md5, _ in file_info]
        cache_status = check_remote_cache_batch(all_hashes, remote, jobs=jobs, progress=progress)

        for dvc_file, data_path, md5, size in file_info:
            in_cache = cache_status.get(md5, False)
            if in_cache:
                cached.append((data_path, md5, size))
                if size:
                    total_cached_size += size
            else:
                missing.append((data_path, md5, size))
                if size:
                    total_missing_size += size
    else:
        # Local cache check is fast, no need for parallel
        for dvc_file, data_path, md5, size in file_info:
            if check_local_cache(md5):
                cached.append((data_path, md5, size))
                if size:
                    total_cached_size += size
            else:
                missing.append((data_path, md5, size))
                if size:
                    total_missing_size += size

    return {
        "missing": missing,
        "cached": cached,
        "errors": errors,
        "total_missing_size": total_missing_size,
        "total_cached_size": total_cached_size,
    }
