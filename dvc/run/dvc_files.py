"""Read/write .dvc files for output tracking.

DVX uses individual .dvc files to track pipeline outputs rather than
a centralized dvc.lock file. This provides:
- Locality: hash and provenance info lives next to each artifact
- Git-friendly: small, independent files instead of one large lock file
- Self-documenting: each artifact knows how it was produced

The DVX .dvc format extends standard DVC with a `computation` block:

```yaml
outs:
- md5: abc123...
  size: 12345
  path: output.parquet

computation:
  cmd: "python process.py --input data.csv"
  code_ref: "a1b2c3d4..."  # git SHA when computed
  deps:
    data.csv: def456...
    process.py: 789abc...
```
"""

import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from dvc.run.hash import compute_md5


def get_git_head_sha(repo_path: Path | None = None) -> str | None:
    """Get the current HEAD commit SHA.

    Args:
        repo_path: Path to git repository (default: current directory)

    Returns:
        Full SHA string, or None if not in a git repo
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def get_git_blob_sha(path: str, ref: str = "HEAD", repo_path: Path | None = None) -> str | None:
    """Get the git blob SHA for a file at a specific ref.

    Args:
        path: Path to the file (relative to repo root)
        ref: Git ref (commit SHA, branch, tag, HEAD, etc.)
        repo_path: Path to git repository (default: current directory)

    Returns:
        Blob SHA string, or None if file doesn't exist at that ref
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", f"{ref}:{path}"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def has_file_changed_since(
    path: str,
    since_ref: str,
    repo_path: Path | None = None,
) -> bool | None:
    """Check if a file has changed between a ref and HEAD.

    This is a fast check using git blob SHAs - no file content reading needed.

    Args:
        path: Path to the file (relative to repo root)
        since_ref: Git ref to compare from (e.g., a commit SHA)
        repo_path: Path to git repository (default: current directory)

    Returns:
        True if file changed, False if unchanged, None if can't determine
        (e.g., file doesn't exist in git, or not in a git repo)
    """
    old_blob = get_git_blob_sha(path, since_ref, repo_path)
    new_blob = get_git_blob_sha(path, "HEAD", repo_path)

    if old_blob is None or new_blob is None:
        # Can't determine - file not tracked or ref invalid
        return None

    return old_blob != new_blob


def have_deps_changed_since(
    deps: dict[str, str],
    code_ref: str,
    repo_path: Path | None = None,
) -> tuple[bool, list[str]]:
    """Check if any dependencies have changed since a commit.

    Uses git blob comparison for tracked files - fast, no hashing needed.
    Falls back to hash comparison for untracked files.

    Args:
        deps: Dict of {dep_path: recorded_hash}
        code_ref: Git SHA when artifact was computed
        repo_path: Path to git repository

    Returns:
        Tuple of (any_changed, list_of_changed_paths)
    """
    changed = []

    for dep_path in deps:
        file_changed = has_file_changed_since(dep_path, code_ref, repo_path)

        if file_changed is True:
            changed.append(dep_path)
        elif file_changed is None:
            # Can't use git - file might be untracked or generated
            # Fall back to hash comparison (handled by caller)
            pass

    return len(changed) > 0, changed


@dataclass
class DVCFileInfo:
    """Content of a .dvc file."""

    path: str
    md5: str
    size: int
    # Provenance via computation block (optional)
    cmd: str | None = None
    code_ref: str | None = None  # git SHA
    deps: dict[str, str] = field(default_factory=dict)  # {path: md5}
    # Directory metadata
    nfiles: int | None = None
    is_dir: bool = False
    # Legacy field for backward compatibility
    stage: str | None = None


def read_dvc_file(output_path: Path) -> DVCFileInfo | None:
    """Read .dvc file for an output.

    Handles both DVX format (computation block) and legacy format (meta block).

    Args:
        output_path: Path to the output file/directory, or directly to the .dvc file

    Returns:
        DVCFileInfo if .dvc file exists and is valid, None otherwise
    """
    # Support both output path and direct .dvc path
    if output_path.suffix == ".dvc":
        dvc_path = output_path
    else:
        dvc_path = Path(str(output_path) + ".dvc")

    if not dvc_path.exists():
        return None

    with open(dvc_path) as f:
        # Use CSafeLoader for ~5x faster parsing (falls back to SafeLoader if unavailable)
        loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
        data = yaml.load(f, Loader=loader)  # noqa: S506

    if not data or "outs" not in data or not data["outs"]:
        return None

    out = data["outs"][0]

    # Handle .dir suffix for directory hashes (DVC convention)
    md5_raw = out.get("md5", "")
    is_dir = md5_raw.endswith(".dir")
    md5 = md5_raw[:-4] if is_dir else md5_raw  # Strip .dir suffix

    # Computation info lives in meta.computation for DVC compatibility
    # Also check top-level computation for backward compat with old DVX files
    meta = data.get("meta", {})
    computation = meta.get("computation", {}) or data.get("computation", {})

    return DVCFileInfo(
        path=out.get("path", str(output_path)),
        md5=md5,
        size=out.get("size", 0),
        # Provenance from computation block
        cmd=computation.get("cmd"),
        code_ref=computation.get("code_ref"),
        deps=computation.get("deps") or {},
        # Directory metadata
        nfiles=out.get("nfiles"),
        is_dir=is_dir,
        stage=meta.get("stage"),  # Legacy only
    )


def write_dvc_file(
    output_path: Path,
    md5: str,
    size: int,
    cmd: str | None = None,
    code_ref: str | None = None,
    deps: dict[str, str] | None = None,
    nfiles: int | None = None,
    is_dir: bool | None = None,
    stage: str | None = None,  # noqa: ARG001 (legacy, deprecated)
) -> Path:
    """Write .dvc file for an output with provenance.

    Args:
        output_path: Path to the output file/directory
        md5: MD5 hash of the output
        size: Size in bytes
        cmd: Command that was run (provenance)
        code_ref: Git SHA when computation was run (provenance)
        deps: {dep_path: md5} of inputs (provenance)
        nfiles: Number of files (for directories)
        is_dir: Whether output is a directory (auto-detected if None)
        stage: Deprecated, kept for backward compatibility

    Returns:
        Path to the created .dvc file
    """
    output_path = Path(output_path)
    dvc_path = Path(str(output_path) + ".dvc")

    # Ensure parent directory exists
    dvc_path.parent.mkdir(parents=True, exist_ok=True)

    # Auto-detect if directory
    if is_dir is None:
        is_dir = output_path.is_dir() if output_path.exists() else False

    # Path in .dvc file should be relative to the .dvc file location (just the filename)
    relative_path = output_path.name

    # For directories, add .dir suffix to hash (DVC convention)
    hash_value = f"{md5}.dir" if is_dir else md5

    out_entry = {
        "md5": hash_value,
        "size": size,
        "path": relative_path,
    }

    # Add nfiles for directories
    if is_dir:
        if nfiles is None and output_path.exists():
            # Count files in directory
            nfiles = sum(1 for f in output_path.rglob("*") if f.is_file())
        if nfiles is not None:
            # Insert nfiles after size (for nicer ordering)
            out_entry = {
                "md5": hash_value,
                "size": size,
                "nfiles": nfiles,
                "path": relative_path,
            }

    data = {"outs": [out_entry]}

    # Add computation block inside meta for DVC compatibility
    # (DVC allows arbitrary data in meta, but rejects unknown top-level keys)
    if cmd or code_ref or deps:
        computation = {}
        if cmd:
            computation["cmd"] = cmd
        if code_ref:
            computation["code_ref"] = code_ref
        if deps:
            computation["deps"] = deps
        data["meta"] = {"computation": computation}

    with open(dvc_path, "w") as f:
        yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    return dvc_path


def is_output_fresh(
    output_path: Path,
    check_deps: bool = True,
    check_code_ref: bool = True,
    use_mtime_cache: bool = True,
    info: DVCFileInfo | None = None,
) -> tuple[bool, str]:
    """Check if output is fresh (up-to-date with its .dvc file and deps).

    Freshness is determined by:
    1. Output file exists
    2. Output hash matches .dvc file (uses mtime cache to avoid redundant hashing)
    3. Dependencies haven't changed since code_ref (via git blob comparison)
    4. For untracked deps, fall back to hash comparison

    Args:
        output_path: Path to the output file/directory
        check_deps: Whether to verify dependencies (default: True)
        check_code_ref: Whether to use code_ref for dep checking (default: True)
        use_mtime_cache: Whether to use mtime cache for output hash (default: True)
        info: Pre-parsed DVCFileInfo (avoids re-reading .dvc file if already parsed)

    Returns:
        Tuple of (is_fresh, reason)
    """
    if info is None:
        info = read_dvc_file(output_path)
    if info is None:
        return False, "no .dvc file"

    path = Path(output_path)
    if not path.exists():
        return False, "output missing"

    # Check output hash (with mtime cache optimization)
    if use_mtime_cache:
        from dvc.run.status import get_artifact_hash_cached

        try:
            current_md5, _, _was_cached = get_artifact_hash_cached(path, compute_md5)
        except (FileNotFoundError, ValueError) as e:
            return False, f"hash error: {e}"
    else:
        try:
            current_md5 = compute_md5(path)
        except (FileNotFoundError, ValueError) as e:
            return False, f"hash error: {e}"

    if current_md5 != info.md5:
        return False, f"output hash mismatch ({info.md5[:8]}... vs {current_md5[:8]}...)"

    # Check dependencies if requested
    if check_deps and info.deps:
        # First, try fast git blob comparison if we have code_ref
        if check_code_ref and info.code_ref:
            git_changed, changed_paths = have_deps_changed_since(info.deps, info.code_ref)
            if git_changed:
                return False, f"dep changed (git): {changed_paths[0]}"

        # For deps not in git (or if no code_ref), fall back to hash comparison
        for dep_path, recorded_md5 in info.deps.items():
            # Skip if we already checked via git and it was unchanged
            if check_code_ref and info.code_ref:
                git_result = has_file_changed_since(dep_path, info.code_ref)
                if git_result is False:
                    # Git says unchanged, trust it
                    continue
                if git_result is True:
                    # Already caught above, but just in case
                    return False, f"dep changed: {dep_path}"
                # git_result is None - file not in git, check hash

            dep = Path(dep_path)
            if not dep.exists():
                return False, f"dep missing: {dep_path}"

            # Use mtime cache for dep hash too
            if use_mtime_cache:
                from dvc.run.status import get_artifact_hash_cached

                try:
                    current_dep_md5, _, _ = get_artifact_hash_cached(dep, compute_md5)
                except (FileNotFoundError, ValueError) as e:
                    return False, f"dep hash error ({dep_path}): {e}"
            else:
                try:
                    current_dep_md5 = compute_md5(dep)
                except (FileNotFoundError, ValueError) as e:
                    return False, f"dep hash error ({dep_path}): {e}"

            if current_dep_md5 != recorded_md5:
                return False, f"dep changed: {dep_path}"

    return True, "up-to-date"


def get_dvc_file_path(output_path: Path) -> Path:
    """Get the .dvc file path for an output.

    Args:
        output_path: Path to the output file/directory

    Returns:
        Path to the .dvc file (may not exist)
    """
    return Path(str(output_path) + ".dvc")
