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

meta:
  computation:
    cmd: "python process.py --input data.csv"
    deps:
      data.csv: def456...
      process.py: 789abc...
```

Note: Computation info is stored in `meta.computation` for DVC compatibility
(DVC allows arbitrary data in `meta`, but rejects unknown top-level keys).
"""

import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from dvx.run.hash import compute_md5


# Simple schedule name → interval mapping
_SCHEDULE_INTERVALS: dict[str, timedelta] = {
    "hourly": timedelta(hours=1),
    "daily": timedelta(days=1),
    "weekly": timedelta(weeks=1),
}


def is_fetch_due(schedule: str, last_run: str | None, now: datetime | None = None) -> bool:
    """Check if a fetch schedule is due for execution.

    Args:
        schedule: Schedule string ("daily", "hourly", "weekly", cron expr, or "manual")
        last_run: ISO 8601 timestamp of last execution, or None if never run
        now: Current time (default: utcnow)

    Returns:
        True if the fetch should be re-executed
    """
    if schedule == "manual":
        return False

    if last_run is None:
        return True  # Never run → always due

    if now is None:
        now = datetime.now(timezone.utc)

    # Parse last_run (ISO 8601); handle Z suffix for Python 3.10 compat
    last = datetime.fromisoformat(last_run.replace("Z", "+00:00"))
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)

    # Simple interval schedules
    interval = _SCHEDULE_INTERVALS.get(schedule)
    if interval is not None:
        return now >= last + interval

    # Cron expression: find next fire time after last_run
    try:
        from croniter import croniter
        cron = croniter(schedule, last)
        next_fire = cron.get_next(datetime)
        if next_fire.tzinfo is None:
            next_fire = next_fire.replace(tzinfo=timezone.utc)
        return now >= next_fire
    except Exception:
        # If croniter not installed or invalid expression, treat as due
        return True


# Cache for git blob SHAs (keyed by (repo_path, ref))
_blob_cache: dict[tuple[str | None, str], dict[str, str]] = {}


def _resolve_dep_paths(deps: dict[str, str], dvc_dir: Path) -> dict[str, str]:
    """Resolve dep paths relative to a .dvc file's directory.

    Paths in .dvc files are relative to the .dvc file's directory.
    This converts them to repo-root-relative paths for internal use.

    - Relative paths (no leading /): resolved relative to dvc_dir
    - Leading /: repo-root-absolute (strip the /)
    - If dvc_dir is "." (repo root): paths are already repo-root-relative

    For backward compat: if a path already contains the dvc_dir prefix
    (e.g. ``njsp/data/crashes.parquet`` in ``njsp/data/refresh.dvc``),
    it's treated as repo-root-relative.
    """
    dvc_dir_str = str(dvc_dir)
    # Only resolve if dvc_dir is a relative subdirectory (not "." or absolute)
    if not deps or dvc_dir_str == "." or dvc_dir.is_absolute():
        return dict(deps)

    resolved = {}
    for dep_path, dep_hash in deps.items():
        if dep_path.startswith("/"):
            # Repo-root-absolute
            resolved[dep_path.lstrip("/")] = dep_hash
        elif dep_path.startswith(dvc_dir_str + "/") or dep_path.startswith(dvc_dir_str + os.sep):
            # Already repo-root-relative (backward compat)
            resolved[dep_path] = dep_hash
        else:
            # Relative to .dvc dir (normalize to remove ../  segments)
            resolved_path = os.path.normpath(str(Path(dvc_dir_str) / dep_path))
            resolved[resolved_path] = dep_hash
    return resolved


def _relativize_dep_paths(deps: dict[str, str], dvc_dir: Path) -> dict[str, str]:
    """Convert repo-root-relative dep paths to .dvc-dir-relative.

    Inverse of ``_resolve_dep_paths``. If dvc_dir is "." (repo root),
    paths are returned unchanged.
    """
    dvc_dir_str = str(dvc_dir)
    # Only relativize if dvc_dir is a relative subdirectory (not "." or absolute)
    if not deps or dvc_dir_str == "." or dvc_dir.is_absolute():
        return dict(deps)

    result = {}
    for dep_path, dep_hash in deps.items():
        if dep_path.startswith(dvc_dir_str + "/"):
            # Strip dvc_dir prefix to make relative
            result[dep_path[len(dvc_dir_str) + 1:]] = dep_hash
        else:
            # Outside dvc_dir — use os.path.relpath
            try:
                rel = os.path.relpath(dep_path, dvc_dir_str)
                result[rel] = dep_hash
            except ValueError:
                # Different drives on Windows, keep absolute
                result[dep_path] = dep_hash
    return result


def _get_blob_cache(ref: str = "HEAD", repo_path: Path | None = None) -> dict[str, str]:
    """Get or build the blob SHA cache for a git ref.

    Uses `git ls-tree -r` to get all blob SHAs in one call, which is
    ~50x faster than individual `git rev-parse` calls per file.
    """
    cache_key = (str(repo_path) if repo_path else None, ref)
    if cache_key in _blob_cache:
        return _blob_cache[cache_key]

    blob_map = {}
    try:
        result = subprocess.run(
            ["git", "ls-tree", "-r", "--format=%(objectname) %(path)", ref],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        for line in result.stdout.strip().split('\n'):
            if line:
                sha, path = line.split(' ', 1)
                blob_map[path] = sha
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    _blob_cache[cache_key] = blob_map
    return blob_map


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
    # Use batched cache for common refs (HEAD, commit SHAs)
    # This is ~50x faster than individual git rev-parse calls
    is_sha_like = ref.isalnum() and len(ref) in (7, 8, 12, 40)  # short or full SHA
    if ref == "HEAD" or is_sha_like:
        blob_map = _get_blob_cache(ref, repo_path)
        return blob_map.get(path)

    # Individual lookup for other refs
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


def get_git_object_sha(path: str, ref: str = "HEAD", repo_path: Path | None = None) -> str | None:
    """Get the git object SHA for a path at a specific ref.

    Works for both files (blob SHAs) and directories (tree SHAs).
    Checks the blob cache first for speed, then falls back to
    ``git rev-parse`` which handles both object types.

    Args:
        path: Path to the file or directory (relative to repo root)
        ref: Git ref (commit SHA, branch, tag, HEAD, etc.)
        repo_path: Path to git repository (default: current directory)

    Returns:
        Object SHA string, or None if path doesn't exist at that ref
    """
    # Strip trailing slash for consistency
    path = path.rstrip("/")

    # Try blob cache first (fast path for files)
    is_sha_like = ref.isalnum() and len(ref) in (7, 8, 12, 40)
    if ref == "HEAD" or is_sha_like:
        blob_map = _get_blob_cache(ref, repo_path)
        blob_sha = blob_map.get(path)
        if blob_sha is not None:
            return blob_sha

    # Fall back to git rev-parse (handles both blobs and trees)
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


def find_hash_commit(
    hash_value: str,
    file_path: str,
    repo_path: Path | None = None,
) -> str | None:
    """Find the commit that introduced a specific hash string in a file.

    Uses `git log -S` to find commits that added/removed the hash string.

    Args:
        hash_value: The hash string to search for
        file_path: Path to the file to search in
        repo_path: Path to git repository (default: current directory)

    Returns:
        Short commit SHA (e.g., "e7e0bbf") or None if not found
    """
    try:
        result = subprocess.run(
            ["git", "log", "-S", hash_value, "--format=%h", "-1", "--", file_path],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        sha = result.stdout.strip()
        return sha if sha else None
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


@dataclass
class DVCFileInfo:
    """Content of a .dvc file.

    For side-effect stages (cmd + deps, no outputs), md5 and size are None.
    """

    path: str
    md5: str | None = None
    size: int | None = None
    # Provenance via computation block (optional)
    cmd: str | None = None
    deps: dict[str, str] = field(default_factory=dict)  # {path: md5}
    git_deps: dict[str, str] = field(default_factory=dict)  # {path: blob_sha}
    # Directory metadata
    nfiles: int | None = None
    is_dir: bool = False
    # Git-tracked import (file in Git, not DVC cache)
    git_tracked: bool = False
    # Explicit side-effect flag (None = infer from md5/cmd)
    side_effect: bool | None = None
    # Fetch/cron schedule (e.g. "daily", "0 15 * * *", "manual")
    fetch_schedule: str | None = None
    fetch_last_run: str | None = None  # ISO 8601 timestamp
    # Legacy field for backward compatibility
    stage: str | None = None

    @property
    def is_side_effect(self) -> bool:
        """True if this is a side-effect stage (has computation but no outputs).

        Respects explicit ``side_effect`` field if set; otherwise inferred
        from having a cmd but no output hash.
        """
        if self.side_effect is not None:
            return self.side_effect
        return self.md5 is None and self.cmd is not None


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

    if not data:
        return None

    # Computation info lives in meta.computation for DVC compatibility
    # Also check top-level computation for backward compat with old DVX files
    meta = data.get("meta", {}) or {}
    computation = meta.get("computation", {}) or data.get("computation", {}) or {}

    has_outs = "outs" in data and data["outs"]

    # Check for explicit side_effect flag in computation block
    explicit_side_effect = computation.get("side_effect")
    if explicit_side_effect is not None:
        explicit_side_effect = bool(explicit_side_effect)

    # Fetch/cron schedule
    fetch = computation.get("fetch") or {}
    fetch_schedule = fetch.get("schedule")
    fetch_last_run = fetch.get("last_run")

    # Resolve dep/git_dep paths relative to .dvc file's directory
    dvc_dir = dvc_path.parent
    raw_deps = computation.get("deps") or {}
    raw_git_deps = computation.get("git_deps") or {}
    deps = _resolve_dep_paths(raw_deps, dvc_dir)
    git_deps = _resolve_dep_paths(raw_git_deps, dvc_dir)

    if not has_outs:
        # Side-effect stage: no outputs, but must have computation
        if not computation.get("cmd"):
            return None
        # Infer path from .dvc filename (strip .dvc suffix, preserve directory)
        inferred_path = str(dvc_path)[:-4] if str(dvc_path).endswith(".dvc") else str(dvc_path)
        return DVCFileInfo(
            path=inferred_path,
            # md5=None, size=None → side-effect stage
            cmd=computation.get("cmd"),
            deps=deps,
            git_deps=git_deps,
            side_effect=explicit_side_effect,
            fetch_schedule=fetch_schedule,
            fetch_last_run=fetch_last_run,
            stage=meta.get("stage"),
        )

    out = data["outs"][0]

    # Handle .dir suffix for directory hashes (DVC convention)
    md5_raw = out.get("md5", "")
    is_dir = md5_raw.endswith(".dir")
    md5 = md5_raw[:-4] if is_dir else md5_raw  # Strip .dir suffix

    return DVCFileInfo(
        path=out.get("path", str(output_path)),
        md5=md5,
        size=out.get("size", 0),
        # Provenance from computation block
        cmd=computation.get("cmd"),
        deps=deps,
        git_deps=git_deps,
        # Directory metadata
        nfiles=out.get("nfiles"),
        is_dir=is_dir,
        # Git-tracked import
        git_tracked=bool(meta.get("git_tracked")),
        side_effect=explicit_side_effect,
        fetch_schedule=fetch_schedule,
        fetch_last_run=fetch_last_run,
        stage=meta.get("stage"),  # Legacy only
    )


def write_dvc_file(
    output_path: Path,
    md5: str | None = None,
    size: int | None = None,
    cmd: str | None = None,
    deps: dict[str, str] | None = None,
    git_deps: dict[str, str] | None = None,
    nfiles: int | None = None,
    is_dir: bool | None = None,
    side_effect: bool | None = None,
    fetch_schedule: str | None = None,
    fetch_last_run: str | None = None,
    stage: str | None = None,  # noqa: ARG001 (legacy, deprecated)
) -> Path:
    """Write .dvc file for an output with provenance.

    Args:
        output_path: Path to the output file/directory
        md5: MD5 hash of the output (omitted from file if None - placeholder mode)
        size: Size in bytes (omitted from file if None - placeholder mode)
        cmd: Command that was run (provenance)
        deps: {dep_path: md5} of inputs (provenance)
        nfiles: Number of files (for directories)
        is_dir: Whether output is a directory (auto-detected if None)
        side_effect: Explicit side-effect flag (written to YAML if True)
        fetch_schedule: Cron/interval schedule (e.g. "daily", "0 15 * * *")
        fetch_last_run: ISO 8601 timestamp of last fetch execution
        stage: Deprecated, kept for backward compatibility

    Returns:
        Path to the created .dvc file
    """
    output_path = Path(output_path)
    dvc_path = Path(str(output_path) + ".dvc")

    # Ensure parent directory exists
    dvc_path.parent.mkdir(parents=True, exist_ok=True)

    # Side-effect stage: explicit flag, or inferred from cmd with no output hash
    if side_effect is not None:
        is_side_effect = side_effect
    else:
        is_side_effect = md5 is None and size is None and cmd is not None

    data = {}

    if not is_side_effect:
        # Auto-detect if directory
        if is_dir is None:
            is_dir = output_path.is_dir() if output_path.exists() else False

        # Path in .dvc file should be relative to the .dvc file location (just the filename)
        relative_path = output_path.name

        # Build out_entry, omitting md5/size if None (placeholder for prep phase)
        out_entry = {}
        if md5 is not None:
            # For directories, add .dir suffix to hash (DVC convention)
            out_entry["md5"] = f"{md5}.dir" if is_dir else md5
        if size is not None:
            out_entry["size"] = size
        # Always specify hash: md5 to use new-style cache (not legacy md5-dos2unix)
        out_entry["hash"] = "md5"

        # Add nfiles for directories (if we have the info)
        if is_dir:
            if nfiles is None and output_path.exists():
                # Count files in directory
                nfiles = sum(1 for f in output_path.rglob("*") if f.is_file())
            if nfiles is not None:
                out_entry["nfiles"] = nfiles

        out_entry["path"] = relative_path
        data["outs"] = [out_entry]

    # Add computation block inside meta for DVC compatibility
    # (DVC allows arbitrary data in meta, but rejects unknown top-level keys)
    if cmd or deps or git_deps or fetch_schedule:
        computation = {}
        if cmd:
            computation["cmd"] = cmd
        # Write dep paths relative to .dvc file's directory
        dvc_dir = dvc_path.parent
        if deps:
            computation["deps"] = _relativize_dep_paths(deps, dvc_dir)
        if git_deps:
            computation["git_deps"] = _relativize_dep_paths(git_deps, dvc_dir)
        if side_effect is True:
            computation["side_effect"] = True
        if fetch_schedule:
            fetch = {"schedule": fetch_schedule}
            if fetch_last_run:
                fetch["last_run"] = fetch_last_run
            computation["fetch"] = fetch
        data["meta"] = {"computation": computation}

    with open(dvc_path, "w") as f:
        yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    return dvc_path


def is_output_fresh(
    output_path: Path,
    check_deps: bool = True,
    use_mtime_cache: bool = True,
    info: DVCFileInfo | None = None,
) -> tuple[bool, str]:
    """Check if output is fresh (up-to-date with its .dvc file and deps).

    Freshness has two components:
    1. Data fresh: output hash matches .dvc file (uses mtime cache to avoid redundant hashing)
    2. Deps fresh: recorded dep hashes match dep's .dvc expected hashes

    Note: Dep checking compares our recorded dep hash against the dep's .dvc file,
    NOT against the dep's actual data. This mirrors git's model - each .dvc file
    declares what it expects, with no transitivity.

    Args:
        output_path: Path to the output file/directory
        check_deps: Whether to verify dependencies (default: True)
        use_mtime_cache: Whether to use mtime cache for output hash (default: True)
        info: Pre-parsed DVCFileInfo (avoids re-reading .dvc file if already parsed)

    Returns:
        Tuple of (is_fresh, reason)
    """
    if info is None:
        info = read_dvc_file(output_path)
    if info is None:
        return False, "no .dvc file"

    # Check fetch schedule staleness (before dep/output checks)
    if info.fetch_schedule and is_fetch_due(info.fetch_schedule, info.fetch_last_run):
        return False, "fetch schedule due"

    # Side-effect stages have no output to check — freshness is purely dep-based
    if not info.is_side_effect:
        path = Path(output_path)
        if not path.exists():
            return False, "output missing"

        # Check output hash (with mtime cache optimization)
        if use_mtime_cache:
            from dvx.run.status import get_artifact_hash_cached

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
            return False, f"data changed ({info.md5[:8]}... vs {current_md5[:8]}...)"

    # Check dependencies if requested
    # Compare recorded dep hashes against dep's .dvc file (not actual data)
    if check_deps and info.deps:
        for dep_path, recorded_md5 in info.deps.items():
            dep = Path(dep_path)
            # Try to read dep's .dvc file
            dep_info = read_dvc_file(dep)
            if dep_info is None:
                # No .dvc file - dep might be a raw file
                if not dep.exists():
                    return False, f"dep missing: {dep_path}"
                # Raw file exists — compute actual hash and compare
                try:
                    actual_md5 = compute_md5(dep)
                except (FileNotFoundError, ValueError) as e:
                    return False, f"dep hash error: {dep_path}: {e}"
                if actual_md5 != recorded_md5:
                    return False, f"dep changed: {dep_path}"
                continue

            # Compare our recorded hash against dep's .dvc expected hash
            if dep_info.md5 != recorded_md5:
                return False, f"dep changed: {dep_path}"

    # Check git dependencies if requested
    # Compare recorded SHAs against current HEAD (blob for files, tree for dirs)
    if check_deps and info.git_deps:
        for dep_path, recorded_sha in info.git_deps.items():
            current_sha = get_git_object_sha(dep_path, "HEAD")
            if current_sha is None:
                return False, f"git dep missing: {dep_path}"
            if current_sha != recorded_sha:
                return False, f"git dep changed: {dep_path}"

    return True, "up-to-date"


@dataclass
class FreshnessDetails:
    """Detailed information about artifact freshness."""
    fresh: bool
    reason: str
    output_expected: str | None = None         # Expected hash from .dvc
    output_expected_commit: str | None = None  # Commit that set this expected hash
    output_actual: str | None = None           # Actual hash on disk
    # {path: {expected, expected_commit, actual}}
    changed_deps: dict[str, dict[str, str | None]] | None = None


def get_freshness_details(
    output_path: Path,
    check_deps: bool = True,
    use_mtime_cache: bool = True,
    info: DVCFileInfo | None = None,
) -> FreshnessDetails:
    """Get detailed freshness info including before/after hashes.

    Similar to is_output_fresh, but returns structured details about
    what changed, useful for debugging and structured output.

    Freshness has two components:
    1. Data fresh: output hash matches .dvc file
    2. Deps fresh: recorded dep hashes match dep's .dvc expected hashes

    Args:
        output_path: Path to the output file/directory
        check_deps: Whether to verify dependencies (default: True)
        use_mtime_cache: Whether to use mtime cache for output hash (default: True)
        info: Pre-parsed DVCFileInfo (avoids re-reading .dvc file if already parsed)

    Returns:
        FreshnessDetails with structured info about freshness status
    """
    if info is None:
        info = read_dvc_file(output_path)
    if info is None:
        return FreshnessDetails(fresh=False, reason="no .dvc file")

    # Check fetch schedule staleness (before dep/output checks)
    if info.fetch_schedule and is_fetch_due(info.fetch_schedule, info.fetch_last_run):
        return FreshnessDetails(fresh=False, reason="fetch schedule due")

    path = Path(output_path)
    current_md5 = None

    # Side-effect stages have no output to check
    if info.is_side_effect:
        pass
    elif not path.exists():
        return FreshnessDetails(
            fresh=False,
            reason="output missing",
            output_expected=info.md5,
        )
    else:
        # Check output hash (with mtime cache optimization)
        if use_mtime_cache:
            from dvx.run.status import get_artifact_hash_cached
            try:
                current_md5, _, _ = get_artifact_hash_cached(path, compute_md5)
            except (FileNotFoundError, ValueError) as e:
                return FreshnessDetails(fresh=False, reason=f"hash error: {e}")
        else:
            try:
                current_md5 = compute_md5(path)
            except (FileNotFoundError, ValueError) as e:
                return FreshnessDetails(fresh=False, reason=f"hash error: {e}")

        if current_md5 != info.md5:
            return FreshnessDetails(
                fresh=False,
                reason="output hash mismatch",
                output_expected=info.md5,
                output_actual=current_md5,
            )

    # Check dependencies if requested
    # Compare recorded dep hashes against dep's .dvc file (not actual data)
    if check_deps and info.deps:
        changed_deps = {}

        for dep_path, recorded_md5 in info.deps.items():
            dep = Path(dep_path)
            # Try to read dep's .dvc file
            dep_info = read_dvc_file(dep)
            if dep_info is None:
                # No .dvc file - dep might be a raw file
                if not dep.exists():
                    changed_deps[dep_path] = {"expected": recorded_md5, "expected_commit": None, "actual": "(missing)"}
                    continue
                # Raw file exists — compute actual hash and compare
                try:
                    actual_md5 = compute_md5(dep)
                except (FileNotFoundError, ValueError):
                    changed_deps[dep_path] = {"expected": recorded_md5, "expected_commit": None, "actual": "(error)"}
                    continue
                if actual_md5 != recorded_md5:
                    changed_deps[dep_path] = {"expected": recorded_md5, "expected_commit": None, "actual": actual_md5}
                continue

            # Compare our recorded hash against dep's .dvc expected hash
            if dep_info.md5 != recorded_md5:
                changed_deps[dep_path] = {"expected": recorded_md5, "expected_commit": None, "actual": dep_info.md5}

        if changed_deps:
            # Look up commits that introduced each expected hash
            dvc_file_path = str(path) + ".dvc"
            for dep_path_key, dep_details in changed_deps.items():
                if dep_details["expected"] and dep_details["expected"] not in ("(missing)", "(error)"):
                    commit = find_hash_commit(dep_details["expected"], dvc_file_path)
                    dep_details["expected_commit"] = commit

            first_dep = next(iter(changed_deps))
            # Also look up commit for output expected hash
            output_expected_commit = find_hash_commit(info.md5, dvc_file_path) if info.md5 else None
            return FreshnessDetails(
                fresh=False,
                reason=f"dep changed: {first_dep}",
                output_expected=info.md5,
                output_expected_commit=output_expected_commit,
                output_actual=current_md5,
                changed_deps=changed_deps,
            )

    # Check git dependencies if requested
    # Compare recorded SHAs against current HEAD (blob for files, tree for dirs)
    if check_deps and info.git_deps:
        changed_deps = {}

        for dep_path, recorded_sha in info.git_deps.items():
            current_sha = get_git_object_sha(dep_path, "HEAD")
            if current_sha is None:
                changed_deps[dep_path] = {"expected": recorded_sha, "expected_commit": None, "actual": "(missing)"}
            elif current_sha != recorded_sha:
                changed_deps[dep_path] = {"expected": recorded_sha, "expected_commit": None, "actual": current_sha}

        if changed_deps:
            dvc_file_path = str(path) + ".dvc"
            for dep_path_key, dep_details in changed_deps.items():
                if dep_details["expected"] and dep_details["expected"] not in ("(missing)", "(error)"):
                    commit = find_hash_commit(dep_details["expected"], dvc_file_path)
                    dep_details["expected_commit"] = commit

            first_dep = next(iter(changed_deps))
            output_expected_commit = find_hash_commit(info.md5, dvc_file_path) if info.md5 else None
            return FreshnessDetails(
                fresh=False,
                reason=f"git dep changed: {first_dep}",
                output_expected=info.md5,
                output_expected_commit=output_expected_commit,
                output_actual=current_md5,
                changed_deps=changed_deps,
            )

    return FreshnessDetails(fresh=True, reason="up-to-date")


def get_dvc_file_path(output_path: Path) -> Path:
    """Get the .dvc file path for an output.

    Args:
        output_path: Path to the output file/directory

    Returns:
        Path to the .dvc file (may not exist)
    """
    return Path(str(output_path) + ".dvc")


def find_parent_dvc_dir(file_path: Path) -> tuple[Path, str] | None:
    """Find a DVC-tracked parent directory containing a file.

    Walks up the directory tree looking for a directory with a .dvc file.

    Args:
        file_path: Path to a file (must be a file, not directory)

    Returns:
        Tuple of (parent_dir_path, relative_path_from_parent) if found, None otherwise
    """
    path = Path(file_path)
    if not path.is_absolute():
        path = path.resolve()

    # Track relative path components as we walk up
    rel_parts = [path.name]
    current = path.parent

    while current != current.parent:  # Stop at filesystem root
        dvc_path = get_dvc_file_path(current)
        if dvc_path.exists():
            # Found a .dvc file for this directory
            info = read_dvc_file(current)
            if info and info.is_dir:
                # Return the directory path and relative path to the file
                relpath = "/".join(reversed(rel_parts))
                return current, relpath

        # Walk up one level
        rel_parts.append(current.name)
        current = current.parent

    return None


def read_dir_manifest(dir_md5: str, cache_dir: Path | None = None) -> dict[str, str]:
    """Read a DVC directory manifest and return file hashes.

    DVC stores directory contents as a JSON manifest in the cache with .dir suffix.

    Args:
        dir_md5: MD5 hash of the directory (without .dir suffix)
        cache_dir: Path to .dvc/cache/files/md5 directory (auto-detected if None)

    Returns:
        Dict mapping relative paths to their MD5 hashes
    """
    import json

    if cache_dir is None:
        # Auto-detect cache directory by walking up from cwd
        cwd = Path.cwd()
        for parent in [cwd, *cwd.parents]:
            potential_cache = parent / ".dvc" / "cache" / "files" / "md5"
            if potential_cache.exists():
                cache_dir = potential_cache
                break
        if cache_dir is None:
            return {}

    # DVC cache structure: .dvc/cache/files/md5/{first2}/{rest}.dir
    # Handle both cases: hash with or without .dir suffix
    if dir_md5.endswith(".dir"):
        hash_base = dir_md5[:-4]  # Strip .dir
    else:
        hash_base = dir_md5
    manifest_path = cache_dir / hash_base[:2] / f"{hash_base[2:]}.dir"
    if not manifest_path.exists():
        return {}

    with open(manifest_path) as f:
        entries = json.load(f)

    # Convert [{md5: ..., relpath: ...}, ...] to {relpath: md5}
    return {entry["relpath"]: entry["md5"] for entry in entries}


def get_file_hash_from_dir(
    file_path: Path,
    cache_dir: Path | None = None,
) -> tuple[str, Path] | None:
    """Get the MD5 hash of a file inside a DVC-tracked directory.

    Walks up the directory tree to find a parent .dvc-tracked directory,
    then reads the directory manifest to get the file's hash.

    Args:
        file_path: Path to the file
        cache_dir: Path to .dvc/cache/files/md5 directory (auto-detected if None)

    Returns:
        Tuple of (md5_hash, parent_dir_path) if found, None otherwise
    """
    result = find_parent_dvc_dir(file_path)
    if result is None:
        return None

    parent_dir, relpath = result
    info = read_dvc_file(parent_dir)
    if info is None or not info.md5:
        return None

    manifest = read_dir_manifest(info.md5, cache_dir)
    if relpath not in manifest:
        return None

    return manifest[relpath], parent_dir
