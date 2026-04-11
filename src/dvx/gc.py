"""Cache GC with version-aware retention.

Lists historical versions of artifacts by walking git log, then
applies retention policies (keep N, older-than) to determine which
cached blobs to prune.
"""

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass
class ArtifactVersion:
    """A historical version of a DVX-tracked artifact."""

    md5: str
    commit_sha: str
    commit_date: datetime
    dvc_path: str


def get_artifact_versions(
    dvc_path: str,
    refs: list[str] | None = None,
    repo_path: Path | None = None,
) -> list[ArtifactVersion]:
    """List all historical versions of an artifact from git log.

    Walks the git log for the .dvc file, extracts the md5 hash at each
    commit that changed the file.

    Args:
        dvc_path: Path to the .dvc file (relative to repo root)
        refs: Git refs to search (default: HEAD). Pass branch names for
            --all-branches.
        repo_path: Path to repo root (default: cwd)

    Returns:
        List of ArtifactVersion, newest first
    """
    if refs is None:
        refs = ["HEAD"]

    versions: dict[str, ArtifactVersion] = {}  # md5 → version (dedup)

    for ref in refs:
        try:
            result = subprocess.run(
                [
                    "git", "log", ref,
                    "--format=%H %aI",
                    "-p", "--", dvc_path,
                ],
                cwd=repo_path,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError:
            continue

        current_sha = None
        current_date = None

        for line in result.stdout.split("\n"):
            # Commit header line: "SHA ISO_DATE"
            if re.match(r"^[0-9a-f]{40} ", line):
                parts = line.split(" ", 1)
                current_sha = parts[0]
                date_str = parts[1].strip()
                current_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                if current_date.tzinfo is None:
                    current_date = current_date.replace(tzinfo=timezone.utc)

            # Diff line adding an md5 hash
            elif line.startswith("+") and "md5:" in line and current_sha:
                md5_match = re.search(r"md5:\s*([0-9a-f]{32})", line)
                if md5_match:
                    md5 = md5_match.group(1)
                    # Keep the earliest commit that introduced this hash
                    if md5 not in versions or current_date < versions[md5].commit_date:
                        versions[md5] = ArtifactVersion(
                            md5=md5,
                            commit_sha=current_sha,
                            commit_date=current_date,
                            dvc_path=dvc_path,
                        )

    # Sort newest first
    return sorted(versions.values(), key=lambda v: v.commit_date, reverse=True)


def get_referenced_hashes(
    refs: list[str] | None = None,
    repo_path: Path | None = None,
) -> set[str]:
    """Get all md5 hashes referenced by .dvc files at given refs.

    Args:
        refs: Git refs to check (default: HEAD)
        repo_path: Path to repo root

    Returns:
        Set of md5 hashes currently referenced
    """
    if refs is None:
        refs = ["HEAD"]

    hashes = set()
    for ref in refs:
        try:
            # List all .dvc files at this ref
            result = subprocess.run(
                ["git", "ls-tree", "-r", "--name-only", ref],
                cwd=repo_path,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError:
            continue

        dvc_files = [
            f for f in result.stdout.strip().split("\n")
            if f.endswith(".dvc") and ".dvc/" not in f
        ]

        for dvc_file in dvc_files:
            try:
                content = subprocess.run(
                    ["git", "show", f"{ref}:{dvc_file}"],
                    cwd=repo_path,
                    capture_output=True,
                    text=True,
                    check=True,
                )
                for match in re.finditer(r"md5:\s*([0-9a-f]{32})", content.stdout):
                    hashes.add(match.group(1))
            except subprocess.CalledProcessError:
                continue

    return hashes


def get_local_branches(repo_path: Path | None = None) -> list[str]:
    """Get all local branch names."""
    try:
        result = subprocess.run(
            ["git", "branch", "--format=%(refname:short)"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return [b.strip() for b in result.stdout.strip().split("\n") if b.strip()]
    except subprocess.CalledProcessError:
        return ["HEAD"]


def list_cache_blobs(repo_path: Path | None = None) -> list[tuple[str, int, Path]]:
    """List all blobs in the local DVC cache.

    Returns:
        List of (md5, size_bytes, path) for each cached blob
    """
    if repo_path is None:
        repo_path = Path.cwd()

    cache_dir = repo_path / ".dvc" / "cache" / "files" / "md5"
    if not cache_dir.exists():
        return []

    blobs = []
    for prefix_dir in cache_dir.iterdir():
        if not prefix_dir.is_dir() or len(prefix_dir.name) != 2:
            continue
        for blob_file in prefix_dir.iterdir():
            if blob_file.is_file():
                md5 = prefix_dir.name + blob_file.stem
                size = blob_file.stat().st_size
                blobs.append((md5, size, blob_file))
    return blobs


def parse_duration(s: str) -> timedelta:
    """Parse a duration string like '30d', '7d', '24h', '1w'.

    Raises ValueError if format is unrecognized.
    """
    match = re.match(r"^(\d+)([dhw])$", s)
    if not match:
        raise ValueError(f"Invalid duration: {s!r} (expected e.g. '30d', '24h', '1w')")

    n = int(match.group(1))
    unit = match.group(2)
    if unit == "d":
        return timedelta(days=n)
    elif unit == "h":
        return timedelta(hours=n)
    elif unit == "w":
        return timedelta(weeks=n)
    raise ValueError(f"Unknown unit: {unit}")


def compute_gc_plan(
    keep: int | None = None,
    older_than: str | None = None,
    all_branches: bool = False,
    targets: list[str] | None = None,
    repo_path: Path | None = None,
) -> tuple[set[str], set[str], list[tuple[str, int, Path]]]:
    """Compute which cache blobs to keep and delete.

    Args:
        keep: Keep the N most recent versions per artifact
        older_than: Delete versions older than this duration
        all_branches: Consider all local branches (not just HEAD)
        targets: Specific .dvc files to GC (default: all)
        repo_path: Repo root path

    Returns:
        (keep_hashes, delete_hashes, deletable_blobs)
        where deletable_blobs is [(md5, size, path), ...]
    """
    if repo_path is None:
        repo_path = Path.cwd()

    # Determine refs to consider
    if all_branches:
        refs = get_local_branches(repo_path)
    else:
        refs = ["HEAD"]

    # Always keep hashes referenced by current refs
    keep_hashes = get_referenced_hashes(refs, repo_path)

    # Find all .dvc files to process
    if targets:
        dvc_files = []
        for t in targets:
            if t.endswith(".dvc"):
                dvc_files.append(t)
            else:
                dvc_files.append(t + ".dvc")
    else:
        # All .dvc files in repo
        try:
            result = subprocess.run(
                ["git", "ls-tree", "-r", "--name-only", "HEAD"],
                cwd=repo_path,
                capture_output=True,
                text=True,
                check=True,
            )
            dvc_files = [
                f for f in result.stdout.strip().split("\n")
                if f.endswith(".dvc") and ".dvc/" not in f
            ]
        except subprocess.CalledProcessError:
            dvc_files = []

    # Apply retention policies
    if keep is not None or older_than is not None:
        now = datetime.now(timezone.utc)
        older_than_td = parse_duration(older_than) if older_than else None

        for dvc_file in dvc_files:
            versions = get_artifact_versions(dvc_file, refs, repo_path)

            for i, v in enumerate(versions):
                should_keep = False

                # --keep N: keep the N newest
                if keep is not None and i < keep:
                    should_keep = True

                # --older-than: keep if newer than cutoff
                if older_than_td is not None and (now - v.commit_date) <= older_than_td:
                    should_keep = True

                if should_keep:
                    keep_hashes.add(v.md5)

    # Find deletable blobs
    all_blobs = list_cache_blobs(repo_path)
    deletable = [(md5, size, path) for md5, size, path in all_blobs if md5 not in keep_hashes]

    delete_hashes = {md5 for md5, _, _ in deletable}

    return keep_hashes, delete_hashes, deletable


def format_size(size: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{size} {unit}"
        size /= 1024
    return f"{size:.1f} PB"
