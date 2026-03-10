"""Scanning and classification logic for DVX blob audit."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dvx.audit.model import (
    AuditSummary,
    BlobInfo,
    BlobKind,
    Reproducibility,
)
from dvx.run.dvc_files import DVCFileInfo

if TYPE_CHECKING:
    from dvx.audit.repo_view import RepoView


def classify_blob(info: DVCFileInfo) -> tuple[BlobKind, Reproducibility]:
    """Classify a blob by its DVCFileInfo.

    Returns:
        (kind, reproducibility) tuple
    """
    if not info.md5:
        return BlobKind.FOREIGN, Reproducibility.UNKNOWN
    if info.cmd:
        # Generated blob — check reproducibility
        if info.reproducible is False:
            repro = Reproducibility.NOT_REPRODUCIBLE
        elif info.reproducible is True:
            repro = Reproducibility.REPRODUCIBLE
        else:
            # Default: blobs with computation are assumed reproducible
            repro = Reproducibility.REPRODUCIBLE
        return BlobKind.GENERATED, repro
    return BlobKind.INPUT, Reproducibility.UNKNOWN


def _default_view() -> RepoView:
    from dvx.audit.repo_view import FilesystemRepoView
    return FilesystemRepoView()


def _blob_info_from_dvc(
    info: DVCFileInfo,
    view: RepoView,
    check_remote: bool = False,
    remote: str | None = None,
) -> BlobInfo:
    """Build a BlobInfo from a parsed DVCFileInfo."""
    kind, repro = classify_blob(info)

    in_local = False
    if info.md5:
        md5_for_cache = f"{info.md5}.dir" if info.is_dir else info.md5
        in_local = view.is_cached(md5_for_cache)

    in_remote: bool | None = None
    if check_remote and info.md5:
        md5_for_cache = f"{info.md5}.dir" if info.is_dir else info.md5
        in_remote = view.is_remote_cached(md5_for_cache, remote)

    return BlobInfo(
        path=info.path,
        md5=info.md5 or None,
        size=info.size,
        kind=kind,
        reproducible=repro,
        cmd=info.cmd,
        deps=info.deps,
        git_deps=info.git_deps,
        in_local_cache=in_local,
        in_remote_cache=in_remote,
        is_dir=info.is_dir,
        nfiles=info.nfiles,
    )


def scan_workspace(
    targets: list[str] | None = None,
    remote: str | None = None,
    check_remote: bool = False,
    view: RepoView | None = None,
) -> AuditSummary:
    """Scan workspace and classify all tracked blobs.

    Args:
        targets: Specific targets to scan (None = all .dvc files)
        remote: Remote name for cache checks
        check_remote: Whether to check remote cache status
        view: RepoView to use (default: FilesystemRepoView)

    Returns:
        AuditSummary with classified blobs
    """
    if view is None:
        view = _default_view()

    dvc_files = view.dvc_files(targets)
    blobs: list[BlobInfo] = []

    for dvc_file in dvc_files:
        info = view.read_dvc(dvc_file)
        if info is None:
            continue
        blob = _blob_info_from_dvc(info, view=view, check_remote=check_remote, remote=remote)
        blobs.append(blob)

    return AuditSummary(blobs=blobs)


def audit_artifact(
    path: str,
    remote: str | None = None,
    check_remote: bool = False,
    view: RepoView | None = None,
) -> BlobInfo | None:
    """Get detailed audit info for a single artifact.

    Args:
        path: Path to the artifact (or its .dvc file)
        remote: Remote name for cache checks
        check_remote: Whether to check remote cache
        view: RepoView to use (default: FilesystemRepoView)

    Returns:
        BlobInfo or None if not found
    """
    if view is None:
        view = _default_view()

    info = view.read_dvc(path)
    if info is None:
        return None
    return _blob_info_from_dvc(info, view=view, check_remote=check_remote, remote=remote)


def find_orphans(
    root: Path | None = None,
    view: RepoView | None = None,
) -> list[tuple[str, int]]:
    """Find cache blobs not referenced by any .dvc file.

    Walks the cache and diffs against all hashes referenced
    by .dvc files (including directory manifest entries).

    Args:
        root: Repository root (ignored if view is provided)
        view: RepoView to use (default: FilesystemRepoView)

    Returns:
        List of (md5, size) for orphaned blobs
    """
    if view is None:
        view = _default_view()

    # Collect all hashes referenced by .dvc files
    referenced: set[str] = set()
    dvc_files = view.dvc_files()
    for dvc_file in dvc_files:
        info = view.read_dvc(dvc_file)
        if info is None or not info.md5:
            continue
        referenced.add(info.md5)
        # Also add hashes from deps (they may be cached too)
        for dep_md5 in info.deps.values():
            referenced.add(dep_md5)
        # For directories, add all file hashes from manifest
        if info.is_dir:
            manifest = view.dir_manifest(info.md5)
            for file_md5 in manifest.values():
                referenced.add(file_md5)

    # Diff cache against referenced set
    orphans: list[tuple[str, int]] = []
    for md5, size in view.cache_entries():
        if md5 not in referenced:
            orphans.append((md5, size))

    return orphans
