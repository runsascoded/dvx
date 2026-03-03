"""Scanning and classification logic for DVX blob audit."""

from pathlib import Path

from dvx.audit.model import (
    AuditSummary,
    BlobInfo,
    BlobKind,
    Reproducibility,
)
from dvx.cache import check_local_cache, find_dvc_files
from dvx.run.dvc_files import DVCFileInfo, read_dir_manifest, read_dvc_file


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


def _blob_info_from_dvc(
    info: DVCFileInfo,
    remote: str | None = None,
    check_remote: bool = False,
) -> BlobInfo:
    """Build a BlobInfo from a parsed DVCFileInfo."""
    kind, repro = classify_blob(info)

    in_local = False
    if info.md5:
        md5_for_cache = f"{info.md5}.dir" if info.is_dir else info.md5
        in_local = check_local_cache(md5_for_cache)

    in_remote: bool | None = None
    if check_remote and info.md5:
        from dvx.cache import check_remote_cache
        md5_for_cache = f"{info.md5}.dir" if info.is_dir else info.md5
        in_remote = check_remote_cache(md5_for_cache, remote)

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
) -> AuditSummary:
    """Scan workspace and classify all tracked blobs.

    Args:
        targets: Specific targets to scan (None = all .dvc files)
        remote: Remote name for cache checks
        check_remote: Whether to check remote cache status

    Returns:
        AuditSummary with classified blobs
    """
    dvc_files = find_dvc_files(targets)
    blobs: list[BlobInfo] = []

    for dvc_file in dvc_files:
        info = read_dvc_file(Path(dvc_file))
        if info is None:
            continue
        blob = _blob_info_from_dvc(info, remote=remote, check_remote=check_remote)
        blobs.append(blob)

    return AuditSummary(blobs=blobs)


def audit_artifact(
    path: str,
    remote: str | None = None,
    check_remote: bool = False,
) -> BlobInfo | None:
    """Get detailed audit info for a single artifact.

    Args:
        path: Path to the artifact (or its .dvc file)
        remote: Remote name for cache checks
        check_remote: Whether to check remote cache

    Returns:
        BlobInfo or None if not found
    """
    info = read_dvc_file(Path(path))
    if info is None:
        return None
    return _blob_info_from_dvc(info, remote=remote, check_remote=check_remote)


def find_orphans(root: Path | None = None) -> list[tuple[str, int]]:
    """Find cache blobs not referenced by any .dvc file.

    Walks .dvc/cache/files/md5/ and diffs against all hashes referenced
    by .dvc files (including directory manifest entries).

    Args:
        root: Repository root (auto-detected if None)

    Returns:
        List of (md5, size) for orphaned blobs
    """
    if root is None:
        from dvc.repo import Repo as DVCRepo
        try:
            root = Path(DVCRepo.find_root())
        except Exception:
            root = Path.cwd()

    cache_dir = root / ".dvc" / "cache" / "files" / "md5"
    if not cache_dir.exists():
        return []

    # Collect all hashes referenced by .dvc files
    referenced: set[str] = set()
    dvc_files = find_dvc_files()
    for dvc_file in dvc_files:
        info = read_dvc_file(Path(dvc_file))
        if info is None or not info.md5:
            continue
        referenced.add(info.md5)
        # Also add hashes from deps (they may be cached too)
        for dep_md5 in info.deps.values():
            referenced.add(dep_md5)
        # For directories, add all file hashes from manifest
        if info.is_dir:
            manifest = read_dir_manifest(info.md5, cache_dir)
            for file_md5 in manifest.values():
                referenced.add(file_md5)

    # Walk cache and find unreferenced blobs
    orphans: list[tuple[str, int]] = []
    for prefix_dir in sorted(cache_dir.iterdir()):
        if not prefix_dir.is_dir() or len(prefix_dir.name) != 2:
            continue
        for cache_file in sorted(prefix_dir.iterdir()):
            if not cache_file.is_file():
                continue
            # Reconstruct md5 from path
            name = cache_file.name
            is_dir_manifest = name.endswith(".dir")
            if is_dir_manifest:
                name = name[:-4]
            md5 = prefix_dir.name + name
            if md5 not in referenced:
                orphans.append((md5, cache_file.stat().st_size))

    return orphans
