"""Abstraction over repo I/O for testable audit logic.

A RepoView provides the minimal interface that audit/scan needs:
listing .dvc files, reading their contents, and checking cache status.

Two implementations:
- FilesystemRepoView: reads from an actual repo on disk (production)
- SnapshotRepoView: reads from a JSON snapshot (testing)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from dvx.run.dvc_files import DVCFileInfo


class RepoView(Protocol):
    """Minimal repo interface for audit operations."""

    def dvc_files(self, targets: list[str] | None = None) -> list[str]:
        """List .dvc file paths (relative to repo root)."""
        ...

    def read_dvc(self, path: str) -> DVCFileInfo | None:
        """Read and parse a .dvc file."""
        ...

    def is_cached(self, md5: str) -> bool:
        """Check if md5 is in local cache."""
        ...

    def is_remote_cached(self, md5: str, remote: str | None = None) -> bool:
        """Check if md5 is in remote cache."""
        ...

    def cache_entries(self) -> list[tuple[str, int]]:
        """List all (md5, size) in local cache."""
        ...

    def dir_manifest(self, md5: str) -> dict[str, str]:
        """Read directory manifest: {relative_path: md5}."""
        ...


class FilesystemRepoView:
    """RepoView backed by an actual filesystem repo."""

    def __init__(self, root: Path | None = None):
        if root is None:
            from dvc.repo import Repo as DVCRepo
            try:
                root = Path(DVCRepo.find_root())
            except Exception:
                root = Path.cwd()
        self.root = root

    def dvc_files(self, targets: list[str] | None = None) -> list[str]:
        from dvx.cache import find_dvc_files
        return find_dvc_files(targets)

    def read_dvc(self, path: str) -> DVCFileInfo | None:
        from dvx.run.dvc_files import read_dvc_file
        return read_dvc_file(Path(path))

    def is_cached(self, md5: str) -> bool:
        from dvx.cache import check_local_cache
        return check_local_cache(md5)

    def is_remote_cached(self, md5: str, remote: str | None = None) -> bool:
        from dvx.cache import check_remote_cache
        return check_remote_cache(md5, remote)

    def cache_entries(self) -> list[tuple[str, int]]:
        cache_dir = self.root / ".dvc" / "cache" / "files" / "md5"
        if not cache_dir.exists():
            return []
        entries = []
        for prefix_dir in sorted(cache_dir.iterdir()):
            if not prefix_dir.is_dir() or len(prefix_dir.name) != 2:
                continue
            for cache_file in sorted(prefix_dir.iterdir()):
                if not cache_file.is_file():
                    continue
                name = cache_file.name
                is_dir_manifest = name.endswith(".dir")
                if is_dir_manifest:
                    name = name[:-4]
                md5 = prefix_dir.name + name
                entries.append((md5, cache_file.stat().st_size))
        return entries

    def dir_manifest(self, md5: str) -> dict[str, str]:
        from dvx.run.dvc_files import read_dir_manifest
        cache_dir = self.root / ".dvc" / "cache" / "files" / "md5"
        return read_dir_manifest(md5, cache_dir)


@dataclass
class SnapshotRepoView:
    """RepoView backed by a JSON snapshot for testing.

    Load from the snapshot files captured by audit tooling:
    - dvc-contents.json: array of {file, md5, size, path, deps, meta, ...}
    - cache-files.txt: "SIZE .dvc/cache/files/md5/XX/YYYYYY" per line

    Usage:
        view = SnapshotRepoView.load(Path("tmp/crashes-snapshot"))
        summary = scan_workspace(view=view)
    """

    entries: list[dict] = field(default_factory=list)
    cache: dict[str, int] = field(default_factory=dict)  # {md5: size}
    remote_cache: set[str] = field(default_factory=set)
    manifests: dict[str, dict[str, str]] = field(default_factory=dict)  # {dir_md5: {path: md5}}

    @classmethod
    def load(cls, snapshot_dir: Path) -> SnapshotRepoView:
        """Load from a snapshot directory."""
        entries = []
        contents_path = snapshot_dir / "dvc-contents.json"
        if contents_path.exists():
            entries = json.loads(contents_path.read_text())

        cache: dict[str, int] = {}
        cache_path = snapshot_dir / "cache-files.txt"
        if cache_path.exists():
            for line in cache_path.read_text().splitlines():
                parts = line.strip().split()
                if len(parts) != 2:
                    continue
                size_str, path = parts
                segs = path.split("/")
                if len(segs) >= 6:
                    md5 = segs[4] + segs[5]
                    cache[md5] = int(size_str)

        return cls(entries=entries, cache=cache)

    def dvc_files(self, targets: list[str] | None = None) -> list[str]:
        files = [e["file"] for e in self.entries if "file" in e]
        if targets:
            target_set = set(targets)
            # Support matching by .dvc path or by output path
            files = [
                f for f in files
                if f in target_set or f.removesuffix(".dvc") in target_set
            ]
        return files

    def read_dvc(self, path: str) -> DVCFileInfo | None:
        for e in self.entries:
            if e.get("file") == path:
                return self._entry_to_info(e)
        return None

    def is_cached(self, md5: str) -> bool:
        clean = md5.removesuffix(".dir")
        return clean in self.cache

    def is_remote_cached(self, md5: str, remote: str | None = None) -> bool:
        clean = md5.removesuffix(".dir")
        return clean in self.remote_cache

    def cache_entries(self) -> list[tuple[str, int]]:
        return list(self.cache.items())

    def dir_manifest(self, md5: str) -> dict[str, str]:
        return self.manifests.get(md5, {})

    @staticmethod
    def _entry_to_info(e: dict) -> DVCFileInfo:
        meta = e.get("meta", {})
        computation = meta.get("computation", {})
        md5_raw = e.get("md5", "")
        is_dir = md5_raw.endswith(".dir")
        md5 = md5_raw[:-4] if is_dir else md5_raw
        return DVCFileInfo(
            path=e.get("path", ""),
            md5=md5,
            size=e.get("size", 0),
            cmd=computation.get("cmd"),
            deps=computation.get("deps") or {},
            git_deps=computation.get("git_deps") or {},
            nfiles=e.get("nfiles"),
            is_dir=is_dir,
            reproducible=meta.get("reproducible"),
            git_tracked=bool(meta.get("git_tracked")),
        )
