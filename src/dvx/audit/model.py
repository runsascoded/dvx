"""Data model for DVX blob audit and classification."""

from dataclasses import dataclass, field
from enum import Enum


class BlobKind(str, Enum):
    """Classification of a DVC-tracked blob."""
    INPUT = "input"           # dvx add'd, no computation
    GENERATED = "generated"   # dvx run output, has computation
    FOREIGN = "foreign"       # import-url --no-download, no md5
    ORPHANED = "orphaned"     # in cache but unreferenced


class Reproducibility(str, Enum):
    """Whether a generated blob can be reproduced from its inputs."""
    REPRODUCIBLE = "reproducible"
    NOT_REPRODUCIBLE = "not_reproducible"
    UNKNOWN = "unknown"


@dataclass
class BlobInfo:
    """Audit information for a single DVC-tracked blob."""
    path: str
    md5: str | None
    size: int
    kind: BlobKind
    reproducible: Reproducibility
    # Provenance
    cmd: str | None = None
    deps: dict[str, str] = field(default_factory=dict)
    git_deps: dict[str, str] = field(default_factory=dict)
    # Cache status
    in_local_cache: bool = False
    in_remote_cache: bool | None = None  # None = not checked
    # Directory metadata
    is_dir: bool = False
    nfiles: int | None = None

    def to_dict(self) -> dict:
        """Serialize to dict for JSON output."""
        d = {
            "path": self.path,
            "md5": self.md5,
            "size": self.size,
            "kind": self.kind.value,
            "reproducible": self.reproducible.value,
        }
        if self.cmd:
            d["cmd"] = self.cmd
        if self.deps:
            d["deps"] = self.deps
        if self.git_deps:
            d["git_deps"] = self.git_deps
        d["in_local_cache"] = self.in_local_cache
        if self.in_remote_cache is not None:
            d["in_remote_cache"] = self.in_remote_cache
        d["is_dir"] = self.is_dir
        if self.nfiles is not None:
            d["nfiles"] = self.nfiles
        return d


@dataclass
class AuditSummary:
    """Aggregated audit results for a workspace."""
    blobs: list[BlobInfo] = field(default_factory=list)

    @property
    def by_kind(self) -> dict[BlobKind, list[BlobInfo]]:
        result: dict[BlobKind, list[BlobInfo]] = {}
        for blob in self.blobs:
            result.setdefault(blob.kind, []).append(blob)
        return result

    @property
    def total_count(self) -> int:
        return len(self.blobs)

    @property
    def total_size(self) -> int:
        return sum(b.size for b in self.blobs)

    def count_by_kind(self, kind: BlobKind) -> int:
        return sum(1 for b in self.blobs if b.kind == kind)

    def size_by_kind(self, kind: BlobKind) -> int:
        return sum(b.size for b in self.blobs if b.kind == kind)

    @property
    def reproducible_count(self) -> int:
        return sum(
            1 for b in self.blobs
            if b.kind == BlobKind.GENERATED
            and b.reproducible == Reproducibility.REPRODUCIBLE
        )

    @property
    def cached_count(self) -> int:
        return sum(1 for b in self.blobs if b.in_local_cache)

    @property
    def cached_size(self) -> int:
        return sum(b.size for b in self.blobs if b.in_local_cache)

    @property
    def missing_count(self) -> int:
        return sum(1 for b in self.blobs if not b.in_local_cache)

    @property
    def missing_size(self) -> int:
        return sum(b.size for b in self.blobs if not b.in_local_cache)

    def to_dict(self) -> dict:
        """Serialize to dict for JSON output."""
        return {
            "blobs": [b.to_dict() for b in self.blobs],
            "summary": {
                "total": {"count": self.total_count, "size": self.total_size},
                "by_kind": {
                    kind.value: {
                        "count": self.count_by_kind(kind),
                        "size": self.size_by_kind(kind),
                    }
                    for kind in BlobKind
                    if self.count_by_kind(kind) > 0
                },
                "reproducible_count": self.reproducible_count,
                "cache": {
                    "cached": {"count": self.cached_count, "size": self.cached_size},
                    "missing": {"count": self.missing_count, "size": self.missing_size},
                },
            },
        }
