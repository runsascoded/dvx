"""Python API for constructing lazy pipeline representations.

This module provides a Dask-inspired API for building computation graphs
that can be serialized to .dvc files and executed by DVX.

Example usage:
    from dvx.run.artifact import Artifact, Computation

    def normalized_month(ym: str) -> Artifact:
        return Artifact(
            path=f"s3/ctbk/normalized/{ym}",
            computation=Computation(
                cmd=f"ctbk normalized create {ym}",
                deps=[tripdata_zip(ym)],
            )
        )

    # Generate .dvc files for a range
    for ym in ["202501", "202502", "202503"]:
        normalized_month(ym).write_dvc()

The `delayed` decorator enables lazy function composition:

    @delayed
    def normalize(ym: str, src: Artifact) -> Artifact:
        return Artifact(
            path=f"normalized/{ym}",
            computation=Computation(cmd=f"ctbk norm {ym}", deps=[src])
        )

    # Build lazy graph (no execution yet)
    artifacts = [normalize(ym, raw) for ym, raw in ...]

    # Option A: Write .dvc files only (prep phase)
    for a in artifacts:
        a.write_dvc()

    # Option B: Write and execute (prep + run)
    materialize(artifacts, parallel=4)
"""

from __future__ import annotations

import functools
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable, TypeVar

from dvx.run.dvc_files import (
    get_git_head_sha,
    read_dvc_file,
    write_dvc_file,
)
from dvx.run.hash import compute_md5, compute_file_size

if TYPE_CHECKING:
    from typing import Any


@dataclass
class Computation:
    """Represents how an artifact was (or will be) produced.

    Attributes:
        cmd: Shell command to produce the artifact
        deps: List of input dependencies (Artifacts or paths)
        params: Optional parameters for inspection/debugging
        code_ref: Git SHA when computation was run (set automatically)
    """

    cmd: str
    deps: list[Artifact | str | Path] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    code_ref: str | None = None

    def get_dep_paths(self) -> list[Path]:
        """Get paths of all dependencies."""
        paths = []
        for dep in self.deps:
            if isinstance(dep, Artifact):
                paths.append(Path(dep.path))
            else:
                paths.append(Path(dep))
        return paths

    def get_dep_hashes(self) -> dict[str, str]:
        """Compute MD5 hashes for all dependencies.

        Returns:
            Dict mapping path strings to MD5 hashes
        """
        hashes = {}
        for dep in self.deps:
            if isinstance(dep, Artifact):
                path = Path(dep.path)
                # If artifact has a known hash, use it
                if dep.md5:
                    hashes[str(path)] = dep.md5
                elif path.exists():
                    hashes[str(path)] = compute_md5(path)
            else:
                path = Path(dep)
                if path.exists():
                    hashes[str(path)] = compute_md5(path)
        return hashes


@dataclass
class Artifact:
    """Represents a data artifact with optional provenance.

    An Artifact can be:
    - A leaf node: existing data with no computation (e.g., imported data)
    - A computed node: output of a computation with dependencies

    Attributes:
        path: Path to the artifact (relative or absolute)
        computation: Optional Computation that produces this artifact
        md5: MD5 hash of the artifact (None if not yet computed)
        size: Size in bytes (None if not yet computed)
    """

    path: str | Path
    computation: Computation | None = None
    md5: str | None = None
    size: int | None = None

    def __post_init__(self):
        """Normalize path to string."""
        self.path = str(self.path)

    @classmethod
    def from_path(cls, path: str | Path) -> Artifact:
        """Create an Artifact from an existing file.

        Reads the file to compute its hash and size.

        Args:
            path: Path to existing file

        Returns:
            Artifact with md5 and size populated
        """
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Artifact not found: {path}")

        return cls(
            path=str(path),
            md5=compute_md5(p),
            size=compute_file_size(p),
        )

    @classmethod
    def from_dvc(cls, path: str | Path) -> Artifact | None:
        """Load an Artifact from its .dvc file.

        Args:
            path: Path to the artifact (not the .dvc file)

        Returns:
            Artifact with all metadata populated, or None if no .dvc file
        """
        info = read_dvc_file(Path(path))
        if info is None:
            return None

        computation = None
        if info.cmd or info.deps:
            # Convert deps dict to Artifact objects
            deps = [
                Artifact(path=dep_path, md5=dep_md5)
                for dep_path, dep_md5 in info.deps.items()
            ]
            computation = Computation(
                cmd=info.cmd or "",
                deps=deps,
                code_ref=info.code_ref,
            )

        return cls(
            path=info.path,
            md5=info.md5,
            size=info.size,
            computation=computation,
        )

    def write_dvc(self, capture_code_ref: bool = True) -> Path:
        """Write this artifact's .dvc file.

        This is the "prep" phase - generates the .dvc file without
        executing the computation. The output hash will be computed
        from the current file if it exists.

        Args:
            capture_code_ref: Whether to capture current git HEAD as code_ref

        Returns:
            Path to the created .dvc file
        """
        path = Path(self.path)

        # Compute output hash if file exists and we don't have it
        md5 = self.md5
        size = self.size
        if md5 is None and path.exists():
            md5 = compute_md5(path)
            size = compute_file_size(path)

        # If still no hash, use placeholder (file will be created during run)
        if md5 is None:
            md5 = ""
            size = 0

        # Get computation metadata
        cmd = None
        code_ref = None
        deps_hashes = None

        if self.computation:
            cmd = self.computation.cmd
            deps_hashes = self.computation.get_dep_hashes()

            if capture_code_ref:
                code_ref = self.computation.code_ref or get_git_head_sha()

        return write_dvc_file(
            output_path=path,
            md5=md5,
            size=size,
            cmd=cmd,
            code_ref=code_ref,
            deps=deps_hashes,
        )

    def is_computed(self) -> bool:
        """Check if this artifact has been computed (has a valid hash)."""
        return bool(self.md5)

    def exists(self) -> bool:
        """Check if the artifact file exists on disk."""
        return Path(self.path).exists()

    def get_upstream(self) -> list[Artifact]:
        """Get all upstream Artifact dependencies.

        Returns only Artifact objects, not string/Path deps.
        """
        if not self.computation:
            return []
        return [d for d in self.computation.deps if isinstance(d, Artifact)]

    def walk_upstream(self) -> list[Artifact]:
        """Recursively collect all upstream Artifacts.

        Returns artifacts in dependency order (leaves first).
        """
        visited = set()
        result = []

        def visit(artifact: Artifact):
            if artifact.path in visited:
                return
            visited.add(artifact.path)
            for upstream in artifact.get_upstream():
                visit(upstream)
            result.append(artifact)

        visit(self)
        return result

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        if not isinstance(other, Artifact):
            return False
        return self.path == other.path


# Type variable for delayed decorator
F = TypeVar("F", bound=Callable[..., Artifact])


def delayed(fn: F) -> F:
    """Decorator for lazy artifact construction.

    Wraps a function that returns an Artifact, allowing it to be
    composed into a lazy computation graph.

    The wrapped function behaves identically to the original,
    but signals intent for lazy evaluation when used with materialize().

    Example:
        @delayed
        def normalize(ym: str, src: Artifact) -> Artifact:
            return Artifact(
                path=f"normalized/{ym}",
                computation=Computation(cmd=f"ctbk norm {ym}", deps=[src])
            )

        # Build graph (no execution)
        result = normalize("202501", raw_data)

        # Execute
        materialize([result])
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs) -> Artifact:
        return fn(*args, **kwargs)

    # Mark as delayed for introspection
    wrapper._dvx_delayed = True  # type: ignore
    return wrapper  # type: ignore


def write_all_dvc(artifacts: list[Artifact], capture_code_ref: bool = True) -> list[Path]:
    """Write .dvc files for all artifacts in dependency order.

    Args:
        artifacts: List of artifacts to write
        capture_code_ref: Whether to capture git HEAD as code_ref

    Returns:
        List of paths to created .dvc files
    """
    # Collect all artifacts including upstream dependencies
    all_artifacts = []
    seen = set()

    for artifact in artifacts:
        for a in artifact.walk_upstream():
            if a.path not in seen:
                seen.add(a.path)
                all_artifacts.append(a)

    # Write .dvc files in dependency order (leaves first)
    paths = []
    for artifact in all_artifacts:
        if artifact.computation:  # Only write computed artifacts
            paths.append(artifact.write_dvc(capture_code_ref))

    return paths


def materialize(
    artifacts: list[Artifact],
    parallel: int = 1,
    force: bool = False,
) -> list[Artifact]:
    """Execute computations for all stale artifacts.

    This is the "run" phase - executes pending computations and
    updates .dvc files with output hashes.

    Args:
        artifacts: List of artifacts to materialize
        parallel: Number of parallel workers (default: 1)
        force: Force recomputation even if fresh (default: False)

    Returns:
        List of artifacts that were computed
    """
    from dvx.run.dvc_files import is_output_fresh

    # Collect all artifacts in dependency order
    all_artifacts = []
    seen = set()

    for artifact in artifacts:
        for a in artifact.walk_upstream():
            if a.path not in seen:
                seen.add(a.path)
                all_artifacts.append(a)

    computed = []

    # TODO: Implement parallel execution
    for artifact in all_artifacts:
        if not artifact.computation:
            continue  # Leaf node, nothing to compute

        path = Path(artifact.path)

        # Check if already fresh
        if not force:
            fresh, reason = is_output_fresh(path)
            if fresh:
                continue

        # Write .dvc file first (prep)
        artifact.write_dvc()

        # Execute computation
        import subprocess

        cmd = artifact.computation.cmd
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(
                f"Computation failed for {artifact.path}:\n"
                f"Command: {cmd}\n"
                f"Stderr: {result.stderr}"
            )

        # Update artifact with computed hash
        if path.exists():
            artifact.md5 = compute_md5(path)
            artifact.size = compute_file_size(path)
            # Rewrite .dvc file with actual hash
            artifact.write_dvc()

        computed.append(artifact)

    return computed
