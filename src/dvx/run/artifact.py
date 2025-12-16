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
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from dvx.run.dvc_files import (
    get_file_hash_from_dir,
    get_git_head_sha,
    read_dvc_file,
    write_dvc_file,
)
from dvx.run.hash import compute_file_size, compute_md5

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

        Supports both direct .dvc files and files inside DVC-tracked directories.
        For files inside tracked directories, walks up the tree to find the parent
        .dvc file and looks up the file hash from the directory manifest.

        Args:
            path: Path to the artifact (not the .dvc file)

        Returns:
            Artifact with all metadata populated, or None if no .dvc file found
        """
        path = Path(path)
        info = read_dvc_file(path)

        if info is None:
            # No direct .dvc file - check if this is a file inside a tracked directory
            result = get_file_hash_from_dir(path)
            if result is None:
                return None

            file_hash, _parent_dir = result
            # Return artifact with just the file hash (no computation - that's on the parent)
            return cls(
                path=str(path),
                md5=file_hash,
                size=None,  # Size not stored in directory manifests
            )

        computation = None
        if info.cmd or info.deps:
            # Convert deps dict to Artifact objects
            deps = [Artifact(path=dep_path, md5=dep_md5) for dep_path, dep_md5 in info.deps.items()]
            computation = Computation(
                cmd=info.cmd or "",
                deps=deps,
                code_ref=info.code_ref,
            )

        # Use the original path passed in (full path), not info.path (relative)
        # This preserves the full path for cross-directory references
        return cls(
            path=str(path),
            md5=info.md5,
            size=info.size,
            computation=computation,
        )

    def write_dvc(self, capture_code_ref: bool = True) -> Path:
        """Write this artifact's .dvc file.

        This is the "prep" phase - generates the .dvc file without
        executing the computation. The output hash will be computed
        from the current file if it exists.

        If the file doesn't exist yet, a placeholder .dvc file is created
        without md5/size fields. This signals "output doesn't exist yet"
        for the two-phase prep/run workflow.

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

        # If still no hash, leave as None - write_dvc_file will omit these fields
        # to signal output doesn't exist yet (placeholder for prep phase)

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
    # Only write computed artifacts
    return [
        artifact.write_dvc(capture_code_ref) for artifact in all_artifacts if artifact.computation
    ]


def _run_one_artifact(
    artifact: Artifact,
    force: bool,
    update_dvc: bool = True,
) -> tuple[Artifact, bool, str | None]:
    """Run computation for a single artifact.

    Args:
        artifact: The artifact to compute
        force: Force recomputation even if fresh
        update_dvc: Whether to update .dvc file after computation

    Returns:
        Tuple of (artifact, success, error_msg)
    """
    import subprocess

    from dvx.run.dvc_files import is_output_fresh

    if not artifact.computation:
        return artifact, True, None  # Leaf node, nothing to compute

    path = Path(artifact.path)

    # Check if already fresh
    if not force:
        fresh, _reason = is_output_fresh(path)
        if fresh:
            return artifact, True, None

    # Execute computation
    cmd = artifact.computation.cmd
    result = subprocess.run(cmd, check=False, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        return artifact, False, f"Command: {cmd}\nStderr: {result.stderr}"

    # Update artifact with computed hash and optionally write .dvc file
    if path.exists():
        artifact.md5 = compute_md5(path)
        artifact.size = compute_file_size(path)
        if update_dvc:
            artifact.write_dvc()

    return artifact, True, None


def materialize(
    artifacts: list[Artifact],
    parallel: int = 1,
    force: bool = False,
    update_dvc: bool = True,
) -> list[Artifact]:
    """Execute computations for all stale artifacts.

    This is the "run" phase - executes pending computations and
    optionally updates .dvc files with output hashes.

    Args:
        artifacts: List of artifacts to materialize
        parallel: Number of parallel workers (default: 1, use -1 for CPU count)
        force: Force recomputation even if fresh (default: False)
        update_dvc: Whether to update .dvc files after computation (default: True)

    Returns:
        List of artifacts that were computed
    """
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from functools import partial

    # Collect all artifacts in dependency order
    all_artifacts = []
    seen = set()

    for artifact in artifacts:
        for a in artifact.walk_upstream():
            if a.path not in seen:
                seen.add(a.path)
                all_artifacts.append(a)

    # Filter to only computable artifacts
    computable = [a for a in all_artifacts if a.computation]

    if not computable:
        return []

    # Determine worker count
    if parallel == -1:
        parallel = os.cpu_count() or 1
    parallel = max(1, parallel)

    computed = []
    errors = []

    run_fn = partial(_run_one_artifact, force=force, update_dvc=update_dvc)

    if parallel == 1:
        # Sequential execution
        for artifact in computable:
            artifact, success, error = run_fn(artifact)
            if success and artifact.md5:
                computed.append(artifact)
            elif not success:
                errors.append((artifact, error))
    else:
        # Parallel execution
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {executor.submit(run_fn, artifact): artifact for artifact in computable}
            for future in as_completed(futures):
                artifact, success, error = future.result()
                if success and artifact.md5:
                    computed.append(artifact)
                elif not success:
                    errors.append((artifact, error))

    if errors:
        error_msgs = "\n".join(f"  {a.path}: {err}" for a, err in errors)
        raise RuntimeError(f"Computation failed for {len(errors)} artifact(s):\n{error_msgs}")

    return computed
