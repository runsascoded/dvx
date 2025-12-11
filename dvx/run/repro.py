"""Reproduce artifacts based on .dvc file provenance.

This module implements DAG-based recomputation using the provenance
information stored in .dvc files (computation blocks).

Unlike `dvx run` which uses dvc.yaml, `dvx repro` works directly
with .dvc files and their embedded computation metadata.

Example usage:
    # Reproduce a single artifact
    dvx repro output.parquet.dvc

    # Reproduce multiple artifacts
    dvx repro normalized/*.dvc aggregated/*.dvc

    # Force recomputation even if fresh
    dvx repro --force output.dvc

    # Force recomputation of specific upstream pattern
    dvx repro --force-upstream "*/normalized/*" output.dvc

    # Use cached version of specific upstream even if stale
    dvx repro --cached "*/raw/*" output.dvc
"""

from __future__ import annotations

import fnmatch
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

from dvx.run.artifact import Artifact
from dvx.run.dvc_files import (
    get_git_head_sha,
    is_output_fresh,
    read_dvc_file,
    write_dvc_file,
)
from dvx.run.hash import compute_md5, compute_file_size


@dataclass
class ReproResult:
    """Result of reproducing a single artifact."""

    path: str
    success: bool
    skipped: bool = False
    reason: str = ""
    duration: float = 0.0


@dataclass
class ReproConfig:
    """Configuration for reproduction."""

    force: bool = False
    force_upstream: list[str] = field(default_factory=list)
    cached: list[str] = field(default_factory=list)
    dry_run: bool = False
    jobs: int = 1
    verbose: bool = False


def matches_patterns(path: str, patterns: list[str]) -> bool:
    """Check if a path matches any of the given glob patterns."""
    for pattern in patterns:
        if fnmatch.fnmatch(path, pattern):
            return True
    return False


def build_dag_from_dvc_files(targets: list[Path]) -> dict[str, Artifact]:
    """Build artifact DAG from .dvc files.

    Recursively follows computation.deps to build full dependency graph.

    Args:
        targets: List of .dvc file paths

    Returns:
        Dict mapping output paths to Artifact objects
    """
    artifacts: dict[str, Artifact] = {}
    pending = list(targets)

    while pending:
        dvc_path = pending.pop(0)

        # Get output path from .dvc path
        if str(dvc_path).endswith('.dvc'):
            output_path = Path(str(dvc_path)[:-4])
        else:
            output_path = dvc_path
            dvc_path = Path(str(dvc_path) + '.dvc')

        output_str = str(output_path)

        # Skip if already processed
        if output_str in artifacts:
            continue

        # Load artifact from .dvc file
        artifact = Artifact.from_dvc(output_path)
        if artifact is None:
            # No .dvc file - treat as leaf (external input)
            artifact = Artifact(path=output_str)

        artifacts[output_str] = artifact

        # Queue up dependencies for processing
        if artifact.computation:
            for dep in artifact.computation.deps:
                if isinstance(dep, Artifact):
                    dep_path = Path(dep.path)
                else:
                    dep_path = Path(str(dep))

                # Check for .dvc file for this dep
                dvc_file = Path(str(dep_path) + '.dvc')
                if dvc_file.exists() and str(dep_path) not in artifacts:
                    pending.append(dvc_file)

    return artifacts


def topological_sort(artifacts: dict[str, Artifact]) -> list[Artifact]:
    """Sort artifacts in dependency order (leaves first).

    Args:
        artifacts: Dict mapping paths to Artifact objects

    Returns:
        List of artifacts in topological order
    """
    visited: set[str] = set()
    result: list[Artifact] = []

    def visit(artifact: Artifact):
        if artifact.path in visited:
            return
        visited.add(artifact.path)

        # Visit dependencies first
        if artifact.computation:
            for dep in artifact.computation.deps:
                if isinstance(dep, Artifact):
                    dep_path = dep.path
                else:
                    dep_path = str(dep)
                if dep_path in artifacts:
                    visit(artifacts[dep_path])

        result.append(artifact)

    for artifact in artifacts.values():
        visit(artifact)

    return result


def should_force(path: str, config: ReproConfig) -> bool:
    """Check if an artifact should be forced to recompute."""
    if config.force:
        return True
    return matches_patterns(path, config.force_upstream)


def should_cache(path: str, config: ReproConfig) -> bool:
    """Check if an artifact should use cached value even if stale."""
    return matches_patterns(path, config.cached)


def repro_artifact(
    artifact: Artifact,
    config: ReproConfig,
    output: TextIO | None = None,
) -> ReproResult:
    """Reproduce a single artifact.

    Args:
        artifact: Artifact to reproduce
        config: Reproduction configuration
        output: Optional output stream for status messages

    Returns:
        ReproResult with status information
    """
    import time

    path = artifact.path

    # Check if this is a leaf node (no computation)
    if not artifact.computation:
        return ReproResult(
            path=path,
            success=True,
            skipped=True,
            reason="leaf node (no computation)",
        )

    # Check if should use cached
    if should_cache(path, config):
        return ReproResult(
            path=path,
            success=True,
            skipped=True,
            reason="cached by pattern",
        )

    # Check freshness
    force = should_force(path, config)
    if not force:
        fresh, reason = is_output_fresh(Path(path))
        if fresh:
            return ReproResult(
                path=path,
                success=True,
                skipped=True,
                reason=reason,
            )

    # Dry run - don't actually execute
    if config.dry_run:
        return ReproResult(
            path=path,
            success=True,
            skipped=False,
            reason="would run" if not force else "would run (forced)",
        )

    # Execute computation
    if output and config.verbose:
        output.write(f"Running: {artifact.computation.cmd}\n")

    start_time = time.time()

    try:
        result = subprocess.run(
            artifact.computation.cmd,
            shell=True,
            capture_output=True,
            text=True,
        )

        duration = time.time() - start_time

        if result.returncode != 0:
            return ReproResult(
                path=path,
                success=False,
                reason=f"command failed: {result.stderr[:200]}",
                duration=duration,
            )

        # Update .dvc file with new hash
        output_path = Path(path)
        if output_path.exists():
            md5 = compute_md5(output_path)
            size = compute_file_size(output_path)
            code_ref = get_git_head_sha()

            # Get dep hashes
            dep_hashes = artifact.computation.get_dep_hashes()

            write_dvc_file(
                output_path=output_path,
                md5=md5,
                size=size,
                cmd=artifact.computation.cmd,
                code_ref=code_ref,
                deps=dep_hashes,
            )

            # Update artifact
            artifact.md5 = md5
            artifact.size = size

        return ReproResult(
            path=path,
            success=True,
            skipped=False,
            reason="executed",
            duration=duration,
        )

    except Exception as e:
        return ReproResult(
            path=path,
            success=False,
            reason=str(e),
            duration=time.time() - start_time,
        )


def repro(
    targets: list[Path],
    config: ReproConfig | None = None,
    output: TextIO | None = None,
) -> list[ReproResult]:
    """Reproduce artifacts from .dvc files.

    Args:
        targets: List of .dvc files or output paths to reproduce
        config: Optional reproduction configuration
        output: Optional output stream for status messages

    Returns:
        List of ReproResult for each artifact
    """
    if config is None:
        config = ReproConfig()

    # Build DAG
    artifacts = build_dag_from_dvc_files(targets)

    if not artifacts:
        return []

    # Topological sort
    sorted_artifacts = topological_sort(artifacts)

    # Execute in order
    results = []
    for artifact in sorted_artifacts:
        result = repro_artifact(artifact, config, output)
        results.append(result)

        if output and config.verbose:
            status = "✓" if result.success else "✗"
            if result.skipped:
                status = "○"
            output.write(f"{status} {result.path}: {result.reason}\n")

        # Stop on failure (unless we want to continue)
        if not result.success:
            break

    return results


def status(targets: list[Path]) -> dict[str, tuple[bool, str]]:
    """Check freshness status of artifacts.

    Args:
        targets: List of .dvc files or output paths to check

    Returns:
        Dict mapping paths to (is_fresh, reason) tuples
    """
    # Build DAG
    artifacts = build_dag_from_dvc_files(targets)

    # Check each artifact
    result = {}
    for path, artifact in artifacts.items():
        if not artifact.computation:
            result[path] = (True, "leaf node")
        else:
            fresh, reason = is_output_fresh(Path(path))
            result[path] = (fresh, reason)

    return result
