"""Parallel executor for DVX artifact computations.

Executes artifact computations in parallel, respecting dependencies.
Uses the provenance information in .dvc files (computation blocks).
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO
import subprocess

from dvx.run.artifact import Artifact
from dvx.run.dvc_files import get_git_head_sha, is_output_fresh, write_dvc_file
from dvx.run.hash import compute_md5, compute_file_size


@dataclass
class ExecutionResult:
    """Result of executing an artifact computation."""

    path: str
    success: bool
    skipped: bool = False
    reason: str = ""
    duration: float = 0.0
    dvc_file: Path | None = None


@dataclass
class ExecutionConfig:
    """Configuration for execution."""

    max_workers: int | None = None
    dry_run: bool = False
    force: bool = False
    force_patterns: list[str] = field(default_factory=list)
    cached_patterns: list[str] = field(default_factory=list)
    provenance: bool = True
    verbose: bool = False


def _matches_patterns(path: str, patterns: list[str]) -> bool:
    """Check if path matches any glob pattern."""
    import fnmatch
    return any(fnmatch.fnmatch(path, p) for p in patterns)


def _group_into_levels(artifacts: list[Artifact]) -> list[list[Artifact]]:
    """Group artifacts into execution levels for parallel execution.

    Artifacts in the same level have no dependencies on each other
    and can be executed in parallel.

    Args:
        artifacts: List of artifacts in topological order (deps first)

    Returns:
        List of levels, where each level is a list of artifacts
    """
    # Track which artifacts are "done" (either executed or scheduled)
    done: set[str] = set()
    levels: list[list[Artifact]] = []

    remaining = list(artifacts)

    while remaining:
        # Find artifacts whose deps are all done
        ready = []
        not_ready = []

        for artifact in remaining:
            if artifact.computation is None:
                # Leaf nodes are always ready
                ready.append(artifact)
            else:
                # Check if all deps are done
                deps_done = True
                for dep in artifact.computation.deps:
                    dep_path = dep.path if isinstance(dep, Artifact) else str(dep)
                    if dep_path not in done:
                        deps_done = False
                        break

                if deps_done:
                    ready.append(artifact)
                else:
                    not_ready.append(artifact)

        if not ready:
            # This shouldn't happen with a valid DAG
            raise RuntimeError("Circular dependency detected")

        # Add ready artifacts to current level
        levels.append(ready)
        for a in ready:
            done.add(a.path)

        remaining = not_ready

    return levels


class ParallelExecutor:
    """Execute artifact computations in parallel."""

    def __init__(
        self,
        artifacts: list[Artifact],
        config: ExecutionConfig | None = None,
        output: TextIO | None = None,
    ):
        """Initialize parallel executor.

        Args:
            artifacts: List of artifacts to execute (in dependency order)
            config: Execution configuration
            output: Stream for logging output (default: stderr)
        """
        self.artifacts = artifacts
        self.config = config or ExecutionConfig()
        self.output = output or sys.stderr
        # Capture git SHA once at start for consistent provenance
        self.code_ref = get_git_head_sha() if self.config.provenance else None

    def execute(self) -> list[ExecutionResult]:
        """Execute all artifacts, respecting dependencies.

        Returns:
            List of ExecutionResult for each artifact
        """
        # Group into levels
        levels = _group_into_levels(self.artifacts)

        # Filter out leaf nodes (no computation)
        levels = [
            [a for a in level if a.computation is not None]
            for level in levels
        ]
        levels = [level for level in levels if level]  # Remove empty levels

        if not levels:
            self._log("No computations to execute")
            return []

        total_stages = sum(len(level) for level in levels)
        self._log(f"Execution plan: {len(levels)} levels, {total_stages} computations")

        if self.config.verbose:
            for i, level in enumerate(levels, 1):
                paths = [a.path for a in level]
                self._log(f"  Level {i}: {', '.join(paths)}")

        if self.config.dry_run:
            self._log("\nDry run - showing what would execute:")
            results = []
            for level in levels:
                for artifact in level:
                    should_run, reason = self._should_run(artifact)
                    status = "would run" if should_run else f"skip ({reason})"
                    self._log(f"  {artifact.path}: {status}")
                    results.append(ExecutionResult(
                        path=artifact.path,
                        success=True,
                        skipped=not should_run,
                        reason=reason,
                    ))
            return results

        self._log("")

        results = []
        for level_num, level in enumerate(levels, 1):
            self._log(f"Level {level_num}/{len(levels)}: {len(level)} computation(s)")
            level_results = self._execute_level(level)
            results.extend(level_results)

            # Check for failures
            failures = [r for r in level_results if not r.success]
            if failures:
                failed = ", ".join(r.path for r in failures)
                self._log(f"\nFailed: {failed}")
                break

        return results

    def _should_run(self, artifact: Artifact) -> tuple[bool, str]:
        """Check if artifact should be executed.

        Returns:
            Tuple of (should_run, reason)
        """
        path = artifact.path

        # Check cached patterns
        if _matches_patterns(path, self.config.cached_patterns):
            return False, "cached by pattern"

        # Check force
        if self.config.force or _matches_patterns(path, self.config.force_patterns):
            return True, "forced"

        # Check freshness
        fresh, reason = is_output_fresh(Path(path))
        if fresh:
            return False, reason

        return True, reason

    def _execute_level(self, artifacts: list[Artifact]) -> list[ExecutionResult]:
        """Execute all artifacts in a level in parallel.

        Args:
            artifacts: List of artifacts to execute

        Returns:
            List of ExecutionResult, one per artifact
        """
        if len(artifacts) == 1:
            # Single artifact - run directly without thread pool overhead
            return [self._execute_artifact(artifacts[0])]

        # Multiple artifacts - run in parallel
        results = []
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {
                executor.submit(self._execute_artifact, artifact): artifact
                for artifact in artifacts
            }

            for future in as_completed(futures):
                artifact = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self._log(f"  ✗ {artifact.path}: {e}")
                    results.append(ExecutionResult(
                        path=artifact.path,
                        success=False,
                        reason=str(e),
                    ))

        return results

    def _execute_artifact(self, artifact: Artifact) -> ExecutionResult:
        """Execute a single artifact computation.

        Args:
            artifact: Artifact to execute

        Returns:
            ExecutionResult for this artifact
        """
        import time

        path = artifact.path

        # Check if should run
        should_run, reason = self._should_run(artifact)
        if not should_run:
            self._log(f"  ○ {path}: {reason}")
            return ExecutionResult(
                path=path,
                success=True,
                skipped=True,
                reason=reason,
            )

        # Run the computation
        cmd = artifact.computation.cmd
        self._log(f"  ⟳ {path}: running...")

        start_time = time.time()

        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
            )
            duration = time.time() - start_time

            # Compute dependency hashes for provenance
            deps_hashes = {}
            if self.config.provenance:
                deps_hashes = artifact.computation.get_dep_hashes()

            # Write .dvc file for output
            out = Path(path)
            dvc_file = None
            if out.exists():
                try:
                    md5 = compute_md5(out)
                    size = compute_file_size(out)

                    dvc_file = write_dvc_file(
                        output_path=out,
                        md5=md5,
                        size=size,
                        cmd=cmd if self.config.provenance else None,
                        code_ref=self.code_ref,
                        deps=deps_hashes if self.config.provenance else None,
                    )
                    if self.config.verbose:
                        self._log(f"       → {dvc_file}")
                except (FileNotFoundError, ValueError) as e:
                    self._log(f"  ⚠ {path}: couldn't write .dvc: {e}")

            self._log(f"  ✓ {path}: completed ({duration:.1f}s)")
            return ExecutionResult(
                path=path,
                success=True,
                reason="completed",
                duration=duration,
                dvc_file=dvc_file,
            )

        except subprocess.CalledProcessError as e:
            duration = time.time() - start_time
            error_msg = e.stderr[:200] if e.stderr else str(e)
            self._log(f"  ✗ {path}: failed")
            if self.config.verbose:
                self._log(f"       {error_msg}")
            return ExecutionResult(
                path=path,
                success=False,
                reason=f"command failed: {error_msg}",
                duration=duration,
            )

    def _log(self, message: str):
        """Write log message to output stream."""
        print(message, file=self.output)


def run(
    targets: list[Path],
    config: ExecutionConfig | None = None,
    output: TextIO | None = None,
) -> list[ExecutionResult]:
    """Execute computations for .dvc file targets.

    This is the main entry point for `dvx run`.

    Args:
        targets: List of .dvc files or output paths
        config: Execution configuration
        output: Output stream for logging

    Returns:
        List of ExecutionResult for each artifact
    """
    from dvx.run.artifact import Artifact

    # Build artifact graph from .dvc files
    artifacts: dict[str, Artifact] = {}
    pending = list(targets)

    while pending:
        target = pending.pop(0)

        # Get output path from .dvc path
        if str(target).endswith('.dvc'):
            output_path = Path(str(target)[:-4])
        else:
            output_path = target

        output_str = str(output_path)

        if output_str in artifacts:
            continue

        # Load artifact from .dvc file
        artifact = Artifact.from_dvc(output_path)
        if artifact is None:
            # No .dvc file - treat as leaf
            artifact = Artifact(path=output_str)

        artifacts[output_str] = artifact

        # Queue dependencies
        if artifact.computation:
            for dep in artifact.computation.deps:
                dep_path = dep.path if isinstance(dep, Artifact) else str(dep)
                dvc_file = Path(str(dep_path) + '.dvc')
                if dvc_file.exists() and dep_path not in artifacts:
                    pending.append(dvc_file)

    # Topological sort (deps first)
    sorted_artifacts = _topological_sort(artifacts)

    # Execute
    executor = ParallelExecutor(sorted_artifacts, config, output)
    return executor.execute()


def _topological_sort(artifacts: dict[str, Artifact]) -> list[Artifact]:
    """Sort artifacts in dependency order (deps first)."""
    visited: set[str] = set()
    result: list[Artifact] = []

    def visit(artifact: Artifact):
        if artifact.path in visited:
            return
        visited.add(artifact.path)

        if artifact.computation:
            for dep in artifact.computation.deps:
                dep_path = dep.path if isinstance(dep, Artifact) else str(dep)
                if dep_path in artifacts:
                    visit(artifacts[dep_path])

        result.append(artifact)

    for artifact in artifacts.values():
        visit(artifact)

    return result
