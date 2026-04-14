"""Parallel executor for DVX artifact computations.

Executes artifact computations in parallel, respecting dependencies.
Uses the provenance information in .dvc files (computation blocks).
"""

import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

from dvx.run.artifact import Artifact
from dvx.run.dvc_files import is_output_fresh, write_dvc_file
from dvx.run.hash import compute_file_size, compute_md5


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
    commit: str = "auto"  # Commit strategy: "auto", "always", "never"
    push: str = "never"  # Push strategy: "never", "each", "end"


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
                # Check if all deps (including git_deps) are done
                deps_done = True
                for dep in artifact.computation.deps:
                    dep_path = dep.path if isinstance(dep, Artifact) else str(dep)
                    if dep_path not in done:
                        deps_done = False
                        break
                if deps_done:
                    for dep in artifact.computation.git_deps:
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

        # Command deduplication state (for multi-output computations)
        self._cmd_lock = threading.Lock()
        self._cmd_events: dict[str, threading.Event] = {}  # cmd -> completion event
        self._cmd_results: dict[str, bool] = {}  # cmd -> success

    def execute(self) -> list[ExecutionResult]:
        """Execute all artifacts, respecting dependencies.

        Returns:
            List of ExecutionResult for each artifact
        """
        # Group into levels
        levels = _group_into_levels(self.artifacts)

        # Filter out leaf nodes (no computation)
        levels = [[a for a in level if a.computation is not None] for level in levels]
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
                    results.append(
                        ExecutionResult(
                            path=artifact.path,
                            success=True,
                            skipped=not should_run,
                            reason=reason,
                        )
                    )
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

        # Push at end if configured (CLI/env > config file)
        import os
        from dvx.config import load_config as _load_config
        _dvx_config = _load_config()
        push_strategy = os.environ.get("DVX_PUSH", self.config.push)
        if push_strategy == "never":
            push_strategy = _dvx_config.push
        if push_strategy == "end":
            executed = [r for r in results if r.success and not r.skipped]
            if executed:
                push_result = subprocess.run(
                    ["git", "push"],
                    capture_output=True, text=True, check=False,
                )
                if push_result.returncode == 0:
                    self._log("\n📤 pushed all commits")
                else:
                    self._log(f"\n⚠ push failed: {push_result.stderr.strip()}")

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
                    results.append(
                        ExecutionResult(
                            path=artifact.path,
                            success=False,
                            reason=str(e),
                        )
                    )

        return results

    def _execute_artifact(self, artifact: Artifact) -> ExecutionResult:
        """Execute a single artifact computation.

        Handles command deduplication: if multiple artifacts share the same cmd,
        only the first one runs the command, others wait and verify output.

        Args:
            artifact: Artifact to execute

        Returns:
            ExecutionResult for this artifact
        """
        import time

        path = artifact.path
        cmd = artifact.computation.cmd if artifact.computation else None

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

        # Command deduplication for multi-output computations
        if cmd:
            with self._cmd_lock:
                if cmd in self._cmd_results:
                    # Command already completed - handle as co-output
                    success = self._cmd_results[cmd]
                    if success:
                        return self._handle_co_output(artifact, cmd)
                    return ExecutionResult(
                        path=path,
                        success=False,
                        reason="command failed (co-output)",
                    )

                if cmd in self._cmd_events:
                    # Command in progress - wait for it
                    event = self._cmd_events[cmd]
                else:
                    # We'll run this command - create event for others to wait on
                    event = threading.Event()
                    self._cmd_events[cmd] = event
                    event = None  # Signal that we're the runner

            if event is not None:
                # Wait for the other thread to complete
                self._log(f"  ◐ {path}: waiting (same cmd running)...")
                event.wait()
                with self._cmd_lock:
                    success = self._cmd_results.get(cmd, False)
                if success:
                    return self._handle_co_output(artifact, cmd)
                return ExecutionResult(
                    path=path,
                    success=False,
                    reason="command failed (co-output)",
                )

        # Run the computation with stage output protocol env vars
        self._log(f"  ⟳ {path}: running...")
        start_time = time.time()

        import os
        import tempfile

        commit_msg_file = tempfile.NamedTemporaryFile(
            mode="w", prefix="dvx-commit-", suffix=".txt", delete=False,
        )
        summary_file = tempfile.NamedTemporaryFile(
            mode="w", prefix="dvx-summary-", suffix=".txt", delete=False,
        )
        push_file = tempfile.NamedTemporaryFile(
            mode="w", prefix="dvx-push-", suffix=".txt", delete=False,
        )
        commit_msg_file.close()
        summary_file.close()
        push_file.close()

        env = os.environ.copy()
        env["DVX_COMMIT_MSG_FILE"] = commit_msg_file.name
        env["DVX_SUMMARY_FILE"] = summary_file.name
        env["DVX_PUSH_FILE"] = push_file.name
        stage_env_extras = {"push_file": push_file.name}

        # Run cmd with CWD set to .dvc file's directory
        dvc_dir = Path(path).parent
        cmd_cwd = str(dvc_dir) if str(dvc_dir) != "." else None

        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=False,
            env=env,
            cwd=cmd_cwd,
        )
        duration = time.time() - start_time

        # Always save output to log file (success or failure)
        safe_name = Path(path).stem.replace("/", "-")
        log_path = Path(f"tmp/dvx-run-{safe_name}.log")
        has_output = bool(result.stdout or result.stderr)
        if has_output:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "w") as f:
                if result.stdout:
                    f.write("=== stdout ===\n")
                    f.write(result.stdout)
                if result.stderr:
                    f.write("=== stderr ===\n")
                    f.write(result.stderr)

        if result.returncode != 0:
            self._log(f"  ✗ {path}: failed (exit code {result.returncode})")
            # Show last N lines of stderr
            stderr_lines = (result.stderr or "").rstrip().split("\n")
            tail_count = 20
            if stderr_lines and stderr_lines != [""]:
                shown = stderr_lines[-tail_count:]
                if len(stderr_lines) > tail_count:
                    self._log(f"\n    stderr (last {tail_count} of {len(stderr_lines)} lines):")
                else:
                    self._log("\n    stderr:")
                for line in shown:
                    self._log(f"      {line}")
                self._log(f"\n    Full output: {log_path}")
            elif self.config.verbose:
                self._log("    (no stderr)")

            error_msg = stderr_lines[-1] if stderr_lines and stderr_lines != [""] else f"exit code {result.returncode}"

            # Record failure and signal waiters
            if cmd:
                with self._cmd_lock:
                    self._cmd_results[cmd] = False
                    if cmd in self._cmd_events:
                        self._cmd_events[cmd].set()

            # Clean up temp files on failure
            for f in (commit_msg_file.name, summary_file.name, push_file.name):
                try:
                    os.unlink(f)
                except OSError:
                    pass

            return ExecutionResult(
                path=path,
                success=False,
                reason=f"command failed: {error_msg}",
                duration=duration,
            )

        # Record success and signal waiters
        if cmd:
            with self._cmd_lock:
                self._cmd_results[cmd] = True
                if cmd in self._cmd_events:
                    self._cmd_events[cmd].set()

        # Check if this is a side-effect or fetch stage
        from dvx.run.dvc_files import read_dvc_file as _read_dvc
        info = _read_dvc(Path(path))
        is_side_effect = info is not None and info.is_side_effect

        # If this is a fetch stage, update last_run timestamp
        fetch_schedule = info.fetch_schedule if info else None
        fetch_last_run = None
        if fetch_schedule:
            from datetime import datetime, timezone
            fetch_last_run = datetime.now(timezone.utc).isoformat()

        if is_side_effect:
            # Side-effect: update dep hashes in .dvc, no output hash
            dvc_file = None
            deps_hashes = {}
            git_deps_hashes = {}
            if self.config.provenance and artifact.computation:
                deps_hashes = artifact.computation.get_dep_hashes(recompute=True)
                git_deps_hashes = artifact.computation.get_git_dep_hashes(recompute=True)

            try:
                dvc_file = write_dvc_file(
                    output_path=Path(path),
                    cmd=cmd if self.config.provenance else None,
                    deps=deps_hashes if self.config.provenance else None,
                    git_deps=git_deps_hashes if self.config.provenance else None,
                    fetch_schedule=fetch_schedule,
                    fetch_last_run=fetch_last_run,
                )
                if self.config.verbose:
                    self._log(f"       → {dvc_file}")
            except (FileNotFoundError, ValueError) as e:
                self._log(f"  ⚠ {path}: couldn't write .dvc: {e}")

            self._log(f"  ✓ {path}: side-effect completed ({duration:.1f}s)")
            self._show_success_output(result, log_path, has_output)
            self._handle_stage_output(path, commit_msg_file.name, summary_file.name, stage_env_extras)
            return ExecutionResult(
                path=path,
                success=True,
                reason="completed",
                duration=duration,
                dvc_file=dvc_file,
            )

        # Verify output exists
        out = Path(path)
        if not out.exists():
            self._log(f"  ✗ {path}: command succeeded but output not created")
            # Clean up temp files
            for f in (commit_msg_file.name, summary_file.name, push_file.name):
                try:
                    os.unlink(f)
                except OSError:
                    pass
            return ExecutionResult(
                path=path,
                success=False,
                reason="output not created by command",
                duration=duration,
            )

        # Compute dependency hashes for provenance
        deps_hashes = {}
        git_deps_hashes = {}
        if self.config.provenance and artifact.computation:
            deps_hashes = artifact.computation.get_dep_hashes(recompute=True)
            git_deps_hashes = artifact.computation.get_git_dep_hashes(recompute=True)

        # Write .dvc file for output
        dvc_file = None
        try:
            md5 = compute_md5(out)
            size = compute_file_size(out)

            # Cache the output blob so historical versions can be retrieved
            try:
                from dvx.cache import cache_blob
                cache_blob(out, md5)
            except Exception as e:
                self._log(f"  ⚠ {path}: couldn't cache output: {e}")

            dvc_file = write_dvc_file(
                output_path=out,
                md5=md5,
                size=size,
                cmd=cmd if self.config.provenance else None,
                deps=deps_hashes if self.config.provenance else None,
                git_deps=git_deps_hashes if self.config.provenance else None,
                fetch_schedule=fetch_schedule,
                fetch_last_run=fetch_last_run,
            )
            if self.config.verbose:
                self._log(f"       → {dvc_file}")
        except (FileNotFoundError, ValueError) as e:
            self._log(f"  ⚠ {path}: couldn't write .dvc: {e}")

        self._log(f"  ✓ {path}: completed ({duration:.1f}s)")
        self._show_success_output(result, log_path, has_output)
        self._handle_stage_output(path, commit_msg_file.name, summary_file.name, stage_env_extras)
        return ExecutionResult(
            path=path,
            success=True,
            reason="completed",
            duration=duration,
            dvc_file=dvc_file,
        )

    def _handle_co_output(self, artifact: Artifact, cmd: str) -> ExecutionResult:
        """Handle an artifact whose command was already run by another artifact.

        Verifies the output exists and updates its .dvc file.

        Args:
            artifact: The co-output artifact
            cmd: The command that produced this output

        Returns:
            ExecutionResult for this artifact
        """
        path = artifact.path
        out = Path(path)

        if not out.exists():
            self._log(f"  ✗ {path}: co-output not produced")
            return ExecutionResult(
                path=path,
                success=False,
                reason="co-output not produced by command",
            )

        # Compute hash and write .dvc file
        try:
            md5 = compute_md5(out)
            size = compute_file_size(out)

            # Cache the co-output blob
            try:
                from dvx.cache import cache_blob
                cache_blob(out, md5)
            except Exception as e:
                self._log(f"  ⚠ {path}: couldn't cache co-output: {e}")

            deps_hashes = {}
            git_deps_hashes = {}
            if self.config.provenance and artifact.computation:
                deps_hashes = artifact.computation.get_dep_hashes(recompute=True)
                git_deps_hashes = artifact.computation.get_git_dep_hashes(recompute=True)

            dvc_file = write_dvc_file(
                output_path=out,
                md5=md5,
                size=size,
                cmd=cmd if self.config.provenance else None,
                deps=deps_hashes if self.config.provenance else None,
                git_deps=git_deps_hashes if self.config.provenance else None,
            )

            self._log(f"  ✓ {path}: co-output ready")
            return ExecutionResult(
                path=path,
                success=True,
                skipped=False,
                reason="co-output",
                dvc_file=dvc_file,
            )
        except (FileNotFoundError, ValueError) as e:
            self._log(f"  ✗ {path}: failed to process co-output: {e}")
            return ExecutionResult(
                path=path,
                success=False,
                reason=f"co-output error: {e}",
            )

    def _handle_stage_output(self, path: str, commit_msg_path: str, summary_path: str, env_extras: dict | None = None):
        """Handle post-cmd stage output: commit message, summary, push.

        Args:
            path: Artifact path (for default commit message)
            commit_msg_path: Path to commit message temp file
            summary_path: Path to summary temp file
            env_extras: Additional temp file paths (e.g. push_file)
        """
        import os

        if env_extras is None:
            env_extras = {}

        try:
            # Check summary file
            if os.path.exists(summary_path) and os.path.getsize(summary_path) > 0:
                with open(summary_path) as f:
                    summary = f.read().strip()
                if summary:
                    self._log(f"    → {summary}")

            # Determine commit strategy for this stage
            from dvx.config import load_config
            dvx_config = load_config()
            # CLI/env override > per-stage config > global config
            commit_strategy = self.config.commit
            if commit_strategy == "auto":
                # Check per-stage override from config file
                stage_commit = dvx_config.should_commit(path)
                if stage_commit != "auto":
                    commit_strategy = stage_commit

            # Check commit message file
            commit_msg = None
            if commit_strategy != "never":
                if os.path.exists(commit_msg_path) and os.path.getsize(commit_msg_path) > 0:
                    with open(commit_msg_path) as f:
                        commit_msg = f.read().strip()

                if not commit_msg and commit_strategy == "always":
                    # Fallback: auto-commit with default message
                    stage_name = Path(path).stem
                    commit_msg = f"Run {stage_name}"

            if commit_msg:
                # Stage tracked changes and commit
                result = subprocess.run(
                    ["git", "add", "-u"],
                    capture_output=True, text=True, check=False,
                )
                if result.returncode == 0:
                    result = subprocess.run(
                        ["git", "commit", "--allow-empty", "-m", commit_msg],
                        capture_output=True, text=True, check=False,
                    )
                    if result.returncode == 0:
                        self._log(f"    📝 committed: {commit_msg.splitlines()[0]}")
                        # Check if stage requested push via $DVX_PUSH_FILE
                        push_file = env_extras.get("push_file", "")
                        stage_wants_push = (
                            os.path.exists(push_file) and os.path.getsize(push_file) > 0
                        ) if push_file else False
                        # Push strategy: CLI/env > global config
                        # Per-stage config only selects *when* to push within a
                        # run that already has push enabled — it doesn't enable
                        # push by itself. stage.push() ($DVX_PUSH_FILE) is the
                        # exception: it's an explicit per-invocation request.
                        push_strategy = os.environ.get("DVX_PUSH", self.config.push)
                        if push_strategy != "never":
                            # Push enabled globally — check per-stage override
                            stage_push = dvx_config.should_push(path)
                            if stage_push is not None and stage_push != "never":
                                push_strategy = stage_push
                        should_push = push_strategy == "each" or stage_wants_push
                        if should_push:
                            push_result = subprocess.run(
                                ["git", "push"],
                                capture_output=True, text=True, check=False,
                            )
                            if push_result.returncode == 0:
                                self._log("    📤 pushed")
                            else:
                                self._log(f"    ⚠ push failed: {push_result.stderr.strip()}")
                    elif "nothing to commit" in result.stdout:
                        pass  # No changes to commit
                    else:
                        self._log(f"    ⚠ commit failed: {result.stderr.strip()}")
        finally:
            # Clean up temp files
            for f in (commit_msg_path, summary_path, env_extras.get("push_file", "")):
                try:
                    if f:
                        os.unlink(f)
                except OSError:
                    pass

    def _show_success_output(self, result, log_path, has_output):
        """Show stage output on success (verbose: inline, otherwise: log path)."""
        if not has_output:
            return
        if self.config.verbose:
            for stream, label in [(result.stdout, "stdout"), (result.stderr, "stderr")]:
                if stream and stream.strip():
                    for line in stream.rstrip().split("\n"):
                        self._log(f"    {label}: {line}")
        else:
            self._log(f"    output: {log_path}")

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
        if str(target).endswith(".dvc"):
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
                if dep_path not in artifacts:
                    dvc_file = Path(str(dep_path) + ".dvc")
                    if dvc_file.exists():
                        pending.append(dvc_file)
                    else:
                        # No .dvc file — add as leaf so _group_into_levels sees it in `done`
                        artifacts[dep_path] = Artifact(path=dep_path)

            # git_deps are always leaf nodes (git-tracked, no .dvc file)
            for dep in artifact.computation.git_deps:
                dep_path = dep.path if isinstance(dep, Artifact) else str(dep)
                if dep_path not in artifacts:
                    artifacts[dep_path] = Artifact(path=dep_path)

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
            for dep in artifact.computation.git_deps:
                dep_path = dep.path if isinstance(dep, Artifact) else str(dep)
                if dep_path in artifacts:
                    visit(artifacts[dep_path])

        result.append(artifact)

    for artifact in artifacts.values():
        visit(artifact)

    return result
