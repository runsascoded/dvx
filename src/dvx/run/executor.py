"""Parallel executor for DVX artifact computations.

Executes artifact computations in parallel, respecting dependencies.
Uses the provenance information in .dvc files (computation blocks).
"""

import json
import subprocess
import sys
import threading
import time
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
    # The regenerated output's md5 differs from the one the .dvc recorded.
    # Success, not failure — but the signal a reproducibility audit exists
    # to find, so it survives to the run summary rather than only the log.
    hash_changed: bool = False


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
    cache_push: bool = True  # When push != "never", also push cache blobs to remote
    # Named DVC remote for both halves of the run's cache traffic: dep
    # materialization (read) and post-stage push (write). None → the repo's
    # default remote. A reproc that regenerates everything must be able to
    # write somewhere other than the remote prod serves from; see
    # specs/done/run-remote-flag.md.
    remote: str | None = None
    # Stop traversing upstream once a fresh artifact is reached. Default True;
    # auto-disabled when --force-upstream patterns are set (we have to walk to
    # find pattern matches).
    prune_fresh: bool = True
    # Try to materialize "materializable" stages (deps fresh, output missing
    # locally) from the configured remote before running the cmd. CI runners
    # on fresh checkouts otherwise rebuild every stage from upstream roots
    # even when the cache already holds byte-identical outputs.
    # See specs/done/run-auto-pull.md.
    pull_deps: bool = True
    # Seconds without a single cache object settling before the post-stage
    # push is abandoned (with a warning) instead of blocking the run.
    push_timeout: float = 600.0
    # Seconds a primary stage waits for a same-level co-output to write its
    # .dvc before proceeding without it. Only reachable if a co-output's
    # thread is wedged — `_execute_artifact` signals on every exit.
    co_output_timeout: float = 1800.0
    # Memory budget (GB) for scheduling a level: a stage acquires its
    # `resources.mem_gb` from a shared budget before its cmd runs and releases
    # after, so a level's heavy stages serialize while light ones stay
    # parallel — instead of the blunt "ceiling the whole job" / "cap -j
    # globally" levers. None + any stage labeled ⇒ introspect total RAM; None +
    # nothing labeled ⇒ disabled (today's fixed fan-out). See
    # specs/done/resource-aware-scheduling.md.
    mem_budget_gb: float | None = None
    # Weight charged to a stage with no `resources.mem_gb`. Default 0 keeps the
    # feature opt-in per stage — an unlabeled stage never blocks on the budget.
    mem_default_gb: float = 0.0


def _total_ram_gb() -> float | None:
    """Total physical RAM in GB, or None if it can't be determined.

    Used as the default memory budget when stages carry `mem_gb` hints but no
    explicit ``--mem`` was given. ``os.sysconf`` covers Linux and macOS (the
    platforms dvx runs its Batch reproc on); anything else falls back to None,
    which disables budget scheduling rather than guessing.
    """
    import os

    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") / 1024**3
    except (ValueError, OSError, AttributeError):
        return None


class _BudgetGate:
    """A shared memory budget that serializes a level's heavy stages.

    ``acquire(w)`` blocks until ``w`` GB fits under the budget alongside
    what's already running, then reserves it; ``release(w)`` frees it and wakes
    waiters. Only the stage that runs a cmd acquires — co-outputs (which run no
    cmd, only verify + write their .dvc) never gate, so the primary's wait for
    its co-outputs can't deadlock against the budget.

    Forward progress is guaranteed two ways: a request larger than the whole
    budget is clamped to it (an over-budget stage runs *alone* rather than
    hanging forever), and the wait condition only holds while something else is
    running — so when the gate is idle the next stage always proceeds, even if
    over budget.
    """

    def __init__(self, budget_gb: float) -> None:
        self._budget = budget_gb
        self._running = 0.0
        self._cond = threading.Condition()

    def acquire(self, weight: float) -> None:
        if weight <= 0:
            return
        w = min(weight, self._budget)
        with self._cond:
            while self._running > 0 and self._running + w > self._budget:
                self._cond.wait()
            self._running += w

    def release(self, weight: float) -> None:
        if weight <= 0:
            return
        w = min(weight, self._budget)
        with self._cond:
            self._running -= w
            self._cond.notify_all()


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

        # Co-output coordination. The primary stage (the one that runs the
        # cmd) waits for every co-output to finish writing its .dvc before
        # committing + cache-pushing — otherwise ``git add -u`` would miss
        # late co-outputs and the cache push manifest would drop their
        # blobs (see specs/done/co-output-push-half-blob.md).
        # Requires ``max_workers`` ≥ largest cmd-group size; with -j 1 and
        # a multi-output cmd, the primary's wait would deadlock the pool.
        self._cmd_artifact_paths: dict[str, list[str]] = {}
        for a in artifacts:
            if a.computation and a.computation.cmd:
                self._cmd_artifact_paths.setdefault(a.computation.cmd, []).append(a.path)
        self._dvc_done_events: dict[str, threading.Event] = {
            a.path: threading.Event() for a in artifacts if a.computation is not None
        }
        # Populated at the start of each level with that level's artifact
        # paths. `_wait_for_co_outputs` filters against this so the primary
        # only blocks on same-level co-outputs; cross-level co-outputs
        # haven't been submitted to the pool yet and can never signal.
        self._scheduled_paths: set[str] = set()

        # Memory-budget gate (None ⇒ disabled). Built once from the config and
        # whether any stage carries a `mem_gb` hint: an explicit --mem always
        # wins; otherwise a labeled run introspects total RAM, and an unlabeled
        # run leaves it off (today's fixed fan-out).
        self._budget_gate = self._build_budget_gate()

    def _build_budget_gate(self) -> "_BudgetGate | None":
        any_labeled = any(
            a.computation and a.computation.resources.get("mem_gb")
            for a in self.artifacts
        )
        budget = self.config.mem_budget_gb
        if budget is None:
            if not any_labeled:
                return None
            budget = _total_ram_gb()
            if budget is None:
                return None
            self._log(f"Memory-budget scheduling: {budget:.0f} GB (total RAM)")
        else:
            self._log(f"Memory-budget scheduling: {budget:.0f} GB (--mem)")
        return _BudgetGate(budget)

    def _mem_weight(self, artifact: Artifact) -> float:
        """GB this stage charges against the budget: its `mem_gb` hint, or the
        configured default for an unlabeled stage."""
        if artifact.computation and "mem_gb" in artifact.computation.resources:
            return artifact.computation.resources["mem_gb"]
        return self.config.mem_default_gb

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

        self._warn_unresolvable_deps()

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
                    from dvx.cache import MaterializeError
                    try:
                        should_run, reason = self._should_run(artifact)
                    except MaterializeError as e:
                        should_run, reason = True, f"materialization failed: {e}"
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
                self._push_cache_blobs([f"{r.path}.dvc" for r in executed])

        return results

    def _warn_unresolvable_deps(self) -> None:
        """Warn about declared deps that name neither a tracked artifact nor a
        file on disk.

        A dep path is resolved relative to its ``.dvc``'s directory unless it
        starts with ``/`` or already carries that directory as a prefix
        (``_resolve_dep_paths``). A cross-directory dep written bare therefore
        resolves *somewhere* — just not where the author meant
        (``njdot/data/x.parquet`` in ``www/public/njdot/y.db.dvc`` becomes
        ``www/public/njdot/njdot/data/x.parquet``). Nothing produces that path,
        so the edge is dropped and the stage runs in Level 1 against a file
        that doesn't exist yet.

        Which is indistinguishable, at every layer, from having declared no dep
        at all — and invisible on a machine where the file happens to be lying
        around. Six of nj-crashes' fourteen Level-1 reproc failures were this
        (``specs/done/co-output-dep-edge-loss.md``).

        Deliberately narrow: a dep with a ``.dvc`` is fine even when its file is
        absent (that's a pruned-fresh leaf, or something a prior level builds).
        Only "no ``.dvc`` AND no file" is unambiguously broken.
        """
        for artifact in self.artifacts:
            if artifact.computation is None:
                continue
            for dep in artifact.computation.deps:
                dep_path = dep.path if isinstance(dep, Artifact) else str(dep)
                if Path(dep_path).exists() or Path(f"{dep_path}.dvc").exists():
                    continue
                self._log(
                    f"⚠ {artifact.path}: dep {dep_path!r} matches no .dvc file and "
                    "no path on disk — no ordering edge"
                )

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

        # Materialization pre-pass: try fetching recorded state from the
        # remote cache before falling back to a rerun. Two triggers, looped
        # because resolving one can surface the other (e.g. pulling the
        # stage's own output re-classifies it as "dep missing" when the dep
        # lives inside a tracked dir that's not in the plan):
        #
        # - "output missing[: <name>]" — pull the stage's own .dvc targets
        #   (see specs/done/run-auto-pull.md).
        # - "dep missing: <path>" — pull the absent dep file(s) by recorded
        #   hash: via the dep's own .dvc, or its parent tracked-dir .dvc for
        #   files inside tracked directories
        #   (see specs/done/batch-run-command-cli-mismatch.md §3).
        #
        # Invariant this preserves: on a fresh clone whose recorded closure
        # exists in the remote, `dvx run <target>` executes zero cmds.
        if self.config.pull_deps:
            # Bounded: each iteration must make progress (a successful pull
            # that changes the freshness reason) or we bail to the rerun.
            for _ in range(4):
                if reason.startswith("output missing"):
                    # Skip placeholder .dvc files (no recorded md5 yet —
                    # nothing to fetch and `repo.pull` would slow the run
                    # unnecessarily).
                    from dvx.run.dvc_files import read_dvc_file
                    info = read_dvc_file(Path(path))
                    if info is None or not any(o.md5 for o in info.outs):
                        break
                    if not self._try_materialize_from_remote([f"{path}.dvc"], label=path):
                        break
                elif reason.startswith("dep missing"):
                    targets = self._missing_dep_pull_targets(path)
                    if not targets:
                        break
                    if not self._try_materialize_from_remote(targets, label=path):
                        break
                else:
                    break
                fresh2, reason2 = is_output_fresh(Path(path))
                if fresh2:
                    return False, f"fetched ({reason2})"
                if reason2 == reason:
                    # Pull "succeeded" but nothing changed — avoid spinning.
                    break
                # Keep the most recent (most accurate) classification for
                # the next iteration / the rerun log line.
                reason = reason2

        return True, reason

    def _missing_dep_pull_targets(self, path: str) -> list[str]:
        """Pull targets for a stage's absent dep files.

        For each recorded dep whose file is missing from the workspace:
        - dep has its own ``.dvc`` → that ``.dvc`` file;
        - dep is inside a DVC-tracked directory → the parent dir's ``.dvc``
          (materializes the whole dir — coarse but correct);
        - otherwise (raw file, never cached) → skip; nothing to pull.
        """
        from dvx.run.dvc_files import find_parent_dvc_dir, read_dvc_file

        info = read_dvc_file(Path(path))
        if info is None:
            return []
        targets = []
        for dep_path in info.deps:
            dep = Path(dep_path)
            if dep.exists():
                continue
            dep_dvc = Path(f"{dep_path}.dvc")
            if dep_dvc.exists():
                targets.append(str(dep_dvc))
                continue
            found = find_parent_dvc_dir(dep)
            if found is not None:
                parent_dir, _rel = found
                # find_parent_dvc_dir resolves to an absolute path; pull
                # targets want workspace-relative when possible.
                try:
                    parent_dir = parent_dir.relative_to(Path.cwd())
                except ValueError:
                    pass
                parent_dvc = f"{parent_dir}.dvc"
                if parent_dvc not in targets:
                    targets.append(parent_dvc)
        return targets

    def _try_materialize_from_remote(self, targets: list[str], label: str) -> bool:
        """Materialize ``targets``' recorded hashes from the remote — lock-free
        (no ``repo.pull``, no repo rwlock; see
        specs/done/parallel-pull-lock-contention.md). Returns True when every
        recorded output landed.

        Blobs *absent from the remote* return False so the stage falls
        through to its normal rerun (a legitimate rebuild trigger).
        Transport-level failures raise MaterializeError — the caller turns
        that into a stage failure rather than a blind rerun against inputs
        that may not exist.
        """
        from dvx.cache import MaterializeError, materialize_targets

        try:
            ok, missing = materialize_targets(targets, remote=self.config.remote)
        except MaterializeError:
            raise
        except Exception as e:
            # Repo-less contexts (no .dvc dir, no remote configured) land
            # here — not materializable, fall through to rerun.
            if self.config.verbose:
                self._log(f"    ↓ {label}: materialize failed ({e})")
            return False
        if not ok and self.config.verbose:
            self._log(f"    ↓ {label}: not in remote ({', '.join(missing[:3])})")
        return ok

    def _execute_level(self, artifacts: list[Artifact]) -> list[ExecutionResult]:
        """Execute all artifacts in a level in parallel.

        Args:
            artifacts: List of artifacts to execute

        Returns:
            List of ExecutionResult, one per artifact
        """
        # Track which artifacts are in-flight this level so `_wait_for_co_outputs`
        # can filter out cross-level co-outputs (their `_dvc_done_events` won't
        # be armed until a later level submits them — waiting would deadlock).
        for a in artifacts:
            self._scheduled_paths.add(a.path)

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
        """Execute a single artifact computation, always signalling dvc-done.

        Every exit path must signal — not just the ones that write a .dvc.
        A same-level co-output that returns early (skipped as fresh, or failed
        materialization, or a failed cmd) is in ``_scheduled_paths``, so the
        primary's `_wait_for_co_outputs` waits on its event; without a signal
        the pool deadlocks. That stranded two nj-crashes Fargate jobs
        indefinitely when a targeted daily run left one co-output's stamps
        fresh and its sibling's stale
        (``specs/done/co-output-wait-deadlock-on-skip.md``).

        Mirrors `_handle_co_output`'s try/finally, which fixed the same class
        of hang for failed co-outputs. ``Event.set()`` is idempotent, so the
        paths that already signal mid-body are unaffected.
        """
        try:
            return self._execute_artifact_inner(artifact)
        finally:
            self._signal_dvc_done(artifact.path)

    def _execute_artifact_inner(self, artifact: Artifact) -> ExecutionResult:
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

        # Check if should run. A transport-level materialization failure is a
        # stage *failure*, not a rebuild trigger — re-running the cmd against
        # possibly-unmaterialized inputs would produce confusing downstream
        # crashes instead of a clear "couldn't materialize X".
        from dvx.cache import MaterializeError
        try:
            should_run, reason = self._should_run(artifact)
            if should_run and cmd and self.config.pull_deps:
                # Execution-time dep materialization: a genuinely-stale (or
                # forced) stage may have declared dep files absent from the
                # workspace — the upstream can be out of the plan (pruned,
                # `--cached` pattern, targeted run) so nothing else pulls
                # them. Pull each absent dep at the md5 its own .dvc records
                # (the upstream out stamp). Invariant: a stage never executes
                # with a declared dep absent when that dep's blob exists in
                # the remote. Deps absent from the remote fall through — the
                # cmd then fails on its own terms.
                # See specs/done/parallel-pull-lock-contention.md (run 5).
                dep_targets = self._missing_dep_pull_targets(path)
                if dep_targets:
                    self._try_materialize_from_remote(dep_targets, label=path)
        except MaterializeError as e:
            self._log(f"  ✗ {path}: materialization failed ({e})")
            return ExecutionResult(
                path=path,
                success=False,
                reason=f"materialization failed: {e}",
            )
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

        # Resume cursor for history deps: the *recorded* shas — where the last
        # successful run left off — not the current tips. A stage walks
        # `$DVX_GIT_LOG_SINCE..HEAD` and processes only what's new, which is
        # what lets it stop reading its own output back in as a cursor: dvx
        # owns freshness, the stage owns the resume point.
        if artifact.computation and artifact.computation.git_log_deps:
            recorded = artifact.computation.get_git_log_dep_shas()
            if recorded:
                env["DVX_GIT_LOG_DEPS"] = json.dumps(recorded, sort_keys=True)
                # Unambiguous only for a single pathspec; with several the
                # stage must read the JSON and pick the cursor it wants.
                if len(recorded) == 1:
                    env["DVX_GIT_LOG_SINCE"] = next(iter(recorded.values()))

        # Run cmd with CWD set to .dvc file's directory
        dvc_dir = Path(path).parent
        cmd_cwd = str(dvc_dir) if str(dvc_dir) != "." else None

        # Charge this stage's memory weight against the level budget while its
        # cmd runs (a no-op when budgeting is off or the weight is 0). Only the
        # primary reaches here — co-outputs verify + write without a cmd — so
        # gating here can't deadlock the primary's wait for its co-outputs.
        weight = self._mem_weight(artifact) if self._budget_gate else 0.0
        if self._budget_gate:
            self._budget_gate.acquire(weight)
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=False,
                env=env,
                cwd=cmd_cwd,
            )
        finally:
            if self._budget_gate:
                self._budget_gate.release(weight)
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
            git_log_deps_shas = {}
            if self.config.provenance and artifact.computation:
                deps_hashes = artifact.computation.get_dep_hashes(recompute=True)
                git_deps_hashes = artifact.computation.get_git_dep_hashes(recompute=True)
                git_log_deps_shas = artifact.computation.get_git_log_dep_shas(recompute=True)

            # Re-assert an author-set `side_effect: true` so it survives the
            # stage's own rewrite even on a fresh write (the merge path in
            # write_dvc_file preserves an existing flag; this covers the
            # no-prior-file case too). Only re-stamp an *explicit* flag —
            # `info.side_effect` is None when side-effect status was merely
            # inferred from "no outs + cmd", and that inference re-derives on
            # read without needing the line written out.
            explicit_side_effect = info.side_effect if info else None
            try:
                dvc_file = write_dvc_file(
                    output_path=Path(path),
                    cmd=cmd if self.config.provenance else None,
                    deps=deps_hashes if self.config.provenance else None,
                    git_deps=git_deps_hashes if self.config.provenance else None,
                    git_log_deps=git_log_deps_shas if self.config.provenance else None,
                    side_effect=explicit_side_effect,
                    fetch_schedule=fetch_schedule,
                    fetch_last_run=fetch_last_run,
                )
                if self.config.verbose:
                    self._log(f"       → {dvc_file}")
            except (FileNotFoundError, ValueError) as e:
                self._log(f"  ⚠ {path}: couldn't write .dvc: {e}")

            self._log(f"  ✓ {path}: side-effect completed ({duration:.1f}s)")
            self._show_success_output(result, log_path, has_output)
            self._signal_dvc_done(path)
            co_paths = self._wait_for_co_outputs(cmd, path)
            self._handle_stage_output(
                path, commit_msg_file.name, summary_file.name, stage_env_extras,
                co_paths=co_paths,
            )
            return ExecutionResult(
                path=path,
                success=True,
                reason="completed",
                duration=duration,
                dvc_file=dvc_file,
            )

        # Verify outputs exist (multi-out aware). For a stage with N outputs
        # declared in its ``.dvc``, every one must be on disk before we hash +
        # write back. Falls back to the single-output ``Path(path)`` check
        # when no ``.dvc`` is present yet (the legacy single-out flow).
        from dvx.run.dvc_files import OutputInfo, read_dvc_file
        existing_info = read_dvc_file(Path(path))
        out = Path(path)
        dvc_dir = out.parent
        declared_outs: list[OutputInfo] = (
            existing_info.outs if existing_info is not None and existing_info.outs else []
        )
        if declared_outs:
            output_paths = [dvc_dir / o.path for o in declared_outs]
        else:
            output_paths = [out]
        missing_outs = [p for p in output_paths if not p.exists()]
        if missing_outs:
            if len(output_paths) > 1:
                missing_str = ", ".join(str(p.relative_to(dvc_dir)) for p in missing_outs)
                self._log(
                    f"  ✗ {path}: command succeeded but output(s) not created: {missing_str}"
                )
            else:
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
        git_log_deps_shas = {}
        if self.config.provenance and artifact.computation:
            deps_hashes = artifact.computation.get_dep_hashes(recompute=True)
            git_deps_hashes = artifact.computation.get_git_dep_hashes(recompute=True)
            git_log_deps_shas = artifact.computation.get_git_log_dep_shas(recompute=True)

        # Write .dvc file for output (multi-out aware).
        dvc_file = None
        hash_changed = False
        try:
            from dvx.cache import cache_blob

            if len(output_paths) > 1:
                # Multi-output: hash + cache each declared out, write the
                # .dvc back with all N entries updated.
                new_outs: list[OutputInfo] = []
                for declared, real_path in zip(declared_outs, output_paths):
                    out_md5 = compute_md5(real_path)
                    out_size = compute_file_size(real_path)
                    hash_changed |= self._report_hash_change(
                        declared.path, declared.md5, declared.size, out_md5, out_size,
                    )
                    is_dir_o = real_path.is_dir()
                    try:
                        cache_blob(real_path, out_md5)
                    except Exception as e:
                        self._log(f"  ⚠ {declared.path}: couldn't cache output: {e}")
                    nfiles_o = None
                    if is_dir_o:
                        nfiles_o = sum(1 for f in real_path.rglob("*") if f.is_file())
                    new_outs.append(
                        OutputInfo(
                            path=declared.path,
                            md5=out_md5,
                            size=out_size,
                            is_dir=is_dir_o,
                            nfiles=nfiles_o,
                        )
                    )

                dvc_file = write_dvc_file(
                    output_path=out,
                    outs=new_outs,
                    cmd=cmd if self.config.provenance else None,
                    deps=deps_hashes if self.config.provenance else None,
                    git_deps=git_deps_hashes if self.config.provenance else None,
                    git_log_deps=git_log_deps_shas if self.config.provenance else None,
                    fetch_schedule=fetch_schedule,
                    fetch_last_run=fetch_last_run,
                )
            else:
                md5 = compute_md5(out)
                size = compute_file_size(out)
                hash_changed |= self._report_hash_change(
                    path,
                    existing_info.md5 if existing_info else None,
                    existing_info.size if existing_info else None,
                    md5,
                    size,
                )
                try:
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
                    git_log_deps=git_log_deps_shas if self.config.provenance else None,
                    fetch_schedule=fetch_schedule,
                    fetch_last_run=fetch_last_run,
                )
            if self.config.verbose:
                self._log(f"       → {dvc_file}")
        except (FileNotFoundError, ValueError) as e:
            self._log(f"  ⚠ {path}: couldn't write .dvc: {e}")

        self._log(f"  ✓ {path}: completed ({duration:.1f}s)")
        self._show_success_output(result, log_path, has_output)
        self._signal_dvc_done(path)
        co_paths = self._wait_for_co_outputs(cmd, path)
        self._handle_stage_output(
            path, commit_msg_file.name, summary_file.name, stage_env_extras,
            co_paths=co_paths,
        )
        return ExecutionResult(
            path=path,
            success=True,
            reason="completed",
            duration=duration,
            dvc_file=dvc_file,
            hash_changed=hash_changed,
        )

    def _report_hash_change(
        self,
        label: str,
        recorded_md5: str | None,
        recorded_size: int | None,
        produced_md5: str,
        produced_size: int | None,
    ) -> bool:
        """Warn when a rerun output's bytes differ from what the .dvc recorded.

        dvx used to swallow this entirely: the .dvc was rewritten and the new
        blob pushed under a bare ``✓ completed``, so a full-DAG reproduction
        that diverged looked exactly like one that didn't. That silence let
        nj-crashes' reproc audit report "byte-identical" for three rounds
        while ~119 of 122 outputs were in fact changing.

        A *first* record (no recorded md5) is not a change — nothing to
        differ from. Returns whether a change was reported, for the summary.
        """
        if not recorded_md5 or recorded_md5 == produced_md5:
            return False
        msg = f"  ⚠ {label}: output hash changed (recorded {recorded_md5} → produced {produced_md5})"
        if recorded_size is not None and produced_size is not None and recorded_size != produced_size:
            delta = produced_size - recorded_size
            pct = f", {delta / recorded_size:+.1%}" if recorded_size else ""
            msg += f"; size {recorded_size:,} → {produced_size:,} ({delta:+,} B{pct})"
        self._log(msg)
        return True

    def _signal_dvc_done(self, path: str) -> None:
        """Mark ``path``'s .dvc as written, releasing any primary waiting on it."""
        ev = self._dvc_done_events.get(path)
        if ev is not None:
            ev.set()

    def _wait_for_co_outputs(self, cmd: str | None, my_path: str) -> list[str]:
        """Block until every same-level co-output of ``cmd`` (other than
        ``my_path``) has finished writing its .dvc. Returns those paths.

        Called from the primary stage's thread before commit + cache push so
        that ``git add -u`` captures every co-output's md5 update and the
        push manifest includes every co-output's blob.

        Cross-level co-outputs are filtered out via ``_scheduled_paths``:
        their ``_execute_artifact`` won't be invoked until a later level
        submits them, so their ``_dvc_done_events`` can never be set —
        waiting would deadlock the pool (as it did on hccs/path when
        ``data/all.{pqt,xlsx}`` landed a level below their ``www/public/``
        siblings despite sharing ``path-data months``).
        """
        if not cmd:
            return []
        co_paths = [
            p for p in self._cmd_artifact_paths.get(cmd, [])
            if p != my_path and p in self._scheduled_paths
        ]
        timeout = self.config.co_output_timeout
        for co_path in co_paths:
            ev = self._dvc_done_events.get(co_path)
            if ev is not None and not ev.wait(timeout=timeout):
                # Belt-and-braces: `_execute_artifact` signals on every exit,
                # so this can only fire if a co-output's thread is wedged.
                # Proceeding with a possibly-incomplete `git add -u` beats
                # hanging the whole run (same principle as the push timeout).
                self._log(
                    f"    ⚠ co-output {co_path} never signalled "
                    f"({timeout:g}s) — proceeding without it"
                )
        return co_paths

    def _handle_co_output(self, artifact: Artifact, cmd: str) -> ExecutionResult:
        """Handle an artifact whose command was already run by another artifact.

        Verifies the output exists and updates its .dvc file. Always signals
        ``_dvc_done_events[path]`` on exit so the primary can proceed even
        if this co-output fails.

        Args:
            artifact: The co-output artifact
            cmd: The command that produced this output

        Returns:
            ExecutionResult for this artifact
        """
        path = artifact.path
        try:
            return self._handle_co_output_inner(artifact, cmd)
        finally:
            ev = self._dvc_done_events.get(path)
            if ev is not None:
                ev.set()

    def _handle_co_output_inner(self, artifact: Artifact, cmd: str) -> ExecutionResult:
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
            from dvx.run.dvc_files import read_dvc_file as _read_dvc
            recorded = _read_dvc(out)
            md5 = compute_md5(out)
            size = compute_file_size(out)
            hash_changed = self._report_hash_change(
                path,
                recorded.md5 if recorded else None,
                recorded.size if recorded else None,
                md5,
                size,
            )

            # Cache the co-output blob
            try:
                from dvx.cache import cache_blob
                cache_blob(out, md5)
            except Exception as e:
                self._log(f"  ⚠ {path}: couldn't cache co-output: {e}")

            deps_hashes = {}
            git_deps_hashes = {}
            git_log_deps_shas = {}
            if self.config.provenance and artifact.computation:
                deps_hashes = artifact.computation.get_dep_hashes(recompute=True)
                git_deps_hashes = artifact.computation.get_git_dep_hashes(recompute=True)
                git_log_deps_shas = artifact.computation.get_git_log_dep_shas(recompute=True)

            dvc_file = write_dvc_file(
                output_path=out,
                md5=md5,
                size=size,
                cmd=cmd if self.config.provenance else None,
                deps=deps_hashes if self.config.provenance else None,
                git_deps=git_deps_hashes if self.config.provenance else None,
                git_log_deps=git_log_deps_shas if self.config.provenance else None,
            )

            self._log(f"  ✓ {path}: co-output ready")
            return ExecutionResult(
                path=path,
                success=True,
                skipped=False,
                reason="co-output",
                dvc_file=dvc_file,
                hash_changed=hash_changed,
            )
        except (FileNotFoundError, ValueError) as e:
            self._log(f"  ✗ {path}: failed to process co-output: {e}")
            return ExecutionResult(
                path=path,
                success=False,
                reason=f"co-output error: {e}",
            )

    def _handle_stage_output(
        self,
        path: str,
        commit_msg_path: str,
        summary_path: str,
        env_extras: dict | None = None,
        co_paths: list[str] | None = None,
    ):
        """Handle post-cmd stage output: commit message, summary, push.

        Args:
            path: Artifact path (for default commit message)
            commit_msg_path: Path to commit message temp file
            summary_path: Path to summary temp file
            env_extras: Additional temp file paths (e.g. push_file)
            co_paths: Other artifact paths sharing this cmd (co-outputs).
                Their .dvc files are included in the cache push manifest
                so their blobs aren't silently left behind in the local
                cache (see specs/done/co-output-push-half-blob.md).
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

            # Push strategy: CLI/env > global config. Resolved independent of
            # commit strategy — the per-stage *cache* push must run even with
            # `--commit never` (the `dvx batch` container default), or a Spot
            # reclaim loses everything since job start instead of one stage.
            # See specs/done/batch-run-command-cli-mismatch.md.
            # Check if stage requested push via $DVX_PUSH_FILE
            push_file = env_extras.get("push_file", "")
            stage_wants_push = (
                os.path.exists(push_file) and os.path.getsize(push_file) > 0
            ) if push_file else False
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
                        # Git push stays commit-gated (a push without a new
                        # commit is a no-op at best, a stale-branch push at
                        # worst); the cache-blob push below is not.
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

            if should_push:
                dvc_paths = [f"{path}.dvc"]
                if co_paths:
                    dvc_paths.extend(f"{p}.dvc" for p in co_paths)
                self._push_cache_blobs(dvc_paths, indent="    ")
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

    def _push_cache_blobs(self, dvc_paths: list[str], indent: str = "") -> None:
        """Push cache blobs for the given .dvc files to the configured remote.

        Lock-free (`dvx.cache.push_targets`): the remote ODB is driven
        directly, so no repo-wide rwlock is taken and every object — file
        blobs, ``.dir`` manifests, AND the inner blobs a manifest names —
        is checked and uploaded individually. That last part subsumes the
        old ``repo.push`` + gap-fill pair, whose existence check
        short-circuited on a present manifest
        (``specs/done/dir-push-shallow-existence-check.md``).

        Bounded: the upload runs in a daemon thread and is abandoned if no
        object settles within ``push_timeout`` seconds. A silent indefinite
        hang here stalled a whole Fargate job for 45 minutes
        (``specs/done/push-each-hang-after-stage.md``); push is non-fatal by
        contract, so a stall must degrade to a warning, not a deadlock.

        Non-fatal: logs warnings on failure but never raises. No-op if
        `cache_push` is disabled or `dvc_paths` is empty.
        """
        if not self.config.cache_push or not dvc_paths:
            return

        from dvx.cache import collect_push_objects, push_targets
        from dvx.gc import format_size

        try:
            keys, local_bytes = collect_push_objects(dvc_paths)
        except Exception as e:
            self._log(f"{indent}⚠ cache push failed: {e}")
            return

        # Logged BEFORE any network call, so a stall is attributable to the
        # push from the log alone (and says how much is in flight).
        obj_word = "object" if len(keys) == 1 else "objects"
        self._log(f"{indent}📤 pushing {len(keys)} {obj_word} ({format_size(local_bytes)})...")

        # Liveness: last time an object settled. A stall timeout (rather than
        # a total one) bounds hangs without capping legitimately long uploads.
        last_progress = [time.monotonic()]
        outcome: list = []

        def _settled(_key, _outcome):
            last_progress[0] = time.monotonic()

        def _work():
            try:
                outcome.append(
                    push_targets(keys, remote=self.config.remote, on_blob=_settled)
                )
            except Exception as e:
                outcome.append(e)

        worker = threading.Thread(target=_work, name="dvx-cache-push", daemon=True)
        worker.start()
        timeout = self.config.push_timeout
        while worker.is_alive():
            worker.join(timeout=1.0)
            if worker.is_alive() and time.monotonic() - last_progress[0] > timeout:
                self._log(
                    f"{indent}⚠ cache push stalled ({timeout:g}s without progress) — "
                    "continuing; re-run `dvx push` to flush"
                )
                return

        result = outcome[0] if outcome else None
        if isinstance(result, Exception):
            self._log(f"{indent}⚠ cache push failed: {result}")
            return
        if result is None:
            self._log(f"{indent}⚠ cache push failed: push produced no result")
            return

        # Both halves, always: "pushed (0 blobs)" after "pushing 3 objects"
        # reads like a failure when it's the healthy byte-identical-rebuild
        # case (nj-crashes run 9). The pre-push line counts candidates; this
        # one splits them into uploaded vs. already-remote.
        self._log(
            f"{indent}📤 cache pushed ({result.uploaded} new, "
            f"{result.already_present} already in remote)"
        )
        if result.missing_locally:
            self._log(
                f"{indent}⚠ {len(result.missing_locally)} blob(s) missing from "
                "remote AND local cache; downstream pulls may fail."
            )


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
    # Paths registered as bare leaves by pruning (below) rather than loaded
    # from their own .dvc. They're provisional: a placeholder means "nothing
    # in the plan so far needs this as a real node". If something later does
    # — it's an explicit target, or a dep of a stage we didn't prune — the
    # placeholder is upgraded rather than treated as an answer. Without that,
    # whichever consumer got popped first silently deleted its own dep from
    # the plan (specs/done/co-output-dep-edge-loss.md).
    placeholders: set[str] = set()
    pending = list(targets)

    # Pruning: skip walking past artifacts that are fresh per their own .dvc.
    # Disabled when --force-upstream patterns are set, since those need a full
    # walk to discover which upstream artifacts to force.
    cfg = config or ExecutionConfig()
    prune_fresh = cfg.prune_fresh and not cfg.force_patterns

    while pending:
        target = pending.pop(0)

        # Get output path from .dvc path
        if str(target).endswith(".dvc"):
            output_path = Path(str(target)[:-4])
        else:
            output_path = target

        output_str = str(output_path)

        if output_str in artifacts and output_str not in placeholders:
            continue

        # Load artifact from .dvc file
        artifact = Artifact.from_dvc(output_path)
        if artifact is None:
            # No .dvc file - treat as leaf
            artifact = Artifact(path=output_str)

        artifacts[output_str] = artifact
        placeholders.discard(output_str)

        # If this artifact is fresh per its own .dvc, stop traversing upstream:
        # any further-upstream state is irrelevant to anything downstream that's
        # already up-to-date. Add deps as bare leaves so _group_into_levels
        # treats them as already satisfied.
        prune_here = (
            prune_fresh
            and artifact.computation is not None
            and is_output_fresh(output_path)[0]
        )

        # Queue dependencies
        if artifact.computation:
            for dep in artifact.computation.deps:
                dep_path = dep.path if isinstance(dep, Artifact) else str(dep)
                if prune_here:
                    if dep_path not in artifacts:
                        # Don't traverse — register as a provisional bare leaf
                        artifacts[dep_path] = Artifact(path=dep_path)
                        placeholders.add(dep_path)
                    continue
                # We're not pruning, so this dep needs to be a real node —
                # even if a pruned consumer already left a placeholder here.
                if dep_path in artifacts and dep_path not in placeholders:
                    continue
                dvc_file = Path(str(dep_path) + ".dvc")
                if dvc_file.exists():
                    pending.append(dvc_file)
                else:
                    # No .dvc file — add as leaf so _group_into_levels sees it in `done`
                    artifacts[dep_path] = Artifact(path=dep_path)
                    placeholders.discard(dep_path)

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
