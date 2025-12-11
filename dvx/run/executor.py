"""Parallel executor for DVC pipeline stages."""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from dvx.run.dag import DAG
from dvx.run.dvc import DVCClient
from dvx.run.dvc_files import get_git_head_sha, is_output_fresh, write_dvc_file
from dvx.run.hash import compute_md5, compute_file_size


@dataclass
class ExecutionResult:
    """Result of executing a stage."""

    stage_name: str
    success: bool
    skipped: bool = False
    message: str = ""
    dvc_files: list[Path] | None = None  # .dvc files created


class ParallelExecutor:
    """Execute DVC pipeline stages in parallel."""

    def __init__(
        self,
        dag: DAG,
        max_workers: int | None = None,
        dry_run: bool = False,
        output: TextIO = sys.stderr,
        force: bool = False,
        provenance: bool = True,
    ):
        """Initialize parallel executor.

        Args:
            dag: Dependency graph of stages
            max_workers: Maximum number of parallel workers (default: CPU count)
            dry_run: If True, don't actually run stages
            output: Stream for logging output (default: stderr)
            force: If True, skip all freshness checks and re-run everything
            provenance: If True, include provenance metadata in .dvc files
        """
        self.dag = dag
        self.max_workers = max_workers
        self.dry_run = dry_run
        self.output = output
        self.force = force
        self.provenance = provenance
        self.dvc = DVCClient()
        # Capture git SHA once at start for consistent provenance
        self.code_ref = get_git_head_sha() if provenance else None

    def execute(self) -> list[ExecutionResult]:
        """Execute all stages in the DAG, respecting dependencies.

        Returns:
            List of ExecutionResult for each stage

        Raises:
            RuntimeError: If any stage fails
        """
        levels = self.dag.topological_sort()

        self._log(f"Execution plan ({len(levels)} levels, {len(self.dag.stages)} stages):")
        for i, level in enumerate(levels, 1):
            self._log(f"  Level {i}: {', '.join(level)}")

        if self.dry_run:
            self._log("\nDry run - no stages will be executed")
            return []

        self._log("")  # blank line before execution

        results = []
        for level_num, level in enumerate(levels, 1):
            self._log(f"Level {level_num}/{len(levels)}: {len(level)} stage(s)")
            level_results = self._execute_level(level)
            results.extend(level_results)

            # Check for failures
            failures = [r for r in level_results if not r.success and not r.skipped]
            if failures:
                failed_stages = ", ".join(r.stage_name for r in failures)
                raise RuntimeError(f"Stage(s) failed: {failed_stages}")

        return results

    def _execute_level(self, stage_names: list[str]) -> list[ExecutionResult]:
        """Execute all stages in a level in parallel.

        Args:
            stage_names: List of stage names to execute

        Returns:
            List of ExecutionResult, one per stage
        """
        if len(stage_names) == 1:
            # Single stage - run directly without thread pool overhead
            return [self._execute_stage(stage_names[0])]

        # Multiple stages - run in parallel
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._execute_stage, stage_name): stage_name
                for stage_name in stage_names
            }

            for future in as_completed(futures):
                stage_name = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self._log(f"  ✗ {stage_name}: {e}")
                    results.append(
                        ExecutionResult(
                            stage_name=stage_name,
                            success=False,
                            message=str(e),
                        )
                    )

        return results

    def _execute_stage(self, stage_name: str) -> ExecutionResult:
        """Execute a single stage.

        Args:
            stage_name: Name of stage to execute

        Returns:
            ExecutionResult for this stage
        """
        stage = self.dag.stages[stage_name]

        # Check if all outputs are fresh (unless forcing)
        if not self.force and stage.outs:
            all_fresh = True
            for out_path in stage.outs:
                fresh, reason = is_output_fresh(Path(out_path))
                if not fresh:
                    all_fresh = False
                    break

            if all_fresh:
                self._log(f"  ⊙ {stage_name}: up-to-date")
                return ExecutionResult(
                    stage_name=stage_name,
                    success=True,
                    skipped=True,
                    message="up-to-date",
                )

        # Run the stage command
        self._log(f"  ⟳ {stage_name}: running...")
        try:
            self.dvc.run_command(stage.cmd)

            # Compute dependency hashes for provenance
            deps_hashes = {}
            if self.provenance:
                for dep_path in stage.deps:
                    try:
                        deps_hashes[dep_path] = compute_md5(Path(dep_path))
                    except (FileNotFoundError, ValueError) as e:
                        self._log(f"  ⚠ {stage_name}: couldn't hash dep {dep_path}: {e}")

            # Write .dvc files for all outputs
            dvc_files = []
            for out_path in stage.outs:
                out = Path(out_path)
                try:
                    md5 = compute_md5(out)
                    size = compute_file_size(out)

                    dvc_file = write_dvc_file(
                        output_path=out,
                        md5=md5,
                        size=size,
                        cmd=stage.cmd if self.provenance else None,
                        code_ref=self.code_ref,
                        deps=deps_hashes if self.provenance else None,
                    )
                    dvc_files.append(dvc_file)
                    self._log(f"       → {dvc_file}")
                except (FileNotFoundError, ValueError) as e:
                    self._log(f"  ⚠ {stage_name}: couldn't write .dvc for {out_path}: {e}")

            self._log(f"  ✓ {stage_name}: completed")
            return ExecutionResult(
                stage_name=stage_name,
                success=True,
                message="completed",
                dvc_files=dvc_files,
            )
        except RuntimeError as e:
            self._log(f"  ✗ {stage_name}: failed")
            return ExecutionResult(
                stage_name=stage_name,
                success=False,
                message=str(e),
            )

    def _log(self, message: str):
        """Write log message to output stream."""
        print(message, file=self.output)
