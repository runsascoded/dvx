"""DVX run module - pipeline execution and artifact management.

This module provides the core DVX functionality:
- Artifact and Computation classes for lazy pipeline construction
- Parallel execution engine for .dvc file computations
- .dvc file read/write with provenance tracking
- Efficient freshness checking with mtime caching

Example usage:
    from dvc.run import Artifact, Computation, delayed, materialize

    @delayed
    def normalize(ym: str, src: Artifact) -> Artifact:
        return Artifact(
            path=f"normalized/{ym}",
            computation=Computation(cmd=f"ctbk norm {ym}", deps=[src])
        )

    # Build lazy graph
    result = normalize("202501", raw_data)

    # Option A: Write .dvc files only (prep)
    result.write_dvc()

    # Option B: Execute with parallel support
    from dvc.run import run, ExecutionConfig
    run([Path("output.dvc")], ExecutionConfig(max_workers=4))
"""

from dvc.run.artifact import (
    Artifact,
    Computation,
    delayed,
    materialize,
    write_all_dvc,
)
from dvc.run.executor import (
    ExecutionConfig,
    ExecutionResult,
    ParallelExecutor,
    run,
)

__all__ = [
    # artifact module
    "Artifact",
    "Computation",
    # executor module
    "ExecutionConfig",
    "ExecutionResult",
    "ParallelExecutor",
    "delayed",
    "materialize",
    "run",
    "write_all_dvc",
]
