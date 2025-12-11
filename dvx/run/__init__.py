"""DVX run module - pipeline execution and artifact management.

This module provides the core DVX functionality:
- Artifact and Computation classes for lazy pipeline construction
- DAG-based execution engine
- .dvc file read/write with provenance tracking
- Efficient freshness checking with mtime caching

Example usage:
    from dvx.run import Artifact, Computation, delayed, materialize

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

    # Option B: Write and execute (prep + run)
    materialize([result])
"""

from dvx.run.artifact import (
    Artifact,
    Computation,
    delayed,
    materialize,
    write_all_dvc,
)

__all__ = [
    "Artifact",
    "Computation",
    "delayed",
    "materialize",
    "write_all_dvc",
]
