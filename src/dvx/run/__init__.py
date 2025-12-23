"""DVX run module - parallel pipeline execution with provenance tracking."""

from dvx.run.artifact import Artifact, Computation, delayed, materialize, write_all_dvc
from dvx.run.dvc_files import (
    DVCFileInfo,
    find_parent_dvc_dir,
    get_dvc_file_path,
    get_file_hash_from_dir,
    get_git_blob_sha,
    get_git_head_sha,
    has_file_changed_since,
    is_output_fresh,
    read_dir_manifest,
    read_dvc_file,
    write_dvc_file,
)
from dvx.run.executor import ExecutionConfig, ExecutionResult, ParallelExecutor, run

__all__ = [
    # Artifact API
    "Artifact",
    "Computation",
    "delayed",
    "materialize",
    "write_all_dvc",
    # DVC file handling
    "DVCFileInfo",
    "find_parent_dvc_dir",
    "get_dvc_file_path",
    "get_file_hash_from_dir",
    "get_git_blob_sha",
    "get_git_head_sha",
    "has_file_changed_since",
    "is_output_fresh",
    "read_dir_manifest",
    "read_dvc_file",
    "write_dvc_file",
    # Execution
    "ExecutionConfig",
    "ExecutionResult",
    "ParallelExecutor",
    "run",
]
