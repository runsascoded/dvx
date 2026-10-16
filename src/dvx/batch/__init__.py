"""Portable Batch backends; legacy AWS imports remain available."""

from typing import Any

from .aws import (
    ECR_LIFECYCLE_POLICY,
    ECS_EXECUTION_POLICY_ARN,
    ECS_TRUST_POLICY,
    EXECUTION_ROLE,
    LOG_GROUP,
    PREFIX,
    RECLAIM_ONLY_RETRY,
    SECRETS_POLICY,
    _clients,
    _drain,
    _ensure_repo,
    _secrets_container_list,
    _wait,
    _watch,
    bootstrap,
    compute_environment_spec,
    execution_role,
    execution_role_secrets_policy,
    job_definition_spec,
    log_group,
    od_name,
    push_commands,
    push_image,
    secrets_policy,
    spot_ce,
    submit,
    submit_overrides,
)
from .base import Backend
from .command import run_command
from .secrets import _reject_env_secret_overlap


def get_backend(name: str, **kwargs: Any) -> Backend:
    if name == "aws":
        from .aws import AwsBackend

        return AwsBackend(**kwargs)
    if name == "gcp":
        from .gcp import GcpBackend

        return GcpBackend(**kwargs)
    raise ValueError(f"Unknown Batch backend: {name!r}")


__all__ = [
    "ECR_LIFECYCLE_POLICY",
    "ECS_EXECUTION_POLICY_ARN",
    "ECS_TRUST_POLICY",
    "EXECUTION_ROLE",
    "LOG_GROUP",
    "PREFIX",
    "RECLAIM_ONLY_RETRY",
    "SECRETS_POLICY",
    "_clients",
    "_drain",
    "_ensure_repo",
    "_reject_env_secret_overlap",
    "_secrets_container_list",
    "_wait",
    "_watch",
    "bootstrap",
    "compute_environment_spec",
    "execution_role",
    "execution_role_secrets_policy",
    "get_backend",
    "job_definition_spec",
    "log_group",
    "od_name",
    "push_commands",
    "push_image",
    "run_command",
    "secrets_policy",
    "spot_ce",
    "submit",
    "submit_overrides",
]
