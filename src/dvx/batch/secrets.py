"""Shared secret-reference validation."""


def _reject_env_secret_overlap(
    environment: dict[str, str],
    secrets: dict[str, str],
) -> None:
    """A name delivered as both plaintext ``environment`` and a ``secrets`` ref
    is ambiguous (which value wins is undefined) — reject it."""
    overlap = sorted(set(environment) & set(secrets))
    if overlap:
        raise ValueError("names set as both environment and secrets: " + ", ".join(overlap))
