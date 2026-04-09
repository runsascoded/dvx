"""Stage ↔ harness IPC library for DVX pipeline stages.

Provides a convenient API for stages running under ``dvx run`` to
communicate back to the DVX harness via env-var-referenced temp files.

Usage::

    from dvx.stage import stage

    stage.commit("Refresh data: 5 new records")
    stage.summary("5 new records")
    stage.push()  # request immediate push after this stage's commit

Non-Python stages can write to the env var paths directly.
"""

import os


class _Stage:
    """Singleton interface for stage → DVX harness communication."""

    @property
    def is_dvx_run(self) -> bool:
        """True if running under ``dvx run`` (env vars are set)."""
        return "DVX_COMMIT_MSG_FILE" in os.environ

    def commit(self, message: str) -> None:
        """Request a git commit after this stage with the given message."""
        path = os.environ.get("DVX_COMMIT_MSG_FILE")
        if path:
            with open(path, "w") as f:
                f.write(message)

    def summary(self, text: str) -> None:
        """Set a short summary line displayed in ``dvx run`` output."""
        path = os.environ.get("DVX_SUMMARY_FILE")
        if path:
            with open(path, "w") as f:
                f.write(text)

    def push(self) -> None:
        """Request an immediate push after this stage's commit."""
        path = os.environ.get("DVX_PUSH_FILE")
        if path:
            with open(path, "w") as f:
                f.write("1")


stage = _Stage()
