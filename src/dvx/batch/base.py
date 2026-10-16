"""Backend-neutral jobs and a bounded, retry-aware log watcher."""

import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class JobRequest:
    command: list[str]
    name: str
    profile: str = "default"
    vcpus: float | None = None
    memory_mib: int | None = None
    env: dict[str, str] = field(default_factory=dict)
    secrets: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)
    task_count: int = 1
    parallelism: int | None = None
    on_demand: bool = False
    max_run_seconds: int | None = None

    def __post_init__(self) -> None:
        if not self.command or not all(isinstance(s, str) and s for s in self.command):
            raise ValueError("command must be a nonempty argv")
        if self.task_count < 1:
            raise ValueError("task_count must be positive")
        if self.parallelism is not None and not 1 <= self.parallelism <= self.task_count:
            raise ValueError("parallelism must be between 1 and task_count")
        for name in ("vcpus", "memory_mib", "max_run_seconds"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive")


@dataclass(frozen=True)
class JobHandle:
    backend: str
    name: str
    uid: str = ""
    profile: str = "default"


@dataclass(frozen=True)
class JobState:
    state: str
    exit_code: int | None = None
    attempt: int = 0
    reason: str = ""
    task_exit_codes: tuple[tuple[int, int], ...] = ()

    @property
    def terminal(self) -> bool:
        return self.state in {"SUCCEEDED", "FAILED"}


@dataclass(frozen=True)
class LogCursor:
    timestamp: str = ""
    insert_ids: frozenset[str] = frozenset()
    source: str = ""
    token: str | None = None


@dataclass(frozen=True)
class LogLine:
    message: str
    task: int | None = None


@dataclass(frozen=True)
class Finding:
    resource: str
    ok: bool
    fix: str = ""


class Backend(Protocol):
    def submit(self, request: JobRequest) -> JobHandle: ...
    def status(self, handle: JobHandle) -> JobState: ...
    def logs(
        self, handle: JobHandle, cursor: LogCursor | None
    ) -> tuple[list[LogLine], LogCursor]: ...
    def cancel(self, handle: JobHandle) -> None: ...
    def push(self, image: str, **kwargs) -> None: ...
    def bootstrap(self, profile: dict, *, check_only: bool) -> list[Finding]: ...


def derive_job_name(targets: tuple[str, ...]) -> str:
    if not targets:
        return "dvx-run"
    return "dvx-" + re.sub(r"[^A-Za-z0-9_-]+", "_", "_".join(targets))[:120]


def watch(
    backend: Backend,
    handle: JobHandle,
    *,
    emit: Callable[[str], None] = print,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    drain_interval: float = 15,
    drain_timeout: float = 180,
    stop_event=None,
) -> int:
    """Back off while running; drain delayed terminal logs until two quiet polls."""
    cursor = None
    previous = None
    delay = 5
    terminal_at = None
    quiet = 0
    warned_missing = False
    while True:
        if stop_event is not None and stop_event.is_set():
            raise InterruptedError("Detached from remote jobs")
        state = backend.status(handle)
        identity = (state.state, state.attempt)
        if identity != previous:
            emit(f"{handle.name}: {state.state} (attempt {state.attempt})")
            previous = identity
            delay = 5
        lines, cursor = backend.logs(handle, cursor)
        if cursor.source == "missing" and not warned_missing:
            emit(f"{handle.name}: log source unavailable")
            warned_missing = True
        for line in lines:
            emit(f"[{line.task}] {line.message}" if line.task is not None else line.message)
        if state.terminal:
            now = clock()
            if terminal_at is None:
                terminal_at = now
                quiet = 0
            else:
                quiet = 0 if lines else quiet + 1
            if quiet >= 2 or now - terminal_at >= drain_timeout:
                if state.reason:
                    emit(f"{handle.name}: {state.reason}")
                for task, code in state.task_exit_codes:
                    if code:
                        emit(f"[{task}] exit {code}")
                if hasattr(backend, "close_logs"):
                    backend.close_logs(handle)
                return (
                    state.exit_code
                    if state.exit_code is not None
                    else (0 if state.state == "SUCCEEDED" else 1)
                )
            stop_event.wait(drain_interval) if stop_event is not None else sleep(drain_interval)
        else:
            stop_event.wait(delay) if stop_event is not None else sleep(delay)
            delay = min(60, delay * 2)
