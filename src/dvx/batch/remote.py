"""Driver/task metadata exchange and durable re-attachment for remote stages."""

import json
import os
import subprocess
import threading
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path, PurePosixPath
from uuid import uuid4

import yaml

from .base import JobHandle, JobRequest, watch
from .command import run_command
from .storage import coordination_root, read_json_url, write_json_url


def safe_sidecar(path: str) -> Path:
    parts = PurePosixPath(path)
    if parts.is_absolute() or ".." in parts.parts or "\\" in path or not path.endswith(".dvc"):
        raise ValueError(f"Invalid sidecar path: {path!r}")
    real = Path(path).resolve()
    if not real.is_relative_to(Path.cwd().resolve()):
        raise ValueError(f"Sidecar escapes worktree: {path!r}")
    return Path(path)


def apply_overlay(url: str) -> None:
    """Apply the driver's sidecar overlay before task planning."""
    overlay = read_json_url(url)
    if (
        not isinstance(overlay, dict)
        or overlay.get("version") != 1
        or not isinstance(overlay.get("files"), dict)
    ):
        raise ValueError("Invalid Batch sidecar overlay")
    files = overlay["files"]
    validated = [(safe_sidecar(path), content) for path, content in files.items()]
    for path, content in validated:
        if not isinstance(content, str) or not isinstance(yaml.safe_load(content), dict):
            raise ValueError(f"Invalid sidecar content: {path}")
    for path, content in validated:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)


def publish_result(url: str, paths: list[str], results: list) -> None:
    """Upload the complete output closure before publishing the success marker."""
    from dvx.cache import collect_push_objects, push_targets

    success = all(result.success for result in results)
    files = {}
    if success:
        for path in paths:
            sidecar = safe_sidecar(f"{path}.dvc")
            files[str(sidecar)] = sidecar.read_text()
        keys, _ = collect_push_objects(list(files))
        outcome = push_targets(keys, remote=os.environ.get("DVX_RESULT_REMOTE") or None)
        if outcome.missing_locally:
            raise ValueError(f"Result upload incomplete: {outcome.missing_locally}")
    messages = {
        "DVX_COMMIT_MSG_FILE": "\n".join(r.commit_message for r in results if r.commit_message),
        "DVX_SUMMARY_FILE": "\n".join(r.summary for r in results if r.summary),
    }
    for key in ("DVX_COMMIT_MSG_FILE", "DVX_SUMMARY_FILE"):
        path = os.environ.get(key)
        if path and Path(path).is_file():
            messages[key] = Path(path).read_text()
    write_json_url(
        url,
        {
            "version": 1,
            "input_key": os.environ.get("DVX_INPUT_KEY", ""),
            "exit_code": 0 if success else 1,
            "files": files,
            "results": [
                {"path": r.path, "success": r.success, "duration": r.duration} for r in results
            ],
            "messages": messages,
        },
    )


def validate_image(profile: dict, allow_mismatch: bool = False) -> str:
    if allow_mismatch:
        return profile.get("image", "")
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "diff", "HEAD", "--name-only"], capture_output=True, text=True, check=True
    ).stdout.splitlines()
    if any(not path.endswith(".dvc") for path in dirty):
        raise ValueError(
            "Remote execution refuses dirty tracked code; commit it or use --allow-image-mismatch"
        )
    image = profile.get("image")
    if not image:
        raise ValueError("Remote execution profile requires image for revision validation")
    inspected = json.loads(
        subprocess.run(
            ["docker", "image", "inspect", "--format", "{{json .}}", image],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )
    revision = (inspected.get("Config", {}).get("Labels") or {}).get(
        "org.opencontainers.image.revision"
    )
    if revision != head:
        raise ValueError(
            f"Image revision {revision!r} differs from HEAD {head}; rebuild or use --allow-image-mismatch"
        )
    if "@sha256:" in image:
        return image
    repository = image.rsplit(":", 1)[0] if ":" in image.rsplit("/", 1)[-1] else image
    pinned = next(
        (
            digest
            for digest in inspected.get("RepoDigests", [])
            if digest.startswith(repository + "@sha256:")
        ),
        None,
    )
    if not pinned:
        raise ValueError(
            "Image has no registry digest; push/pull it before remote execution or use --allow-image-mismatch"
        )
    return pinned


class RemoteRunner:
    def __init__(self, config, *, backend_factory=None) -> None:
        from . import get_backend

        self.config = config
        self.backend_factory = backend_factory or get_backend
        self.run_id = uuid4().hex
        self.validated = set()
        self.providers = {}
        self._resolved_profiles = {}
        self._provider_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._active = {}
        self._stopped_active = []

    def stop(self, cancel: bool = False) -> bool:
        with self._provider_lock:
            if not self._stop_event.is_set():
                self._stopped_active = list(self._active.values())
            active = self._stopped_active[:]
        self._stop_event.set()
        if cancel:
            for provider, handle in active:
                provider.cancel(handle)
        return bool(active)

    def execute(self, artifacts: list, placement: str, emit=print) -> dict:
        from .config import load_profile

        backend_name, _, profile_name = placement.partition(":")
        profile_name = profile_name or ("dvx" if backend_name == "aws" else "default")
        profile = load_profile(profile_name)
        identity = (backend_name, profile_name)
        with self._provider_lock:
            if identity not in self.validated:
                pinned_image = validate_image(profile, self.config.allow_image_mismatch)
                profile["image"] = pinned_image
                self._resolved_profiles[identity] = profile
                self.validated.add(identity)
            profile = self._resolved_profiles[identity]
            if identity not in self.providers:
                self.providers[identity] = self.backend_factory(backend_name, profile=profile)
            provider = self.providers[identity]
        paths = [a.path for a in artifacts]
        files = {}
        from dvx.run.dvc_files import find_producer

        for artifact in artifacts:
            sidecar = safe_sidecar(f"{artifact.path}.dvc")
            files[str(sidecar)] = sidecar.read_text()
            for dep in artifact.computation.deps:
                path = dep.path if hasattr(dep, "path") else str(dep)
                producer = find_producer(Path(path))
                if producer is not None:
                    sidecar = safe_sidecar(f"{producer}.dvc")
                    files[str(sidecar)] = sidecar.read_text()
        inputs = [
            {
                "path": a.path,
                "cmd": a.computation.cmd,
                "deps": a.computation.get_dep_hashes(recompute=True),
                "git_deps": a.computation.get_git_dep_hashes(recompute=True),
                "git_log_deps": a.computation.get_git_log_dep_shas(recompute=True),
            }
            for a in artifacts
        ]
        input_key = sha256(
            json.dumps(
                {"inputs": inputs, "profile": profile, "placement": placement}, sort_keys=True
            ).encode()
        ).hexdigest()
        stage = sha256("\n".join(sorted(paths)).encode()).hexdigest()[:24]
        record = Path(f".dvx/batch-jobs/{stage}.json")
        prior = json.loads(record.read_text()) if record.is_file() else None
        handle = None
        if prior and prior["input_key"] == input_key:
            handle = JobHandle(**prior["handle"])
            state = provider.status(handle)
            if state.state == "FAILED":
                if not self.config.retry_failed:
                    raise ValueError(
                        f"Previous remote job failed: {handle.name}; use --retry-failed"
                    )
                handle = None
            else:
                emit(f"  ↪ {paths[0]}: attaching to {handle.name}")
        elif prior:
            emit(
                f"  ⚠ {paths[0]}: input key changed; prior job {prior['handle']['name']} is left running if active"
            )
        if handle is None:
            root = profile.get("results_url") or coordination_root(self.config.remote)
            result_url = f"{root.rstrip('/')}/dvx-jobs/{self.run_id}/{stage}/result.json"
            overlay_url = result_url.removesuffix("result.json") + "overlay.json"
            write_json_url(overlay_url, {"version": 1, "files": files})
            resources = artifacts[0].computation.resources
            req = JobRequest(
                run_command(
                    tuple(f"{path}.dvc" for path in paths),
                    force=self.config.force,
                    force_upstream=tuple(self.config.force_patterns),
                    remote=self.config.remote,
                    fallback_remotes=self.config.fallback_remotes,
                ),
                "dvx-" + stage,
                profile=profile_name,
                vcpus=resources.get("cpus"),
                memory_mib=int(resources["mem_gb"] * 1024) if resources.get("mem_gb") else None,
                env={
                    "DVX_RESULT_URL": result_url,
                    "DVX_OVERLAY_URL": overlay_url,
                    "DVX_INPUT_KEY": input_key,
                    "DVX_RESULT_REMOTE": self.config.remote or "",
                },
                labels={"dvx-run": self.run_id, "dvx-stage": paths[0]},
            )
            # The task owns computation, while this process owns placement.
            # A baked per-stage executor setting must not recursively submit.
            req.command[1:1] = ["--executor-for", "*=local"]
            handle = provider.submit(req)
            prior = {"handle": asdict(handle), "input_key": input_key, "result_url": result_url}
            record.parent.mkdir(parents=True, exist_ok=True)
            temp = record.with_suffix("." + uuid4().hex + ".tmp")
            temp.write_text(json.dumps(prior, sort_keys=True) + "\n")
            temp.replace(record)
        with self._provider_lock:
            self._active[handle.name] = (provider, handle)
        try:
            code = watch(provider, handle, emit=emit, stop_event=self._stop_event)
        finally:
            with self._provider_lock:
                self._active.pop(handle.name, None)
        if code:
            raise ValueError(f"Remote job {handle.name} failed with exit {code}")
        result = read_json_url(prior["result_url"])
        expected_paths = sorted(f"{path}.dvc" for path in paths)
        if (
            result.get("version") != 1
            or result.get("input_key") != input_key
            or result.get("exit_code") != 0
            or sorted(result.get("files", {})) != expected_paths
        ):
            raise ValueError("Remote result does not match this stage/input key")
        for path, content in result["files"].items():
            safe_sidecar(path)
            before, after = yaml.safe_load(files[path]), yaml.safe_load(content)
            if not isinstance(after, dict) or [o["path"] for o in before.get("outs", [])] != [
                o["path"] for o in after.get("outs", [])
            ]:
                raise ValueError(f"Remote result changed declared output paths: {path}")
            if before.get("meta", {}).get("computation", {}).get("cmd") != after.get(
                "meta", {}
            ).get("computation", {}).get("cmd"):
                raise ValueError(f"Remote result changed stage command: {path}")
        for path, content in result["files"].items():
            destination = Path(path)
            temporary = destination.with_name(destination.name + "." + uuid4().hex + ".tmp")
            temporary.write_text(content)
            temporary.replace(destination)
        return result
