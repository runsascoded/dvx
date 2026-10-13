"""Explicit composition of content-addressed remotes.

One invocation owns its probes and plan. No remote indexes or listings are
consulted, and only the primary remote can receive uploads. DVC still owns
configuration, object paths, target selection, and workspace checkout.
"""

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from hashlib import md5
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4

from dvc.repo.index import index_from_targets


class TransferError(Exception):
    """A required object is unavailable, invalid, or inaccessible."""


@dataclass(frozen=True)
class Blob:
    oid: str
    path: str
    size: int | None
    action: str
    remote: str | None = None


@dataclass(frozen=True)
class TransferPlan:
    blobs: tuple[Blob, ...]
    errors: tuple[str, ...]

    def classified(self, action: str) -> tuple[Blob, ...]:
        return tuple(blob for blob in self.blobs if blob.action == action)


@dataclass(frozen=True)
class Remote:
    name: str
    odb: Any

    def exists(self, oid: str) -> bool:
        """Probe exactly one object; only definite not-found means False."""
        fs = self.odb.fs
        path = self.odb.oid_to_path(oid)
        try:
            if fs.protocol in {"http", "https"}:
                # fsspec's default HTTP exists() suppresses 403/5xx/timeouts.
                # Its strict form uses DVC's configured auth and retry client.
                return fs.fs.exists(path, strict=True)
            if fs.protocol == "s3":
                # S3 info() may turn a missing key into a prefix listing.
                # A HEAD works for bucket-scoped readers without ListBucket.
                bucket, key, version = fs.fs.split_path(path)
                kwargs = {"Bucket": bucket, "Key": key}
                if version:
                    kwargs["VersionId"] = version
                fs.fs.call_s3("head_object", **kwargs)
            else:
                fs.fs.invalidate_cache(path)
                fs.info(path)
        except FileNotFoundError:
            return False
        except Exception as exc:
            raise TransferError(
                f"{oid}: probe failed on {self.name}: {type(exc).__name__}: {exc}"
            ) from exc
        return True


def _valid_oid(oid: str) -> None:
    if not isinstance(oid, str) or not re.fullmatch(r"[0-9a-f]{32}(?:\.dir)?", oid):
        raise TransferError(f"Invalid MD5 object hash: {oid!r}")


def _valid_blob(path: Path, oid: str) -> bool:
    if not path.is_file():
        return False
    digest = md5(usedforsecurity=False)
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest() == oid.removesuffix(".dir")


def read_manifest_entries(path: Path, oid: str) -> list[dict]:
    """Parse a verified directory blob without silently dropping entries."""
    try:
        entries = json.loads(path.read_bytes())
    except (ValueError, UnicodeError) as exc:
        raise TransferError(f"{oid}: invalid directory manifest") from exc
    if not isinstance(entries, list):
        raise TransferError(f"{oid}: invalid directory manifest")
    seen = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise TransferError(f"{oid}: invalid directory manifest entry")
        relpath = entry.get("relpath")
        if (
            not isinstance(relpath, str)
            or not relpath
            or PurePosixPath(relpath).is_absolute()
            or any(part in {"", ".", ".."} for part in relpath.split("/"))
            or "\\" in relpath
            or "\0" in relpath
            or relpath in seen
        ):
            raise TransferError(f"{oid}: invalid manifest path: {relpath!r}")
        seen.add(relpath)
        _valid_oid(entry.get("md5", ""))
        if entry["md5"].endswith(".dir"):
            raise TransferError(f"{oid}: nested directory manifests are unsupported")
    return entries


class ComposedTransfer:
    """Plan and execute one push, fetch, or pull against named DVC remotes.

    Use as a context manager inside a live DVC Repo context. Temporary
    manifests needed for planning stay outside the permanent local cache.
    """

    def __init__(
        self,
        repo: Any,
        remote: str | None,
        additional: tuple[str, ...],
        jobs: int | None = None,
    ) -> None:
        if jobs is not None and jobs < 1:
            raise TransferError("--jobs must be positive")
        self.repo = repo
        self.jobs = jobs or 32
        primary = repo.cloud.get_remote(name=remote)
        names = list(dict.fromkeys([primary.name, *additional]))
        self.remotes = []
        for name in names:
            configured = repo.cloud.get_remote(name=name)
            if configured.worktree or configured.fs.version_aware:
                raise TransferError(
                    f"{name}: cross-remote transfers require a content-addressed, "
                    "non-versioned remote"
                )
            self.remotes.append(Remote(name, repo.cloud.get_remote_odb(name=name)))
        self.local = repo.cache.local
        self._presence: dict[tuple[str, str], bool] = {}
        self._local_valid: dict[str, bool] = {}
        self._manifests: dict[str, Path] = {}
        self._scratch = TemporaryDirectory(prefix="cross-transfer-", dir=repo.tmp_dir)

    def __enter__(self) -> "ComposedTransfer":
        return self

    def __exit__(self, *args: Any) -> None:
        self._scratch.cleanup()

    def local_path(self, oid: str) -> Path:
        return Path(self.local.oid_to_path(oid))

    def local_valid(self, oid: str) -> bool:
        if oid not in self._local_valid:
            self._local_valid[oid] = _valid_blob(self.local_path(oid), oid)
        return self._local_valid[oid]

    def exists(self, remote: Remote, oid: str) -> bool:
        key = (remote.name, oid)
        if key not in self._presence:
            self._presence[key] = remote.exists(oid)
        return self._presence[key]

    def _source(self, oid: str) -> Remote:
        for remote in self.remotes:
            if self.exists(remote, oid):
                return remote
        names = ", ".join(remote.name for remote in self.remotes)
        raise TransferError(f"{oid}: not found; attempted remotes: {names}")

    def _download(self, oid: str, dest: Path) -> Remote:
        for remote in self.remotes:
            if not self.exists(remote, oid):
                continue
            try:
                remote.odb.fs.get(remote.odb.oid_to_path(oid), str(dest))
            except FileNotFoundError:
                # An object can disappear between planning and download.
                self._presence[(remote.name, oid)] = False
                continue
            except Exception as exc:
                raise TransferError(
                    f"{oid}: download failed on {remote.name}: {type(exc).__name__}: {exc}"
                ) from exc
            if not _valid_blob(dest, oid):
                raise TransferError(f"{oid}: integrity check failed on {remote.name}")
            return remote
        names = ", ".join(remote.name for remote in self.remotes)
        raise TransferError(f"{oid}: not found; attempted remotes: {names}")

    def _manifest(self, oid: str) -> Path:
        if oid not in self._manifests:
            if self.local_valid(oid):
                path = self.local_path(oid)
            else:
                path = Path(self._scratch.name) / oid
                self._download(oid, path)
            self._manifests[oid] = path
        return self._manifests[oid]

    def _entries(self, oid: str) -> list[dict]:
        return read_manifest_entries(self._manifest(oid), oid)

    def plan(
        self,
        targets: list[str] | None,
        direction: str,
        glob: bool = False,
        meta_only: bool = False,
    ) -> TransferPlan:
        from dvc.utils import glob_targets

        from dvx.git_import import is_git_tracked_import

        if direction not in {"push", "pull"}:
            raise ValueError(f"Invalid direction: {direction}")
        selected = glob_targets(targets, glob=glob) if targets else None
        if targets and not selected:
            raise TransferError("No targets matched")
        view = index_from_targets(self.repo, selected, recursive=True)
        candidates: dict[str, tuple[str, int | None]] = {}
        errors = []

        def visit(
            oid: str,
            path: str,
            size: int | None,
            prefix: str | None = None,
        ) -> None:
            _valid_oid(oid)
            candidates.setdefault(oid, (path, size))
            if not oid.endswith(".dir") or meta_only:
                return
            for entry in self._entries(oid):
                relpath = entry["relpath"]
                if prefix and relpath != prefix and not relpath.startswith(prefix + "/"):
                    continue
                child = entry["md5"]
                candidates.setdefault(child, (f"{path}/{relpath}", entry.get("size")))

        for out, filter_info in sorted(view._filtered_outs, key=lambda item: (item[0].fs_path, item[1] or "")):
            if not out.use_cache or is_git_tracked_import(out.stage.path):
                continue
            if direction == "push" and (not out.can_push or out.stage.is_repo_import):
                continue
            if out.remote and out.remote != self.remotes[0].name:
                continue
            if out.protocol != "local" or not out.is_in_repo:
                raise TransferError("Cross-remote transfers support repository-local outputs only")
            if not out.hash_info or out.hash_info.name != "md5":
                raise TransferError(f"{out}: cross-remote transfers require MD5 outputs")
            oid = out.hash_info.value
            if meta_only and not oid.endswith(".dir"):
                continue
            path = os.path.relpath(out.fs_path, self.repo.root_dir)
            prefix = None
            if filter_info and out.fs.isin(filter_info, out.fs_path):
                prefix = out.fs.relpath(filter_info, out.fs_path).replace(os.sep, "/")
            try:
                visit(oid, path, out.meta.size, prefix)
            except TransferError as exc:
                errors.append(str(exc))

        def classify(item: tuple[str, tuple[str, int | None]]) -> Blob | str:
            oid, (path, size) = item
            try:
                if direction == "push":
                    for index, remote in enumerate(self.remotes):
                        if self.exists(remote, oid):
                            action = "destination" if index == 0 else "excluded"
                            return Blob(oid, path, size, action, remote.name)
                    if not self.local_valid(oid):
                        raise TransferError(f"{oid}: missing or invalid local blob required for upload")
                    return Blob(oid, path, self.local_path(oid).stat().st_size, "upload")
                if self.local_valid(oid):
                    return Blob(oid, path, self.local_path(oid).stat().st_size, "local")
                source = self._source(oid)
                if oid in self._manifests:
                    size = self._manifests[oid].stat().st_size
                return Blob(oid, path, size, "download", source.name)
            except TransferError as exc:
                return str(exc)

        with ThreadPoolExecutor(max_workers=self.jobs) as pool:
            results = list(pool.map(classify, sorted(candidates.items())))
        blobs = tuple(result for result in results if isinstance(result, Blob))
        errors.extend(result for result in results if isinstance(result, str))
        return TransferPlan(blobs, tuple(dict.fromkeys(errors)))

    def execute(self, plan: TransferPlan, direction: str) -> int:
        """Transfer the planned objects; a failed plan performs no writes.

        Runtime failures can leave successfully transferred blobs in place,
        but are reported together and never trigger workspace checkout.
        """
        if direction not in {"push", "pull"}:
            raise ValueError(f"Invalid direction: {direction}")
        if plan.errors:
            raise TransferError("\n".join(plan.errors))
        action = "upload" if direction == "push" else "download"
        selected = plan.classified(action)

        def transfer(blob: Blob) -> str | None:
            try:
                if direction == "push":
                    self.remotes[0].odb.fs.put(
                        str(self.local_path(blob.oid)),
                        self.remotes[0].odb.oid_to_path(blob.oid),
                    )
                else:
                    dest = self.local_path(blob.oid)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    temporary = dest.with_name(f"{dest.name}.{uuid4().hex}.tmp")
                    try:
                        manifest = self._manifests.get(blob.oid)
                        if manifest:
                            # Already downloaded and verified during expansion.
                            temporary.write_bytes(manifest.read_bytes())
                        else:
                            self._download(blob.oid, temporary)
                        os.replace(temporary, dest)
                    finally:
                        temporary.unlink(missing_ok=True)
                return None
            except TransferError as exc:
                return str(exc)
            except Exception as exc:
                return f"{blob.oid}: {action} failed: {type(exc).__name__}: {exc}"

        with ThreadPoolExecutor(max_workers=self.jobs) as pool:
            errors = [error for error in pool.map(transfer, selected) if error is not None]
        if errors:
            raise TransferError(
                f"Partial transfer: {len(selected) - len(errors)}/{len(selected)} blob(s) "
                "transferred; target incomplete\n" + "\n".join(errors)
            )
        return len(selected)

    def verify(self, plan: TransferPlan) -> None:
        """Freshly validate availability of the complete planned union."""
        if plan.errors:
            raise TransferError("\n".join(plan.errors))
        self._presence.clear()
        with ThreadPoolExecutor(max_workers=self.jobs) as pool:
            list(pool.map(self._source, (blob.oid for blob in plan.blobs)))
