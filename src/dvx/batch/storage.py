"""Small coordination objects on ordinary DVC-supported storage."""

import json
from typing import Any


def _filesystem(url: str) -> tuple[Any, str]:
    from dvc.exceptions import NotDvcRepoError
    from dvc.fs import get_cloud_fs
    from dvc.repo import Repo

    try:
        with Repo() as repo:
            config = repo.config
    except NotDvcRepoError:
        config = {}
    matching = sorted(
        (
            c
            for c in config.get("remote", {}).values()
            if url.startswith(str(c.get("url", "!")).rstrip("/") + "/")
        ),
        key=lambda c: len(c["url"]),
        reverse=True,
    )
    cloud = {**matching[0], "url": url} if matching else {"url": url}
    cls, options, path = get_cloud_fs(config, **cloud)
    return cls(**options), path


def read_json_url(url: str) -> dict | list:
    fs, path = _filesystem(url)
    with fs.open(path, "rb") as stream:
        raw = stream.read(16 * 1024 * 1024 + 1)
    if len(raw) > 16 * 1024 * 1024:
        raise ValueError("Batch coordination object exceeds 16 MiB")
    return json.loads(raw)


def write_json_url(url: str, value: dict | list) -> None:
    fs, path = _filesystem(url)
    raw = json.dumps(value, sort_keys=True).encode()
    if len(raw) > 16 * 1024 * 1024:
        raise ValueError("Batch coordination object exceeds 16 MiB")
    if fs.protocol == "local":
        from pathlib import Path
        from uuid import uuid4

        dest = Path(path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        temporary = dest.with_name(dest.name + "." + uuid4().hex + ".tmp")
        try:
            temporary.write_bytes(raw)
            temporary.replace(dest)
        finally:
            temporary.unlink(missing_ok=True)
    else:
        with fs.open(path, "wb") as stream:
            stream.write(raw)


def coordination_root(remote: str | None = None) -> str:
    from dvc.fs import get_fs_config
    from dvc.repo import Repo

    with Repo() as repo:
        name = remote or repo.config.get("core", {}).get("remote")
        if not name:
            raise ValueError("Remote execution requires a DVC remote or profile results_url")
        url = get_fs_config(repo.config, name=name)["url"]
    if url.startswith(("http://", "https://")):
        raise ValueError("Remote execution requires writable results_url")
    return url.rstrip("/")
