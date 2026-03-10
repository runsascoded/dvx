"""Git-tracked URL imports: download files, commit to Git with URL provenance.

For small files that should live in Git (not DVC cache) but still need
provenance tracking: source URL, download date, ETag, Last-Modified.
"""

import hashlib
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit
from urllib.request import Request, urlopen

import yaml

DEFAULT_USER_AGENT = "dvx/0.1"


def _default_out(url: str) -> str:
    """Derive output filename from URL path."""
    path = urlsplit(url).path
    name = unquote(path.rsplit("/", 1)[-1])
    if not name:
        raise ValueError(f"Cannot derive filename from URL: {url}")
    return name


def _download(
    url: str,
    out: Path,
    user_agent: str | None = None,
) -> tuple[str, int, dict[str, str]]:
    """Download URL to `out`, returning (md5, size, http_headers).

    Headers dict includes ETag, Last-Modified, Content-Length if present.
    """
    ua = user_agent or DEFAULT_USER_AGENT
    req = Request(url, headers={"User-Agent": ua})
    with urlopen(req) as resp:  # noqa: S310
        data = resp.read()
        headers = {
            k: resp.headers[k]
            for k in ("ETag", "Last-Modified", "Content-Length")
            if resp.headers.get(k)
        }

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(data)

    md5 = hashlib.md5(data).hexdigest()  # noqa: S324
    return md5, len(data), headers


def _head_metadata(url: str, user_agent: str | None = None) -> dict[str, str]:
    """HEAD request to get ETag/Last-Modified without downloading."""
    ua = user_agent or DEFAULT_USER_AGENT
    req = Request(url, method="HEAD", headers={"User-Agent": ua})
    with urlopen(req) as resp:  # noqa: S310
        return {
            k: resp.headers[k]
            for k in ("ETag", "Last-Modified", "Content-Length")
            if resp.headers.get(k)
        }


def _parse_last_modified(raw: str) -> str:
    """Parse HTTP Last-Modified header to ISO 8601 string."""
    from email.utils import parsedate_to_datetime

    dt = parsedate_to_datetime(raw)
    return dt.astimezone(timezone.utc).isoformat()


def _build_dvc_data(
    url: str,
    md5: str,
    size: int,
    headers: dict[str, str],
    out_name: str,
    user_agent: str | None = None,
) -> dict:
    """Build the .dvc YAML structure for a git-tracked import."""
    dep: dict = {"path": url}
    if "ETag" in headers:
        dep["checksum"] = headers["ETag"]
    if size:
        dep["size"] = size
    if "Last-Modified" in headers:
        dep["mtime"] = _parse_last_modified(headers["Last-Modified"])
    if user_agent:
        dep["user_agent"] = user_agent

    out_entry = {
        "md5": md5,
        "size": size,
        "hash": "md5",
        "path": out_name,
    }

    data: dict = {
        "deps": [dep],
        "outs": [out_entry],
        "meta": {
            "git_tracked": True,
            "import": {"fetched": date.today().isoformat()},
        },
    }
    return data


def git_import_url(
    url: str,
    out: str | None = None,
    no_download: bool = False,
    user_agent: str | None = None,
) -> Path:
    """Import a URL as a git-tracked file with DVX provenance.

    Downloads the file, computes its MD5, writes a .dvc file with URL
    metadata, and does NOT add to .gitignore or DVC cache.

    Args:
        url: HTTP(S) URL to import.
        out: Output path (default: derived from URL filename).
        no_download: If True, only create .dvc with metadata (HEAD request).
        user_agent: Custom User-Agent header (persisted in .dvc for updates).

    Returns:
        Path to the created .dvc file.
    """
    out_path = Path(out or _default_out(url))

    if no_download:
        headers = _head_metadata(url, user_agent=user_agent)
        size = int(headers.get("Content-Length", 0))
        dvc_data = _build_dvc_data(url, "", size, headers, out_path.name, user_agent=user_agent)
        del dvc_data["outs"][0]["md5"]
    else:
        md5, size, headers = _download(url, out_path, user_agent=user_agent)
        dvc_data = _build_dvc_data(url, md5, size, headers, out_path.name, user_agent=user_agent)

    dvc_path = Path(str(out_path) + ".dvc")
    dvc_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dvc_path, "w") as f:
        yaml.dump(dvc_data, f, sort_keys=False, default_flow_style=False)

    # Remove from .gitignore if DVC previously added it
    _ensure_not_gitignored(out_path)

    return dvc_path


def _ensure_not_gitignored(path: Path) -> None:
    """Remove `path` from .gitignore if present (DVC adds tracked files there)."""
    gitignore = path.parent / ".gitignore"
    if not gitignore.exists():
        return

    target = f"/{path.name}"
    lines = gitignore.read_text().splitlines()
    filtered = [line for line in lines if line.strip() != target]
    if len(filtered) == len(lines):
        return  # not found

    if all(not line.strip() for line in filtered):
        # .gitignore is now empty, remove it
        gitignore.unlink()
    else:
        gitignore.write_text("\n".join(filtered) + "\n")


def update_git_import(dvc_path: Path, no_download: bool = False) -> bool:
    """Re-check a git-tracked import and update if changed.

    Args:
        dvc_path: Path to the .dvc file.
        no_download: If True, only update metadata (HEAD request).

    Returns:
        True if the import was updated, False if unchanged.
    """
    with open(dvc_path) as f:
        data = yaml.safe_load(f)

    if not data or not data.get("deps"):
        return False

    meta = data.get("meta", {})
    if not meta.get("git_tracked"):
        return False

    dep = data["deps"][0]
    url = dep["path"]
    old_checksum = dep.get("checksum")
    user_agent = dep.get("user_agent")
    out_name = data["outs"][0]["path"]
    out_path = dvc_path.parent / out_name

    if no_download:
        headers = _head_metadata(url, user_agent=user_agent)
        new_checksum = headers.get("ETag")
        if new_checksum and new_checksum == old_checksum:
            return False
        size = int(headers.get("Content-Length", 0))
        new_data = _build_dvc_data(url, "", size, headers, out_name, user_agent=user_agent)
        if "md5" in data["outs"][0]:
            new_data["outs"][0]["md5"] = data["outs"][0]["md5"]
        else:
            del new_data["outs"][0]["md5"]
    else:
        md5, size, headers = _download(url, out_path, user_agent=user_agent)
        new_checksum = headers.get("ETag")
        if new_checksum and new_checksum == old_checksum:
            pass
        new_data = _build_dvc_data(url, md5, size, headers, out_name, user_agent=user_agent)

    with open(dvc_path, "w") as f:
        yaml.dump(new_data, f, sort_keys=False, default_flow_style=False)

    return True


def is_git_tracked_import(dvc_path: str | Path) -> bool:
    """Check if a .dvc file represents a git-tracked import."""
    try:
        with open(dvc_path) as f:
            data = yaml.safe_load(f)
        return bool(data and data.get("meta", {}).get("git_tracked"))
    except (OSError, yaml.YAMLError):
        return False
