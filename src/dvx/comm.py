"""Set accounting between local cache, remote(s), and referenced objects.

Powers ``dvx cache comm`` (Venn-style membership table across *locations*)
and the object-accounting side of ``dvx gc`` (``--safe``, `.dir`-manifest
expansion for keep-sets). See ``specs/done/cache-comm-remote-audit.md``.

An *object key* is a cache key: 32 hex chars, with a ``.dir`` suffix for
directory manifests. ``.dir`` manifests and their inner file blobs are
distinct objects — a referenced directory contributes its manifest key AND
every inner blob key (via expansion).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

# key → size in bytes (None = unknown)
ObjectMap = dict[str, "int | None"]

_MD5_RE = re.compile(r"^[0-9a-f]{32}(\.dir)?$")


def _repo_root(repo_path: Path | None = None) -> Path:
    if repo_path is not None:
        return Path(repo_path)
    from dvc.repo import Repo as DVCRepo

    return Path(DVCRepo.find_root())


# ─── location: local cache ──────────────────────────────────────────────────

def local_objects(repo_path: Path | None = None) -> ObjectMap:
    """Objects in the local cache, keyed with ``.dir`` suffix preserved.

    Supports both the modern layout (``.dvc/cache/files/md5/xx/…``) and the
    legacy one (``.dvc/cache/xx/…``).
    """
    root = _repo_root(repo_path)
    objects: ObjectMap = {}
    for cache_dir in (
        root / ".dvc" / "cache" / "files" / "md5",
        root / ".dvc" / "cache",
    ):
        if not cache_dir.is_dir():
            continue
        for prefix_dir in cache_dir.iterdir():
            if not prefix_dir.is_dir() or len(prefix_dir.name) != 2:
                continue
            if not re.match(r"^[0-9a-f]{2}$", prefix_dir.name):
                continue
            for blob in prefix_dir.iterdir():
                if not blob.is_file():
                    continue
                key = prefix_dir.name + blob.name  # preserves .dir suffix
                if _MD5_RE.match(key):
                    objects.setdefault(key, blob.stat().st_size)
    return objects


# ─── location: remote ───────────────────────────────────────────────────────

def _comm_cache_dir(repo_path: Path | None = None) -> Path:
    d = _repo_root(repo_path) / ".dvc" / "tmp" / "comm"
    d.mkdir(parents=True, exist_ok=True)
    return d


def remote_objects(
    remote: str | None = None,
    repo_path: Path | None = None,
    fresh: bool = False,
) -> ObjectMap:
    """Objects in a remote, via one recursive listing (with sizes).

    The listing is the slow step on big remotes, so it's cached under
    ``.dvc/tmp/comm/`` keyed by remote name; ``fresh=True`` refetches.
    """
    cache_file = _comm_cache_dir(repo_path) / f"remote-{remote or 'default'}.json"
    if not fresh and cache_file.exists():
        return json.loads(cache_file.read_text())

    from dvx.cache import _get_remote_odb

    odb = _get_remote_odb(remote)
    objects: ObjectMap = {}

    def _key(path: str) -> str | None:
        parts = path.rsplit("/", 2)
        if len(parts) < 2:
            return None
        key = parts[-2] + parts[-1]
        return key if _MD5_RE.match(key) else None

    # odb.path is the remote's root (…/files/md5 for modern layouts).
    # `find(detail=True)` shape varies by backend: object stores (s3fs, …)
    # return {path: info} with sizes in one pass; dvc's local wrapper
    # degrades to a generator of bare paths (stat per file instead — cheap
    # on local fs, which is the only backend that degrades).
    listing = odb.fs.find(odb.path, detail=True)
    if isinstance(listing, dict):
        for path, info in listing.items():
            if info.get("type") == "directory":
                continue
            key = _key(path)
            if key:
                objects[key] = info.get("size")
    else:
        for entry in listing:
            if isinstance(entry, dict):
                path = entry.get("name", "")
                size = entry.get("size")
            else:
                path = entry
                try:
                    size = odb.fs.size(path)
                except Exception:
                    size = None
            key = _key(path)
            if key:
                objects[key] = size

    cache_file.write_text(json.dumps(objects))
    return objects


# ─── location: ref-sets ─────────────────────────────────────────────────────

@dataclass
class RefSet:
    """Objects referenced by ``.dvc`` files at a rev (or the workspace),
    with ``.dir`` manifests expanded into their inner blob keys.

    ``unexpandable`` lists dir-manifest keys whose manifest couldn't be
    found (local cache or remote) — their children are unknown, so callers
    deciding deletions must treat unknown objects conservatively.
    """

    objects: ObjectMap = field(default_factory=dict)
    unexpandable: list[str] = field(default_factory=list)


def _outs_at_rev(rev: str, repo_path: Path | None = None) -> ObjectMap:
    """(key → size) for every ``outs`` entry of every ``.dvc`` file at a rev."""
    root = _repo_root(repo_path)
    objects: ObjectMap = {}
    result = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", rev],
        cwd=root, capture_output=True, text=True, check=True,
    )
    dvc_files = [
        f for f in result.stdout.strip().split("\n")
        if f.endswith(".dvc") and ".dvc/" not in f
    ]
    for f in dvc_files:
        show = subprocess.run(
            ["git", "show", f"{rev}:{f}"],
            cwd=root, capture_output=True, text=True, check=False,
        )
        if show.returncode != 0:
            continue
        _collect_outs(show.stdout, objects)
    return objects


def _outs_in_workspace(repo_path: Path | None = None) -> ObjectMap:
    """(key → size) for every ``outs`` entry of workspace ``.dvc`` files."""
    root = _repo_root(repo_path)
    objects: ObjectMap = {}
    for p in root.glob("**/*.dvc"):
        if ".dvc/" in str(p) or not p.is_file():
            continue
        try:
            _collect_outs(p.read_text(), objects)
        except OSError:
            continue
    return objects


def _collect_outs(dvc_text: str, objects: ObjectMap) -> None:
    """Parse a .dvc file's ``outs`` entries into ``objects`` (key → size)."""
    import yaml
    try:
        data = yaml.safe_load(dvc_text)
    except yaml.YAMLError:
        return
    if not isinstance(data, dict):
        return
    for out in data.get("outs") or []:
        if not isinstance(out, dict):
            continue
        md5 = out.get("md5")
        if not md5 or not _MD5_RE.match(str(md5)):
            continue
        size = out.get("size")
        objects.setdefault(str(md5), size if isinstance(size, int) else None)


def _all_commit_revs(all_branches: bool, repo_path: Path | None = None) -> list[str]:
    root = _repo_root(repo_path)
    cmd = ["git", "rev-list", "--all"] if all_branches else ["git", "rev-list", "HEAD"]
    result = subprocess.run(cmd, cwd=root, capture_output=True, text=True, check=True)
    return [line for line in result.stdout.strip().split("\n") if line]


def expand_dirs(
    objects: ObjectMap,
    repo_path: Path | None = None,
    remotes: list[str | None] | None = None,
) -> RefSet:
    """Expand every ``.dir`` manifest in ``objects`` into its inner blob keys.

    Manifest lookup order: local cache → each remote (fetched into the local
    cache via the lock-free `_fetch_blob`) → unexpandable.
    """
    from dvx.run.dvc_files import read_dir_manifest

    root = _repo_root(repo_path)
    cache_md5_dir = root / ".dvc" / "cache" / "files" / "md5"
    result = RefSet(objects=dict(objects))
    for key in list(objects):
        if not key.endswith(".dir"):
            continue
        bare = key[:-4]
        manifest = read_dir_manifest(bare, cache_dir=cache_md5_dir)
        if not manifest:
            # Try fetching the manifest from remote(s) into the local cache.
            for remote in (remotes if remotes is not None else [None]):
                try:
                    from dvx.cache import _fetch_blob
                    if _fetch_blob(key, remote):
                        manifest = read_dir_manifest(bare, cache_dir=cache_md5_dir)
                        if manifest:
                            break
                except Exception:
                    continue
        if not manifest:
            result.unexpandable.append(key)
            continue
        for _relpath, file_md5 in manifest.items():
            result.objects.setdefault(file_md5, None)
    return result


def ref_objects(
    refspec: str,
    repo_path: Path | None = None,
    remotes: list[str | None] | None = None,
) -> RefSet:
    """Objects referenced by a ref-set, with ``.dir`` manifests expanded.

    ``refspec`` is one of:
    - ``workspace`` — the checked-out ``.dvc`` files
    - a git rev (``HEAD``, a sha, a branch/tag name)
    - ``--all-commits`` / ``--all-branches`` (every commit reachable from
      HEAD / from any ref)
    """
    if refspec == "workspace":
        outs = _outs_in_workspace(repo_path)
    elif refspec in ("--all-commits", "--all-branches"):
        outs = {}
        for rev in _all_commit_revs(refspec == "--all-branches", repo_path):
            for k, v in _outs_at_rev(rev, repo_path).items():
                outs.setdefault(k, v)
    else:
        outs = _outs_at_rev(refspec, repo_path)
    return expand_dirs(outs, repo_path, remotes)


# ─── membership table ───────────────────────────────────────────────────────

@dataclass
class CommRow:
    """One membership pattern: which locations contain these objects."""

    pattern: tuple[bool, ...]
    count: int
    size: int          # sum of known sizes
    unknown_sizes: int  # objects in this row with no known size
    keys: list[str]


def compute_comm(location_maps: list[ObjectMap]) -> list[CommRow]:
    """Set-arithmetic across N locations.

    Returns one row per membership pattern (all-False excluded), ordered by
    pattern as a binary number with the first location as the high bit —
    i.e. ``(✓,✓,✓), (✓,✓,–), (✓,–,✓), …`` for three locations.
    """
    universe: dict[str, tuple[bool, ...]] = {}
    all_keys: set[str] = set()
    for m in location_maps:
        all_keys.update(m)
    for key in all_keys:
        universe[key] = tuple(key in m for m in location_maps)

    # Best-known size per key across locations.
    def best_size(key: str) -> int | None:
        for m in location_maps:
            s = m.get(key)
            if s is not None:
                return s
        return None

    n = len(location_maps)
    rows = []
    for bits in range(2**n - 1, 0, -1):
        pattern = tuple(bool(bits & (1 << (n - 1 - i))) for i in range(n))
        keys = sorted(k for k, p in universe.items() if p == pattern)
        size = 0
        unknown = 0
        for k in keys:
            s = best_size(k)
            if s is None:
                unknown += 1
            else:
                size += s
        rows.append(CommRow(pattern=pattern, count=len(keys), size=size, unknown_sizes=unknown, keys=keys))
    return rows


def parse_only_pattern(spec: str, labels: list[str]) -> tuple[bool, ...]:
    """Parse ``-o/--only`` patterns like ``local,s3,!HEAD``.

    Every location must be mentioned, either bare (member) or ``!``-prefixed
    (non-member); unmentioned locations are an error (ambiguous).
    """
    wanted: dict[str, bool] = {}
    for token in spec.split(","):
        token = token.strip()
        if not token:
            continue
        if token.startswith("!"):
            wanted[token[1:]] = False
        else:
            wanted[token] = True
    unknown = set(wanted) - set(labels)
    if unknown:
        raise ValueError(f"unknown location(s) in --only: {', '.join(sorted(unknown))}")
    unmentioned = set(labels) - set(wanted)
    if unmentioned:
        raise ValueError(
            f"--only must mention every location (missing: {', '.join(sorted(unmentioned))}); "
            "prefix with '!' for non-membership"
        )
    return tuple(wanted[label] for label in labels)
