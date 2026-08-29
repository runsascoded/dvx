"""Tests for reflinking generated outputs onto their cache blobs.

dvx's ingest path copies a worktree output *into* the cache and used to leave
the original as an independent copy — so every ``dvx run``-generated or
``dvx add``-ed file cost 2x on disk until a future re-checkout re-linked it
(DVC reflinks on checkout, but not on generate-in-place). ``_cache_file`` now
replaces the worktree copy with a copy-on-write clone of the cache blob, so it
costs 1x immediately, with a safe copy-mode fallback where reflinks aren't
supported.

Regression of ``specs/done/reflink-generated-outputs.md``.
"""

import fcntl
import os
import struct
import subprocess
import sys
from pathlib import Path

import pytest

from dvx.cache import _reflink, _relink_worktree_from_cache, cache_blob
from dvx.run.hash import compute_md5


def _shares_extents_darwin(a: Path, b: Path) -> bool:
    """Whether ``a`` and ``b`` map file offset 0 to the same physical block.

    Uses ``F_LOG2PHYS_EXT`` (fcntl 49) — the userspace way to tell a CoW clone
    (shared extents) from an independent copy on APFS, without deleting either
    and probing free space. Reflinks and copies are otherwise indistinguishable
    by ``stat``/``du`` (separate inodes, ``nlink=1``, full apparent size).
    """
    def phys(path: Path) -> int:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            out = fcntl.fcntl(fd, 49, struct.pack("=IqQ", 0, 0, 0))
            return struct.unpack("=IqQ", out)[2]
        finally:
            os.close(fd)

    return phys(a) == phys(b)


@pytest.fixture
def reflink_supported(tmp_path) -> bool:
    """True if the filesystem under ``tmp_path`` actually clones."""
    src = tmp_path / ".probe-src"
    dst = tmp_path / ".probe-dst"
    src.write_bytes(b"x" * 4096)
    ok = _reflink(src, dst)
    src.unlink()
    if dst.exists():
        dst.unlink()
    return ok


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A minimal dvc repo so the cache root resolves under ``tmp_path``."""
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["dvc", "init", "-q"], cwd=tmp_path, check=True)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_generated_output_shares_extents_with_its_cache_blob(repo, reflink_supported):
    """The headline: after caching, the worktree file is a CoW clone of the
    blob — 1x on disk, not 2x — and byte-for-byte unchanged."""
    if not reflink_supported:
        pytest.skip("filesystem does not support reflinks")

    out = repo / "big.bin"
    out.write_bytes(os.urandom(2 * 1024 * 1024))
    md5 = compute_md5(out)
    before = out.stat()

    cache_path = cache_blob(out, md5)

    assert out.read_bytes() == Path(cache_path).read_bytes()
    assert compute_md5(out) == md5
    # mtime preserved so the freshness mtime-cache stays valid.
    assert out.stat().st_mtime == before.st_mtime
    assert _shares_extents_darwin(out, Path(cache_path)) is True


def test_a_second_cache_of_a_fresh_copy_relinks_it(repo, reflink_supported):
    """When the blob already exists (a prior identical run) but the worktree
    holds a fresh independent copy, caching again still dedups it."""
    if not reflink_supported:
        pytest.skip("filesystem does not support reflinks")

    payload = os.urandom(1024 * 1024)
    out = repo / "x.bin"
    out.write_bytes(payload)
    md5 = compute_md5(out)
    cache_path = cache_blob(out, md5)  # first run: caches + relinks

    # Simulate a fresh run writing an independent copy of identical bytes.
    out.unlink()
    out.write_bytes(payload)
    assert _shares_extents_darwin(out, Path(cache_path)) is False

    cache_blob(out, md5)  # blob already present; should still relink
    assert _shares_extents_darwin(out, Path(cache_path)) is True


def test_relink_reports_success(repo, reflink_supported):
    out = repo / "y.bin"
    out.write_bytes(b"y" * 65536)
    md5 = compute_md5(out)
    cache_path = repo / ".dvc" / "cache" / "files" / "md5" / md5[:2] / md5[2:]
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(out.read_bytes())

    assert _relink_worktree_from_cache(out, cache_path) is reflink_supported


def test_missing_blob_is_a_safe_noop(repo):
    """No blob to clone from → leave the worktree copy alone, report False."""
    out = repo / "z.bin"
    out.write_bytes(b"z" * 4096)
    assert _relink_worktree_from_cache(out, repo / "does-not-exist") is False
    assert out.read_bytes() == b"z" * 4096


def test_env_kill_switch_disables_relink(repo, monkeypatch):
    """``DVX_NO_REFLINK`` forces the copy path; the output is still a correct,
    independent copy of its blob."""
    monkeypatch.setenv("DVX_NO_REFLINK", "1")
    out = repo / "k.bin"
    out.write_bytes(os.urandom(1024 * 1024))
    md5 = compute_md5(out)

    cache_path = cache_blob(out, md5)

    assert out.read_bytes() == Path(cache_path).read_bytes()
    assert compute_md5(out) == md5
    if sys.platform == "darwin":
        assert _shares_extents_darwin(out, Path(cache_path)) is False


def test_cache_type_copy_opts_out(repo, monkeypatch, reflink_supported):
    """An explicit ``cache.type: copy`` means the user chose copies — honor it
    even where reflinks would work."""
    if not reflink_supported:
        pytest.skip("filesystem does not support reflinks")
    monkeypatch.setattr("dvx.gc.cache_link_types", lambda *a, **k: ["copy"])

    out = repo / "c.bin"
    out.write_bytes(os.urandom(1024 * 1024))
    md5 = compute_md5(out)
    cache_path = cache_blob(out, md5)

    assert _shares_extents_darwin(out, Path(cache_path)) is False


def test_copy_fallback_preserves_content(repo, monkeypatch):
    """Where ``_reflink`` returns False (unsupported FS), the worktree keeps a
    valid independent copy — never a truncated or half-cloned file."""
    monkeypatch.setattr("dvx.cache._reflink", lambda *a, **k: False)
    out = repo / "f.bin"
    payload = os.urandom(3 * 1024 * 1024)
    out.write_bytes(payload)
    md5 = compute_md5(out)

    cache_path = cache_blob(out, md5)

    assert out.read_bytes() == payload
    assert Path(cache_path).read_bytes() == payload
    assert compute_md5(out) == md5


def test_directory_inner_files_are_relinked(repo, reflink_supported):
    """A directory output's inner files ingest through the same ``_cache_file``
    chokepoint, so they dedup too."""
    if not reflink_supported:
        pytest.skip("filesystem does not support reflinks")
    from dvx.cache import add_to_cache

    d = repo / "pyramid"
    d.mkdir()
    (d / "a.bin").write_bytes(os.urandom(512 * 1024))
    (d / "b.bin").write_bytes(os.urandom(512 * 1024))

    add_to_cache(str(d))

    cache_root = repo / ".dvc" / "cache" / "files" / "md5"
    for inner in (d / "a.bin", d / "b.bin"):
        blob = cache_root / compute_md5(inner)[:2] / compute_md5(inner)[2:]
        assert _shares_extents_darwin(inner, blob) is True
