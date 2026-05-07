"""Regression test for cold-start ``database is locked`` race.

Two worker threads calling ``is_output_fresh`` simultaneously on a fresh
``.dvc/dvx.db`` used to race on:

1. ``get_status_db()`` singleton (no lock — both threads instantiate
   ``ArtifactStatusDB``).
2. ``PRAGMA journal_mode=WAL`` per connection (ignores ``busy_timeout``,
   fails instantly with ``database is locked`` if any other connection
   holds a lock).

Caused intermittent ``✗ <stage>: database is locked`` failures in
``hccs/path``'s daily CI on partial-cache runs (see GHA run 25469756488).
"""

import hashlib
import os
import threading
from pathlib import Path


def _md5(path: Path) -> str:
    h = hashlib.md5()
    h.update(path.read_bytes())
    return h.hexdigest()


def test_concurrent_is_output_fresh_no_db_lock(tmp_path, monkeypatch):
    """Repro: 2 threads × is_output_fresh on a cold ``.dvc/dvx.db``, 100 trials.

    Pre-fix this raced ~10–20% of trials with
    ``OperationalError: database is locked`` from
    ``ArtifactStatusDB._get_connection`` running ``PRAGMA journal_mode=WAL``
    on each per-thread connection. Fix: do the WAL pragma once at DB
    creation (it's persistent in the file) and serialize singleton init.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".dvc").mkdir()
    (tmp_path / "data").mkdir()

    a = tmp_path / "data" / "a.pqt"
    b = tmp_path / "data" / "b.pqt"
    a.write_bytes(b"hello\n" * 100)
    b.write_bytes(b"world\n" * 100)
    a_md5, b_md5 = _md5(a), _md5(b)

    (tmp_path / "data" / "a.pqt.dvc").write_text(
        f"outs:\n- md5: {a_md5}\n  size: {a.stat().st_size}\n  hash: md5\n  path: a.pqt\n"
    )
    (tmp_path / "data" / "b.pqt.dvc").write_text(
        f"outs:\n- md5: {b_md5}\n  size: {b.stat().st_size}\n  hash: md5\n  path: b.pqt\n"
    )

    from dvx.run import status as _status
    from dvx.run.dvc_files import is_output_fresh

    errors: list[BaseException] = []

    def worker(p: Path) -> None:
        try:
            is_output_fresh(p)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    db_files = [tmp_path / ".dvc" / n for n in ("dvx.db", "dvx.db-wal", "dvx.db-shm")]
    for _ in range(100):
        for f in db_files:
            if f.exists():
                f.unlink()
        # Force a fresh singleton each trial so we hit the cold-start path.
        _status._default_db = None

        t1 = threading.Thread(target=worker, args=(a,))
        t2 = threading.Thread(target=worker, args=(b,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

    assert errors == [], (
        f"{len(errors)} thread(s) raised under cold-start race; "
        f"first: {errors[0]!r}"
    )
