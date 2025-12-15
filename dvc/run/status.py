"""Local artifact status tracking with SQLite.

Tracks mtime and hash of artifacts locally to avoid redundant hash computations.
This is host-local state (not committed to git) that caches the result of
expensive hash computations.

The key insight: if an artifact's mtime hasn't changed since we last hashed it,
we can assume the hash is still valid. This avoids O(file_size) hash computation
on every freshness check.

Usage:
    status_db = ArtifactStatusDB()  # uses .dvc/dvc.db by default

    # Check if we have a cached hash for an artifact
    cached = status_db.get(path)
    if cached and cached.mtime == current_mtime:
        # Use cached hash, skip computation
        hash = cached.hash
    else:
        # Compute hash, update cache
        hash = compute_md5(path)
        status_db.set(path, mtime=current_mtime, hash_value=hash, size=size)
"""

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from threading import local


@dataclass
class ArtifactStatus:
    """Cached status of an artifact."""

    path: str
    mtime: float
    hash: str
    size: int
    updated_at: float


class ArtifactStatusDB:
    """SQLite-based artifact status cache.

    Thread-safe via per-thread connections. Uses WAL mode for better
    concurrent read/write performance.
    """

    def __init__(self, db_path: Path | None = None):
        """Initialize the status database.

        Args:
            db_path: Path to SQLite database. Defaults to .dvc/dvc.db
                     in the current directory (or nearest .dvc parent).
        """
        if db_path is None:
            db_path = self._find_db_path()
        self.db_path = db_path
        self._local = local()  # Thread-local storage for connections
        self._ensure_schema()

    def _find_db_path(self) -> Path:
        """Find or create .dvc/dvc.db path."""
        # Look for .dvc directory starting from cwd and going up
        cwd = Path.cwd()
        for parent in [cwd, *cwd.parents]:
            dvc_dir = parent / ".dvc"
            if dvc_dir.is_dir():
                return dvc_dir / "dvc.db"
        # Fall back to cwd/.dvc/dvc.db (will be created)
        return cwd / ".dvc" / "dvc.db"

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            # Ensure parent directory exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(
                str(self.db_path),
                timeout=30.0,  # Wait up to 30s for locks
                isolation_level=None,  # Autocommit mode
            )
            # Enable WAL mode for better concurrent access
            conn.execute("PRAGMA journal_mode=WAL")
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return self._local.conn

    def _ensure_schema(self):
        """Create tables if they don't exist."""
        conn = self._get_connection()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS artifact_status (
                path TEXT PRIMARY KEY,
                mtime REAL NOT NULL,
                hash TEXT NOT NULL,
                size INTEGER NOT NULL,
                updated_at REAL NOT NULL
            )
        """)
        # Index for quick lookups
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_artifact_status_path
            ON artifact_status(path)
        """)

    def get(self, path: str | Path) -> ArtifactStatus | None:
        """Get cached status for an artifact.

        Args:
            path: Path to the artifact (relative or absolute)

        Returns:
            ArtifactStatus if cached, None otherwise
        """
        path_str = str(path)
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT path, mtime, hash, size, updated_at FROM artifact_status WHERE path = ?",
            (path_str,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return ArtifactStatus(
            path=row[0],
            mtime=row[1],
            hash=row[2],
            size=row[3],
            updated_at=row[4],
        )

    def set(
        self,
        path: str | Path,
        mtime: float,
        hash_value: str,
        size: int,
    ) -> None:
        """Set or update cached status for an artifact.

        Args:
            path: Path to the artifact
            mtime: File modification time
            hash_value: MD5 hash of the artifact
            size: Size in bytes
        """
        path_str = str(path)
        conn = self._get_connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO artifact_status (path, mtime, hash, size, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (path_str, mtime, hash_value, size, time.time()),
        )

    def delete(self, path: str | Path) -> bool:
        """Delete cached status for an artifact.

        Args:
            path: Path to the artifact

        Returns:
            True if a record was deleted, False otherwise
        """
        path_str = str(path)
        conn = self._get_connection()
        cursor = conn.execute(
            "DELETE FROM artifact_status WHERE path = ?",
            (path_str,),
        )
        return cursor.rowcount > 0

    def clear(self) -> int:
        """Clear all cached status records.

        Returns:
            Number of records deleted
        """
        conn = self._get_connection()
        cursor = conn.execute("DELETE FROM artifact_status")
        return cursor.rowcount

    def close(self):
        """Close the database connection."""
        if hasattr(self._local, "conn") and self._local.conn is not None:
            self._local.conn.close()
            self._local.conn = None


# Module-level singleton for convenience
_default_db: ArtifactStatusDB | None = None


def get_status_db() -> ArtifactStatusDB:
    """Get the default status database instance."""
    global _default_db
    if _default_db is None:
        _default_db = ArtifactStatusDB()
    return _default_db


def get_artifact_hash_cached(
    path: Path,
    compute_hash_fn,
) -> tuple[str, int, bool]:
    """Get artifact hash, using cache if mtime unchanged.

    Args:
        path: Path to the artifact
        compute_hash_fn: Function to compute hash if needed (path -> str)

    Returns:
        Tuple of (hash, size, was_cached)
    """
    db = get_status_db()
    path_str = str(path)

    # Get current mtime
    try:
        stat = path.stat()
        current_mtime = stat.st_mtime
        current_size = stat.st_size if path.is_file() else None
    except FileNotFoundError:
        return None, 0, False

    # Check cache
    cached = db.get(path_str)
    if cached is not None and cached.mtime == current_mtime:
        # Cache hit - mtime unchanged, assume hash is still valid
        return cached.hash, cached.size, True

    # Cache miss - compute hash
    hash_value = compute_hash_fn(path)

    # Get size (for directories, need to compute)
    if current_size is None:
        # Directory - sum file sizes
        current_size = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

    # Update cache
    db.set(path_str, mtime=current_mtime, hash_value=hash_value, size=current_size)

    return hash_value, current_size, False
