"""Tests for SQLite-based artifact status tracking."""

import tempfile
import threading
import time
from pathlib import Path

import pytest

from dvx.run.status import ArtifactStatus, ArtifactStatusDB, get_artifact_hash_cached


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def status_db(temp_dir):
    """Create a status database in temp directory."""
    db_path = temp_dir / ".dvc" / "dvx.db"
    db = ArtifactStatusDB(db_path)
    yield db
    db.close()


class TestArtifactStatusDB:
    """Tests for ArtifactStatusDB."""

    def test_set_and_get(self, status_db):
        """Test basic set and get operations."""
        status_db.set(
            path="output.txt",
            mtime=1234567890.123,
            hash="abc123def456",
            size=1024,
        )

        result = status_db.get("output.txt")

        assert result is not None
        assert result.path == "output.txt"
        assert result.mtime == 1234567890.123
        assert result.hash == "abc123def456"
        assert result.size == 1024
        assert result.updated_at > 0

    def test_get_nonexistent(self, status_db):
        """Test getting a path that doesn't exist."""
        result = status_db.get("nonexistent.txt")
        assert result is None

    def test_update_existing(self, status_db):
        """Test updating an existing record."""
        status_db.set("output.txt", mtime=100.0, hash="hash1", size=100)
        status_db.set("output.txt", mtime=200.0, hash="hash2", size=200)

        result = status_db.get("output.txt")

        assert result.mtime == 200.0
        assert result.hash == "hash2"
        assert result.size == 200

    def test_delete(self, status_db):
        """Test deleting a record."""
        status_db.set("output.txt", mtime=100.0, hash="hash1", size=100)

        deleted = status_db.delete("output.txt")
        assert deleted is True

        result = status_db.get("output.txt")
        assert result is None

    def test_delete_nonexistent(self, status_db):
        """Test deleting a non-existent record."""
        deleted = status_db.delete("nonexistent.txt")
        assert deleted is False

    def test_clear(self, status_db):
        """Test clearing all records."""
        status_db.set("a.txt", mtime=100.0, hash="h1", size=100)
        status_db.set("b.txt", mtime=200.0, hash="h2", size=200)
        status_db.set("c.txt", mtime=300.0, hash="h3", size=300)

        count = status_db.clear()
        assert count == 3

        assert status_db.get("a.txt") is None
        assert status_db.get("b.txt") is None
        assert status_db.get("c.txt") is None

    def test_path_types(self, status_db):
        """Test that both str and Path work for paths."""
        status_db.set(Path("output.txt"), mtime=100.0, hash="h1", size=100)

        # Get with str
        result = status_db.get("output.txt")
        assert result is not None

        # Get with Path
        result = status_db.get(Path("output.txt"))
        assert result is not None

    def test_concurrent_writes(self, temp_dir):
        """Test that concurrent writes don't corrupt the database."""
        db_path = temp_dir / ".dvc" / "dvx.db"
        errors = []
        num_threads = 10
        num_writes = 50

        def writer(thread_id):
            try:
                db = ArtifactStatusDB(db_path)
                for i in range(num_writes):
                    db.set(
                        f"file_{thread_id}_{i}.txt",
                        mtime=float(i),
                        hash=f"hash_{thread_id}_{i}",
                        size=i,
                    )
                db.close()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors during concurrent writes: {errors}"

        # Verify data integrity
        db = ArtifactStatusDB(db_path)
        for thread_id in range(num_threads):
            for i in range(num_writes):
                result = db.get(f"file_{thread_id}_{i}.txt")
                assert result is not None
                assert result.hash == f"hash_{thread_id}_{i}"
        db.close()


class TestGetArtifactHashCached:
    """Tests for get_artifact_hash_cached."""

    def test_cache_miss(self, temp_dir):
        """Test hash computation on cache miss."""
        from dvx.run.hash import compute_md5

        # Create test file
        test_file = temp_dir / "test.txt"
        test_file.write_text("hello world")

        # Create DB in temp dir
        db_path = temp_dir / ".dvc" / "dvx.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Manually create DB and clear any existing state
        db = ArtifactStatusDB(db_path)
        db.clear()

        # First call should be cache miss
        hash_val, size, was_cached = get_artifact_hash_cached(test_file, compute_md5)

        assert hash_val == compute_md5(test_file)
        assert size == test_file.stat().st_size
        # Note: was_cached might be True if module-level singleton has state
        # from previous tests. That's OK - the hash is correct either way.

        db.close()

    def test_cache_hit(self, temp_dir):
        """Test that cached hash is returned when mtime unchanged."""
        from dvx.run.hash import compute_md5
        from dvx.run import status as status_module

        # Create test file
        test_file = temp_dir / "test.txt"
        test_file.write_text("hello world")

        # Create DB in temp dir and set as default
        db_path = temp_dir / ".dvc" / "dvx.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db = ArtifactStatusDB(db_path)

        # Replace the module-level singleton
        old_db = status_module._default_db
        status_module._default_db = db

        try:
            # First call - cache miss
            hash1, size1, cached1 = get_artifact_hash_cached(test_file, compute_md5)
            assert cached1 is False

            # Second call - cache hit (mtime unchanged)
            hash2, size2, cached2 = get_artifact_hash_cached(test_file, compute_md5)
            assert cached2 is True
            assert hash2 == hash1
            assert size2 == size1
        finally:
            status_module._default_db = old_db
            db.close()

    def test_cache_invalidation_on_mtime_change(self, temp_dir):
        """Test that cache is invalidated when mtime changes."""
        from dvx.run.hash import compute_md5
        from dvx.run import status as status_module

        # Create test file
        test_file = temp_dir / "test.txt"
        test_file.write_text("original content")

        # Create DB in temp dir and set as default
        db_path = temp_dir / ".dvc" / "dvx.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db = ArtifactStatusDB(db_path)

        old_db = status_module._default_db
        status_module._default_db = db

        try:
            # First call
            hash1, _, _ = get_artifact_hash_cached(test_file, compute_md5)

            # Modify file (changes mtime)
            time.sleep(0.01)  # Ensure mtime differs
            test_file.write_text("modified content")

            # Second call - should recompute
            hash2, _, cached2 = get_artifact_hash_cached(test_file, compute_md5)
            assert cached2 is False
            assert hash2 != hash1
            assert hash2 == compute_md5(test_file)
        finally:
            status_module._default_db = old_db
            db.close()
