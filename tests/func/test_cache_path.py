import os

from dvx.cli import main


def test_cache_path(tmp_dir, dvc, capsys):
    """Test dvx cache path returns the cache path for a tracked file."""
    tmp_dir.dvc_gen("file.txt", "test content")

    assert main(["cache", "path", "file.txt"]) == 0
    out = capsys.readouterr()[0].strip()

    # Should be a path containing the cache structure
    assert "cache" in out or ".dvc" in out
    assert os.path.exists(out)


def test_cache_path_with_dvc_extension(tmp_dir, dvc, capsys):
    """Test that .dvc extension is optional."""
    tmp_dir.dvc_gen("file.txt", "test content")

    # Both should work
    assert main(["cache", "path", "file.txt"]) == 0
    out1 = capsys.readouterr()[0].strip()

    assert main(["cache", "path", "file.txt.dvc"]) == 0
    out2 = capsys.readouterr()[0].strip()

    assert out1 == out2


def test_cache_path_relative(tmp_dir, dvc, capsys):
    """Test --relative flag outputs relative path."""
    tmp_dir.dvc_gen("file.txt", "test content")

    assert main(["cache", "path", "--relative", "file.txt"]) == 0
    out = capsys.readouterr()[0].strip()

    # Should be relative (not starting with /)
    assert not out.startswith("/")


def test_cache_md5(tmp_dir, dvc, capsys):
    """Test dvx cache md5 returns the MD5 hash for a tracked file."""
    tmp_dir.dvc_gen("file.txt", "test content")

    assert main(["cache", "md5", "file.txt"]) == 0
    out = capsys.readouterr()[0].strip()

    # MD5 hash should be 32 hex characters
    assert len(out) == 32
    assert all(c in "0123456789abcdef" for c in out)


def test_cache_md5_with_dvc_extension(tmp_dir, dvc, capsys):
    """Test that .dvc extension is optional for md5."""
    tmp_dir.dvc_gen("file.txt", "test content")

    assert main(["cache", "md5", "file.txt"]) == 0
    out1 = capsys.readouterr()[0].strip()

    assert main(["cache", "md5", "file.txt.dvc"]) == 0
    out2 = capsys.readouterr()[0].strip()

    assert out1 == out2


def test_cache_path_nonexistent(tmp_dir, dvc, capsys):
    """Test error handling for nonexistent file."""
    ret = main(["cache", "path", "nonexistent.txt"])
    assert ret != 0
