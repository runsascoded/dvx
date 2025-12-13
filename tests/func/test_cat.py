import os

import pytest

from dvx.cli import main


def test_cat(tmp_dir, dvc, capsys):
    """Test dvx cat outputs file contents from cache."""
    content = "test content for cat"
    tmp_dir.dvc_gen("file.txt", content)

    assert main(["cat", "file.txt"]) == 0
    out = capsys.readouterr()[0]

    assert content in out


def test_cat_with_dvc_extension(tmp_dir, dvc, capsys):
    """Test that .dvc extension is optional."""
    content = "test content"
    tmp_dir.dvc_gen("file.txt", content)

    assert main(["cat", "file.txt"]) == 0
    out1 = capsys.readouterr()[0]

    assert main(["cat", "file.txt.dvc"]) == 0
    out2 = capsys.readouterr()[0]

    assert out1 == out2


def test_cat_nonexistent(tmp_dir, dvc, capsys):
    """Test error handling for nonexistent file."""
    ret = main(["cat", "nonexistent.txt"])
    assert ret != 0
