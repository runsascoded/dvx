"""Tests for dvx diff command."""

import pytest

from dvx.cli import main


def test_diff_no_args(tmp_dir, dvc, capsys):
    """Test diff with no args shows error."""
    ret = main(["diff"])
    # Should fail - no target specified (unless using --summary)
    assert ret != 0


def test_diff_simple(tmp_dir, scm, dvc):
    """Test basic diff between commits."""
    # Create initial file
    tmp_dir.dvc_gen("file.txt", "initial content", commit="add file")

    # Modify file
    tmp_dir.dvc_gen("file.txt", "modified content", commit="modify file")

    # Diff between commits - should find differences (exit code 1)
    ret = main(["diff", "-r", "HEAD^..HEAD", "file.txt"])
    assert ret == 1


def test_diff_with_dvc_extension(tmp_dir, scm, dvc):
    """Test that .dvc extension is optional."""
    tmp_dir.dvc_gen("data.csv", "a,b,c\n1,2,3\n", commit="add data")
    tmp_dir.dvc_gen("data.csv", "a,b,c\n1,2,3\n4,5,6\n", commit="modify data")

    # Both should work and find differences
    ret1 = main(["diff", "-r", "HEAD^..HEAD", "data.csv"])
    ret2 = main(["diff", "-r", "HEAD^..HEAD", "data.csv.dvc"])

    assert ret1 == 1
    assert ret2 == 1


def test_diff_no_difference(tmp_dir, scm, dvc):
    """Test diff when comparing worktree to HEAD with no changes."""
    tmp_dir.dvc_gen("file.txt", "same content", commit="add file")

    # Compare HEAD to worktree (default) - should be identical since file unchanged
    ret = main(["diff", "file.txt"])
    assert ret == 0


def test_diff_ref_shorthand(tmp_dir, scm, dvc):
    """Test -R shorthand for comparing commit to its parent."""
    tmp_dir.dvc_gen("file.txt", "version 1", commit="v1")
    tmp_dir.dvc_gen("file.txt", "version 2", commit="v2")

    # -R HEAD should compare HEAD^ to HEAD
    ret = main(["diff", "-R", "HEAD", "file.txt"])
    assert ret == 1  # Files differ


def test_diff_nonexistent_file(tmp_dir, dvc):
    """Test error handling for nonexistent file."""
    ret = main(["diff", "nonexistent.txt"])
    assert ret != 0


def test_diff_summary(tmp_dir, scm, dvc, capsys):
    """Test --summary flag shows file/hash changes."""
    tmp_dir.dvc_gen("file.txt", "version 1", commit="v1")
    tmp_dir.dvc_gen("file.txt", "version 2", commit="v2")

    # -s/--summary should show which files changed
    ret = main(["diff", "-s", "-R", "HEAD"])
    assert ret == 0
