"""Tests for dvx.stage module."""

import os
import tempfile

from dvx.stage import _Stage


def test_commit_writes_to_env_file(tmp_path):
    """stage.commit() writes message to $DVX_COMMIT_MSG_FILE."""
    msg_file = tmp_path / "commit.txt"
    os.environ["DVX_COMMIT_MSG_FILE"] = str(msg_file)
    try:
        s = _Stage()
        s.commit("Refresh data\n\n3 new records")
        assert msg_file.read_text() == "Refresh data\n\n3 new records"
    finally:
        del os.environ["DVX_COMMIT_MSG_FILE"]


def test_summary_writes_to_env_file(tmp_path):
    """stage.summary() writes text to $DVX_SUMMARY_FILE."""
    summary_file = tmp_path / "summary.txt"
    os.environ["DVX_SUMMARY_FILE"] = str(summary_file)
    try:
        s = _Stage()
        s.summary("5 new crashes found")
        assert summary_file.read_text() == "5 new crashes found"
    finally:
        del os.environ["DVX_SUMMARY_FILE"]


def test_push_writes_to_env_file(tmp_path):
    """stage.push() writes '1' to $DVX_PUSH_FILE."""
    push_file = tmp_path / "push.txt"
    os.environ["DVX_PUSH_FILE"] = str(push_file)
    try:
        s = _Stage()
        s.push()
        assert push_file.read_text() == "1"
    finally:
        del os.environ["DVX_PUSH_FILE"]


def test_is_dvx_run_true(tmp_path):
    """is_dvx_run is True when env vars are set."""
    os.environ["DVX_COMMIT_MSG_FILE"] = str(tmp_path / "x")
    try:
        assert _Stage().is_dvx_run is True
    finally:
        del os.environ["DVX_COMMIT_MSG_FILE"]


def test_is_dvx_run_false():
    """is_dvx_run is False when env vars are not set."""
    env_key = "DVX_COMMIT_MSG_FILE"
    old = os.environ.pop(env_key, None)
    try:
        assert _Stage().is_dvx_run is False
    finally:
        if old is not None:
            os.environ[env_key] = old


def test_noop_without_env_vars():
    """Methods are no-ops when env vars are not set."""
    env_keys = ["DVX_COMMIT_MSG_FILE", "DVX_SUMMARY_FILE", "DVX_PUSH_FILE"]
    saved = {k: os.environ.pop(k, None) for k in env_keys}
    try:
        s = _Stage()
        # Should not raise
        s.commit("test")
        s.summary("test")
        s.push()
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v
