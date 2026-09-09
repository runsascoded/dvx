"""Tests for dvx.gc module."""

import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from dvx.gc import (
    ArtifactVersion,
    compute_gc_plan,
    format_size,
    get_artifact_versions,
    get_referenced_hashes,
    list_cache_blobs,
    parse_duration,
)


def test_parse_duration():
    """Parse duration strings."""
    assert parse_duration("7d") == timedelta(days=7)
    assert parse_duration("24h") == timedelta(hours=24)
    assert parse_duration("1w") == timedelta(weeks=1)
    assert parse_duration("30d") == timedelta(days=30)


def test_parse_duration_invalid():
    """Invalid duration raises ValueError."""
    with pytest.raises(ValueError):
        parse_duration("abc")
    with pytest.raises(ValueError):
        parse_duration("30m")  # minutes not supported


def test_format_size():
    """Format bytes as human-readable."""
    assert format_size(100) == "100 B"
    assert format_size(1024) == "1.0 KB"
    assert format_size(1048576) == "1.0 MB"
    assert format_size(1073741824) == "1.0 GB"


def test_list_cache_blobs(tmp_path):
    """List blobs in the local cache."""
    # Create a fake cache structure
    cache_dir = tmp_path / ".dvc" / "cache" / "files" / "md5"
    (cache_dir / "ab").mkdir(parents=True)
    (cache_dir / "cd").mkdir(parents=True)

    (cache_dir / "ab" / "c123def456").write_text("blob1")
    (cache_dir / "cd" / "e789f01234").write_text("blob2data")

    blobs = list_cache_blobs(tmp_path)
    assert len(blobs) == 2

    md5s = {md5 for md5, _, _ in blobs}
    assert md5s == {"abc123def456", "cde789f01234"}


@pytest.fixture
def git_repo_with_versions(tmp_path):
    """Create a git repo with multiple versions of a .dvc file."""
    repo = tmp_path / "repo"
    repo.mkdir()

    subprocess.run(["git", "init"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.email", "test@test.com"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, capture_output=True, check=True)

    # Create .dvc dir
    (repo / ".dvc").mkdir()

    # Version 1
    dvc_content = {"outs": [{"md5": "aaaa1111bbbb2222cccc3333dddd4444", "size": 100, "path": "data.txt"}]}
    with open(repo / "data.txt.dvc", "w") as f:
        yaml.dump(dvc_content, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "v1"], cwd=repo, capture_output=True, check=True)

    # Version 2
    dvc_content["outs"][0]["md5"] = "eeee5555ffff6666aaaa7777bbbb8888"
    with open(repo / "data.txt.dvc", "w") as f:
        yaml.dump(dvc_content, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "v2"], cwd=repo, capture_output=True, check=True)

    # Version 3
    dvc_content["outs"][0]["md5"] = "cccc9999dddd0000eeee1111ffff2222"
    with open(repo / "data.txt.dvc", "w") as f:
        yaml.dump(dvc_content, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "v3"], cwd=repo, capture_output=True, check=True)

    return repo


def test_get_artifact_versions(git_repo_with_versions):
    """List all versions of an artifact from git history."""
    versions = get_artifact_versions("data.txt.dvc", repo_path=git_repo_with_versions)

    assert len(versions) == 3
    md5s = [v.md5 for v in versions]
    # Newest first
    assert md5s[0] == "cccc9999dddd0000eeee1111ffff2222"
    assert md5s[1] == "eeee5555ffff6666aaaa7777bbbb8888"
    assert md5s[2] == "aaaa1111bbbb2222cccc3333dddd4444"


def test_get_referenced_hashes(git_repo_with_versions):
    """Get hashes referenced at HEAD."""
    hashes = get_referenced_hashes(repo_path=git_repo_with_versions)

    # Only the current (v3) hash should be referenced at HEAD.
    assert hashes == {"cccc9999dddd0000eeee1111ffff2222"}


def test_compute_gc_plan_keep(git_repo_with_versions):
    """--keep N retains the N newest versions."""
    repo = git_repo_with_versions
    os.chdir(repo)

    # Create cache blobs for all 3 versions
    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in [
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
        "cccc9999dddd0000eeee1111ffff2222",
    ]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    # Keep 2 → oldest version should be deletable
    keep_hashes, delete_hashes, deletable = compute_gc_plan(keep=2, repo_path=repo)

    assert keep_hashes == {
        "eeee5555ffff6666aaaa7777bbbb8888",
        "cccc9999dddd0000eeee1111ffff2222",
    }
    assert delete_hashes == {"aaaa1111bbbb2222cccc3333dddd4444"}
    assert len(deletable) == 1


def test_compute_gc_plan_no_flags(git_repo_with_versions):
    """No --keep/--older-than: keep only HEAD-referenced hashes."""
    repo = git_repo_with_versions
    os.chdir(repo)

    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in [
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
        "cccc9999dddd0000eeee1111ffff2222",
    ]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    # No retention flags → only HEAD hash kept
    keep_hashes, delete_hashes, deletable = compute_gc_plan(repo_path=repo)

    assert keep_hashes == {"cccc9999dddd0000eeee1111ffff2222"}
    assert delete_hashes == {
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
    }
    assert len(deletable) == 2


def test_compute_gc_plan_older_than(git_repo_with_versions):
    """--older-than retains versions newer than the cutoff."""
    repo = git_repo_with_versions
    os.chdir(repo)

    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in [
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
        "cccc9999dddd0000eeee1111ffff2222",
    ]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    # All commits just happened → all are within 1d
    keep_hashes, delete_hashes, deletable = compute_gc_plan(
        older_than="1d", repo_path=repo,
    )
    # Everything recent → nothing to delete
    assert len(deletable) == 0

    # older_than=0d → everything is "older than 0 days"... but HEAD is always kept
    keep_hashes, delete_hashes, deletable = compute_gc_plan(
        older_than="0d", repo_path=repo,
    )
    # HEAD hash always kept (referenced), older versions become deletable.
    assert keep_hashes == {"cccc9999dddd0000eeee1111ffff2222"}
    assert delete_hashes == {
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
    }


def test_compute_gc_plan_keep_and_older_than(git_repo_with_versions):
    """--keep and --older-than combine: keep if EITHER criterion matches."""
    repo = git_repo_with_versions
    os.chdir(repo)

    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in [
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
        "cccc9999dddd0000eeee1111ffff2222",
    ]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    # --keep 1 --older-than 1d: keep 1 newest by count + all within 1d by age
    # Since all commits are recent, all 3 are within 1d → all kept
    keep_hashes, _, deletable = compute_gc_plan(
        keep=1, older_than="1d", repo_path=repo,
    )
    assert len(deletable) == 0


def test_compute_gc_plan_target_specific(git_repo_with_versions):
    """GC targeting a specific .dvc file only considers that artifact."""
    repo = git_repo_with_versions
    os.chdir(repo)

    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in [
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
        "cccc9999dddd0000eeee1111ffff2222",
    ]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    keep_hashes, delete_hashes, deletable = compute_gc_plan(
        keep=1, targets=["data.txt.dvc"], repo_path=repo,
    )
    # Keep newest + HEAD (same hash), delete 2 older
    assert keep_hashes == {"cccc9999dddd0000eeee1111ffff2222"}
    assert delete_hashes == {
        "aaaa1111bbbb2222cccc3333dddd4444",
        "eeee5555ffff6666aaaa7777bbbb8888",
    }
    assert len(deletable) == 2


def test_compute_gc_plan_all_branches(tmp_path):
    """--all-branches considers hashes from all local branches."""
    repo = tmp_path / "repo"
    repo.mkdir()

    subprocess.run(["git", "init", "-b", "main"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.email", "t@t.com"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.name", "T"], cwd=repo, capture_output=True, check=True)
    (repo / ".dvc").mkdir()

    # Main branch: hash A
    dvc = {"outs": [{"md5": "aaaa1111bbbb2222cccc3333dddd4444", "size": 100, "path": "data.txt"}]}
    with open(repo / "data.txt.dvc", "w") as f:
        yaml.dump(dvc, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "main"], cwd=repo, capture_output=True, check=True)

    # Feature branch: hash B
    subprocess.run(["git", "checkout", "-b", "feat"], cwd=repo, capture_output=True, check=True)
    dvc["outs"][0]["md5"] = "bbbb2222cccc3333dddd4444eeee5555"
    with open(repo / "data.txt.dvc", "w") as f:
        yaml.dump(dvc, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "feat"], cwd=repo, capture_output=True, check=True)

    subprocess.run(["git", "checkout", "main"], cwd=repo, capture_output=True, check=True)
    os.chdir(repo)

    # Create cache for both
    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in ["aaaa1111bbbb2222cccc3333dddd4444", "bbbb2222cccc3333dddd4444eeee5555"]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    # Without --all-branches: only main's hash kept, feat's deleted
    keep_hashes, delete_hashes, deletable = compute_gc_plan(repo_path=repo)
    assert keep_hashes == {"aaaa1111bbbb2222cccc3333dddd4444"}
    assert delete_hashes == {"bbbb2222cccc3333dddd4444eeee5555"}
    assert len(deletable) == 1

    # With --all-branches: both kept
    keep_hashes, delete_hashes, deletable = compute_gc_plan(all_branches=True, repo_path=repo)
    assert keep_hashes == {
        "aaaa1111bbbb2222cccc3333dddd4444",
        "bbbb2222cccc3333dddd4444eeee5555",
    }
    assert delete_hashes == set()
    assert len(deletable) == 0


def test_gc_cli_dry_run(tmp_path):
    """CLI dvx gc --keep --dry shows plan without deleting."""
    from click.testing import CliRunner
    from dvx.cli import cli

    repo = tmp_path / "repo"
    repo.mkdir()

    subprocess.run(["git", "init"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.email", "t@t.com"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.name", "T"], cwd=repo, capture_output=True, check=True)

    subprocess.run(["dvc", "init"], cwd=repo, capture_output=True, check=True)

    dvc = {"outs": [{"md5": "aaaa1111bbbb2222cccc3333dddd4444", "size": 100, "path": "d.txt"}]}
    with open(repo / "d.txt.dvc", "w") as f:
        yaml.dump(dvc, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "v1"], cwd=repo, capture_output=True, check=True)

    dvc["outs"][0]["md5"] = "bbbb2222cccc3333dddd4444eeee5555"
    with open(repo / "d.txt.dvc", "w") as f:
        yaml.dump(dvc, f)
    subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "v2"], cwd=repo, capture_output=True, check=True)

    # Create cache blobs
    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in ["aaaa1111bbbb2222cccc3333dddd4444", "bbbb2222cccc3333dddd4444eeee5555"]:
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")

    os.chdir(repo)
    runner = CliRunner()
    # --unsafe: exercise the raw retention plan without a remote (safe is the
    # default on --keep now — see test_gc_keep_* below).
    result = runner.invoke(cli, ["gc", "--keep", "1", "--dry", "--unsafe"])
    assert result.exit_code == 0

    # Output shape: "Would delete N blob(s) (<size>):" then one line per
    # blob ("  <md5-prefix>...  <size>"). md5s are truncated to 12 chars.
    import re
    listed_prefixes = set(re.findall(r"^  ([0-9a-f]{12})\.\.\.", result.output, re.M))
    assert listed_prefixes == {"aaaa1111bbbb"}
    count_match = re.search(r"Would delete (\d+) blob", result.output)
    assert count_match is not None and count_match.group(1) == "1"

    # Blob should NOT be deleted (dry run)
    assert (cache_dir / "aa" / "aa1111bbbb2222cccc3333dddd4444").exists()


# ── safe-by-default on the retention paths (specs/done/gc-safe-by-default-retention.md) ──

_V1 = "aaaa1111bbbb2222cccc3333dddd4444"
_V2 = "eeee5555ffff6666aaaa7777bbbb8888"
_V3 = "cccc9999dddd0000eeee1111ffff2222"


def _cli_repo_3_versions(tmp_path):
    """A real git+dvc repo with 3 committed versions of ``d.txt.dvc`` (md5s
    _V1→_V2→_V3, oldest→newest) and a cache blob on disk for each. Returns
    ``(repo, cache_md5_dir)``; ``blob(md5)`` locates each."""
    repo = tmp_path / "repo"
    repo.mkdir()
    for cmd in (
        ["git", "init"],
        ["git", "config", "user.email", "t@t.com"],
        ["git", "config", "user.name", "T"],
        ["dvc", "init"],
    ):
        subprocess.run(cmd, cwd=repo, capture_output=True, check=True)

    for i, md5 in enumerate((_V1, _V2, _V3), 1):
        dvc = {"outs": [{"md5": md5, "size": 100, "path": "d.txt"}]}
        with open(repo / "d.txt.dvc", "w") as f:
            yaml.dump(dvc, f)
        subprocess.run(["git", "add", "."], cwd=repo, capture_output=True, check=True)
        subprocess.run(["git", "commit", "-m", f"v{i}"], cwd=repo, capture_output=True, check=True)

    cache_dir = repo / ".dvc" / "cache" / "files" / "md5"
    for md5 in (_V1, _V2, _V3):
        d = cache_dir / md5[:2]
        d.mkdir(parents=True, exist_ok=True)
        (d / md5[2:]).write_text("data")
    return repo, cache_dir


def _counts(output):
    """(skipped, deleted) integer counts parsed from a gc run's output."""
    import re
    sk = re.search(r"Skipping (\d+) blob", output)
    dl = re.search(r"Deleted (\d+) blob", output)
    return (int(sk.group(1)) if sk else 0, int(dl.group(1)) if dl else 0)


def test_gc_keep_safe_by_default_retains_local_only_blob(tmp_path, monkeypatch):
    """`--keep` defaults to safe: a superseded version absent from the remote
    is retained + reported, only the remote-backed one is deleted."""
    from click.testing import CliRunner

    from dvx.cli import cli

    repo, cache_dir = _cli_repo_3_versions(tmp_path)
    # Remote holds only _V1 (superseded, backed); _V2 (superseded) is local-only.
    monkeypatch.setattr("dvx.comm.remote_objects", lambda r, fresh=False: {_V1})
    os.chdir(repo)

    result = CliRunner().invoke(cli, ["gc", "--keep", "1", "-f"])
    assert result.exit_code == 0, result.output

    def blob(md5):
        return cache_dir / md5[:2] / md5[2:]

    # _V1 deleted (backed), _V2 retained (local-only, skipped), _V3 kept (policy).
    assert (blob(_V1).exists(), blob(_V2).exists(), blob(_V3).exists()) == (False, True, True)
    assert _counts(result.output) == (1, 1)


def test_gc_keep_unsafe_deletes_local_only_blob(tmp_path):
    """`--unsafe` restores the pre-change behavior: every superseded version is
    deleted regardless of remote membership (and needs no remote)."""
    from click.testing import CliRunner

    from dvx.cli import cli

    repo, cache_dir = _cli_repo_3_versions(tmp_path)
    os.chdir(repo)

    result = CliRunner().invoke(cli, ["gc", "--keep", "1", "-f", "--unsafe"])
    assert result.exit_code == 0, result.output

    def blob(md5):
        return cache_dir / md5[:2] / md5[2:]

    assert (blob(_V1).exists(), blob(_V2).exists(), blob(_V3).exists()) == (False, False, True)
    assert _counts(result.output) == (0, 2)


def test_gc_keep_safe_default_requires_a_remote(tmp_path):
    """Safe-by-default fails loud (deletes nothing) when no remote can be
    verified, pointing at --unsafe — rather than silently deleting."""
    import re

    from click.testing import CliRunner

    from dvx.cli import cli

    repo, cache_dir = _cli_repo_3_versions(tmp_path)
    os.chdir(repo)

    result = CliRunner().invoke(cli, ["gc", "--keep", "1", "-f"])
    assert result.exit_code == 1

    normalized = re.sub(
        r"could not verify a remote \(.*?\); push first",
        "could not verify a remote (<err>); push first",
        result.output,
        flags=re.S,
    )
    assert normalized == (
        "Error: gc is safe by default on --keep/--older-than and could not "
        "verify a remote (<err>); push first, or pass --unsafe to delete "
        "local-only blobs.\n"
    )

    def blob(md5):
        return cache_dir / md5[:2] / md5[2:]

    assert (blob(_V1).exists(), blob(_V2).exists(), blob(_V3).exists()) == (True, True, True)
