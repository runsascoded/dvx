"""Tests for `dvx cache comm` + `dvx gc --safe` + `.dir`-aware gc accounting.

See specs/done/cache-comm-remote-audit.md. The fixture builds a real repo
with a local-directory remote, one file stage and one directory stage, via
`dvx run --no-commit --push each` — so cache/remote/HEAD membership are all
genuine, not mocked.
"""

import json
import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dvx.cli import cli
from dvx.comm import (
    compute_comm,
    local_objects,
    parse_only_pattern,
    ref_objects,
    remote_objects,
)


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def comm_repo(tmp_path, monkeypatch):
    """Repo with a local remote, one file stage + one dir stage, built+pushed.

    Returns (repo, remote, keys) where keys maps logical names to cache keys:
    ``file`` (out.txt blob), ``manifest`` (data.dir), ``inner`` (data/f.txt
    blob).
    """
    repo = tmp_path / "repo"
    remote = tmp_path / "remote"
    repo.mkdir()
    remote.mkdir()
    for cmd in (
        ["git", "init", "-b", "main"],
        ["git", "config", "user.email", "t@t"],
        ["git", "config", "user.name", "t"],
        ["dvc", "init"],
        ["dvc", "remote", "add", "-d", "store", str(remote)],
        ["git", "add", "."],
        ["git", "commit", "-m", "init"],
    ):
        subprocess.run(cmd, cwd=repo, check=True, capture_output=True)
    monkeypatch.chdir(repo)

    with open(repo / "out.txt.dvc", "w") as f:
        yaml.dump({
            "outs": [{"path": "out.txt"}],
            "meta": {"computation": {"cmd": "echo 'file-content' > out.txt"}},
        }, f)
    with open(repo / "data.dvc", "w") as f:
        yaml.dump({
            "outs": [{"path": "data"}],
            "meta": {"computation": {"cmd": "mkdir -p data && echo 'inner-content' > data/f.txt"}},
        }, f)
    subprocess.run(["git", "add", "out.txt.dvc", "data.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stubs"], cwd=repo, check=True, capture_output=True)

    result = CliRunner().invoke(cli, ["run", "--no-commit", "--push", "each"])
    assert result.exit_code == 0, result.output
    subprocess.run(["git", "add", "out.txt.dvc", "data.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "built"], cwd=repo, check=True, capture_output=True)

    # Resolve the three cache keys from the written .dvc files.
    out_info = yaml.safe_load((repo / "out.txt.dvc").read_text())
    dir_info = yaml.safe_load((repo / "data.dvc").read_text())
    file_key = out_info["outs"][0]["md5"]
    manifest_key = dir_info["outs"][0]["md5"]
    assert manifest_key.endswith(".dir")
    from dvx.run.dvc_files import read_dir_manifest
    manifest = read_dir_manifest(
        manifest_key[:-4],
        cache_dir=repo / ".dvc" / "cache" / "files" / "md5",
    )
    assert list(manifest) == ["f.txt"]
    inner_key = manifest["f.txt"]

    return repo, remote, {"file": file_key, "manifest": manifest_key, "inner": inner_key}


# ─── object-set primitives ──────────────────────────────────────────────────

def test_local_objects_keys_and_sizes(comm_repo):
    repo, _remote, keys = comm_repo
    objects = local_objects(repo)
    assert sorted(objects) == sorted(keys.values())
    # Sizes are real stat sizes: the file blob is `echo 'file-content'`.
    assert objects[keys["file"]] == len("file-content\n")
    assert objects[keys["inner"]] == len("inner-content\n")


def test_remote_objects_listing_and_cache(comm_repo):
    repo, _remote, keys = comm_repo
    objects = remote_objects("store", repo_path=repo)
    assert sorted(objects) == sorted(keys.values())
    assert objects[keys["file"]] == len("file-content\n")

    # Listing is cached under .dvc/tmp/comm/; a remote-side add doesn't
    # appear until fresh=True.
    cache_file = repo / ".dvc" / "tmp" / "comm" / "remote-store.json"
    assert json.loads(cache_file.read_text()) == objects


def test_ref_objects_head_expands_dir_manifest(comm_repo):
    repo, _remote, keys = comm_repo
    rs = ref_objects("HEAD", repo_path=repo, remotes=["store"])
    assert rs.unexpandable == []
    assert sorted(rs.objects) == sorted(keys.values())
    # Sizes for outs come from the .dvc; inner blobs are unknown (manifest
    # entries carry no size).
    assert rs.objects[keys["file"]] == len("file-content\n")
    assert rs.objects[keys["inner"]] is None


def test_ref_objects_expansion_fetches_manifest_from_remote(comm_repo):
    """A HEAD-referenced `.dir` manifest absent from local cache is fetched
    from the remote for expansion (spec edge case #1)."""
    repo, _remote, keys = comm_repo
    manifest_key = keys["manifest"]
    h = manifest_key[:-4]
    local_manifest = repo / ".dvc" / "cache" / "files" / "md5" / h[:2] / (h[2:] + ".dir")
    local_manifest.unlink()

    rs = ref_objects("HEAD", repo_path=repo, remotes=["store"])
    assert rs.unexpandable == []
    assert sorted(rs.objects) == sorted(keys.values())
    # Side effect: the manifest is back in the local cache.
    assert local_manifest.exists()


def test_ref_objects_unexpandable_when_manifest_nowhere(comm_repo):
    """Manifest missing locally AND remotely → reported unexpandable, not
    silently dropped (children treated conservatively by callers)."""
    repo, remote, keys = comm_repo
    manifest_key = keys["manifest"]
    h = manifest_key[:-4]
    (repo / ".dvc" / "cache" / "files" / "md5" / h[:2] / (h[2:] + ".dir")).unlink()
    (remote / "files" / "md5" / h[:2] / (h[2:] + ".dir")).unlink()

    rs = ref_objects("HEAD", repo_path=repo, remotes=["store"])
    assert rs.unexpandable == [manifest_key]
    # The manifest key itself is still a referenced object; only its
    # children are unknown.
    assert sorted(rs.objects) == sorted([keys["file"], keys["manifest"]])


def test_compute_comm_patterns():
    a = {"k1": 10, "k2": 20}
    b = {"k2": 20, "k3": None}
    rows = compute_comm([a, b])
    by_pattern = {r.pattern: (r.count, r.size, r.unknown_sizes, r.keys) for r in rows}
    assert by_pattern == {
        (True, True): (1, 20, 0, ["k2"]),
        (True, False): (1, 10, 0, ["k1"]),
        (False, True): (1, 0, 1, ["k3"]),
    }


def test_parse_only_pattern():
    assert parse_only_pattern("local,s3,!HEAD", ["local", "s3", "HEAD"]) == (True, True, False)
    assert parse_only_pattern("!local,s3,HEAD", ["local", "s3", "HEAD"]) == (False, True, True)
    with pytest.raises(ValueError) as excinfo:
        parse_only_pattern("local,!HEAD", ["local", "s3", "HEAD"])
    assert str(excinfo.value) == (
        "--only must mention every location (missing: s3); "
        "prefix with '!' for non-membership"
    )
    with pytest.raises(ValueError) as excinfo:
        parse_only_pattern("local,bogus,!HEAD", ["local", "s3", "HEAD"])
    assert str(excinfo.value) == "unknown location(s) in --only: bogus"


# ─── CLI ────────────────────────────────────────────────────────────────────

def test_cache_comm_table_default(runner, comm_repo):
    """No-arg `dvx cache comm` = local vs default remote vs HEAD, one row per
    membership pattern. All three objects are healthy (everywhere)."""
    repo, _remote, _keys = comm_repo
    result = runner.invoke(cli, ["cache", "comm"])
    assert result.exit_code == 0, result.output

    lines = result.output.rstrip().split("\n")
    assert lines[0].split() == ["local", "store", "HEAD", "objects", "size"]
    # Row 1 (✓✓✓) holds all 3 objects; every other pattern is empty.
    rows = [line.split() for line in lines[1:]]
    assert [r[:3] for r in rows] == [
        ["✓", "✓", "✓"],
        ["✓", "✓", "–"],
        ["✓", "–", "✓"],
        ["✓", "–", "–"],
        ["–", "✓", "✓"],
        ["–", "✓", "–"],
        ["–", "–", "✓"],
    ]
    assert rows[0][3] == "3"
    assert [r[3] for r in rows[1:]] == ["–"] * 6


def test_cache_comm_membership_moves(runner, comm_repo):
    """Evicting a local blob & adding unpushed junk move rows as expected."""
    repo, _remote, keys = comm_repo
    # Evict the file blob locally → row (–,✓,✓).
    fk = keys["file"]
    (repo / ".dvc" / "cache" / "files" / "md5" / fk[:2] / fk[2:]).unlink()
    # Add unpushed junk → row (✓,–,–).
    junk_key = "a" * 32
    junk = repo / ".dvc" / "cache" / "files" / "md5" / junk_key[:2] / junk_key[2:]
    junk.parent.mkdir(parents=True, exist_ok=True)
    junk.write_text("junk\n")

    result = runner.invoke(cli, ["cache", "comm"])
    assert result.exit_code == 0, result.output
    rows = [line.split() for line in result.output.rstrip().split("\n")[1:]]
    counts = {tuple(r[:3]): r[3] for r in rows}
    assert counts == {
        ("✓", "✓", "✓"): "2",   # manifest + inner
        ("✓", "✓", "–"): "–",
        ("✓", "–", "✓"): "–",
        ("✓", "–", "–"): "1",   # junk (unpushed garbage)
        ("–", "✓", "✓"): "1",   # evicted file (recoverable)
        ("–", "✓", "–"): "–",
        ("–", "–", "✓"): "–",
    }


def test_cache_comm_only_pattern_output(runner, comm_repo):
    """`-o` emits one `key size` line per object on stdout."""
    repo, _remote, keys = comm_repo
    junk_key = "a" * 32
    junk = repo / ".dvc" / "cache" / "files" / "md5" / junk_key[:2] / junk_key[2:]
    junk.parent.mkdir(parents=True, exist_ok=True)
    junk.write_text("junk\n")

    result = runner.invoke(cli, ["cache", "comm", "-o", "local,!store,!HEAD"])
    assert result.exit_code == 0, result.output
    # CliRunner interleaves the stderr summary with the stdout object list
    # (flush-order dependent) — compare as a set.
    assert set(result.output.rstrip().split("\n")) == {
        "1 object(s), 5 B",
        f"{junk_key} 5",
    }


def test_cache_comm_json(runner, comm_repo):
    repo, _remote, _keys = comm_repo
    result = runner.invoke(cli, ["cache", "comm", "-j"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["locations"] == ["local", "store", "HEAD"]
    assert data["unexpandable_dirs"] == []
    healthy = data["rows"][0]
    assert healthy["pattern"] == {"local": True, "store": True, "HEAD": True}
    assert healthy["objects"] == 3


# ─── gc: `.dir` expansion + --safe ──────────────────────────────────────────

def test_gc_keep_retains_dir_inner_blobs(comm_repo):
    """Version-aware gc must keep the inner blobs of kept dir manifests.

    Regression: `get_referenced_hashes` never expanded `.dir` manifests, so
    `dvx gc --keep 1` computed the inner blob of a HEAD-referenced directory
    as deletable (verified pre-fix: the plan deleted `data/f.txt`'s blob).
    """
    from dvx.gc import compute_gc_plan

    repo, _remote, keys = comm_repo
    keep_hashes, delete_hashes, deletable = compute_gc_plan(keep=1, repo_path=repo)
    assert delete_hashes == set()
    assert keys["inner"] in keep_hashes
    assert keys["manifest"] in keep_hashes


def test_gc_safe_skips_unpushed_deletes_pushed(runner, comm_repo):
    """`gc -w --safe`: unpushed-unreferenced → skipped + reported;
    pushed-unreferenced → deleted; referenced → kept."""
    repo, remote, keys = comm_repo

    # Unpushed junk (must be skipped).
    junk_key = "b" * 32
    junk = repo / ".dvc" / "cache" / "files" / "md5" / junk_key[:2] / junk_key[2:]
    junk.parent.mkdir(parents=True, exist_ok=True)
    junk.write_text("junk\n")

    # Pushed-but-unreferenced blob (must be deleted): plant it both sides.
    old_key = "c" * 32
    for base in (repo / ".dvc" / "cache" / "files" / "md5", remote / "files" / "md5"):
        p = base / old_key[:2] / old_key[2:]
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("old-version\n")

    result = runner.invoke(cli, ["gc", "-w", "--safe", "-f"])
    assert result.exit_code == 0, result.output

    assert result.output.rstrip().split("\n") == [
        "⚠ Skipping 1 blob(s) (5 B) not present in any checked remote — push first or gc without --safe:",
        f"  {junk_key[:12]}...  5 B",
        "Would delete 1 blob(s) (12 B):",
        f"  {old_key[:12]}...  12 B",
        "",
        "Deleted 1 blob(s), freed 12 B.",
    ]

    # Referenced objects + skipped junk survive; the pushed-unreferenced is gone.
    remaining = set(local_objects(repo))
    assert remaining == {keys["file"], keys["manifest"], keys["inner"], junk_key}


def test_gc_safe_composes_with_keep(runner, comm_repo):
    """`gc --keep 1 --safe`: retention computes deletable, --safe partitions."""
    repo, _remote, keys = comm_repo

    # Unpushed junk is deletable-by-retention but must be skipped by --safe.
    junk_key = "d" * 32
    junk = repo / ".dvc" / "cache" / "files" / "md5" / junk_key[:2] / junk_key[2:]
    junk.parent.mkdir(parents=True, exist_ok=True)
    junk.write_text("junk\n")

    result = runner.invoke(cli, ["gc", "--keep", "1", "--safe", "-f"])
    assert result.exit_code == 0, result.output
    assert result.output.rstrip().split("\n") == [
        "⚠ Skipping 1 blob(s) (5 B) not present in any checked remote — push first or gc without --safe:",
        f"  {junk_key[:12]}...  5 B",
        "Nothing to delete.",
    ]
    assert junk.exists()
