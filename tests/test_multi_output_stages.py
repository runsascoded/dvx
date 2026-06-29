"""Tests for multi-output ``.dvc`` files.

DVC's ``.dvc`` format supports ``outs: [...]`` with N entries; DVX
historically modeled only ``outs[0]``. These tests verify the multi-output
MVP scope: parsing, freshness checks, post-run verification + cache + .dvc
rewrite, and end-to-end ``dvx push``/``dvx status`` flows.

Regression of ``specs/done/multi-output-stages.md``.
"""

import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dvx.cli import cli
from dvx.run.dvc_files import (
    DVCFileInfo,
    OutputInfo,
    get_freshness_details,
    is_output_fresh,
    read_dvc_file,
)


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def repo_with_remote(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    remote = tmp_path / "remote"
    repo.mkdir()
    remote.mkdir()
    for cmd in (
        ["git", "init", "-b", "main"],
        ["git", "config", "user.email", "t@t"],
        ["git", "config", "user.name", "t"],
        ["dvc", "init"],
        ["dvc", "remote", "add", "-d", "local", str(remote)],
        ["git", "add", "."],
        ["git", "commit", "-m", "init"],
    ):
        subprocess.run(cmd, cwd=repo, check=True, capture_output=True)
    monkeypatch.chdir(repo)
    return repo, remote


# ────────────────────────────────────────────────────────────────────────────
# Parser: DVCFileInfo.outs is populated
# ────────────────────────────────────────────────────────────────────────────


def test_read_dvc_file_populates_outs_for_single_output(tmp_path, monkeypatch):
    """Single-out .dvc still produces a 1-element ``outs`` list and the
    scalar fields ``md5/size/path/is_dir`` mirror ``outs[0]``."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "a.txt").write_text("aaa\n")
    dvc_path = tmp_path / "a.txt.dvc"
    dvc_path.write_text(yaml.dump({
        "outs": [{
            "md5": "abc1234567890abcdef1234567890abcd",
            "size": 4,
            "hash": "md5",
            "path": "a.txt",
        }],
    }))

    info = read_dvc_file(tmp_path / "a.txt")
    assert info is not None
    assert info.outs == [OutputInfo(
        path="a.txt",
        md5="abc1234567890abcdef1234567890abcd",
        size=4,
        is_dir=False,
        nfiles=None,
    )]
    # Back-compat scalars match outs[0].
    assert info.md5 == "abc1234567890abcdef1234567890abcd"
    assert info.size == 4
    assert info.path == "a.txt"
    assert info.is_dir is False


def test_read_dvc_file_populates_outs_for_multi_output(tmp_path, monkeypatch):
    """Multi-out .dvc surfaces every entry in ``outs``.

    The historical bug: ``read_dvc_file`` read only ``data["outs"][0]`` and
    dropped the rest, so any code path keyed on ``info`` lost N-1 outputs.
    """
    monkeypatch.chdir(tmp_path)
    dvc_path = tmp_path / "csvs.dvc"
    dvc_path.write_text(yaml.dump({
        "outs": [
            {"md5": "1" * 32, "size": 5, "hash": "md5", "path": "ytd.parquet"},
            {"md5": "2" * 32, "size": 6, "hash": "md5", "path": "monthly.parquet"},
            {"md5": "3" * 32, "size": 7, "hash": "md5", "path": "month-year.parquet"},
        ],
        "meta": {"computation": {"cmd": "echo run"}},
    }))

    info = read_dvc_file(tmp_path / "csvs")
    assert info is not None
    assert info.outs == [
        OutputInfo(path="ytd.parquet", md5="1" * 32, size=5, is_dir=False, nfiles=None),
        OutputInfo(path="monthly.parquet", md5="2" * 32, size=6, is_dir=False, nfiles=None),
        OutputInfo(path="month-year.parquet", md5="3" * 32, size=7, is_dir=False, nfiles=None),
    ]
    # Scalar shims mirror outs[0].
    assert info.md5 == "1" * 32
    assert info.path == "ytd.parquet"
    assert info.size == 5


def test_read_dvc_file_preserves_dir_outputs_in_multi(tmp_path, monkeypatch):
    """Mixed multi-out (one dir + files): ``.dir`` suffix is stripped on the
    OutputInfo.md5 but ``is_dir`` reflects it."""
    monkeypatch.chdir(tmp_path)
    dvc_path = tmp_path / "mixed.dvc"
    dvc_path.write_text(yaml.dump({
        "outs": [
            {"md5": ("a" * 32) + ".dir", "size": 100, "hash": "md5",
             "path": "d", "nfiles": 3},
            {"md5": "b" * 32, "size": 5, "hash": "md5", "path": "f.txt"},
        ],
    }))
    info = read_dvc_file(tmp_path / "mixed")
    assert info is not None
    assert info.outs == [
        OutputInfo(path="d", md5="a" * 32, size=100, is_dir=True, nfiles=3),
        OutputInfo(path="f.txt", md5="b" * 32, size=5, is_dir=False, nfiles=None),
    ]


# ────────────────────────────────────────────────────────────────────────────
# Freshness: is_output_fresh + get_freshness_details iterate every out
# ────────────────────────────────────────────────────────────────────────────


def _write_multi_out_dvc(tmp_path: Path, dvc_name: str, entries: dict[str, str]) -> Path:
    """Write a multi-out .dvc file. `entries` is {filename: contents}."""
    import hashlib
    outs = []
    for fname, content in entries.items():
        (tmp_path / fname).write_text(content)
        outs.append({
            "md5": hashlib.md5(content.encode()).hexdigest(),  # noqa: S324
            "size": len(content),
            "hash": "md5",
            "path": fname,
        })
    dvc_path = tmp_path / f"{dvc_name}.dvc"
    dvc_path.write_text(yaml.dump({
        "outs": outs,
        "meta": {"computation": {"cmd": "true"}},
    }))
    return dvc_path


def test_is_output_fresh_multi_out_all_present(tmp_path, monkeypatch):
    """All outs present and matching → fresh."""
    monkeypatch.chdir(tmp_path)
    _write_multi_out_dvc(tmp_path, "csvs", {
        "ytd.parquet": "ytd\n", "monthly.parquet": "monthly\n",
    })
    fresh, reason = is_output_fresh(tmp_path / "csvs")
    assert (fresh, reason) == (True, "up-to-date")


def test_is_output_fresh_multi_out_missing_named(tmp_path, monkeypatch):
    """Missing out is named in the reason for multi-out stages."""
    monkeypatch.chdir(tmp_path)
    _write_multi_out_dvc(tmp_path, "csvs", {
        "ytd.parquet": "ytd\n", "monthly.parquet": "monthly\n",
    })
    (tmp_path / "monthly.parquet").unlink()
    fresh, reason = is_output_fresh(tmp_path / "csvs")
    assert (fresh, reason) == (False, "output missing: monthly.parquet")


def test_is_output_fresh_multi_out_data_change_named(tmp_path, monkeypatch):
    """A changed out is named in the reason."""
    monkeypatch.chdir(tmp_path)
    _write_multi_out_dvc(tmp_path, "csvs", {
        "ytd.parquet": "ytd\n", "monthly.parquet": "monthly\n",
    })
    (tmp_path / "ytd.parquet").write_text("ytd-modified\n")
    fresh, reason = is_output_fresh(tmp_path / "csvs", use_mtime_cache=False)
    assert not fresh
    assert reason.startswith("data changed: ytd.parquet ")


def test_get_freshness_details_multi_out_names_path(tmp_path, monkeypatch):
    """Structured freshness output identifies the failing path for multi-out."""
    monkeypatch.chdir(tmp_path)
    _write_multi_out_dvc(tmp_path, "csvs", {
        "ytd.parquet": "ytd\n", "monthly.parquet": "monthly\n",
    })
    (tmp_path / "monthly.parquet").write_text("monthly-modified\n")
    details = get_freshness_details(tmp_path / "csvs", use_mtime_cache=False)
    assert not details.fresh
    assert details.reason == "output hash mismatch: monthly.parquet"
    # Expected hash matches the changed out's recorded md5 (not outs[0]'s).
    import hashlib
    expected_monthly = hashlib.md5(b"monthly\n").hexdigest()  # noqa: S324
    assert details.output_expected == expected_monthly


# ────────────────────────────────────────────────────────────────────────────
# Executor: post-run multi-out verification + .dvc rewrite
# ────────────────────────────────────────────────────────────────────────────


def _write_stub_multi_out_dvc(repo: Path, dvc_name: str, names: list[str], cmd: str) -> Path:
    """Write a placeholder multi-out .dvc (no md5/size yet)."""
    dvc_path = repo / f"{dvc_name}.dvc"
    dvc_path.write_text(yaml.dump({
        "outs": [{"path": n} for n in names],
        "meta": {"computation": {"cmd": cmd}},
    }))
    return dvc_path


def test_run_multi_out_creates_caches_and_writes_back(runner, repo_with_remote):
    """Full `dvx run` flow on a multi-out stub: cmd executes, all outs are
    verified, hashed, cached, and the .dvc gets rewritten with N entries.

    Pre-fix: the post-run check looked for ``Path(<stage>)`` which is none of
    the actual outs → ``output not created``. Even when the cmd succeeded,
    only ``outs[0]`` would have been hashed and the .dvc would be rewritten
    with a single entry, dropping the other N-1 declared outputs.
    """
    repo, _remote = repo_with_remote
    cmd = (
        "echo 'ytd' > ytd.parquet && "
        "echo 'monthly' > monthly.parquet && "
        "echo 'month-year' > month-year.parquet"
    )
    _write_stub_multi_out_dvc(
        repo, "csvs",
        ["ytd.parquet", "monthly.parquet", "month-year.parquet"],
        cmd,
    )
    subprocess.run(["git", "add", "csvs.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stub"], cwd=repo, check=True, capture_output=True)

    result = runner.invoke(cli, ["run"])
    assert result.exit_code == 0, result.output

    # All 3 outs were created.
    for name in ("ytd.parquet", "monthly.parquet", "month-year.parquet"):
        assert (repo / name).exists(), name

    # .dvc rewritten with 3 entries, each with a real md5 + size.
    info = read_dvc_file(repo / "csvs")
    assert info is not None
    assert [o.path for o in info.outs] == [
        "ytd.parquet", "monthly.parquet", "month-year.parquet",
    ]
    import hashlib
    for o, content in zip(info.outs, (b"ytd\n", b"monthly\n", b"month-year\n")):
        assert o.md5 == hashlib.md5(content).hexdigest()  # noqa: S324
        assert o.size == len(content)
        assert o.is_dir is False

    # All 3 blobs cached locally.
    cache = repo / ".dvc" / "cache" / "files" / "md5"
    for o in info.outs:
        blob = cache / o.md5[:2] / o.md5[2:]
        assert blob.exists(), f"missing cache blob for {o.path}"


def test_run_multi_out_skipped_when_all_fresh(runner, repo_with_remote):
    """After producing all outs, re-running skips the stage."""
    repo, _remote = repo_with_remote
    cmd = "echo 'a' > a.txt && echo 'b' > b.txt"
    _write_stub_multi_out_dvc(repo, "ab", ["a.txt", "b.txt"], cmd)
    subprocess.run(["git", "add", "ab.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stub"], cwd=repo, check=True, capture_output=True)

    result = runner.invoke(cli, ["run"])
    assert result.exit_code == 0, result.output
    # Second run: skipped.
    result = runner.invoke(cli, ["run"])
    assert result.exit_code == 0, result.output
    stage_lines = [
        line for line in result.output.split("\n")
        if line.startswith("  ○ ab")
    ]
    assert stage_lines == ["  ○ ab: up-to-date"]


def test_run_multi_out_names_missing_output(runner, repo_with_remote):
    """Cmd writes only 2 of 3 declared outs → failure names which one(s)."""
    repo, _remote = repo_with_remote
    cmd = "echo 'a' > a.txt && echo 'b' > b.txt"   # NOT writing c.txt
    _write_stub_multi_out_dvc(repo, "abc", ["a.txt", "b.txt", "c.txt"], cmd)
    subprocess.run(["git", "add", "abc.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stub"], cwd=repo, check=True, capture_output=True)

    result = runner.invoke(cli, ["run"])
    assert result.exit_code == 1, result.output
    fail_lines = [line for line in result.output.split("\n") if "✗" in line]
    assert fail_lines == [
        "  ✗ abc: command succeeded but output(s) not created: c.txt",
    ]


def test_run_multi_out_pushes_all_blobs(runner, repo_with_remote):
    """``dvx run --push each`` on a multi-out stage uploads every out's blob.

    DVC's native ``repo.push`` handles multi-out file outputs correctly; this
    test confirms DVX's wrapper doesn't break the end-to-end flow.
    """
    repo, remote = repo_with_remote
    cmd = "echo 'x' > x.txt && echo 'y' > y.txt && echo 'z' > z.txt"
    _write_stub_multi_out_dvc(repo, "xyz", ["x.txt", "y.txt", "z.txt"], cmd)
    subprocess.run(["git", "add", "xyz.dvc"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "stub"], cwd=repo, check=True, capture_output=True)

    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output

    info = read_dvc_file(repo / "xyz")
    assert info is not None
    assert len(info.outs) == 3
    for o in info.outs:
        blob = remote / "files" / "md5" / o.md5[:2] / o.md5[2:]
        assert blob.exists(), f"missing remote blob for {o.path}"
