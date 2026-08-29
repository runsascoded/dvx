"""Tests for the ``output hash changed`` warning.

A rerun stage that produces *different bytes* than its ``.dvc`` recorded used
to be indistinguishable from one that reproduced them exactly: dvx rewrote the
``.dvc``, cached the new blob, and logged a bare ``✓ completed``. That silence
is what let nj-crashes' full-DAG reproducibility audit report "byte-identical"
for three rounds while nearly every output was in fact drifting (a pyarrow
20→21 footer change).

The warning is not a failure — regenerating changed bytes is legal and often
intended. It's a signal, so it also survives to the run summary.
"""

import subprocess
from io import StringIO
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dvx.cli import cli
from dvx.run.dvc_files import write_dvc_file
from dvx.run.executor import ExecutionConfig, run
from dvx.run.hash import compute_file_size, compute_md5


@pytest.fixture
def repo(tmp_path, monkeypatch):
    r = tmp_path / "repo"
    r.mkdir()
    for cmd in (
        ["git", "init", "-b", "main"],
        ["git", "config", "user.email", "t@t"],
        ["git", "config", "user.name", "t"],
        ["git", "commit", "--allow-empty", "-m", "init"],
    ):
        subprocess.run(cmd, cwd=r, check=True, capture_output=True)
    monkeypatch.chdir(r)
    return r


def _stage(rel: str, recorded: str, cmd: str) -> str:
    """Record ``recorded`` as ``rel``'s output hash, then dirty the file on
    disk so the stage reads as stale and ``cmd`` actually reruns.

    Without the dirtying step the stage is fresh and gets skipped, and a
    skipped stage hashes nothing — there'd be no comparison to make.
    Returns the recorded md5.
    """
    path = Path(rel)
    path.write_text(recorded)
    md5 = compute_md5(path)
    write_dvc_file(
        output_path=path, md5=md5, size=compute_file_size(path), cmd=cmd,
    )
    path.write_text("dirty\n")
    return md5


def _run(target: str) -> tuple[list[str], list]:
    """Run one target; return (log lines, results)."""
    output = StringIO()
    results = run(
        [Path(target)],
        ExecutionConfig(cache_push=False, pull_deps=False, commit="never"),
        output=output,
    )
    return output.getvalue().rstrip("\n").split("\n"), results


# ────────────────────────────────────────────────────────────────────────────
# Single output
# ────────────────────────────────────────────────────────────────────────────

def test_changed_bytes_are_reported_with_both_hashes(repo):
    """The recorded and produced md5s both appear, so a reader can diff them."""
    recorded = _stage("out.txt", "old\n", cmd="printf 'new\\n' > out.txt")

    lines, results = _run("out.txt.dvc")
    produced = compute_md5(Path("out.txt"))

    assert recorded != produced
    assert [line for line in lines if "hash changed" in line] == [
        f"  ⚠ out.txt: output hash changed (recorded {recorded} → produced {produced})"
    ]
    assert [(r.path, r.success, r.hash_changed) for r in results] == [
        ("out.txt", True, True)
    ]


def test_a_size_change_is_reported_alongside_the_hash(repo):
    """Same-size drift (a parquet footer) reads differently from a real
    content change, so the size delta is part of the line when it moved."""
    recorded = _stage(
        "out.txt", "a" * 100, cmd="printf '%0.sb' $(seq 1 150) > out.txt",
    )

    lines, _ = _run("out.txt.dvc")
    produced = compute_md5(Path("out.txt"))

    assert [line for line in lines if "hash changed" in line] == [
        f"  ⚠ out.txt: output hash changed (recorded {recorded} → produced {produced}); "
        f"size 100 → 150 (+50 B, +50.0%)"
    ]


def test_a_byte_identical_rerun_says_nothing(repo):
    """The warning must not fire on the reproducible case — that's the whole
    point of it being a signal."""
    # Recorded md5 == what the cmd will produce; only the on-disk bytes are
    # stale, so the stage reruns and lands right back on its recorded hash.
    _stage("out.txt", "same\n", cmd="printf 'same\\n' > out.txt")

    lines, results = _run("out.txt.dvc")

    assert [line for line in lines if "hash changed" in line] == []
    assert [(r.path, r.hash_changed) for r in results] == [("out.txt", False)]


def test_a_first_recording_is_not_a_change(repo):
    """A hand-written stage declares its out with no md5 yet; there is nothing
    for the first run to differ from, so producing one must not warn."""
    Path("out.txt.dvc").write_text(yaml.dump({
        "outs": [{"path": "out.txt"}],
        "meta": {"computation": {"cmd": "printf 'first\\n' > out.txt"}},
    }))

    lines, results = _run("out.txt.dvc")

    assert [line for line in lines if "hash changed" in line] == []
    assert [(r.path, r.skipped, r.hash_changed) for r in results] == [
        ("out.txt", False, False)
    ]
    assert Path("out.txt").read_text() == "first\n"


# ────────────────────────────────────────────────────────────────────────────
# Multi-output and co-output
# ────────────────────────────────────────────────────────────────────────────

def test_each_changed_out_of_a_multi_out_stage_is_named(repo):
    """One warning per drifting out, labelled by the out's own path — a
    stage whose 2nd of 3 outputs drifted shouldn't report as "the stage"."""
    for name, text in (("a.txt", "a1\n"), ("b.txt", "b1\n"), ("c.txt", "c1\n")):
        (repo / name).write_text(text)
    recorded = {n: compute_md5(Path(n)) for n in ("a.txt", "b.txt", "c.txt")}
    Path("a.txt.dvc").write_text(yaml.dump({
        "outs": [
            {"path": n, "md5": recorded[n], "size": 3, "hash": "md5"}
            for n in ("a.txt", "b.txt", "c.txt")
        ],
        "meta": {"computation": {
            "cmd": "printf 'a1\\n' > a.txt; printf 'b2\\n' > b.txt; printf 'c1\\n' > c.txt",
        }},
    }))
    (repo / "b.txt").write_text("stale\n")  # force a rerun

    lines, results = _run("a.txt.dvc")

    assert [line for line in lines if "hash changed" in line] == [
        f"  ⚠ b.txt: output hash changed "
        f"(recorded {recorded['b.txt']} → produced {compute_md5(Path('b.txt'))})"
    ]
    assert [(r.path, r.hash_changed) for r in results] == [("a.txt", True)]


def test_a_co_output_reports_its_own_drift(repo):
    """Co-outputs are hashed on a separate path from the primary's outs; a
    drifting co-output must warn there too."""
    cmd = "printf 'p\\n' > p.txt; printf 'chgd\\n' > q.txt"
    _stage("p.txt", "p\n", cmd=cmd)                  # reproduces exactly
    recorded_q = _stage("q.txt", "orig\n", cmd=cmd)   # drifts, same size

    output = StringIO()
    results = run(
        [Path("p.txt.dvc"), Path("q.txt.dvc")],
        ExecutionConfig(cache_push=False, pull_deps=False, commit="never"),
        output=output,
    )
    lines = output.getvalue().rstrip("\n").split("\n")

    assert [line for line in lines if "hash changed" in line and "q.txt" in line] == [
        f"  ⚠ q.txt: output hash changed "
        f"(recorded {recorded_q} → produced {compute_md5(Path('q.txt'))})"
    ]
    assert sorted((r.path, r.hash_changed) for r in results) == [
        ("p.txt", False), ("q.txt", True),
    ]


# ────────────────────────────────────────────────────────────────────────────
# Summary
# ────────────────────────────────────────────────────────────────────────────

def test_summary_counts_changed_outputs(repo):
    """`Hash changed: N` lands in the CLI summary — the number an audit
    actually reads, without scrolling a 136-stage log."""
    _stage("out.txt", "old\n", cmd="printf 'new\\n' > out.txt")

    result = CliRunner().invoke(
        cli, ["run", "--no-cache-push", "--no-commit", "out.txt.dvc"],
    )

    assert result.exit_code == 0
    assert result.stdout.rstrip("\n").split("\n") == [
        "",
        "Summary:",
        "  Total: 1",
        "  Executed: 1",
        "  Skipped: 0",
        "  Hash changed: 1",
    ]


def test_summary_omits_the_line_when_nothing_changed(repo):
    """A stage that reran and reproduced its recorded bytes exactly."""
    _stage("out.txt", "same\n", cmd="printf 'same\\n' > out.txt")

    result = CliRunner().invoke(
        cli, ["run", "--no-cache-push", "--no-commit", "out.txt.dvc"],
    )

    assert result.exit_code == 0
    assert result.stdout.rstrip("\n").split("\n") == [
        "",
        "Summary:",
        "  Total: 1",
        "  Executed: 1",
        "  Skipped: 0",
    ]
