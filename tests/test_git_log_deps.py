"""Tests for ``git_log_deps``: deps on a path's *history*, not its content.

A stage that consumes every past version of a file (nj-crashes' `crash_log`
walks the git history of `data/FAUQStats*.xml`) has no dep declaration in
DVX's file-at-HEAD model, so it ends up smuggling its own output back in as
a resume cursor — which makes it unreproducible from scratch. `git_log_deps`
splits that into the two jobs it conflated: dvx owns freshness (has a new
commit touched the pathspec?), the stage owns the resume point (via
``$DVX_GIT_LOG_SINCE``).

See ``specs/done/git-log-deps.md``.
"""

import json
import subprocess
from io import StringIO
from pathlib import Path

import pytest
import yaml

from dvx.run.dvc_files import (
    get_git_log_dep_sha,
    is_output_fresh,
    is_shallow_repo,
    read_dvc_file,
    write_dvc_file,
)
from dvx.run.executor import ExecutionConfig, run
from dvx.run.hash import compute_file_size, compute_md5


def _git(*args, cwd=None):
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """Git repo with a `history/` dir of files whose commits we'll walk."""
    r = tmp_path / "repo"
    r.mkdir()
    _git("init", "-b", "main", cwd=r)
    _git("config", "user.email", "t@t", cwd=r)
    _git("config", "user.name", "t", cwd=r)
    (r / "history").mkdir()
    (r / "history" / "a.xml").write_text("v1\n")
    (r / "other.txt").write_text("unrelated\n")
    _git("add", ".", cwd=r)
    _git("commit", "-m", "init", cwd=r)
    monkeypatch.chdir(r)
    return r


def _commit(repo: Path, rel: str, text: str) -> str:
    (repo / rel).write_text(text)
    _git("add", rel, cwd=repo)
    _git("commit", "-m", f"touch {rel}", cwd=repo)
    return _git("rev-parse", "HEAD", cwd=repo)


def _record(rel: str, git_log_deps: dict[str, str], cmd: str = "true") -> None:
    """Write ``rel`` as a fresh, fully-recorded stage with history deps.

    Hashes what's actually on disk, so freshness turns purely on the history
    deps rather than tripping on a stale output hash first.
    """
    path = Path(rel)
    write_dvc_file(
        output_path=path,
        md5=compute_md5(path),
        size=compute_file_size(path),
        cmd=cmd,
        git_log_deps=git_log_deps,
    )


# ────────────────────────────────────────────────────────────────────────────
# The primitive
# ────────────────────────────────────────────────────────────────────────────

def test_sha_is_the_tip_commit_touching_the_pathspec(repo):
    """A glob pathspec resolves to the newest commit touching any match."""
    assert get_git_log_dep_sha("history/*.xml") == _git("rev-parse", "HEAD", cwd=repo)

    unrelated = _commit(repo, "other.txt", "changed\n")
    assert get_git_log_dep_sha("history/*.xml") != unrelated

    touched = _commit(repo, "history/a.xml", "v2\n")
    assert get_git_log_dep_sha("history/*.xml") == touched


def test_sha_is_none_when_nothing_matches(repo):
    assert get_git_log_dep_sha("no/such/path/*.xml") is None


def test_fresh_clone_is_not_shallow(repo):
    assert is_shallow_repo() is False


# ────────────────────────────────────────────────────────────────────────────
# Freshness
# ────────────────────────────────────────────────────────────────────────────

def test_fresh_until_a_commit_touches_the_pathspec(repo):
    tip = _git("rev-parse", "HEAD", cwd=repo)
    (repo / "log.txt").write_text("built\n")
    _record("log.txt", {"history/*.xml": tip})

    # Recorded tip == current tip.
    assert is_output_fresh(Path("log.txt")) == (True, "up-to-date")

    # A commit elsewhere doesn't touch the pathspec.
    _commit(repo, "other.txt", "changed\n")
    assert is_output_fresh(Path("log.txt")) == (True, "up-to-date")

    # A commit that does touch it makes the stage stale.
    _commit(repo, "history/a.xml", "v2\n")
    assert is_output_fresh(Path("log.txt")) == (
        False,
        "git history dep changed: history/*.xml",
    )


def test_a_new_matching_file_makes_it_stale(repo):
    """The dep is the pathspec, not the file — a *new* match counts."""
    tip = _git("rev-parse", "HEAD", cwd=repo)
    (repo / "log.txt").write_text("built\n")
    _record("log.txt", {"history/*.xml": tip})
    assert is_output_fresh(Path("log.txt")) == (True, "up-to-date")

    _commit(repo, "history/b.xml", "new file\n")
    assert is_output_fresh(Path("log.txt")) == (
        False,
        "git history dep changed: history/*.xml",
    )


def test_unmatched_pathspec_is_reported_missing(repo):
    (repo / "log.txt").write_text("built\n")
    _record("log.txt", {"nope/*.xml": "0" * 40})
    assert is_output_fresh(Path("log.txt")) == (
        False,
        "git history dep missing: nope/*.xml",
    )


def test_shallow_clone_reruns_rather_than_claiming_freshness(repo, tmp_path):
    """`git rev-list` truncates silently at the shallow boundary.

    A `--depth 1` clone answers "HEAD touched everything", so a naive check
    would call a stale stage fresh. dvx must refuse to answer instead.
    """
    tip = _git("rev-parse", "HEAD", cwd=repo)
    _commit(repo, "history/a.xml", "v2\n")

    shallow = tmp_path / "shallow"
    _git("clone", "--depth", "1", f"file://{repo}", str(shallow))
    (shallow / "log.txt").write_text("built\n")

    import os
    cwd = os.getcwd()
    os.chdir(shallow)
    try:
        assert is_shallow_repo() is True
        _record("log.txt", {"history/*.xml": tip})
        assert is_output_fresh(Path("log.txt")) == (
            False,
            "git history dep unverifiable (shallow clone): history/*.xml",
        )
    finally:
        os.chdir(cwd)


# ────────────────────────────────────────────────────────────────────────────
# Round-trip + the resume cursor
# ────────────────────────────────────────────────────────────────────────────

def test_round_trips_through_the_dvc_file(repo):
    tip = _git("rev-parse", "HEAD", cwd=repo)
    (repo / "sub").mkdir()
    (repo / "sub" / "log.txt").write_text("built\n")
    _record("sub/log.txt", {"history/*.xml": tip})
    raw = yaml.safe_load((repo / "sub" / "log.txt.dvc").read_text())
    # Cross-directory pathspec is written in the `/`-rooted form, like deps.
    assert raw["meta"]["computation"]["git_log_deps"] == {"/history/*.xml": tip}

    info = read_dvc_file(Path("sub/log.txt"))
    assert info.git_log_deps == {"history/*.xml": tip}


def test_run_exports_the_recorded_sha_as_a_resume_cursor(repo):
    """The cmd sees where the *last* run left off, not the current tip."""
    tip = _git("rev-parse", "HEAD", cwd=repo)
    (repo / "log.txt").write_text("built\n")
    _record(
        "log.txt",
        {"history/*.xml": tip},
        cmd='echo "$DVX_GIT_LOG_SINCE" > log.txt; echo "$DVX_GIT_LOG_DEPS" >> log.txt',
    )
    new_tip = _commit(repo, "history/a.xml", "v2\n")

    results = run(
        [Path("log.txt.dvc")],
        ExecutionConfig(cache_push=False, pull_deps=False),
        output=StringIO(),
    )
    assert [(r.path, r.success) for r in results] == [("log.txt", True)]

    since, deps_json = (repo / "log.txt").read_text().rstrip("\n").split("\n")
    assert since == tip
    assert json.loads(deps_json) == {"history/*.xml": tip}

    # And the run advances the cursor to the new tip.
    assert read_dvc_file(Path("log.txt")).git_log_deps == {"history/*.xml": new_tip}


def test_history_dep_is_not_an_ordering_edge(repo):
    """A pathspec isn't an artifact — it must not add a level to the plan."""
    tip = _git("rev-parse", "HEAD", cwd=repo)
    write_dvc_file(
        output_path=Path("log.txt"),
        cmd="echo built > log.txt",
        git_log_deps={"history/*.xml": tip},
    )
    output = StringIO()
    run(
        [Path("log.txt.dvc")],
        ExecutionConfig(cache_push=False, pull_deps=False, dry_run=True),
        output=output,
    )
    assert output.getvalue().split("\n")[0] == "Execution plan: 1 levels, 1 computations"
