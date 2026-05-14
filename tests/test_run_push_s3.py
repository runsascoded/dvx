"""Tests that `dvx run --push each|end` also pushes cache blobs to the remote.

Uses a local directory as the remote (DVC treats all remotes uniformly).

Assertions parse ``dvx run``'s output into structured form (``Stage``,
``StageActions``, ``Summary``) and compare for exact equality. Avoid
``assert <substring> in result.output`` — those silently tolerate
regressions that mutate adjacent output.
"""

import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dvx.cli import cli
from dvx.run.hash import compute_md5


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def repo_with_remote(tmp_path, monkeypatch):
    """DVC+git repo with a local remote at ``remote/``."""
    repo = tmp_path / "repo"
    remote = tmp_path / "remote"
    repo.mkdir()
    remote.mkdir()

    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["dvc", "init"], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["dvc", "remote", "add", "-d", "local", str(remote)],
        cwd=repo, check=True, capture_output=True,
    )
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True, capture_output=True)

    monkeypatch.chdir(repo)
    return repo, remote


def _write_stage(repo: Path, name: str, cmd: str) -> Path:
    """Write a .dvc file with a cmd that produces ``name``. Returns dvc path."""
    dvc_path = repo / f"{name}.dvc"
    with open(dvc_path, "w") as f:
        yaml.dump({"outs": [{"path": name}], "meta": {"computation": {"cmd": cmd}}}, f)
    return dvc_path


def _commit_stubs(repo: Path, names: list[str]) -> None:
    """Track + commit .dvc stubs so ``git add -u`` picks up md5 updates on rerun."""
    subprocess.run(
        ["git", "add", *[f"{n}.dvc" for n in names]],
        cwd=repo, check=True, capture_output=True,
    )
    subprocess.run(["git", "commit", "-m", "stubs"], cwd=repo, check=True, capture_output=True)


def _remote_has_blob(remote: Path, md5: str) -> bool:
    return (remote / "files" / "md5" / md5[:2] / md5[2:]).exists()


# ────────────────────────────────────────────────────────────────────────────
# Output parsing
# ────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Stage:
    """A per-artifact status line.

    ``kind`` is one of the categorical outcomes the executor logs:
    running, waiting, completed, side-effect, co-output, skipped,
    output-not-created, co-output-not-produced, failed-exit.
    """
    name: str
    kind: str


@dataclass
class StageActions:
    """Commit + push messages emitted under a stage (``--push each``)."""
    committed: str | None = None        # commit subject (after "📝 committed: ")
    pushed: bool | None = None           # True=git push OK, False=failed, None=not attempted
    cache_blobs: int | None = None       # int → success; None → no cache push line
    cache_failed: str | None = None      # error msg → cache push failed; None → success


@dataclass(frozen=True)
class Summary:
    total: int
    executed: int
    skipped: int
    failed: int = 0


@dataclass
class ParsedRun:
    plan: tuple[int, int] = (0, 0)              # (levels, computations)
    stages: list[Stage] = field(default_factory=list)
    actions: list[StageActions] = field(default_factory=list)  # one entry per "📝 committed" block
    end_pushed: bool | None = None              # --push end: True/False, or None if not used
    end_cache_blobs: int | None = None          # --push end: blob count
    failed_stages: list[str] = field(default_factory=list)
    summary: Summary | None = None


_STAGE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^  ⟳ (?P<name>.+?): running\.\.\.$"), "running"),
    (re.compile(r"^  ◐ (?P<name>.+?): waiting \(same cmd running\)\.\.\.$"), "waiting"),
    (re.compile(r"^  ✓ (?P<name>.+?): completed \([\d.]+s\)$"), "completed"),
    (re.compile(r"^  ✓ (?P<name>.+?): side-effect completed \([\d.]+s\)$"), "side-effect"),
    (re.compile(r"^  ✓ (?P<name>.+?): co-output ready$"), "co-output"),
    (re.compile(r"^  ○ (?P<name>.+?): .+$"), "skipped"),
    (re.compile(r"^  ✗ (?P<name>.+?): command succeeded but output not created$"), "output-not-created"),
    (re.compile(r"^  ✗ (?P<name>.+?): co-output not produced$"), "co-output-not-produced"),
    (re.compile(r"^  ✗ (?P<name>.+?): failed \(exit code \d+\)$"), "failed-exit"),
]
_PLAN_RE = re.compile(r"^Execution plan: (\d+) levels, (\d+) computations$")
_COMMITTED_RE = re.compile(r"^    📝 committed: (.+)$")
_PUSHED_RE = re.compile(r"^    📤 pushed$")
_PUSH_FAILED_RE = re.compile(r"^    ⚠ push failed: .+$")
_CACHE_PUSHED_RE = re.compile(r"^    📤 cache pushed \((\d+) blobs?\)$")
_CACHE_FAILED_RE = re.compile(r"^    ⚠ cache push failed: (.+)$")
_END_PUSHED_RE = re.compile(r"^📤 pushed all commits$")
_END_PUSH_FAILED_RE = re.compile(r"^⚠ push failed: .+$")
_END_CACHE_PUSHED_RE = re.compile(r"^📤 cache pushed \((\d+) blobs?\)$")
_FAILED_RE = re.compile(r"^Failed: (.+)$")
_SUMMARY_TOTAL_RE = re.compile(r"^  Total: (\d+)$")
_SUMMARY_EXECUTED_RE = re.compile(r"^  Executed: (\d+)$")
_SUMMARY_SKIPPED_RE = re.compile(r"^  Skipped: (\d+)$")
_SUMMARY_FAILED_RE = re.compile(r"^  Failed: (\d+)$")


def parse_run(output: str) -> ParsedRun:
    """Parse ``dvx run`` output into structured form.

    Unknown lines are silently dropped — that's by design for lines whose
    content is downstream/variable (``Level N/M:``, blank lines, the
    ``Summary:`` header, ``warning:`` from git). The fields a test wants
    to assert are explicit dataclass attrs; if the parser doesn't surface
    something a test needs, extend it rather than falling back to ``in``.
    """
    run = ParsedRun()
    current_action: StageActions | None = None
    summary_total = summary_executed = summary_skipped = summary_failed = None
    for line in output.split("\n"):
        m = _PLAN_RE.match(line)
        if m:
            run.plan = (int(m.group(1)), int(m.group(2)))
            continue
        matched = False
        for pat, kind in _STAGE_PATTERNS:
            m = pat.match(line)
            if m:
                run.stages.append(Stage(name=m.group("name"), kind=kind))
                matched = True
                break
        if matched:
            continue
        if (m := _COMMITTED_RE.match(line)):
            current_action = StageActions(committed=m.group(1))
            run.actions.append(current_action)
            continue
        if _PUSHED_RE.match(line):
            assert current_action is not None, f"pushed without preceding committed: {line!r}"
            current_action.pushed = True
            continue
        if _PUSH_FAILED_RE.match(line):
            assert current_action is not None, f"push failed without preceding committed: {line!r}"
            current_action.pushed = False
            continue
        if (m := _CACHE_PUSHED_RE.match(line)):
            assert current_action is not None, f"cache pushed without preceding committed: {line!r}"
            current_action.cache_blobs = int(m.group(1))
            continue
        if (m := _CACHE_FAILED_RE.match(line)):
            assert current_action is not None, f"cache push failed without preceding committed: {line!r}"
            current_action.cache_failed = m.group(1)
            continue
        if _END_PUSHED_RE.match(line):
            run.end_pushed = True
            continue
        if _END_PUSH_FAILED_RE.match(line):
            run.end_pushed = False
            continue
        if (m := _END_CACHE_PUSHED_RE.match(line)):
            run.end_cache_blobs = int(m.group(1))
            continue
        if (m := _FAILED_RE.match(line)):
            run.failed_stages = [s.strip() for s in m.group(1).split(",")]
            continue
        if (m := _SUMMARY_TOTAL_RE.match(line)):
            summary_total = int(m.group(1))
        elif (m := _SUMMARY_EXECUTED_RE.match(line)):
            summary_executed = int(m.group(1))
        elif (m := _SUMMARY_SKIPPED_RE.match(line)):
            summary_skipped = int(m.group(1))
        elif (m := _SUMMARY_FAILED_RE.match(line)):
            summary_failed = int(m.group(1))
    if summary_total is not None and summary_executed is not None and summary_skipped is not None:
        run.summary = Summary(
            total=summary_total,
            executed=summary_executed,
            skipped=summary_skipped,
            failed=summary_failed or 0,
        )
    return run


def _sorted_stages(stages: list[Stage]) -> list[Stage]:
    """Sort stages by (name, kind) — race-tolerant comparison."""
    return sorted(stages, key=lambda s: (s.name, s.kind))


# ────────────────────────────────────────────────────────────────────────────
# Tests
# ────────────────────────────────────────────────────────────────────────────


def test_push_each_uploads_blob_to_remote(runner, repo_with_remote):
    repo, remote = repo_with_remote
    _write_stage(repo, "out.txt", "echo hello > out.txt")

    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output

    md5 = compute_md5(repo / "out.txt")
    assert _remote_has_blob(remote, md5)

    run = parse_run(result.output)
    assert run.plan == (1, 1)
    assert run.stages == [
        Stage("out.txt", "running"),
        Stage("out.txt", "completed"),
    ]
    # One commit, git push fails (no remote configured), cache push pushes 1 blob.
    assert run.actions == [StageActions(committed="Run out", pushed=False, cache_blobs=1)]
    assert run.summary == Summary(total=1, executed=1, skipped=0)


def test_push_end_batches_cache_pushes(runner, repo_with_remote):
    repo, remote = repo_with_remote
    _write_stage(repo, "a.txt", "echo aaa > a.txt")
    _write_stage(repo, "b.txt", "echo bbb > b.txt")

    result = runner.invoke(cli, ["run", "--commit", "--push", "end"])
    assert result.exit_code == 0, result.output

    md5_a = compute_md5(repo / "a.txt")
    md5_b = compute_md5(repo / "b.txt")
    assert _remote_has_blob(remote, md5_a)
    assert _remote_has_blob(remote, md5_b)

    run = parse_run(result.output)
    assert run.plan == (1, 2)
    # Both stages run independent cmds → each emits running + completed.
    assert _sorted_stages(run.stages) == [
        Stage("a.txt", "completed"),
        Stage("a.txt", "running"),
        Stage("b.txt", "completed"),
        Stage("b.txt", "running"),
    ]
    # Pre-existing race: per-stage ``git add -u`` + ``git commit`` runs
    # from each artifact's thread in parallel; the second one's
    # ``git add -u`` races on ``.git/index.lock`` and is silently
    # dropped. With ``--push end`` this is harmless — the end-of-run
    # cache push uses ``executed`` from all results, not the commits.
    # We assert it observably: 1 commit succeeded, with no per-stage
    # push or cache push (those are deferred to end).
    assert len(run.actions) == 1
    committed = run.actions[0]
    assert committed.committed in ("Run a", "Run b")
    assert committed.pushed is None
    assert committed.cache_blobs is None
    assert committed.cache_failed is None
    # End-of-run: git push attempted (fails, no remote) + cache push of both blobs.
    assert run.end_pushed is False
    assert run.end_cache_blobs == 2
    assert run.summary == Summary(total=2, executed=2, skipped=0)


def test_push_each_uploads_all_co_output_blobs(runner, repo_with_remote):
    """``--push each`` must push EVERY co-output's blob, not just the primary's.

    Regression: when one cmd produces multiple outputs (co-outputs), the
    primary stage's ``_handle_stage_output`` built the cache-push manifest
    from only its own ``.dvc``, dropping the co-output's blob. The git
    commit + git-push pick up both ``.dvc`` files (``git add -u``), so the
    bug is silent: the .dvc updates land on the remote git ref with new
    md5s, but the co-output's blob never reaches the data remote. The
    next day's ``dvx pull`` fails on the dangling md5.

    See ``specs/done/co-output-push-half-blob.md``.
    """
    repo, remote = repo_with_remote
    # Same cmd in both .dvc files triggers the co-output dedup path.
    # ``sleep`` widens the race window so the second artifact reliably
    # enters the dedup check while the first is still running.
    cmd = "sleep 0.1 && echo aaa > a.txt && echo bbb > b.txt"
    _write_stage(repo, "a.txt", cmd)
    _write_stage(repo, "b.txt", cmd)
    _commit_stubs(repo, ["a.txt", "b.txt"])

    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output

    md5_a = compute_md5(repo / "a.txt")
    md5_b = compute_md5(repo / "b.txt")
    assert _remote_has_blob(remote, md5_a)
    assert _remote_has_blob(remote, md5_b)

    run = parse_run(result.output)
    assert run.plan == (1, 2)
    # Race-tolerant: one artifact ran cmd (running + completed), the
    # other waited and became co-output (waiting + co-output). Sort by
    # name and check the set of (name, kind) pairs. The roles split:
    # whichever artifact is primary, its pair is {running, completed};
    # the other's pair is {waiting, co-output}.
    by_name: dict[str, set[str]] = {}
    for s in run.stages:
        by_name.setdefault(s.name, set()).add(s.kind)
    primary_kinds = {"running", "completed"}
    coop_kinds = {"waiting", "co-output"}
    assert (
        (by_name == {"a.txt": primary_kinds, "b.txt": coop_kinds})
        or (by_name == {"a.txt": coop_kinds, "b.txt": primary_kinds})
    ), by_name
    # Exactly one commit covers both .dvc files; cache push manifest
    # includes both blobs (the bug under test).
    assert len(run.actions) == 1
    assert run.actions[0].cache_blobs == 2
    assert run.actions[0].pushed is False  # no remote
    assert run.actions[0].cache_failed is None
    assert run.summary == Summary(total=2, executed=2, skipped=0)


def test_push_each_uploads_all_co_output_blobs_3way(runner, repo_with_remote):
    """Same cmd → 3 outputs. All 3 blobs must reach the remote.

    Generalizes ``test_push_each_uploads_all_co_output_blobs`` past N=2:
    ``_wait_for_co_outputs`` has to barrier on every co-output, not just
    the first one to finish.
    """
    repo, remote = repo_with_remote
    cmd = "sleep 0.1 && echo aaa > a.txt && echo bbb > b.txt && echo ccc > c.txt"
    names = ["a.txt", "b.txt", "c.txt"]
    for name in names:
        _write_stage(repo, name, cmd)
    _commit_stubs(repo, names)

    result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
    assert result.exit_code == 0, result.output

    for name in names:
        assert _remote_has_blob(remote, compute_md5(repo / name))

    run = parse_run(result.output)
    assert run.plan == (1, 3)
    # Race-tolerant: 1 primary (running + completed) + 2 co-outputs
    # (waiting + co-output each). Group by artifact: exactly one
    # artifact has primary-kinds, the other two have co-output-kinds.
    by_name: dict[str, set[str]] = {}
    for s in run.stages:
        by_name.setdefault(s.name, set()).add(s.kind)
    assert set(by_name) == set(names)
    role_counts = {frozenset({"running", "completed"}): 0, frozenset({"waiting", "co-output"}): 0}
    for kinds in by_name.values():
        role_counts[frozenset(kinds)] += 1
    assert role_counts == {
        frozenset({"running", "completed"}): 1,
        frozenset({"waiting", "co-output"}): 2,
    }
    # One commit, manifest has all 3 blobs.
    assert len(run.actions) == 1
    assert run.actions[0].cache_blobs == 3
    assert run.summary == Summary(total=3, executed=3, skipped=0)


def test_push_each_partial_output_does_not_hang(runner, repo_with_remote):
    """Cmd produces only one of its two declared outputs — must not hang.

    Whichever artifact wins the co-output-dedup race (becomes the primary)
    is non-deterministic. Either:

    - ``a.txt`` is primary, succeeds, waits for ``b.txt``'s dvc-done
      event; ``b.txt``'s ``_handle_co_output`` returns ``co-output not
      produced`` — the ``try/finally`` in ``_handle_co_output`` must
      signal the event anyway, else the primary's wait deadlocks.
    - ``b.txt`` is primary, fails on "output not created"; ``a.txt``'s
      ``_handle_co_output`` succeeds. No barrier engages.

    Both branches must finish — without the ``try/finally``, branch #1
    hangs CI forever. Verified to fail-fast when the ``try/finally``
    is removed.
    """
    repo, _remote = repo_with_remote
    cmd = "sleep 0.1 && echo aaa > a.txt"
    _write_stage(repo, "a.txt", cmd)
    _write_stage(repo, "b.txt", cmd)
    _commit_stubs(repo, ["a.txt", "b.txt"])

    # Hard timeout via worker thread — without try/finally a regression
    # hangs forever in the (a-primary, b-co-output) branch.
    import threading
    result_holder: list = []
    def _go():
        result_holder.append(runner.invoke(cli, ["run", "--commit", "--push", "each"]))
    t = threading.Thread(target=_go, daemon=True)
    t.start()
    t.join(timeout=30)
    assert not t.is_alive(), (
        "executor hung — likely missing dvc-done signal from a failed "
        "co-output (regression of try/finally in _handle_co_output)"
    )
    assert result_holder, "worker produced no result"
    result = result_holder[0]
    run = parse_run(result.output)
    assert run.plan == (1, 2)
    # The run finished with a failure: exactly one artifact pulled the
    # "not produced" path (which one depends on the race), the other was
    # the success-path co-output.
    failure_kinds = {"output-not-created", "co-output-not-produced"}
    failures = [s for s in run.stages if s.kind in failure_kinds]
    successes = [s for s in run.stages if s.kind in ("completed", "co-output")]
    assert len(failures) == 1
    assert len(successes) == 1
    # The failed artifact appears in `Failed:`.
    assert run.failed_stages == [failures[0].name]
    assert run.summary == Summary(total=2, executed=1, skipped=0, failed=1)


def test_no_cache_push_opt_out(runner, repo_with_remote):
    repo, remote = repo_with_remote
    _write_stage(repo, "out.txt", "echo opt-out > out.txt")

    result = runner.invoke(cli, ["run", "--commit", "--push", "each", "--no-cache-push"])
    assert result.exit_code == 0, result.output

    md5 = compute_md5(repo / "out.txt")
    assert not _remote_has_blob(remote, md5)

    run = parse_run(result.output)
    assert run.stages == [
        Stage("out.txt", "running"),
        Stage("out.txt", "completed"),
    ]
    # Commit happens, git push attempted, but no cache push.
    assert run.actions == [StageActions(committed="Run out", pushed=False, cache_blobs=None)]
    assert run.summary == Summary(total=1, executed=1, skipped=0)


def test_skipped_stages_dont_push(runner, repo_with_remote):
    """A fresh (skipped) stage must not trigger commit or push."""
    repo, remote = repo_with_remote

    # Pre-create output + matching .dvc → stage is fresh, will be skipped.
    (repo / "fresh.txt").write_text("already here\n")
    md5 = compute_md5(repo / "fresh.txt")
    size = (repo / "fresh.txt").stat().st_size
    with open(repo / "fresh.txt.dvc", "w") as f:
        yaml.dump({
            "outs": [{"md5": md5, "size": size, "path": "fresh.txt"}],
            "meta": {"computation": {"cmd": "echo already here > fresh.txt"}},
        }, f)

    result = runner.invoke(cli, ["run", "--commit", "--push", "end"])
    assert result.exit_code == 0, result.output

    assert not _remote_has_blob(remote, md5)
    run = parse_run(result.output)
    assert run.stages == [Stage("fresh.txt", "skipped")]
    assert run.actions == []
    assert run.end_pushed is None
    assert run.end_cache_blobs is None
    assert run.summary == Summary(total=1, executed=0, skipped=1)


def test_cache_push_failure_is_non_fatal(runner, repo_with_remote, monkeypatch):
    """A failing cache push logs a warning but doesn't abort the run."""
    repo, _remote = repo_with_remote
    _write_stage(repo, "resilient.txt", "echo survive > resilient.txt")

    from dvx import repo as repo_module
    original_push = repo_module.Repo.push

    def boom(self, *args, **kwargs):
        raise RuntimeError("simulated remote outage")

    monkeypatch.setattr(repo_module.Repo, "push", boom)

    try:
        result = runner.invoke(cli, ["run", "--commit", "--push", "each"])
        assert result.exit_code == 0, result.output
        run = parse_run(result.output)
        assert run.stages == [
            Stage("resilient.txt", "running"),
            Stage("resilient.txt", "completed"),
        ]
        assert run.actions == [StageActions(
            committed="Run resilient",
            pushed=False,
            cache_blobs=None,
            cache_failed="simulated remote outage",
        )]
        # .dvc commit happened despite cache push failure.
        commits = subprocess.run(
            ["git", "log", "--pretty=%s"], cwd=repo, capture_output=True, text=True,
        ).stdout.rstrip().split("\n")
        assert commits == ["Run resilient", "stubs", "init"] or commits == ["Run resilient", "init"]
        assert run.summary == Summary(total=1, executed=1, skipped=0)
    finally:
        monkeypatch.setattr(repo_module.Repo, "push", original_push)
