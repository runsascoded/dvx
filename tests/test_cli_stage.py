"""Tests for the ``dvx stage`` command group (`dvx.cli.stage`).

Exercises the SDK-backed CLI over a small fixture graph. Assertions compare
exact line lists / parsed `.dvc` structure, not substrings.

Fixture graph (all computed stages except ``raw``):

    raw ─┐
         ├─▶ b ─┬─▶ c
    a ───┘      └─▶ d   (c and d both consume b)
"""

from pathlib import Path

import pytest
from click.testing import CliRunner

from dvx.cli import cli
from dvx.run.dvc_files import read_dvc_file, write_dvc_file


def _h(c: str) -> str:
    return c * 32


@pytest.fixture
def graph(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    write_dvc_file(Path("raw"), md5=_h("r"), size=1)
    write_dvc_file(Path("a"), md5=_h("a"), size=1, cmd="make a")
    write_dvc_file(
        Path("b"), md5=_h("b"), size=1, cmd="make b",
        deps={"a": _h("a"), "raw": _h("r")}, resources={"cpus": 2},
    )
    write_dvc_file(Path("c"), md5=_h("c"), size=1, cmd="make c", deps={"b": _h("b")})
    write_dvc_file(Path("d"), md5=_h("d"), size=1, cmd="make d", deps={"b": _h("b")})
    return tmp_path


def _run(*args: str):
    return CliRunner().invoke(cli, ["stage", *args])


class TestSubset:
    def test_target_plus_ancestors_in_dep_order(self, graph):
        r = _run("subset", "c")
        assert r.exit_code == 0, r.output
        # a before b before c; the sibling d is excluded.
        assert r.output.rstrip("\n").split("\n") == ["a", "b", "c"]

    def test_dvc_paths_flag(self, graph):
        r = _run("subset", "-d", "c")
        assert r.exit_code == 0, r.output
        assert r.output.rstrip("\n").split("\n") == ["a.dvc", "b.dvc", "c.dvc"]

    def test_multiple_targets_union(self, graph):
        r = _run("subset", "c", "d")
        assert r.exit_code == 0, r.output
        assert r.output.rstrip("\n").split("\n") == ["a", "b", "c", "d"]

    def test_dvc_suffix_target_accepted(self, graph):
        """A ``c.dvc`` target normalizes to the ``c`` stage."""
        r = _run("subset", "c.dvc")
        assert r.exit_code == 0, r.output
        assert r.output.rstrip("\n").split("\n") == ["a", "b", "c"]

    def test_unknown_target_errors(self, graph):
        r = _run("subset", "nope")
        assert r.exit_code == 1
        assert r.output.rstrip("\n") == "Error: not a stage in this Dag: 'nope'"


class TestResources:
    def test_merge_onto_existing(self, graph):
        """Setting --mem-gb merges onto b's existing cpus:2."""
        r = _run("resources", "b", "--mem-gb", "16")
        assert r.exit_code == 0, r.output
        assert r.output.rstrip("\n") == "b.dvc"
        info = read_dvc_file(Path("b"))
        assert info.resources == {"mem_gb": 16, "cpus": 2}
        # Outs and dep hashes are preserved across the re-emit.
        assert info.md5 == _h("b")
        assert info.deps == {"a": _h("a"), "raw": _h("r")}

    def test_set_both(self, graph):
        r = _run("resources", "c", "--cpus", "8", "--mem-gb", "32")
        assert r.exit_code == 0, r.output
        assert read_dvc_file(Path("c")).resources == {"mem_gb": 32, "cpus": 8}

    def test_no_flags_errors(self, graph):
        r = _run("resources", "b")
        assert r.exit_code == 1
        assert r.output.rstrip("\n") == "Error: specify --cpus and/or --mem-gb"

    def test_not_a_stage_errors(self, graph):
        """``raw`` is a leaf input (no cmd), not a computed stage."""
        r = _run("resources", "raw", "--cpus", "4")
        assert r.exit_code == 1
        assert r.output.rstrip("\n") == "Error: not a computed stage: 'raw'"
