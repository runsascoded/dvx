"""Characterization tests for the ``dvx dag`` visualizer (`dvx.cli.dag`).

These lock the command's current output exactly, as a safety net for the
refactor that routes its graph algorithms through ``dvx.sdk.Dag``. Assertions
compare exact line lists / parsed JSON, not substrings — a one-token drift in
any renderer fails loudly.

Fixture graph (all in cwd, one dir):

    raw ──▶ b ──▶ c
    a   ──▶ b

``a`` and ``raw`` are roots (no deps); ``c`` is the sole leaf; ``raw`` has no
``cmd`` (a raw input), the other three are computed stages.
"""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from dvx.cli import cli
from dvx.run.dvc_files import write_dvc_file

A, B, C, R = "a" * 32, "b" * 32, "c" * 32, "r" * 32


@pytest.fixture
def dag_repo(monkeypatch, tmp_path):
    """A 4-node graph: raw (input), a, b (deps a+raw), c (deps b)."""
    monkeypatch.chdir(tmp_path)
    write_dvc_file(Path("raw"), md5=R, size=10)
    write_dvc_file(Path("a"), md5=A, size=11, cmd="make a")
    write_dvc_file(Path("b"), md5=B, size=12, cmd="make b", deps={"a": A, "raw": R})
    write_dvc_file(Path("c"), md5=C, size=13, cmd="make c", deps={"b": B})
    return tmp_path


def _run(*args: str) -> str:
    result = CliRunner().invoke(cli, ["dag", *args])
    assert result.exit_code == 0, result.output
    return result.output


class TestDagRenderers:
    def test_ascii_default(self, dag_repo):
        assert _run().rstrip("\n").split("\n") == [
            "a  aaaaaaaa...",
            "b  bbbbbbbb...  (2 deps)",
            "c  cccccccc...  (1 deps)",
            "raw  rrrrrrrr...",
        ]

    def test_ascii_show_cmd(self, dag_repo):
        assert _run("-c").rstrip("\n").split("\n") == [
            "a  aaaaaaaa...",
            "  cmd: make a",
            "b  bbbbbbbb...  (2 deps)",
            "  cmd: make b",
            "c  cccccccc...  (1 deps)",
            "  cmd: make c",
            "raw  rrrrrrrr...",
        ]

    def test_stats(self, dag_repo):
        assert _run("-s").rstrip("\n").split("\n") == [
            "Nodes: 4",
            "Edges: 3",
            "Roots (no deps): 2",
            "Leaves (no dependents): 1",
            "Nodes with cmd: 3",
        ]

    def test_json(self, dag_repo):
        assert json.loads(_run("--json")) == {
            "nodes": {
                "a": {"md5": A, "size": 11, "cmd": "make a", "deps": {}, "is_dir": False},
                "b": {
                    "md5": B, "size": 12, "cmd": "make b",
                    "deps": {"a": A, "raw": R}, "is_dir": False,
                },
                "c": {
                    "md5": C, "size": 13, "cmd": "make c",
                    "deps": {"b": B}, "is_dir": False,
                },
                "raw": {"md5": R, "size": 10, "cmd": None, "deps": {}, "is_dir": False},
            },
            "edges": [
                {"from": "a", "to": "b"},
                {"from": "b", "to": "c"},
                {"from": "raw", "to": "b"},
            ],
        }

    def test_mermaid(self, dag_repo):
        assert _run("--mermaid").rstrip("\n").split("\n") == [
            "flowchart TD",
            '  n0["a"]',
            '  n1["b"]',
            '  n2["c"]',
            '  n3["raw"]',
            "",
            "  n0 --> n1",
            "  n3 --> n1",
            "  n1 --> n2",
        ]

    def test_target_filter_includes_ancestors(self, dag_repo):
        """``dvx dag c`` filters to c plus its transitive ancestors (a, b, raw)."""
        assert _run("c").rstrip("\n").split("\n") == [
            "a  aaaaaaaa...",
            "b  bbbbbbbb...  (2 deps)",
            "c  cccccccc...  (1 deps)",
            "raw  rrrrrrrr...",
        ]
