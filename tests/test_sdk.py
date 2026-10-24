"""Tests for the dvx DAG-authoring SDK (`dvx.sdk`).

Assertions compare exact structures (dicts/lists) and the parsed ``.dvc``
provenance, not substrings — these read as the SDK's behavior spec.
"""

from pathlib import Path

import pytest

from dvx.run.artifact import Artifact, Computation
from dvx.run.dvc_files import read_dvc_file
from dvx.sdk import Dag, Resources

# ────────────────────────────────────────────────────────────────────────────
# Resources
# ────────────────────────────────────────────────────────────────────────────


class TestResources:
    def test_to_dict_omits_unset(self):
        assert Resources(cpus=16, mem_gb=64).to_dict() == {"mem_gb": 64, "cpus": 16}
        assert Resources(cpus=4).to_dict() == {"cpus": 4}
        assert Resources(mem_gb=8).to_dict() == {"mem_gb": 8}
        assert Resources().to_dict() == {}

    def test_reused_across_stages_renders_identically(self, monkeypatch, tmp_path):
        """One Resources instance applied to many stages → identical blocks."""
        monkeypatch.chdir(tmp_path)
        big = Resources(cpus=16, mem_gb=64)
        dag = Dag()
        for ym in ("a", "b", "c"):
            dag.stage(path=ym, cmd=f"make {ym}", resources=big)
        dag.to_dvc()

        rendered = [read_dvc_file(Path(ym)).resources for ym in ("a", "b", "c")]
        assert rendered == [{"mem_gb": 64, "cpus": 16}] * 3


# ────────────────────────────────────────────────────────────────────────────
# get_dep_hashes — authored-edge preservation (TFFP for the prep-path fix)
# ────────────────────────────────────────────────────────────────────────────


class TestAuthoredEdgePreservation:
    def test_prep_preserves_edge_to_unbuilt_upstream(self, monkeypatch, tmp_path):
        """A dep on an unbuilt upstream stage survives prep with an empty hash.

        Without the fix the dep falls through every branch of get_dep_hashes
        and is dropped — silently severing the DAG edge.
        """
        monkeypatch.chdir(tmp_path)
        upstream = Artifact(path="up")  # no md5, no .dvc, not on disk
        comp = Computation(cmd="make down", deps=[upstream])
        assert comp.get_dep_hashes() == {"up": ""}

    def test_post_run_rewrite_semantics_unchanged(self, monkeypatch, tmp_path):
        """recompute=True (post-run rewrite) still drops an unresolvable dep."""
        monkeypatch.chdir(tmp_path)
        upstream = Artifact(path="up")
        comp = Computation(cmd="make down", deps=[upstream])
        assert comp.get_dep_hashes(recompute=True) == {}


# ────────────────────────────────────────────────────────────────────────────
# Dag construction + validation
# ────────────────────────────────────────────────────────────────────────────


class TestDag:
    def test_stage_builds_artifact(self):
        dag = Dag()
        s = dag.stage(path="out", cmd="build", deps=["in"], resources=Resources(cpus=2))
        assert isinstance(s, Artifact)
        assert s.path == "out"
        assert s.computation.cmd == "build"
        assert s.computation.deps == ["in"]
        assert s.computation.resources == {"cpus": 2}
        assert dag.artifacts == [s]

    def test_duplicate_path_raises(self):
        dag = Dag()
        dag.stage(path="out", cmd="a")
        with pytest.raises(ValueError, match="duplicate stage path: out"):
            dag.stage(path="out", cmd="b")

    def test_validate_rejects_unregistered_artifact_dep(self):
        dag = Dag()
        orphan = Artifact(path="orphan", computation=Computation(cmd="x"))
        dag.stage(path="out", cmd="build", deps=[orphan])
        with pytest.raises(ValueError, match="depends on unregistered artifact 'orphan'"):
            dag.validate()

    def test_validate_detects_cycle(self):
        dag = Dag()
        a = dag.stage(path="a", cmd="make a")
        b = dag.stage(path="b", cmd="make b", deps=[a])
        # Re-point a to depend on b → cycle a -> b -> a.
        a.computation.deps = [b]
        with pytest.raises(ValueError, match=r"cycle in Dag: a -> b -> a"):
            dag.validate()

    def test_bare_path_dep_is_not_required_to_be_registered(self, monkeypatch, tmp_path):
        """A string dep may name a raw input outside the DAG — allowed."""
        monkeypatch.chdir(tmp_path)
        dag = Dag()
        dag.stage(path="out", cmd="build", deps=["raw/input.csv"])
        dag.validate()  # does not raise


# ────────────────────────────────────────────────────────────────────────────
# Dag.to_dvc — emits the graph, edges preserved, topo order
# ────────────────────────────────────────────────────────────────────────────


class TestToDvc:
    def test_writes_placeholder_graph_preserving_edges(self, monkeypatch, tmp_path):
        """A 3-stage chain authored before anything runs keeps its inter-stage edges.

        The b→a and c→b edges are Artifact deps on upstream stages that
        haven't been built — they must survive into the .dvc (empty hash,
        filled by the first run), or the DAG's ordering is silently severed.
        """
        monkeypatch.chdir(tmp_path)
        dag = Dag()
        a = dag.stage(path="a", cmd="make a")
        b = dag.stage(path="b", cmd="make b", deps=[a])
        dag.stage(path="c", cmd="make c", deps=[b])

        written = dag.to_dvc()
        assert sorted(str(p) for p in written) == ["a.dvc", "b.dvc", "c.dvc"]
        assert read_dvc_file(Path("a")).deps == {}
        assert read_dvc_file(Path("b")).deps == {"a": ""}
        assert read_dvc_file(Path("c")).deps == {"b": ""}
        assert read_dvc_file(Path("b")).cmd == "make b"

    def test_existing_raw_input_dep_is_hashed(self, monkeypatch, tmp_path):
        """A present raw-input (bare-path) dep is recorded with its real hash."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "raw").write_text("payload")
        from dvx.run.hash import compute_md5

        dag = Dag()
        dag.stage(path="out", cmd="build", deps=["raw"])
        dag.to_dvc()
        assert read_dvc_file(Path("out")).deps == {"raw": compute_md5(Path("raw"))}

    def test_to_dvc_order_is_topological(self, monkeypatch, tmp_path):
        """to_dvc writes upstream before downstream regardless of add order."""
        monkeypatch.chdir(tmp_path)
        dag = Dag()
        # Add downstream first, then its upstream.
        down = Artifact(path="down", computation=Computation(cmd="make down"))
        up = Artifact(path="up", computation=Computation(cmd="make up"))
        down.computation.deps = [up]
        dag.add(down)
        dag.add(up)

        written = [Path(p).stem for p in dag.to_dvc()]
        assert written == ["up", "down"]
