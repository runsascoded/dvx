"""Tests for dvx.run.artifact module."""

import os
from pathlib import Path

import pytest
import yaml

from dvx.run.artifact import Artifact, Computation, delayed, materialize, write_all_dvc


def test_artifact_basic(tmp_path):
    """Test basic Artifact creation."""
    artifact = Artifact(path="output.txt")

    assert artifact.path == "output.txt"
    assert artifact.computation is None
    assert artifact.md5 is None
    assert artifact.size is None


def test_artifact_with_computation():
    """Test Artifact with computation."""
    comp = Computation(
        cmd="python process.py",
        deps=["input.txt"],
    )
    artifact = Artifact(
        path="output.txt",
        computation=comp,
    )

    assert artifact.computation is not None
    assert artifact.computation.cmd == "python process.py"
    assert artifact.computation.deps == ["input.txt"]


def test_artifact_from_path(tmp_path):
    """Test Artifact.from_path creates artifact with hash."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello world\n")

    artifact = Artifact.from_path(test_file)

    assert artifact.md5 is not None
    assert artifact.size == 12
    assert len(artifact.md5) == 32


def test_artifact_from_path_missing():
    """Test Artifact.from_path raises for missing file."""
    with pytest.raises(FileNotFoundError):
        Artifact.from_path("/nonexistent/file.txt")


def test_artifact_from_dvc(tmp_path):
    """Test Artifact.from_dvc loads from .dvc file."""
    # Create .dvc file
    dvc_file = tmp_path / "output.txt.dvc"
    dvc_content = {
        "outs": [
            {
                "md5": "abc123",
                "size": 100,
                "path": "output.txt",
            }
        ],
        "meta": {
            "computation": {
                "cmd": "python process.py",
                "deps": {"input.txt": "111222"},
            }
        },
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    artifact = Artifact.from_dvc(tmp_path / "output.txt")

    assert artifact is not None
    assert artifact.md5 == "abc123"
    assert artifact.size == 100
    assert artifact.computation is not None
    assert artifact.computation.cmd == "python process.py"


def test_artifact_from_dvc_missing(tmp_path):
    """Test Artifact.from_dvc returns None for missing .dvc file."""
    artifact = Artifact.from_dvc(tmp_path / "nonexistent.txt")
    assert artifact is None


def test_artifact_write_dvc(tmp_path):
    """Test Artifact.write_dvc creates .dvc file."""
    # Create output file
    output = tmp_path / "output.txt"
    output.write_text("result data\n")

    artifact = Artifact(
        path=str(output),
        computation=Computation(cmd="echo test > output.txt"),
    )

    dvc_path = artifact.write_dvc()

    assert dvc_path.exists()
    with open(dvc_path) as f:
        data = yaml.safe_load(f)

    assert data["outs"][0]["path"] == "output.txt"
    assert data["outs"][0]["md5"] is not None
    assert data["meta"]["computation"]["cmd"] == "echo test > output.txt"


def test_artifact_exists(tmp_path):
    """Test Artifact.exists() method."""
    existing = tmp_path / "existing.txt"
    existing.write_text("hello")

    artifact1 = Artifact(path=str(existing))
    assert artifact1.exists() is True

    artifact2 = Artifact(path=str(tmp_path / "nonexistent.txt"))
    assert artifact2.exists() is False


def test_artifact_is_computed():
    """Test Artifact.is_computed() method."""
    artifact1 = Artifact(path="test.txt", md5="abc123")
    assert artifact1.is_computed() is True

    artifact2 = Artifact(path="test.txt")
    assert artifact2.is_computed() is False


def test_computation_get_dep_paths():
    """Test Computation.get_dep_paths()."""
    dep1 = Artifact(path="input1.txt")
    comp = Computation(
        cmd="python process.py",
        deps=[dep1, "input2.txt", Path("input3.txt")],
    )

    paths = comp.get_dep_paths()

    assert len(paths) == 3
    assert Path("input1.txt") in paths
    assert Path("input2.txt") in paths
    assert Path("input3.txt") in paths


def test_computation_get_dep_hashes(tmp_path):
    """Test Computation.get_dep_hashes()."""
    # Create dep file
    dep_file = tmp_path / "input.txt"
    dep_file.write_text("input data\n")

    # Artifact with known hash - use a different path so it doesn't get overwritten
    dep1 = Artifact(path=str(tmp_path / "other.txt"), md5="known_hash")

    comp = Computation(
        cmd="python process.py",
        deps=[dep1, str(dep_file)],
    )

    hashes = comp.get_dep_hashes()

    # First dep should use known hash (since file doesn't exist, it uses the provided md5)
    assert hashes[str(tmp_path / "other.txt")] == "known_hash"
    # Second dep should compute hash from file
    assert len(hashes[str(dep_file)]) == 32  # MD5 hex digest length


def test_artifact_get_upstream():
    """Test Artifact.get_upstream() returns only Artifact deps."""
    dep1 = Artifact(path="input1.txt")
    dep2 = Artifact(path="input2.txt")

    artifact = Artifact(
        path="output.txt",
        computation=Computation(
            cmd="cat input*.txt > output.txt",
            deps=[dep1, "string_dep.txt", dep2],
        ),
    )

    upstream = artifact.get_upstream()

    assert len(upstream) == 2
    assert dep1 in upstream
    assert dep2 in upstream


def test_artifact_walk_upstream():
    """Test Artifact.walk_upstream() collects all ancestors."""
    # Build a simple DAG: leaf -> mid -> output
    leaf = Artifact(path="leaf.txt")
    mid = Artifact(
        path="mid.txt",
        computation=Computation(cmd="process leaf", deps=[leaf]),
    )
    output = Artifact(
        path="output.txt",
        computation=Computation(cmd="process mid", deps=[mid]),
    )

    ancestors = output.walk_upstream()

    # Should be in dependency order (leaf first)
    assert len(ancestors) == 3
    assert ancestors[0] == leaf
    assert ancestors[1] == mid
    assert ancestors[2] == output


def test_delayed_decorator():
    """Test @delayed decorator marks functions."""

    @delayed
    def make_artifact(name: str) -> Artifact:
        return Artifact(path=f"{name}.txt")

    # Should work normally
    result = make_artifact("test")
    assert isinstance(result, Artifact)
    assert result.path == "test.txt"

    # Should be marked as delayed
    assert hasattr(make_artifact, "_dvx_delayed")
    assert make_artifact._dvx_delayed is True


def test_artifact_hash_eq():
    """Test Artifact __hash__ and __eq__."""
    a1 = Artifact(path="same.txt")
    a2 = Artifact(path="same.txt")
    a3 = Artifact(path="different.txt")

    assert a1 == a2
    assert a1 != a3
    assert hash(a1) == hash(a2)

    # Should be usable in sets
    s = {a1, a2, a3}
    assert len(s) == 2


def test_computation_with_git_deps():
    """Test Computation with git_deps field."""
    comp = Computation(
        cmd="python process.py",
        deps=["input.txt"],
        git_deps=["script.py", "lib.py"],
    )

    assert comp.git_deps == ["script.py", "lib.py"]


def test_computation_get_git_dep_hashes_with_known_md5():
    """Test get_git_dep_hashes() uses md5 from Artifact when available."""
    dep = Artifact(path="script.py", md5="known_blob_sha")
    comp = Computation(
        cmd="python process.py",
        git_deps=[dep],
    )

    hashes = comp.get_git_dep_hashes()

    assert hashes == {"script.py": "known_blob_sha"}


def test_artifact_from_dvc_with_git_deps(tmp_path):
    """Test Artifact.from_dvc() populates git_deps from .dvc file."""
    dvc_file = tmp_path / "output.txt.dvc"
    dvc_content = {
        "outs": [{"md5": "abc123", "size": 100, "path": "output.txt"}],
        "meta": {
            "computation": {
                "cmd": "python process.py",
                "deps": {"input.txt": "111222"},
                "git_deps": {"script.py": "aabbccdd", "lib.py": "eeff0011"},
            }
        },
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    artifact = Artifact.from_dvc(tmp_path / "output.txt")

    assert artifact is not None
    assert artifact.computation is not None
    assert len(artifact.computation.deps) == 1
    assert len(artifact.computation.git_deps) == 2

    git_dep_paths = {d.path for d in artifact.computation.git_deps}
    assert git_dep_paths == {"script.py", "lib.py"}

    # Check blob SHAs are stored as md5 on the Artifact objects
    git_dep_map = {d.path: d.md5 for d in artifact.computation.git_deps}
    assert git_dep_map == {"script.py": "aabbccdd", "lib.py": "eeff0011"}


def test_artifact_from_dvc_side_effect(tmp_path):
    """Test Artifact.from_dvc() for side-effect stages (no outs)."""
    dvc_file = tmp_path / "deploy.dvc"
    dvc_content = {
        "meta": {
            "computation": {
                "cmd": "wrangler pages deploy dist",
                "deps": {"dist/index.html": "aaa111"},
            }
        }
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    artifact = Artifact.from_dvc(tmp_path / "deploy")

    assert artifact is not None
    assert artifact.path == str(tmp_path / "deploy")
    assert artifact.md5 is None
    assert artifact.computation is not None
    assert artifact.computation.cmd == "wrangler pages deploy dist"
    assert len(artifact.computation.deps) == 1


def test_artifact_from_dvc_side_effect_subdirectory(tmp_path):
    """Artifact.from_dvc() preserves directory path for side-effect in subdirs."""
    subdir = tmp_path / "api"
    subdir.mkdir()
    dvc_file = subdir / "deploy.dvc"
    dvc_content = {
        "meta": {
            "computation": {
                "cmd": "wrangler deploy",
                "deps": {"dist/index.html": "aaa"},
            }
        }
    }
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    artifact = Artifact.from_dvc(subdir / "deploy")

    assert artifact is not None
    assert artifact.path == str(subdir / "deploy")
    assert artifact.md5 is None


def test_materialize_single(tmp_path):
    """Test materialize() runs a computation and updates the artifact."""
    os.chdir(tmp_path)

    output = tmp_path / "result.txt"
    artifact = Artifact(
        path=str(output),
        computation=Computation(cmd=f"echo hello > {output}"),
    )

    computed = materialize([artifact], update_dvc=False)

    assert len(computed) == 1
    assert output.exists()
    assert output.read_text().strip() == "hello"
    assert computed[0].md5 is not None


def test_materialize_skips_fresh(tmp_path):
    """Test materialize() skips already-fresh artifacts (doesn't re-run cmd)."""
    os.chdir(tmp_path)

    output = tmp_path / "result.txt"
    output.write_text("existing\n")

    from dvx.run.hash import compute_md5
    from dvx.run.dvc_files import write_dvc_file
    md5 = compute_md5(output)

    # Write .dvc so it's "fresh"
    write_dvc_file(output_path=output, md5=md5, size=output.stat().st_size)

    artifact = Artifact(
        path=str(output),
        md5=md5,
        computation=Computation(cmd="echo should-not-run"),
    )

    materialize([artifact], update_dvc=False)

    # File content should be unchanged (cmd was not executed)
    assert output.read_text() == "existing\n"


def test_materialize_error_raises(tmp_path):
    """Test materialize() raises on command failure."""
    os.chdir(tmp_path)

    output = tmp_path / "result.txt"
    artifact = Artifact(
        path=str(output),
        computation=Computation(cmd="false"),  # always fails
    )

    with pytest.raises(RuntimeError, match="Computation failed"):
        materialize([artifact], update_dvc=False)


def test_walk_upstream_prunes_at_fresh(tmp_path):
    """walk_upstream stops at fresh artifacts; further-upstream is not visited."""
    os.chdir(tmp_path)

    from dvx.run.dvc_files import write_dvc_file
    from dvx.run.hash import compute_md5

    # Build chain bottom-up: leaf.txt -> mid.txt -> top.txt
    leaf_path = tmp_path / "leaf.txt"
    leaf_path.write_text("leaf-data\n")
    leaf_md5 = compute_md5(leaf_path)
    # leaf has no .dvc — simulate raw input file
    leaf = Artifact(path=str(leaf_path), md5=leaf_md5)

    mid_path = tmp_path / "mid.txt"
    mid_path.write_text("mid-data\n")
    mid_md5 = compute_md5(mid_path)
    write_dvc_file(
        output_path=mid_path,
        md5=mid_md5,
        size=mid_path.stat().st_size,
        cmd="touch mid.txt",
        deps={str(leaf_path): leaf_md5},
    )
    mid = Artifact(
        path=str(mid_path),
        md5=mid_md5,
        computation=Computation(cmd="touch mid.txt", deps=[leaf]),
    )

    top_path = tmp_path / "top.txt"
    top_path.write_text("top-data\n")
    top_md5 = compute_md5(top_path)
    write_dvc_file(
        output_path=top_path,
        md5=top_md5,
        size=top_path.stat().st_size,
        cmd="touch top.txt",
        deps={str(mid_path): mid_md5},
    )
    top = Artifact(
        path=str(top_path),
        md5=top_md5,
        computation=Computation(cmd="touch top.txt", deps=[mid]),
    )

    # Default: prune at fresh — top is fresh, so mid (and leaf) are not visited
    walked = top.walk_upstream()
    assert [a.path for a in walked] == [str(top_path)]

    # Disable pruning: full chain is walked
    walked_full = top.walk_upstream(prune_fresh=False)
    assert [a.path for a in walked_full] == [str(leaf_path), str(mid_path), str(top_path)]


def test_walk_upstream_prune_skips_missing_raw_dep(tmp_path):
    """A fresh chain stays prunable even when an ancestor's raw input is gone."""
    os.chdir(tmp_path)

    from dvx.run.dvc_files import write_dvc_file
    from dvx.run.hash import compute_md5

    raw = tmp_path / "raw.txt"
    raw.write_text("raw-data\n")
    raw_md5 = compute_md5(raw)

    a_path = tmp_path / "a.txt"
    a_path.write_text("a-data\n")
    a_md5 = compute_md5(a_path)
    write_dvc_file(
        output_path=a_path,
        md5=a_md5,
        size=a_path.stat().st_size,
        cmd="touch a.txt",
        deps={str(raw): raw_md5},
    )

    b_path = tmp_path / "b.txt"
    b_path.write_text("b-data\n")
    b_md5 = compute_md5(b_path)
    write_dvc_file(
        output_path=b_path,
        md5=b_md5,
        size=b_path.stat().st_size,
        cmd="touch b.txt",
        deps={str(a_path): a_md5},
    )

    # Now delete the raw input — `a` would no longer be runnable from sources,
    # but `b` is fresh w.r.t. its recorded dep on `a`, so walking from `b`
    # should not trip over the missing raw.
    raw.unlink()

    a_artifact = Artifact(
        path=str(a_path),
        md5=a_md5,
        computation=Computation(cmd="touch a.txt", deps=[Artifact(path=str(raw), md5=raw_md5)]),
    )
    b_artifact = Artifact(
        path=str(b_path),
        md5=b_md5,
        computation=Computation(cmd="touch b.txt", deps=[a_artifact]),
    )

    # With pruning (default), only b is visited — a's missing raw never gets checked.
    walked = b_artifact.walk_upstream()
    assert [a.path for a in walked] == [str(b_path)]


def test_get_dep_hashes_recompute_falls_back_to_recorded(tmp_path):
    """recompute=True must not drop deps whose files aren't on disk locally.

    Reproduces the spec's bonus footgun: a no-op rebuild used to silently
    strip the deps map from the .dvc file when the upstream wasn't materialized
    locally (e.g. with --cached or pruned upstream).
    """
    os.chdir(tmp_path)

    # Dep with a known md5 but no file on disk
    dep = Artifact(path=str(tmp_path / "missing.txt"), md5="recorded-md5")
    comp = Computation(cmd="noop", deps=[dep])

    hashes = comp.get_dep_hashes(recompute=True)

    assert hashes == {str(tmp_path / "missing.txt"): "recorded-md5"}
