"""Demand-driven restoration: assert actual downloads, commands and workspace data."""

import json
from dataclasses import dataclass, field
from hashlib import md5
from io import StringIO
from pathlib import Path

import pytest
import yaml
from dvc.repo import Repo

from dvx.cache import MaterializationSession, MaterializeError, _get_remote_odb
from dvx.run.executor import ExecutionConfig, run


@dataclass
class Pipeline:
    root: Path
    remote: Path
    hashes: dict[str, str] = field(default_factory=dict)
    downloads: list[str] = field(default_factory=list)

    def blob(self, content: bytes) -> str:
        oid = md5(content).hexdigest()
        dest = self.remote / "files" / "md5" / oid[:2] / oid[2:]
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        return oid

    def stage(
        self,
        name: str,
        content: bytes,
        dep: str | None = None,
        prefix: str = "",
    ) -> str:
        oid = self.blob(content)
        self.hashes[name] = oid
        data = {"outs": [{"path": name, "md5": oid, "size": len(content)}]}
        if dep:
            cmd = (f"echo {name} >> tmp/commands.txt; "
                   f"printf '{prefix}' > {name}; cat {dep} >> {name}")
            data["meta"] = {"computation": {"cmd": cmd, "deps": {dep: self.hashes[dep]}}}
        (self.root / (name + ".dvc")).write_text(yaml.safe_dump(data))
        return oid

    def execute(self, targets: list[str], **kwargs):
        kwargs.setdefault("max_workers", 1)
        config = ExecutionConfig(commit="never", **kwargs)
        output = StringIO()
        results = run([Path(target + ".dvc") for target in targets], config, output)
        assert [(result.path, result.success) for result in results] == [
            (result.path, True) for result in results
        ], output.getvalue()
        return results, output.getvalue()

    def commands(self) -> list[str]:
        path = self.root / "tmp" / "commands.txt"
        return path.read_text().splitlines() if path.exists() else []

    def outputs(self) -> dict[str, bytes]:
        return {name: (self.root / name).read_bytes() for name in self.hashes
                if (self.root / name).is_file()}

    def cache(self) -> dict[str, bytes]:
        return {path.parent.name + path.name: path.read_bytes()
                for path in (self.root / ".dvc/cache/files/md5").glob("*/*") if path.is_file()}

    def change_stamp(self, name: str, dep: str, oid: str) -> None:
        path = self.root / (name + ".dvc")
        data = yaml.safe_load(path.read_text())
        data["meta"]["computation"]["deps"][dep] = oid
        path.write_text(yaml.safe_dump(data))


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    remote = tmp_path / "remote"
    root.mkdir()
    remote.mkdir()
    monkeypatch.chdir(root)
    Repo.init(no_scm=True).close()
    with Repo() as repo, repo.config.edit() as config:
        config["core"]["remote"] = "local"
        config["remote"]["local"] = {"url": str(remote)}
    (root / "tmp").mkdir()
    pipeline = Pipeline(root, remote)
    pipeline.stage("raw.csv", b"raw")
    pipeline.stage("normalized.parquet", b"norm/raw", "raw.csv", "norm/")
    pipeline.stage("combined.parquet", b"combined/norm/raw", "normalized.parquet", "combined/")
    pipeline.stage("export.parquet", b"export/combined/norm/raw", "combined.parquet", "export/")
    odb = _get_remote_odb()
    filesystem = type(odb.fs)
    original_get = filesystem.get

    def get(fs, src, dest, *args, **kwargs):
        path = Path(src)
        pipeline.downloads.append(path.parent.name + path.name)
        return original_get(fs, src, dest, *args, **kwargs)

    monkeypatch.setattr(filesystem, "get", get)
    return pipeline


def test_fresh_terminal_restores_only_its_own_output(pipeline):
    results, _output = pipeline.execute(["export.parquet"])
    assert pipeline.downloads == [pipeline.hashes["export.parquet"]]
    assert pipeline.commands() == []
    assert [(result.path, result.skipped) for result in results] == [("export.parquet", True)]
    assert pipeline.outputs() == {"export.parquet": b"export/combined/norm/raw"}


def test_stale_terminal_restores_current_intermediate_only(pipeline):
    pipeline.change_stamp("export.parquet", "combined.parquet", "obsolete")
    results, _output = pipeline.execute(["export.parquet"])
    assert pipeline.downloads == [pipeline.hashes["combined.parquet"]]
    assert pipeline.commands() == ["export.parquet"]
    assert [(result.path, result.skipped) for result in results] == [
        ("combined.parquet", True), ("export.parquet", False),
    ]
    assert pipeline.outputs() == {
        "combined.parquet": b"combined/norm/raw", "export.parquet": b"export/combined/norm/raw",
    }


def test_missing_terminal_rebuilds_from_nearest_current_input(pipeline):
    oid = pipeline.hashes["export.parquet"]
    (pipeline.remote / "files/md5" / oid[:2] / oid[2:]).unlink()
    pipeline.execute(["export.parquet"])
    assert pipeline.downloads == [pipeline.hashes["combined.parquet"]]
    assert pipeline.commands() == ["export.parquet"]
    assert pipeline.outputs() == {
        "combined.parquet": b"combined/norm/raw", "export.parquet": b"export/combined/norm/raw",
    }


def test_stale_intermediate_rebuilds_and_updates_downstream_stamps(pipeline):
    new = pipeline.stage("raw.csv", b"updated raw")
    pipeline.change_stamp("combined.parquet", "normalized.parquet", "obsolete")
    pipeline.change_stamp("export.parquet", "combined.parquet", "obsolete")
    pipeline.execute(["export.parquet"])
    assert pipeline.downloads == [new]
    assert pipeline.commands() == ["normalized.parquet", "combined.parquet", "export.parquet"]
    assert pipeline.outputs() == {
        "raw.csv": b"updated raw", "normalized.parquet": b"norm/updated raw",
        "combined.parquet": b"combined/norm/updated raw", "export.parquet": b"export/combined/norm/updated raw",
    }
    combined = yaml.safe_load((pipeline.root / "combined.parquet.dvc").read_text())
    exported = yaml.safe_load((pipeline.root / "export.parquet.dvc").read_text())
    assert combined["meta"]["computation"]["deps"] == {
        "normalized.parquet": md5(b"norm/updated raw").hexdigest(),
    }
    assert exported["meta"]["computation"]["deps"] == {
        "combined.parquet": md5(b"combined/norm/updated raw").hexdigest(),
    }


def test_dry_run_fetches_nothing_and_preserves_metadata(pipeline):
    before = {path.name: path.read_bytes() for path in pipeline.root.glob("*.dvc") if path.is_file()}
    results, output = pipeline.execute(["export.parquet"], dry_run=True)
    assert pipeline.downloads == []
    assert pipeline.commands() == []
    assert pipeline.outputs() == {}
    assert pipeline.cache() == {}
    assert {path.name: path.read_bytes() for path in pipeline.root.glob("*.dvc") if path.is_file()} == before
    assert [(result.path, result.reason) for result in results] == [
        ("export.parquet", "restore required (cache availability unknown)"),
    ]
    assert output.splitlines() == [
        "Execution plan: 1 levels, 1 computations", "",
        "Dry run - showing what would execute:",
        "  export.parquet: would restore (cache availability unknown)",
    ]


def test_fresh_boundary_keeps_nontransitive_contract(pipeline):
    pipeline.stage("raw.csv", b"changed behind the boundary")
    pipeline.execute(["export.parquet"])
    assert pipeline.downloads == [pipeline.hashes["export.parquet"]]
    assert pipeline.commands() == []
    assert pipeline.outputs() == {"export.parquet": b"export/combined/norm/raw"}


@pytest.mark.parametrize("workers", [1, 4])
def test_shared_input_and_multiple_explicit_targets(pipeline, workers):
    pipeline.stage("report.parquet", b"report/combined/norm/raw", "combined.parquet", "report/")
    pipeline.stage("daily.parquet", b"daily/combined/norm/raw", "combined.parquet", "daily/")
    for name in ("report.parquet", "daily.parquet"):
        pipeline.change_stamp(name, "combined.parquet", "obsolete")
    results, _output = pipeline.execute(
        ["export.parquet", "report.parquet", "daily.parquet"], max_workers=workers,
    )
    assert sorted(pipeline.downloads) == sorted([
        pipeline.hashes["export.parquet"], pipeline.hashes["combined.parquet"],
    ])
    assert sorted(pipeline.commands()) == ["daily.parquet", "report.parquet"]
    assert sorted((result.path, result.skipped) for result in results) == [
        ("combined.parquet", True), ("daily.parquet", False),
        ("export.parquet", True), ("report.parquet", False),
    ]
    assert pipeline.outputs() == {
        "combined.parquet": b"combined/norm/raw", "export.parquet": b"export/combined/norm/raw",
        "daily.parquet": b"daily/combined/norm/raw", "report.parquet": b"report/combined/norm/raw",
    }


def test_force_restores_inputs_without_fetching_its_obsolete_output(pipeline):
    pipeline.execute(["export.parquet"], force=True)
    assert pipeline.downloads == [pipeline.hashes["combined.parquet"]]
    assert pipeline.commands() == ["export.parquet"]
    assert pipeline.outputs() == {
        "combined.parquet": b"combined/norm/raw", "export.parquet": b"export/combined/norm/raw",
    }


def test_force_expands_a_missing_input_producer(pipeline):
    oid = pipeline.hashes["combined.parquet"]
    (pipeline.remote / "files/md5" / oid[:2] / oid[2:]).unlink()
    pipeline.execute(["export.parquet"], force=True)
    assert pipeline.downloads == [pipeline.hashes["normalized.parquet"]]
    assert pipeline.commands() == ["combined.parquet", "export.parquet"]


def test_cached_target_is_trusted_without_transfers(pipeline):
    results, _output = pipeline.execute(["export.parquet"], cached_patterns=["export.*"])
    assert pipeline.downloads == []
    assert pipeline.commands() == []
    assert pipeline.outputs() == {}
    assert [(result.path, result.reason) for result in results] == [
        ("export.parquet", "cached by pattern"),
    ]


def test_no_prune_fresh_walks_and_restores_full_chain(pipeline):
    pipeline.execute(["export.parquet"], prune_fresh=False)
    assert pipeline.downloads == [pipeline.hashes[name] for name in (
        "normalized.parquet", "combined.parquet", "export.parquet",
    )]
    assert pipeline.commands() == []


def test_no_pull_deps_rebuilds_locally(pipeline):
    (pipeline.root / "raw.csv").write_bytes(b"raw")
    pipeline.execute(["export.parquet"], pull_deps=False)
    assert pipeline.downloads == []
    assert pipeline.commands() == ["normalized.parquet", "combined.parquet", "export.parquet"]


def test_force_upstream_preserves_pattern_intent(pipeline):
    pipeline.execute(["export.parquet"], force_patterns=["normalized.*"])
    assert pipeline.downloads == [pipeline.hashes[name] for name in (
        "raw.csv", "combined.parquet", "export.parquet",
    )]
    assert pipeline.commands() == ["normalized.parquet"]


@pytest.mark.parametrize("stale", [False, True])
def test_side_effect_stage_fetches_inputs_only_when_stale(pipeline, stale):
    receipt = pipeline.root / "deploy.dvc"
    receipt.write_text(yaml.safe_dump({"meta": {"computation": {
        "cmd": "echo deploy >> tmp/commands.txt", "side_effect": True,
        "deps": {"combined.parquet": "obsolete" if stale else pipeline.hashes["combined.parquet"]},
    }}}))
    pipeline.execute(["deploy"])
    assert pipeline.downloads == ([pipeline.hashes["combined.parquet"]] if stale else [])
    assert pipeline.commands() == (["deploy"] if stale else [])
    assert pipeline.outputs() == ({"combined.parquet": b"combined/norm/raw"} if stale else {})


@pytest.mark.parametrize("terminal_stale", [False, True])
def test_multi_output_producer_is_a_complete_boundary(pipeline, terminal_stale):
    extra = pipeline.blob(b"extra")
    (pipeline.root / "combined.parquet.dvc").unlink()
    (pipeline.root / "both.dvc").write_text(yaml.safe_dump({
        "outs": [
            {"path": "combined.parquet", "md5": pipeline.hashes["combined.parquet"]},
            {"path": "extra.parquet", "md5": extra},
        ],
        "meta": {"computation": {"cmd": "false", "deps": {
            "normalized.parquet": pipeline.hashes["normalized.parquet"],
        }}},
    }))
    if terminal_stale:
        pipeline.change_stamp("export.parquet", "combined.parquet", "obsolete")
    pipeline.execute(["export.parquet"])
    assert sorted(pipeline.downloads) == sorted(
        [pipeline.hashes["combined.parquet"], extra] if terminal_stale
        else [pipeline.hashes["export.parquet"]]
    )
    assert pipeline.commands() == (["export.parquet"] if terminal_stale else [])
    assert (pipeline.root / "extra.parquet").exists() is terminal_stale


def seed_tracked_directory(pipeline):
    child = pipeline.blob(b"directory input")
    data = json.dumps([{"md5": child, "relpath": "nested/input.txt"}], sort_keys=True).encode()
    bare = md5(data).hexdigest()
    path = pipeline.remote / "files/md5" / bare[:2] / (bare[2:] + ".dir")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    (pipeline.root / "data.dvc").write_text(yaml.safe_dump({
        "outs": [{"path": "data", "md5": bare + ".dir"}],
        "meta": {"computation": {"cmd": "false", "deps": {
            "raw.csv": pipeline.hashes["raw.csv"],
        }}},
    }))
    return bare + ".dir", child


@pytest.mark.parametrize("stale", [False, True])
def test_tracked_directory_inputs_are_demanded_only_for_rebuild(pipeline, stale):
    manifest, child = seed_tracked_directory(pipeline)
    exported = pipeline.stage("directory.txt", b"directory input", "raw.csv")
    sidecar = pipeline.root / "directory.txt.dvc"
    data = yaml.safe_load(sidecar.read_text())
    data["meta"]["computation"] = {
        "cmd": "echo directory.txt >> tmp/commands.txt; cat data/nested/input.txt > directory.txt",
        "deps": {"data": "obsolete" if stale else manifest.removesuffix(".dir")},
    }
    sidecar.write_text(yaml.safe_dump(data))
    pipeline.execute(["directory.txt"])
    assert pipeline.downloads == ([manifest, child] if stale else [exported])
    assert pipeline.commands() == (["directory.txt"] if stale else [])
    assert (pipeline.root / "data").exists() is stale
    assert (pipeline.root / "raw.csv").exists() is False


@pytest.mark.parametrize("failure", [PermissionError("denied"), TimeoutError("timed out")])
def test_remote_probe_errors_never_trigger_rebuild(pipeline, monkeypatch, failure):
    odb = _get_remote_odb()

    def info(_path):
        raise failure

    monkeypatch.setattr(odb.fs, "info", info)
    results = run([Path("export.parquet.dvc")], ExecutionConfig(commit="never"), StringIO())
    assert [(result.path, result.success, result.reason) for result in results] == [
        ("export.parquet", False, f"materialization failed: {pipeline.hashes['export.parquet']}: "
         f"probe failed on default: {type(failure).__name__}: {failure}"),
    ]
    assert pipeline.downloads == []
    assert pipeline.commands() == []
    assert pipeline.outputs() == {}


def test_corrupt_recorded_output_fails_instead_of_rebuild(pipeline):
    oid = pipeline.hashes["export.parquet"]
    (pipeline.remote / "files/md5" / oid[:2] / oid[2:]).write_bytes(b"corrupt")
    results = run([Path("export.parquet.dvc")], ExecutionConfig(commit="never"), StringIO())
    assert [(result.path, result.success, result.reason) for result in results] == [
        ("export.parquet", False, f"materialization failed: integrity check failed for {oid}"),
    ]
    assert pipeline.downloads == [oid]
    assert pipeline.commands() == []
    assert pipeline.outputs() == {}
    assert pipeline.cache() == {}


def test_missing_source_without_producer_fails_before_commands(pipeline):
    for name in pipeline.hashes:
        oid = pipeline.hashes[name]
        (pipeline.remote / "files/md5" / oid[:2] / oid[2:]).unlink()
    results = run([Path("export.parquet.dvc")], ExecutionConfig(commit="never"), StringIO())
    assert [(result.path, result.success, result.reason) for result in results] == [
        ("normalized.parquet", False, "materialization failed: missing required artifact(s): raw.csv"),
    ]
    assert pipeline.downloads == []
    assert pipeline.commands() == []
    assert pipeline.outputs() == {}


def test_local_changes_still_require_rebuilding(pipeline):
    (pipeline.root / "export.parquet").write_bytes(b"local modification")
    pipeline.execute(["export.parquet"])
    assert pipeline.downloads == [pipeline.hashes["combined.parquet"]]
    assert pipeline.commands() == ["export.parquet"]
    assert pipeline.outputs() == {
        "combined.parquet": b"combined/norm/raw", "export.parquet": b"export/combined/norm/raw",
    }


def test_named_remote_and_ordered_read_fallbacks(pipeline):
    empty = pipeline.root / "staging"
    empty.mkdir()
    with Repo() as repo, repo.config.edit() as config:
        config["remote"]["staging"] = {"url": str(empty)}
    pipeline.execute(["export.parquet"], remote="staging", fallback_remotes=("local",))
    assert pipeline.downloads == [pipeline.hashes["export.parquet"]]
    assert pipeline.commands() == []
    assert list(empty.rglob("*")) == []


def test_due_fetch_schedule_rebuilds_without_fetching_obsolete_output(pipeline):
    sidecar = pipeline.root / "export.parquet.dvc"
    data = yaml.safe_load(sidecar.read_text())
    data["meta"]["computation"]["fetch"] = {"schedule": "daily", "last_run": "2000-01-01T00:00:00Z"}
    sidecar.write_text(yaml.safe_dump(data))
    pipeline.execute(["export.parquet"])
    assert pipeline.downloads == [pipeline.hashes["combined.parquet"]]
    assert pipeline.commands() == ["export.parquet"]


def test_valid_local_cache_restores_without_remote_reads(pipeline):
    oid = pipeline.hashes["export.parquet"]
    cached = pipeline.root / ".dvc/cache/files/md5" / oid[:2] / oid[2:]
    cached.parent.mkdir(parents=True, exist_ok=True)
    cached.write_bytes(b"export/combined/norm/raw")
    pipeline.execute(["export.parquet"])
    assert pipeline.downloads == []
    assert pipeline.commands() == []
    assert pipeline.outputs() == {"export.parquet": b"export/combined/norm/raw"}


def test_shared_integrity_failure_is_fetched_once(pipeline):
    oid = pipeline.hashes["export.parquet"]
    (pipeline.remote / "files/md5" / oid[:2] / oid[2:]).write_bytes(b"corrupt")
    session = MaterializationSession()
    for _ in range(2):
        with pytest.raises(MaterializeError) as error:
            session.fetch(oid)
        assert str(error.value) == f"integrity check failed for {oid}"
    assert pipeline.downloads == [oid]
    assert pipeline.cache() == {}


def test_incomplete_directory_rebuild_avoids_existing_old_children(pipeline):
    present = pipeline.blob(b"old child")
    missing = md5(b"extra").hexdigest()
    data = json.dumps([
        {"md5": present, "relpath": "a.txt"},
        {"md5": missing, "relpath": "b.txt"},
    ], sort_keys=True).encode()
    bare = md5(data).hexdigest()
    manifest = bare + ".dir"
    dest = pipeline.remote / "files/md5" / bare[:2] / (bare[2:] + ".dir")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    (pipeline.root / "data.dvc").write_text(yaml.safe_dump({
        "outs": [{"path": "data", "md5": manifest}],
        "meta": {"computation": {
            "cmd": "echo data >> tmp/commands.txt; mkdir -p data; cat raw.csv > /dev/null; "
                   "printf 'old child' > data/a.txt; printf 'extra' > data/b.txt",
            "deps": {"raw.csv": pipeline.hashes["raw.csv"]},
        }},
    }))
    pipeline.stage("directory-export.txt", b"old child", "raw.csv")
    sidecar = pipeline.root / "directory-export.txt.dvc"
    info = yaml.safe_load(sidecar.read_text())
    info["meta"]["computation"] = {
        "cmd": "echo directory-export.txt >> tmp/commands.txt; cat data/a.txt > directory-export.txt",
        "deps": {"data": "obsolete"},
    }
    sidecar.write_text(yaml.safe_dump(info))
    pipeline.execute(["directory-export.txt"])
    assert pipeline.downloads == [manifest, pipeline.hashes["raw.csv"]]
    assert pipeline.commands() == ["data", "directory-export.txt"]
    assert (pipeline.root / "data/a.txt").read_bytes() == b"old child"
    assert (pipeline.root / "data/b.txt").read_bytes() == b"extra"


def test_cached_wins_over_force_without_input_probes(pipeline, monkeypatch):
    odb = _get_remote_odb()

    def info(path):
        pytest.fail(f"cached target unexpectedly probed {path}")

    monkeypatch.setattr(odb.fs, "info", info)
    pipeline.execute(["export.parquet"], force=True, cached_patterns=["export.*"])
    assert pipeline.downloads == []
    assert pipeline.commands() == []
    assert pipeline.outputs() == {}


def test_dry_run_does_not_trust_a_corrupt_cached_manifest(pipeline):
    manifest, child = seed_tracked_directory(pipeline)
    cached = pipeline.root / ".dvc/cache/files/md5" / manifest[:2] / manifest[2:]
    cached.parent.mkdir(parents=True, exist_ok=True)
    cached.write_bytes(b"corrupt")
    sidecar = pipeline.root / "export.parquet.dvc"
    data = yaml.safe_load(sidecar.read_text())
    data["meta"]["computation"]["deps"] = {"data/nested/input.txt": child}
    sidecar.write_text(yaml.safe_dump(data))
    results, _output = pipeline.execute(["export.parquet"], dry_run=True)
    assert [(result.path, result.reason) for result in results] == [
        ("export.parquet", "recorded currency unknown (directory manifest unavailable)"),
    ]
    assert pipeline.downloads == []
    assert pipeline.commands() == []
    assert pipeline.outputs() == {}
    assert pipeline.cache() == {manifest: b"corrupt"}
