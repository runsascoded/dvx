"""Cross-remote composition against real DVC ODBs and an object-only HTTP server."""

import json
from dataclasses import dataclass, field
from hashlib import md5
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from time import sleep
from types import SimpleNamespace

import pytest
import yaml
from click.testing import CliRunner
from dvc.repo import Repo

from dvx.cli import cli
from dvx.transfer import Blob, ComposedTransfer, Remote, TransferError, TransferPlan


@dataclass
class Repository:
    root: Path
    remotes: dict[str, Path]

    def cache_path(self, oid: str, location: str = "cache") -> Path:
        root = self.root / ".dvc" / "cache" if location == "cache" else self.remotes[location]
        return root / "files" / "md5" / oid[:2] / oid[2:]

    def store(
        self,
        data: bytes,
        locations: tuple[str, ...],
        directory: bool = False,
    ) -> str:
        oid = md5(data).hexdigest() + (".dir" if directory else "")
        for location in locations:
            path = self.cache_path(oid, location)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return oid

    def track(
        self,
        name: str,
        data: bytes,
        locations: tuple[str, ...] = ("cache",),
        directory: bool = False,
    ) -> str:
        oid = self.store(data, locations, directory)
        sidecar = self.root / (name + ".dvc")
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        sidecar.write_text(yaml.safe_dump({"outs": [{
            "md5": oid, "size": len(data), "hash": "md5", "path": Path(name).name,
        }]}))
        return oid

    def contents(self, location: str) -> dict[str, bytes]:
        root = self.root / ".dvc" / "cache" if location == "cache" else self.remotes[location]
        return {path.parent.name + path.name: path.read_bytes()
                for path in root.glob("files/md5/*/*") if path.is_file()}


@pytest.fixture
def repository(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    root.mkdir()
    monkeypatch.chdir(root)
    Repo.init(no_scm=True).close()
    remotes = {name: tmp_path / name for name in ("staging", "canonical", "second")}
    for path in remotes.values():
        path.mkdir()
    with Repo() as repo, repo.config.edit() as config:
        config["core"]["remote"] = "staging"
        config["remote"] = {name: {"url": str(path)} for name, path in remotes.items()}
    return Repository(root, remotes)


@dataclass
class HTTPObjects:
    objects: dict[str, bytes] = field(default_factory=dict)
    failures: dict[str, int | str] = field(default_factory=dict)
    requests: list[tuple[str, str]] = field(default_factory=list)
    url: str = ""

    def put(self, oid: str, content: bytes) -> str:
        path = f"/files/md5/{oid[:2]}/{oid[2:]}"
        self.objects[path] = content
        return path


@pytest.fixture
def public(repository, monkeypatch):
    from dvc_http import HTTPFileSystem

    monkeypatch.setattr(HTTPFileSystem, "SESSION_RETRIES", 1)
    objects = HTTPObjects()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.serve()

        def do_HEAD(self):
            self.serve()

        def do_PUT(self):
            self.serve()

        def do_POST(self):
            self.serve()

        def serve(self):
            objects.requests.append((self.command, self.path))
            if self.command not in {"GET", "HEAD"} or not self.path.startswith("/files/md5/"):
                self.send_response(405)
                self.end_headers()
                return
            failure = objects.failures.get(self.path)
            if failure == "timeout":
                sleep(0.2)
                return
            content = objects.objects.get(self.path)
            status = failure or (200 if content is not None else 404)
            self.send_response(status)
            self.send_header("Content-Length", str(len(content) if status == 200 else 0))
            self.end_headers()
            if self.command == "GET" and status == 200:
                self.wfile.write(content)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    objects.url = f"http://127.0.0.1:{server.server_port}"
    with Repo() as repo, repo.config.edit() as config:
        config["remote"]["public"] = {"url": objects.url, "read_timeout": 0.05}
    try:
        yield objects
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_push_plan_and_execution(repository):
    new = repository.track("new.txt", b"new")
    excluded = repository.track("shared.txt", b"shared", ("canonical",))
    present = repository.track("present.txt", b"present", ("staging",))
    repository.store(b"unrelated", ("cache",))
    repository.track("duplicate.txt", b"shared", ("canonical",))
    with Repo() as repo, ComposedTransfer(repo, None, ("canonical",), 2) as transfer:
        plan = transfer.plan(None, "push")
        assert plan == TransferPlan(tuple(sorted([
            Blob(new, "new.txt", 3, "upload"),
            Blob(excluded, "duplicate.txt", 6, "excluded", "canonical"),
            Blob(present, "present.txt", 7, "destination", "staging"),
        ], key=lambda blob: blob.oid)), ())
        assert repository.contents("staging") == {present: b"present"}
        assert transfer.execute(plan, "push") == 1
        transfer.verify(plan)
    assert repository.contents("staging") == {new: b"new", present: b"present"}
    assert repository.contents("canonical") == {excluded: b"shared"}


def test_push_dry_run_matches_real_plan(repository):
    oid = repository.track("new.txt", b"new")
    shared = repository.track("shared.txt", b"shared", ("canonical",))
    args = ["push", "--exclude-remote", "canonical", "-V"]
    dry = CliRunner().invoke(cli, [*args, "-n"])
    assert dry.exit_code == 0, dry.output
    assert repository.contents("staging") == {}
    actual = CliRunner().invoke(cli, args)
    assert actual.exit_code == 0, actual.output
    lines = [
        "Already at staging: 0 blob(s).",
        "Excluded via canonical: 1 blob(s).",
        *[f"  {blob.action}{' (canonical)' if blob.remote else ''}: {blob.oid}  {blob.path}"
          for blob in sorted([
              Blob(oid, "new.txt", 3, "upload"),
              Blob(shared, "shared.txt", 6, "excluded", "canonical"),
          ], key=lambda blob: blob.oid)],
        "Errors: 0.",
    ]
    assert dry.output.splitlines() == ["Would upload 1 blob(s) (3 B).", *lines]
    assert actual.output.splitlines() == [
        "Upload 1 blob(s) (3 B).", *lines, "1 blob(s) pushed.",
        "Verified 2 blob(s) across remote union: staging, canonical.",
    ]
    assert repository.contents("staging") == {oid: b"new"}


def test_missing_local_aborts_plan_before_any_upload(repository):
    repository.track("good.txt", b"good")
    missing = repository.track("missing.txt", b"missing", ())
    message = f"{missing}: missing or invalid local blob required for upload"
    with Repo() as repo, ComposedTransfer(repo, "staging", ("canonical",)) as transfer:
        plan = transfer.plan(None, "push")
        assert plan.errors == (message,)
        with pytest.raises(TransferError) as error:
            transfer.execute(plan, "push")
        assert str(error.value) == message
    assert repository.contents("staging") == {}


def seed_directory(repository, manifest_location=("canonical",)):
    new = repository.store(b"new", ("cache",))
    shared = repository.store(b"shared", ("canonical",))
    present = repository.store(b"present", ("staging",))
    entries = [{"md5": oid, "relpath": path} for oid, path in [
        (new, "nested/new.txt"), (shared, "nested/shared.txt"),
        (present, "present.txt"), (shared, "duplicate.txt"),
    ]]
    manifest_data = json.dumps(entries, sort_keys=True).encode()
    manifest = repository.track("data", manifest_data, manifest_location, directory=True)
    return manifest, manifest_data, new, shared, present


def test_directory_push_remote_manifest_and_inner_backfill(repository):
    manifest, data, new, shared, present = seed_directory(repository, ("staging",))
    with Repo() as repo, ComposedTransfer(repo, "staging", ("canonical",)) as transfer:
        plan = transfer.plan(["data"], "push")
        assert plan.errors == ()
        assert {blob.oid: (blob.action, blob.remote) for blob in plan.blobs} == {
            manifest: ("destination", "staging"), new: ("upload", None),
            shared: ("excluded", "canonical"), present: ("destination", "staging"),
        }
        assert repository.contents("cache") == {new: b"new"}
        assert transfer.execute(plan, "push") == 1
        transfer.verify(plan)
    assert repository.contents("staging") == {manifest: data, new: b"new", present: b"present"}


@pytest.mark.parametrize("command", ["fetch", "pull"])
def test_directory_fallback_and_checkout(repository, command):
    manifest, data, new, shared, present = seed_directory(repository)
    result = CliRunner().invoke(cli, [command, "-r", "staging", "--fallback-remote", "canonical", "data"])
    assert result.exit_code == 0, result.output
    assert repository.contents("cache") == {
        manifest: data, new: b"new", shared: b"shared", present: b"present",
    }
    if command == "pull":
        assert {str(path.relative_to(repository.root / "data")): path.read_bytes()
                for path in (repository.root / "data").rglob("*") if path.is_file()} == {
            "nested/new.txt": b"new", "nested/shared.txt": b"shared",
            "present.txt": b"present", "duplicate.txt": b"shared",
        }
    else:
        assert (repository.root / "data").exists() is False


def test_missing_manifest_fails_clearly(repository):
    oid = repository.track("data", b"[]", (), directory=True)
    with Repo() as repo, ComposedTransfer(repo, "staging", ("canonical",)) as transfer:
        plan = transfer.plan(["data"], "pull")
    assert plan.errors == (f"{oid}: not found; attempted remotes: staging, canonical",)


def test_union_verification_checks_excluded_blobs_again(repository):
    shared = repository.track("shared.txt", b"shared", ("canonical",))
    with Repo() as repo, ComposedTransfer(repo, "staging", ("canonical",)) as transfer:
        plan = transfer.plan(None, "push")
        assert transfer.execute(plan, "push") == 0
        repository.cache_path(shared, "canonical").unlink()
        with pytest.raises(TransferError) as error:
            transfer.verify(plan)
        assert str(error.value) == f"{shared}: not found; attempted remotes: staging, canonical"


def test_http_exclusion_probes_only_candidates(repository, public):
    shared = repository.track("shared.txt", b"shared", ())
    new = repository.track("new.txt", b"new")
    shared_path = public.put(shared, b"shared")
    new_path = f"/files/md5/{new[:2]}/{new[2:]}"
    repository.store(b"unrelated", ("cache",))
    with Repo() as repo, ComposedTransfer(repo, "staging", ("public",), 1) as transfer:
        plan = transfer.plan(None, "push")
        assert plan.errors == ()
        assert [blob.oid for blob in plan.classified("excluded")] == [shared]
        assert transfer.execute(plan, "push") == 1
    assert sorted(public.requests) == sorted([("GET", shared_path), ("GET", new_path)])
    assert repository.contents("staging") == {new: b"new"}
    assert public.objects == {shared_path: b"shared"}


@pytest.mark.parametrize("command", ["fetch", "pull"])
def test_http_fallback_directory_gets_no_listing(repository, public, command):
    child = repository.store(b"hello", ())
    data = json.dumps([{"md5": child, "relpath": "nested/hello.txt"}], sort_keys=True).encode()
    manifest = repository.track("data", data, (), directory=True)
    child_path = public.put(child, b"hello")
    manifest_path = public.put(manifest, data)
    result = CliRunner().invoke(cli, [command, "--fallback-remote", "public", "data"])
    assert result.exit_code == 0, result.output
    assert sorted(public.requests) == sorted([
        ("GET", manifest_path), ("GET", manifest_path),
        ("GET", child_path), ("GET", child_path),
    ])
    assert repository.contents("cache") == {manifest: data, child: b"hello"}
    assert (repository.root / "data").exists() is (command == "pull")


def test_ordered_fallback_stops_at_first_match_and_uses_local(repository, public):
    oid = repository.track("hello.txt", b"hello", ("canonical", "second"))
    local = repository.track("local.txt", b"local")
    primary = repository.track("primary.txt", b"primary", ("staging",))
    with Repo() as repo, ComposedTransfer(repo, None, ("canonical", "second", "public")) as transfer:
        plan = transfer.plan(None, "pull")
        assert {blob.oid: (blob.action, blob.remote) for blob in plan.blobs} == {
            oid: ("download", "canonical"), local: ("local", None),
            primary: ("download", "staging"),
        }
        assert transfer.execute(plan, "pull") == 2
    assert public.requests == []
    assert repository.contents("cache") == {oid: b"hello", local: b"local", primary: b"primary"}


@pytest.mark.parametrize("failure", [403, 500, "timeout"])
@pytest.mark.parametrize("direction", ["push", "pull"])
def test_http_failure_is_never_absence(repository, public, failure, direction):
    oid = repository.track("hello.txt", b"hello", ("cache",) if direction == "push" else ())
    path = public.put(oid, b"hello")
    public.failures[path] = failure
    with Repo() as repo, ComposedTransfer(repo, "public", ("staging",)) as transfer:
        plan = transfer.plan(None, direction)
        assert len(plan.errors) == 1
        assert plan.blobs == ()
        normalized = plan.errors[0].replace(public.url, "<public>")
        detail = {
            403: f"ClientResponseError: 403, message='Forbidden', url='<public>{path}'",
            500: f"ClientResponseError: 500, message='Internal Server Error', url='<public>{path}'",
            "timeout": "FSTimeoutError: ",
        }[failure]
        assert normalized == f"{oid}: probe failed on public: {detail}"
        with pytest.raises(TransferError) as error:
            transfer.execute(plan, direction)
        assert str(error.value) == plan.errors[0]
    assert public.requests == [("GET", path)]
    assert repository.contents("staging") == {}


def test_corrupt_download_fails_without_trying_next_remote(repository, public):
    oid = repository.track("hello.txt", b"hello", ("canonical",))
    path = public.put(oid, b"bad")
    with Repo() as repo, ComposedTransfer(repo, "public", ("canonical",)) as transfer:
        plan = transfer.plan(None, "pull")
        assert plan.errors == ()
        with pytest.raises(TransferError) as error:
            transfer.execute(plan, "pull")
        assert str(error.value) == (
            "Partial transfer: 0/1 blob(s) transferred; target incomplete\n"
            f"{oid}: integrity check failed on public"
        )
    assert public.requests == [("GET", path), ("GET", path)]
    assert repository.contents("cache") == {}
    assert (repository.root / "hello.txt").exists() is False


def test_invalid_local_cache_is_replaced(repository):
    oid = repository.track("hello.txt", b"hello", ("cache", "canonical"))
    repository.cache_path(oid).write_bytes(b"corrupt")
    result = CliRunner().invoke(cli, ["fetch", "--fallback-remote", "canonical", "hello.txt"])
    assert result.exit_code == 0, result.output
    assert repository.contents("cache") == {oid: b"hello"}


@pytest.mark.parametrize("command", ["push", "pull", "fetch"])
@pytest.mark.parametrize("flag", ["-a", "-A", "-T"])
def test_history_flags_are_explicitly_rejected(repository, command, flag):
    option = "--exclude-remote" if command == "push" else "--fallback-remote"
    result = CliRunner().invoke(cli, [command, option, "canonical", flag])
    assert result.exit_code == 1
    assert result.output == (
        "Error: Cross-remote transfers do not support --all-branches, --all-commits, "
        "--all-tags, or --ref. Use current-worktree targets.\n"
    )


def test_pull_ref_is_explicitly_rejected(repository):
    result = CliRunner().invoke(cli, ["pull", "--fallback-remote", "canonical", "-R", "HEAD"])
    assert result.exit_code == 1
    assert result.output == (
        "Error: Cross-remote transfers do not support --all-branches, --all-commits, "
        "--all-tags, or --ref. Use current-worktree targets.\n"
    )


def test_fallback_dry_run_does_not_change_cache_or_workspace(repository):
    manifest, _data, new, _shared, _present = seed_directory(repository)
    before = repository.contents("cache")
    result = CliRunner().invoke(cli, ["pull", "-n", "--fallback-remote", "canonical", "data"])
    assert result.exit_code == 0, result.output
    assert repository.contents("cache") == before == {new: b"new"}
    assert repository.cache_path(manifest).exists() is False
    assert (repository.root / "data").exists() is False


def test_meta_only_fallback_does_not_need_children(repository):
    oid = md5(b"missing child").hexdigest()
    data = json.dumps([{"md5": oid, "relpath": "missing.txt"}], sort_keys=True).encode()
    manifest = repository.track("data", data, ("canonical",), directory=True)
    result = CliRunner().invoke(cli, ["pull", "-m", "--fallback-remote", "canonical", "data"])
    assert result.exit_code == 0, result.output
    assert repository.contents("cache") == {manifest: data}
    assert (repository.root / "data").exists() is False


@pytest.mark.parametrize("target", ["group", "group/one.txt", "group/one.txt.dvc", "group/*.dvc"])
def test_target_selection(repository, target):
    one = repository.track("group/one.txt", b"one", ("canonical",))
    repository.track("other.txt", b"other", ("canonical",))
    args = ["fetch", "--fallback-remote", "canonical"]
    if "*" in target:
        args.append("--glob")
    result = CliRunner().invoke(cli, [*args, target])
    assert result.exit_code == 0, result.output
    assert repository.contents("cache") == {one: b"one"}


def test_pull_preserves_dvc_overwrite_protection(repository):
    oid = repository.track("hello.txt", b"hello", ("canonical",))
    output = repository.root / "hello.txt"
    output.write_bytes(b"unsaved")
    args = ["pull", "--fallback-remote", "canonical", "hello.txt"]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 1
    assert output.read_bytes() == b"unsaved"
    forced = CliRunner().invoke(cli, [*args, "-f"])
    assert forced.exit_code == 0, forced.output
    assert output.read_bytes() == b"hello"
    assert repository.contents("cache") == {oid: b"hello"}


def test_repeatable_exclusions_are_a_union(repository):
    first = repository.track("first.txt", b"first", ("canonical",))
    second = repository.track("second.txt", b"second", ("second",))
    with Repo() as repo, ComposedTransfer(repo, None, ("canonical", "second", "canonical")) as transfer:
        plan = transfer.plan(None, "push")
        assert plan.errors == ()
        assert {blob.oid: blob.remote for blob in plan.classified("excluded")} == {
            first: "canonical", second: "second",
        }
        assert transfer.execute(plan, "push") == 0
    assert repository.contents("staging") == {}


def test_missing_child_fails_instead_of_reporting_directory_complete(repository):
    missing = repository.store(b"missing", ())
    data = json.dumps([{"md5": missing, "relpath": "nested/missing.txt"}], sort_keys=True).encode()
    manifest = repository.track("data", data, ("canonical",), directory=True)
    result = CliRunner().invoke(cli, ["pull", "--fallback-remote", "canonical", "data"])
    assert result.exit_code == 1
    assert result.output.splitlines() == [
        f"Download 1 blob(s) ({len(data)} B).",
        "Already cached: 0 blob(s).",
        f"  download (canonical): {manifest}  data",
        "Errors: 1.",
        f"Error: {missing}: not found; attempted remotes: staging, canonical",
    ]
    assert repository.contents("cache") == {}
    assert (repository.root / "data").exists() is False


@pytest.mark.parametrize("data,message", [
    (b"not json", "invalid directory manifest"),
    (b"{}", "invalid directory manifest"),
    (b'[{}]', "invalid manifest path: None"),
    (b'[{"md5": null, "relpath": "a"}]', "Invalid MD5 object hash: None"),
])
def test_invalid_manifest_never_omits_unknown_children(repository, data, message):
    oid = repository.track("data", data, ("canonical",), directory=True)
    with Repo() as repo, ComposedTransfer(repo, None, ("canonical",)) as transfer:
        plan = transfer.plan(None, "push")
        assert plan.errors == (message if message.startswith("Invalid MD5") else f"{oid}: {message}",)
        with pytest.raises(TransferError) as error:
            transfer.execute(plan, "push")
        assert str(error.value) == plan.errors[0]
    assert repository.contents("staging") == {}


def test_partial_transfer_is_explicit_and_does_not_checkout(repository, public):
    good = repository.track("good.txt", b"good", ("staging",))
    bad = repository.track("bad.txt", b"bad", ())
    public.put(bad, b"corrupt")
    result = CliRunner().invoke(cli, ["pull", "--fallback-remote", "public"])
    assert result.exit_code == 1
    assert result.output.splitlines()[-2:] == [
        "Error: Partial transfer: 1/2 blob(s) transferred; target incomplete",
        f"{bad}: integrity check failed on public",
    ]
    assert repository.contents("cache") == {good: b"good"}
    assert (repository.root / "good.txt").exists() is False
    assert (repository.root / "bad.txt").exists() is False


def test_multi_output_sidecar_transfers_all_hashes(repository):
    one = repository.store(b"one", ("canonical",))
    two = repository.store(b"two", ("staging",))
    (repository.root / "both.dvc").write_text(yaml.safe_dump({"outs": [
        {"md5": one, "size": 3, "hash": "md5", "path": "one.txt"},
        {"md5": two, "size": 3, "hash": "md5", "path": "two.txt"},
    ]}))
    result = CliRunner().invoke(cli, ["pull", "--fallback-remote", "canonical", "both.dvc"])
    assert result.exit_code == 0, result.output
    assert repository.contents("cache") == {one: b"one", two: b"two"}
    assert (repository.root / "one.txt").read_bytes() == b"one"
    assert (repository.root / "two.txt").read_bytes() == b"two"


def test_no_targets_with_glob_still_selects_all_outputs(repository):
    oid = repository.track("one.txt", b"one", ("canonical",))
    result = CliRunner().invoke(cli, ["pull", "--fallback-remote", "canonical", "--glob"])
    assert result.exit_code == 0, result.output
    assert (repository.root / "one.txt").read_bytes() == b"one"
    assert repository.contents("cache") == {oid: b"one"}


@pytest.mark.parametrize("failure,expected", [(None, True), (FileNotFoundError(), False), (PermissionError("denied"), None)])
def test_s3_candidate_probe_is_head_only(failure, expected):
    oid = md5(b"hello").hexdigest()
    calls = []

    def call_s3(method, **kwargs):
        calls.append((method, kwargs))
        if failure:
            raise failure

    fs = SimpleNamespace(protocol="s3", fs=SimpleNamespace(
        split_path=lambda path: ("test-bucket", path, None), call_s3=call_s3,
    ))
    odb = SimpleNamespace(fs=fs, oid_to_path=lambda oid: f"files/md5/{oid[:2]}/{oid[2:]}")
    remote = Remote("staging", odb)
    if expected is None:
        with pytest.raises(TransferError) as error:
            remote.exists(oid)
        assert str(error.value) == f"{oid}: probe failed on staging: PermissionError: denied"
    else:
        assert remote.exists(oid) is expected
    assert calls == [("head_object", {"Bucket": "test-bucket", "Key": f"files/md5/{oid[:2]}/{oid[2:]}"})]
