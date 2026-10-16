"""Offline API contracts for both clouds and the common control plane."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest
from click.testing import CliRunner

from dvx.batch import get_backend
from dvx.batch.aws import AwsBackend, job_definition_spec
from dvx.batch.base import JobHandle, JobRequest, JobState, LogCursor, LogLine, watch
from dvx.batch.command import array_environment, run_command, select_task_targets
from dvx.batch.gcp import GcpBackend, job_id, job_spec, label, log_filter, secret_ref
from dvx.cli.batch_cmd import batch

PROFILE = {
    "project": "project",
    "region": "us-central1",
    "image": "image",
    "service_account": "job@project.iam.gserviceaccount.com",
}


def expected_job():
    labels = {"dvx-profile": "default", "dvx-run": "test"}
    return {
        "task_groups": [
            {
                "task_count": 1,
                "parallelism": 1,
                "task_spec": {
                    "runnables": [
                        {
                            "container": {
                                "image_uri": "image",
                                "entrypoint": "dvx",
                                "commands": ["run"],
                                "volumes": [],
                            }
                        }
                    ],
                    "compute_resource": {
                        "cpu_milli": 16000,
                        "memory_mib": 65536,
                        "boot_disk_mib": 102400,
                    },
                    "max_retry_count": 2,
                    "lifecycle_policies": [
                        {
                            "action": "RETRY_TASK",
                            "action_condition": {"exit_codes": [50001, 50002, 50003, 50006]},
                        }
                    ],
                    "max_run_duration": "14400s",
                    "environment": {
                        "variables": {"PYTHONFAULTHANDLER": "1"},
                        "secret_variables": {},
                    },
                    "volumes": [],
                },
            }
        ],
        "allocation_policy": {
            "instances": [
                {"policy": {"provisioning_model": "SPOT", "boot_disk": {"size_gb": 100}}}
            ],
            "service_account": {"email": "job@project.iam.gserviceaccount.com"},
            "location": {"allowed_locations": ["regions/us-central1"]},
            "labels": labels,
        },
        "labels": labels,
        "logs_policy": {"destination": "CLOUD_LOGGING"},
    }


def test_gcp_job_defaults_and_real_proto():
    from google.cloud.batch_v1 import Job

    spec = job_spec(PROFILE, JobRequest(["run"], "test"))
    assert spec == expected_job()
    # The full literal spec is checked above; protobuf normalization adds defaults.
    assert Job.from_json(Job.to_json(Job(spec))) == Job(spec)


def test_gcp_full_profile_and_proto():
    from google.cloud.batch_v1 import Job

    profile = {
        **PROFILE,
        "machine_type": "n2-highmem-16",
        "scratch": {"type": "local-ssd", "gb": 375},
        "secrets": {"TOKEN": "secret"},
        "labels": {"App": "My App"},
        "volumes": [
            {"gcs": {"remote_path": "bucket"}, "mount_path": "/gcs/data", "mount_options": ["ro"]}
        ],
    }
    spec = job_spec(
        profile,
        JobRequest(
            ["run"], "test", on_demand=True, vcpus=2, memory_mib=1024, task_count=3, parallelism=2
        ),
    )
    expected = expected_job()
    expected["labels"].update(app="my-app")
    task = expected["task_groups"][0]["task_spec"]
    expected["task_groups"][0].update(task_count=3, parallelism=2)
    task["compute_resource"].update(cpu_milli=2000, memory_mib=1024)
    task["environment"] = {
        "variables": {
            "PYTHONFAULTHANDLER": "1",
            "DVX_SCRATCH": "/scratch",
            "DVC_CACHE_DIR": "/scratch/dvc-cache",
        },
        "secret_variables": {"TOKEN": "projects/project/secrets/secret/versions/latest"},
    }
    task["runnables"][0]["container"]["volumes"] = [
        "/mnt/disks/scratch:/scratch:rw",
        "/gcs/data:/gcs/data:rw",
    ]
    task["volumes"] = [
        {"device_name": "scratch", "mount_path": "/mnt/disks/scratch"},
        profile["volumes"][0],
    ]
    expected["allocation_policy"]["instances"][0]["policy"].update(
        provisioning_model="STANDARD",
        machine_type="n2-highmem-16",
        disks=[{"new_disk": {"type_": "local-ssd", "size_gb": 375}, "device_name": "scratch"}],
    )
    assert spec == expected
    assert Job.from_json(Job.to_json(Job(spec))) == Job(spec)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Secret_1", "projects/p/secrets/Secret_1/versions/latest"),
        ("projects/p/secrets/a/versions/2", "projects/p/secrets/a/versions/2"),
    ],
)
def test_secret_reference(value, expected):
    assert secret_ref("p", value) == expected


def test_invalid_secret_and_env_overlap():
    with pytest.raises(ValueError, match="Invalid Secret Manager version reference"):
        secret_ref("p", "bad/path")
    with pytest.raises(ValueError, match="names set as both environment and secrets: TOKEN"):
        job_spec(
            {**PROFILE, "env": {"TOKEN": "x"}, "secrets": {"TOKEN": "s"}},
            JobRequest(["run"], "test"),
        )


def test_deterministic_names_and_labels():
    assert (
        job_id("my_profile", "Big Stage", datetime(2026, 10, 16, tzinfo=timezone.utc), "abcd")
        == "my-profile-big-stage-20261016-000000-abcd"
    )
    assert label("My Stage/File") == "my-stage-file"
    assert label("a" * 70) == "a" * 54 + "-6bd5e503"


def test_log_filters():
    assert (
        log_filter("p", "u")
        == '(logName="projects/p/logs/batch_task_logs") AND (labels.job_uid="u")'
    )
    assert (
        log_filter("p", ["a", "b"], "2026-10-16T00:00:00Z")
        == '(logName="projects/p/logs/batch_task_logs") AND (labels.job_uid="a" OR labels.job_uid="b") AND timestamp>="2026-10-16T00:00:00Z"'
    )


def test_log_boundary_cursor():
    stamp = datetime(2026, 10, 16, tzinfo=timezone.utc)

    def entry(uid, text):
        return SimpleNamespace(
            timestamp=stamp, insert_id=uid, payload=text, labels={"task_id": "group0-2"}
        )

    logging = MagicMock()
    logging.list_entries.side_effect = [
        [entry("a", "first")],
        [entry("a", "first"), entry("b", "last")],
    ]
    now = [0]
    provider = GcpBackend(PROFILE, logging_client=logging, clock=lambda: now[0])
    handle = JobHandle("gcp", "job", "uid")
    lines, cursor = provider.logs(handle, None)
    assert (lines, cursor) == (
        [LogLine("first", 2)],
        LogCursor("2026-10-16T00:00:00.000000Z", frozenset({"a"}), "uid"),
    )
    now[0] = 5
    assert provider.logs(handle, cursor) == (
        [LogLine("last", 2)],
        LogCursor("2026-10-16T00:00:00.000000Z", frozenset({"a", "b"}), "uid"),
    )


def test_watch_retry_and_delayed_tail():
    class Fake:
        tick = 0

        def status(self, handle):
            self.tick += 1
            return (
                JobState("RUNNING", attempt=self.tick)
                if self.tick < 3
                else JobState("SUCCEEDED", 0, 2)
            )

        def logs(self, handle, cursor):
            return ([LogLine("late summary")] if self.tick == 5 else []), LogCursor()

    output, sleeps = [], []
    assert (
        watch(
            Fake(),
            JobHandle("fake", "job"),
            emit=output.append,
            sleep=sleeps.append,
            clock=lambda: sum(sleeps),
        )
        == 0
    )
    assert output == [
        "job: RUNNING (attempt 1)",
        "job: RUNNING (attempt 2)",
        "job: SUCCEEDED (attempt 2)",
        "late summary",
    ]
    assert sleeps == [5, 5, 15, 15, 15, 15]


def test_gcp_task_exit_codes():
    from google.cloud.batch_v1 import Job, Task

    client = MagicMock()
    client.get_job.return_value = Job(status={"state": "FAILED"})
    client.list_tasks.return_value = [
        Task(
            name="job/tasks/2",
            status={"state": "FAILED", "status_events": [{"task_execution": {"exit_code": 7}}]},
        ),
        Task(name="job/tasks/0", status={"state": "SUCCEEDED"}),
    ]
    assert GcpBackend(PROFILE, client=client).status(JobHandle("gcp", "job")) == JobState(
        "FAILED", 7, 1, "", ((0, 0), (2, 7))
    )


def test_aws_request_passes_real_botocore_schema():
    from botocore.session import Session
    from botocore.validate import validate_parameters

    client = MagicMock()
    client.submit_job.return_value = {"jobId": "job"}
    provider = AwsBackend(clients={"batch": client})
    assert provider.submit(
        JobRequest(["run"], "test", profile="app", task_count=3, labels={"app": "test"})
    ) == JobHandle("aws", "job", profile="app")
    actual = client.submit_job.call_args.kwargs
    assert actual == {
        "jobName": "test",
        "jobQueue": "app",
        "jobDefinition": "app",
        "containerOverrides": {"command": ["run"]},
        "arrayProperties": {"size": 3},
        "tags": {"app": "test"},
        "propagateTags": True,
    }
    validate_parameters(
        actual, Session().get_service_model("batch").operation_model("SubmitJob").input_shape
    )


def test_aws_task_role():
    spec = job_definition_spec(image="image", execution_role_arn="exec", job_role_arn="task")
    assert (
        spec["containerProperties"]["executionRoleArn"],
        spec["containerProperties"]["jobRoleArn"],
    ) == ("exec", "task")


def test_aws_explicit_network(monkeypatch):
    from tests.test_batch import _bootstrap_clients

    module, clients = _bootstrap_clients(monkeypatch)
    module.bootstrap(image="image", subnets=["sn"], security_group_ids=["sg"], job_role_arn="task")
    assert (
        clients["ec2"].describe_subnets.call_count,
        clients["ec2"].describe_security_groups.call_count,
    ) == (0, 0)
    assert clients["batch"].create_compute_environment.call_count == 0
    assert (
        clients["batch"].register_job_definition.call_args.kwargs["containerProperties"][
            "jobRoleArn"
        ]
        == "task"
    )


def test_run_flags_round_trip():
    from dvx.cli.run_cmd import run_cmd

    command = run_command(
        ("out.dvc",), cached=("data/*",), force_upstream=("raw/*",), fallback_remotes=("public",)
    )
    assert command == [
        "run",
        "--no-commit",
        "--push",
        "each",
        "--cached",
        "data/*",
        "--force-upstream",
        "raw/*",
        "--fallback-remote",
        "public",
        "-v",
        "out.dvc",
    ]
    ctx = run_cmd.make_context("run", command[1:])
    assert (
        ctx.params["cached"],
        ctx.params["force_upstream"],
        ctx.params["fallback_remote"],
        ctx.params["targets"],
    ) == (("data/*",), ("raw/*",), ("public",), ("out.dvc",))


def test_array_targets_inline_and_url(tmp_path):
    from dvx.batch.storage import write_json_url

    targets = [["a.dvc"], ["b.dvc", "c.dvc"]]
    inline = array_environment(targets)
    assert inline == {"DVX_TASK_TARGETS": '[["a.dvc"],["b.dvc","c.dvc"]]'}
    assert select_task_targets({**inline, "BATCH_TASK_INDEX": "1"}) == ["b.dvc", "c.dvc"]
    url = str(tmp_path / "tasks.json")
    write_json_url(url, targets)
    assert select_task_targets({"DVX_TASK_TARGETS_URL": url, "AWS_BATCH_JOB_ARRAY_INDEX": "0"}) == [
        "a.dvc"
    ]
    with pytest.raises(ValueError, match="Task index 2 is outside 0..1"):
        select_task_targets({**inline, "BATCH_TASK_INDEX": "2"})


def test_gcp_dry_run_never_constructs_client(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "dvx.yml").write_text(
        "batch:\n  backend: gcp\n  profiles:\n    default:\n      project: project\n      region: us-central1\n      image: image\n      service_account: job@project.iam.gserviceaccount.com\n"
    )
    result = CliRunner().invoke(batch, ["submit", "--dry-run", "out.dvc"])
    assert result.exit_code == 0, result.output
    from google.cloud.batch_v1 import Job

    expected = job_spec(PROFILE, JobRequest(run_command(("out.dvc",)), "dvx-out_dvc"))
    assert Job.from_json(result.output) == Job(expected)


def test_gcp_rejects_aws_flags():
    result = CliRunner().invoke(batch, ["bootstrap", "-b", "gcp", "--arch", "ARM64"])
    assert (result.exit_code, result.output.splitlines()[-1]) == (
        2,
        "Error: --arch is not supported by gcp",
    )


def test_backend_resolution():
    assert type(get_backend("aws")) is AwsBackend
    assert type(get_backend("gcp")) is GcpBackend
    with pytest.raises(ValueError, match="Unknown Batch backend"):
        get_backend("invalid")


def test_aws_secret_definition_is_pinned_and_valid():
    from botocore.session import Session
    from botocore.validate import validate_parameters

    client = MagicMock()
    definition = job_definition_spec(name="app", image="image", execution_role_arn="exec")
    client.describe_job_definitions.return_value = {
        "jobDefinitions": [{**definition, "revision": 2}]
    }
    client.register_job_definition.return_value = {"jobDefinitionArn": "pinned-definition:3"}
    client.submit_job.return_value = {"jobId": "job"}
    provider = AwsBackend(clients={"batch": client})
    provider.submit(
        JobRequest(
            ["run"],
            "test",
            profile="app",
            secrets={"TOKEN": "arn:aws:secretsmanager:us-east-1:123:secret:x"},
        )
    )
    actual = client.submit_job.call_args.kwargs
    assert actual == {
        "jobName": "test",
        "jobQueue": "app",
        "jobDefinition": "pinned-definition:3",
        "containerOverrides": {"command": ["run"]},
    }
    registered = client.register_job_definition.call_args.kwargs
    assert registered["containerProperties"]["secrets"] == [
        {"name": "TOKEN", "valueFrom": "arn:aws:secretsmanager:us-east-1:123:secret:x"}
    ]
    model = Session().get_service_model("batch")
    validate_parameters(actual, model.operation_model("SubmitJob").input_shape)
    validate_parameters(registered, model.operation_model("RegisterJobDefinition").input_shape)


def test_gcp_log_queries_are_shared_across_jobs():
    logging = MagicMock()
    stamp = datetime(2026, 10, 16, tzinfo=timezone.utc)
    logging.list_entries.return_value = [
        SimpleNamespace(timestamp=stamp, insert_id=uid, payload=uid, labels={"job_uid": uid})
        for uid in ("a", "b")
    ]
    provider = GcpBackend(PROFILE, logging_client=logging, clock=lambda: 0)
    provider._log_handles.update(a="job-a", b="job-b")
    first, _ = provider.logs(JobHandle("gcp", "job-a", "a"), None)
    second, _ = provider.logs(JobHandle("gcp", "job-b", "b"), None)
    assert (first, second) == ([LogLine("a")], [LogLine("b")])
    assert logging.list_entries.call_args_list == [
        call(
            filter_='(logName="projects/project/logs/batch_task_logs") AND (labels.job_uid="a" OR labels.job_uid="b")',
            order_by="timestamp asc",
        )
    ]


def test_large_array_dry_run_writes_nothing(tmp_path):
    targets = [["a" * 1000] for _ in range(20)]
    environment = array_environment(targets, str(tmp_path), dry_run=True)
    assert list(environment) == ["DVX_TASK_TARGETS_URL"]
    assert list(tmp_path.iterdir()) == []
    assert select_task_targets(
        {"AWS_BATCH_JOB_ARRAY_INDEX": "19", **array_environment(targets, str(tmp_path))}
    ) == ["a" * 1000]


def test_parallel_materialization_deduplicates_hashes(monkeypatch):
    import threading

    from dvx.cache import MaterializationSession

    session = MaterializationSession(jobs=3)
    barrier = threading.Barrier(3)
    calls = []

    def fetch(oid):
        calls.append(oid)
        barrier.wait(timeout=5)
        return oid != "missing"

    monkeypatch.setattr(session, "fetch", fetch)
    assert session.fetch_many(["a", "b", "a", "missing"]) == ["missing"]
    assert sorted(calls) == ["a", "b", "missing"]


def test_bootstrap_audits_all_required_resources_without_writes(monkeypatch):
    import dvx.batch.gcp as module
    from dvx.batch.base import Finding

    profile = {
        **PROFILE,
        "image": "us-central1-docker.pkg.dev/project/repo/image:v1",
        "cache_url": "gs://cache",
        "secrets": {"TOKEN": "token"},
    }
    roles = [
        "roles/batch.agentReporter",
        "roles/logging.logWriter",
        "roles/secretmanager.secretAccessor",
        "roles/artifactregistry.reader",
        "roles/storage.objectUser",
    ]

    class Session:
        def get(self, url, **kwargs):
            return self.response(url)

        def post(self, url, json, **kwargs):
            return self.response(url)

        def response(self, url):
            result = MagicMock(status_code=200)
            result.json.return_value = {
                "state": "ENABLED",
                "permissions": ["iam.serviceAccounts.actAs"],
                "bindings": [
                    {
                        "role": role,
                        "members": ["serviceAccount:job@project.iam.gserviceaccount.com"],
                    }
                    for role in roles
                ],
            }
            return result

    seen = []

    def repository(image, *, create):
        seen.append((image, create))
        return Finding("registry", True)

    monkeypatch.setattr(module, "ensure_repository", repository)
    findings = module.bootstrap(profile, check_only=True, session=Session())
    assert [(f.resource, f.ok) for f in findings] == [
        ("batch.googleapis.com", True),
        ("compute.googleapis.com", True),
        ("logging.googleapis.com", True),
        ("artifactregistry.googleapis.com", True),
        ("secretmanager.googleapis.com", True),
        ("job@project.iam.gserviceaccount.com", True),
        ("roles/batch.agentReporter", True),
        ("roles/logging.logWriter", True),
        ("iam.serviceAccounts.actAs", True),
        ("projects/project/secrets/token/versions/latest", True),
        ("projects/project/secrets/token: roles/secretmanager.secretAccessor", True),
        (
            "projects/project/locations/us-central1/repositories/repo: roles/artifactregistry.reader",
            True,
        ),
        ("cache: roles/storage.objectUser", True),
        ("projects/project/global/networks/default", True),
        ("registry", True),
    ]
    assert seen == [(profile["image"], False)]


def test_aws_pins_image_without_mutating_shared_definition():
    from botocore.session import Session
    from botocore.validate import validate_parameters

    client = MagicMock()
    definition = job_definition_spec(name="app", image="repo/app:latest", execution_role_arn="exec")
    client.describe_job_definitions.return_value = {
        "jobDefinitions": [{**definition, "revision": 2, "jobDefinitionArn": "app:2"}]
    }
    client.register_job_definition.return_value = {"jobDefinitionArn": "variant:3"}
    client.submit_job.return_value = {"jobId": "job"}
    provider = AwsBackend(profile={"image": "repo/app@sha256:digest"}, clients={"batch": client})
    provider.submit(JobRequest(["run"], "test", profile="app"))
    actual = client.register_job_definition.call_args.kwargs
    assert actual["containerProperties"]["image"] == "repo/app@sha256:digest"
    assert client.submit_job.call_args.kwargs["jobDefinition"] == "variant:3"
    assert definition["containerProperties"]["image"] == "repo/app:latest"
    validate_parameters(
        actual,
        Session().get_service_model("batch").operation_model("RegisterJobDefinition").input_shape,
    )
