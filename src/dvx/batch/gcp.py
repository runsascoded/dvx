"""GCP Batch builders and lazy SDK orchestration. No gcloud subprocesses."""

import json
import re
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from uuid import uuid4

from .base import Finding, JobHandle, JobRequest, JobState, LogCursor, LogLine
from .secrets import _reject_env_secret_overlap

PREEMPTION_ONLY_RETRY = [
    {"action": "RETRY_TASK", "action_condition": {"exit_codes": [50001, 50002, 50003, 50006]}},
]


def label(value: str, *, key: bool = False) -> str:
    cleaned = re.sub(r"[^a-z0-9_-]", "-", value.lower())
    if key and (not cleaned or not cleaned[0].isalpha()):
        cleaned = "dvx-" + cleaned
    if key and cleaned.startswith(("goog-", "google-")):
        raise ValueError("GCP label prefixes goog- and google- are reserved")
    if len(cleaned) > 63:
        cleaned = cleaned[:54] + "-" + sha256(value.encode()).hexdigest()[:8]
    return cleaned


def job_id(profile: str, name: str, now: datetime | None = None, suffix: str | None = None) -> str:
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%d-%H%M%S")
    suffix = suffix if suffix is not None else uuid4().hex[:4]
    slug = re.sub(r"[^a-z0-9-]+", "-", f"{profile}-{name}".lower()).strip("-")
    if not slug or not slug[0].isalpha():
        slug = "dvx-" + slug
    return f"{slug[:42].rstrip('-')}-{stamp}-{suffix}"


def secret_ref(project: str, reference: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_-]+", reference):
        return f"projects/{project}/secrets/{reference}/versions/latest"
    if re.fullmatch(r"projects/[^/]+/secrets/[A-Za-z0-9_-]+/versions/(latest|[0-9]+)", reference):
        return reference
    raise ValueError(f"Invalid Secret Manager version reference: {reference!r}")


def duration_seconds(value: str | int) -> int:
    if isinstance(value, int) and value > 0:
        return value
    match = re.fullmatch(r"([1-9][0-9]*)([smhd])", str(value))
    if not match:
        raise ValueError(f"Invalid duration {value!r}; use seconds or e.g. 30m, 4h")
    return int(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match[2]]


def job_spec(profile: dict, request: JobRequest) -> dict:
    for key in ("project", "region", "image", "service_account"):
        if not isinstance(profile.get(key), str) or not profile[key]:
            raise ValueError(f"GCP profile requires {key}")
    cpu = request.vcpus if request.vcpus is not None else profile.get("vcpus", 16)
    memory = (
        request.memory_mib if request.memory_mib is not None else profile.get("memory_mib", 65536)
    )
    if cpu <= 0 or memory <= 0 or int(cpu * 1000) != cpu * 1000:
        raise ValueError("GCP resources require positive CPU milli-units and memory MiB")
    retries = profile.get("max_retries", 2)
    if not isinstance(retries, int) or not 0 <= retries <= 10:
        raise ValueError("max_retries must be between 0 and 10")
    variables = {"PYTHONFAULTHANDLER": "1", **profile.get("env", {}), **request.env}
    secrets = {**profile.get("secrets", {}), **request.secrets}
    _reject_env_secret_overlap(variables, secrets)
    labels = {
        label(str(k), key=True): label(str(v))
        for k, v in {**profile.get("labels", {}), **request.labels}.items()
    }
    labels["dvx-profile"] = label(request.profile)
    labels.setdefault("dvx-run", label(request.name))
    policy = {
        "provisioning_model": "STANDARD"
        if request.on_demand or not profile.get("spot", True)
        else "SPOT"
    }
    if profile.get("machine_type"):
        policy["machine_type"] = profile["machine_type"]
    boot = profile.get("boot_disk_gb", 100)
    if not isinstance(boot, int) or boot <= 0:
        raise ValueError("boot_disk_gb must be a positive integer")
    policy["boot_disk"] = {"size_gb": boot}
    volumes = []
    bindings = []
    scratch = profile.get("scratch")
    if scratch:
        disk_type = scratch.get("type", "pd-ssd")
        size = scratch.get("gb", 375 if disk_type == "local-ssd" else 100)
        if (
            disk_type not in {"local-ssd", "pd-ssd", "pd-balanced", "pd-standard"}
            or not isinstance(size, int)
            or size <= 0
        ):
            raise ValueError("Invalid scratch disk type or size")
        if disk_type == "local-ssd" and size % 375:
            raise ValueError("local-ssd scratch size must be a multiple of 375 GB")
        policy["disks"] = [
            {"new_disk": {"type_": disk_type, "size_gb": size}, "device_name": "scratch"}
        ]
        volumes.append({"device_name": "scratch", "mount_path": "/mnt/disks/scratch"})
        bindings.append("/mnt/disks/scratch:/scratch:rw")
        variables.update(DVX_SCRATCH="/scratch", DVC_CACHE_DIR="/scratch/dvc-cache")
    for volume in profile.get("volumes", []):
        if not str(volume.get("mount_path", "")).startswith("/") or not volume.get("gcs", {}).get(
            "remote_path"
        ):
            raise ValueError("Volumes require an absolute mount_path and gcs.remote_path")
        if volume["mount_path"] in {"/scratch", "/mnt/disks/scratch"}:
            raise ValueError("GCS volume conflicts with scratch disk")
        volumes.append(dict(volume))
        bindings.append(f"{volume['mount_path']}:{volume['mount_path']}:rw")
    task = {
        "runnables": [
            {
                "container": {
                    "image_uri": profile["image"],
                    "entrypoint": "dvx",
                    "commands": request.command,
                    "volumes": bindings,
                }
            }
        ],
        "compute_resource": {
            "cpu_milli": int(cpu * 1000),
            "memory_mib": int(memory),
            "boot_disk_mib": boot * 1024,
        },
        "max_retry_count": retries,
        "lifecycle_policies": PREEMPTION_ONLY_RETRY,
        "max_run_duration": f"{request.max_run_seconds or duration_seconds(profile.get('max_run', '4h'))}s",
        "environment": {
            "variables": variables,
            "secret_variables": {k: secret_ref(profile["project"], v) for k, v in secrets.items()},
        },
        "volumes": volumes,
    }
    allocation = {
        "instances": [{"policy": policy}],
        "service_account": {"email": profile["service_account"]},
        "location": {"allowed_locations": [f"regions/{profile['region']}"]},
        "labels": labels,
    }
    if profile.get("network") or profile.get("subnetwork"):
        interface = {k: profile[k] for k in ("network", "subnetwork") if profile.get(k)}
        interface["no_external_ip_address"] = profile.get("no_external_ip", False)
        allocation["network"] = {"network_interfaces": [interface]}
    return {
        "task_groups": [
            {
                "task_count": request.task_count,
                "parallelism": request.parallelism or request.task_count,
                "task_spec": task,
            }
        ],
        "allocation_policy": allocation,
        "labels": labels,
        "logs_policy": {"destination": "CLOUD_LOGGING"},
    }


def log_filter(
    project: str, uids: str | list[str], since: str | None = None, agent_logs: bool = False
) -> str:
    uids = [uids] if isinstance(uids, str) else uids
    if not uids or len(uids) > 50:
        raise ValueError("Log queries require between 1 and 50 job UIDs")
    names = [f"projects/{project}/logs/batch_task_logs"]
    if agent_logs:
        names.append(f"projects/{project}/logs/batch_agent_logs")
    logs = " OR ".join(f"logName={json.dumps(name)}" for name in names)
    jobs = " OR ".join(f"labels.job_uid={json.dumps(uid)}" for uid in uids)
    query = f"({logs}) AND ({jobs})"
    if since:
        query += f" AND timestamp>={json.dumps(since)}"
    return query


class GcpBackend:
    def __init__(
        self,
        profile: dict | None = None,
        client: Any = None,
        logging_client: Any = None,
        agent_logs: bool = False,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.profile = profile or {}
        self._client = client
        self._logging_client = logging_client
        self.agent_logs = agent_logs
        self._clock = clock
        self._log_lock = threading.Lock()
        self._log_handles = {}
        self._log_buffers = {}
        self._log_cursors = {}
        self._last_log_poll = float("-inf")
        self._log_delay = 5
        self._terminal_uids = set()

    @property
    def client(self) -> Any:
        if self._client is None:
            from google.cloud.batch_v1 import BatchServiceClient

            self._client = BatchServiceClient()
        return self._client

    @property
    def logging_client(self) -> Any:
        if self._logging_client is None:
            from google.cloud.logging import Client

            self._logging_client = Client(project=self.profile["project"])
        return self._logging_client

    def submit(self, request: JobRequest) -> JobHandle:
        from google.cloud.batch_v1 import Job

        spec = Job(job_spec(self.profile, request))
        parent = f"projects/{self.profile['project']}/locations/{self.profile['region']}"
        job = self.client.create_job(
            parent=parent, job_id=job_id(request.profile, request.name), job=spec
        )
        with self._log_lock:
            self._log_handles[job.uid] = job.name
        return JobHandle("gcp", job.name, job.uid, request.profile)

    def status(self, handle: JobHandle) -> JobState:
        from google.cloud.batch_v1 import JobStatus, TaskStatus

        job = self.client.get_job(name=handle.name)
        state = JobStatus.State(job.status.state).name
        normalized = (
            state
            if state in {"RUNNING", "SUCCEEDED", "FAILED"}
            else "FAILED"
            if state == "DELETION_IN_PROGRESS"
            else "QUEUED"
        )
        if normalized in {"SUCCEEDED", "FAILED"}:
            with self._log_lock:
                self._terminal_uids.add(job.uid)
        exits = []
        attempts = 0
        if normalized in {"RUNNING", "SUCCEEDED", "FAILED"}:
            for task in self.client.list_tasks(parent=f"{handle.name}/taskGroups/group0"):
                codes = [
                    e.task_execution.exit_code
                    for e in task.status.status_events
                    if e._pb.HasField("task_execution")
                ]
                attempts = max(attempts, len(codes))
                if codes:
                    exits.append((int(task.name.rsplit("/", 1)[-1]), codes[-1]))
                elif TaskStatus.State(task.status.state).name == "SUCCEEDED":
                    exits.append((int(task.name.rsplit("/", 1)[-1]), 0))
        exits = tuple(sorted(exits))
        code = next((c for _, c in exits if c), 0 if normalized == "SUCCEEDED" else None)
        reason = job.status.status_events[-1].description if job.status.status_events else ""
        return JobState(normalized, code, attempts, reason, exits)

    def logs(self, handle: JobHandle, cursor: LogCursor | None) -> tuple[list[LogLine], LogCursor]:
        if not handle.uid:
            job = self.client.get_job(name=handle.name)
            handle = JobHandle("gcp", handle.name, job.uid, handle.profile)
        with self._log_lock:
            self._log_handles[handle.uid] = handle.name
            now = self._clock()
            interval = min(self._log_delay, 15) if self._terminal_uids else self._log_delay
            if now - self._last_log_poll >= interval:
                found = False
                uids = list(self._log_handles)
                for start in range(0, len(uids), 50):
                    chunk = uids[start : start + 50]
                    stamps = [self._log_cursors.get(uid, LogCursor()).timestamp for uid in chunk]
                    since = min(stamps) if all(stamps) else None
                    entries = self.logging_client.list_entries(
                        filter_=log_filter(self.profile["project"], chunk, since, self.agent_logs),
                        order_by="timestamp asc",
                    )
                    for entry in entries:
                        uid = entry.labels.get("job_uid") or (chunk[0] if len(chunk) == 1 else None)
                        if uid not in self._log_handles:
                            continue
                        stamp = entry.timestamp.astimezone(timezone.utc).strftime(
                            "%Y-%m-%dT%H:%M:%S.%fZ"
                        )
                        prior = self._log_cursors.get(uid, LogCursor())
                        if stamp < prior.timestamp or (
                            stamp == prior.timestamp and entry.insert_id in prior.insert_ids
                        ):
                            continue
                        seen = set(prior.insert_ids) if stamp == prior.timestamp else set()
                        seen.add(entry.insert_id)
                        self._log_cursors[uid] = LogCursor(stamp, frozenset(seen), uid)
                        self._log_buffers.setdefault(uid, []).append(entry)
                        found = True
                self._last_log_poll = now
                self._log_delay = 5 if found else min(60, self._log_delay * 2)
            entries = self._log_buffers.pop(handle.uid, [])
        timestamp = cursor.timestamp if cursor else ""
        seen = set(cursor.insert_ids) if cursor else set()
        lines = []
        for entry in entries:
            stamp = entry.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            if stamp < timestamp or (stamp == timestamp and entry.insert_id in seen):
                continue
            if stamp > timestamp:
                timestamp, seen = stamp, set()
            seen.add(entry.insert_id)
            payload = entry.payload
            message = payload if isinstance(payload, str) else json.dumps(payload, sort_keys=True)
            task_id = entry.labels.get("task_id", "")
            match = re.search(r"(?:^|[/-])(\d+)$", task_id)
            lines.append(LogLine(message, int(match[1]) if match else None))
        return lines, LogCursor(timestamp, frozenset(seen), source=handle.uid)

    def close_logs(self, handle: JobHandle) -> None:
        with self._log_lock:
            uid = handle.uid or next(
                (u for u, name in self._log_handles.items() if name == handle.name), ""
            )
            self._log_handles.pop(uid, None)
            self._log_buffers.pop(uid, None)
            self._log_cursors.pop(uid, None)
            self._terminal_uids.discard(uid)

    def cancel(self, handle: JobHandle) -> None:
        self.client.delete_job(name=handle.name).result()

    def push(self, image: str, **kwargs: Any) -> None:
        push_image(image, **kwargs)

    def bootstrap(self, profile: dict | None = None, *, check_only: bool = True) -> list[Finding]:
        return bootstrap(profile or self.profile, check_only=check_only)


def _registry_parts(image: str) -> tuple[str, str, str, str]:
    match = re.fullmatch(r"([a-z0-9-]+)-docker\.pkg\.dev/([^/]+)/([^/]+)/[^/]+", image)
    if not match:
        raise ValueError("Expected LOCATION-docker.pkg.dev/PROJECT/REPO/IMAGE:TAG")
    return match[1], match[2], match[3], image.split("/", 1)[0]


def ensure_repository(image: str, *, create: bool = True, client: Any = None) -> Finding:
    from google.api_core.exceptions import NotFound
    from google.cloud.artifactregistry_v1 import ArtifactRegistryClient

    region, project, repo, _ = _registry_parts(image)
    client = client or ArtifactRegistryClient()
    parent = f"projects/{project}/locations/{region}"
    name = f"{parent}/repositories/{repo}"
    try:
        client.get_repository(name=name)
    except NotFound:
        if not create:
            return Finding(
                name, False, f"Create Docker Artifact Registry repository {repo} in {region}"
            )
        client.create_repository(
            parent=parent,
            repository_id=repo,
            repository={
                "format_": "DOCKER",
                "cleanup_policies": {
                    "untagged": {
                        "id": "untagged",
                        "action": "DELETE",
                        "condition": {"tag_state": "UNTAGGED", "older_than": "604800s"},
                    },
                    "recent": {
                        "id": "recent",
                        "action": "KEEP",
                        "most_recent_versions": {"keep_count": 4},
                    },
                },
            },
        ).result()
    return Finding(name, True)


def push_image(image: str, **kwargs: Any) -> None:
    import subprocess

    import google.auth
    from google.auth.transport.requests import Request

    from .aws import push_commands

    _, _, _, registry = _registry_parts(image)
    ensure_repository(image)
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(Request())
    subprocess.run(
        ["docker", "login", "-u", "oauth2accesstoken", "--password-stdin", registry],
        input=credentials.token.encode(),
        check=True,
    )
    kwargs.setdefault("platform", "linux/amd64")
    if kwargs.get("build", True):
        kwargs["revision"] = subprocess.run(
            ["git", "-C", kwargs.get("context", "."), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    for command in push_commands(image, **kwargs):
        subprocess.run(command, check=True)


def bootstrap(profile: dict, *, check_only: bool = True, session: Any = None) -> list[Finding]:
    """Read infrastructure/IAM; only the AR repo may be created explicitly."""
    if session is None:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        session = AuthorizedSession(credentials)
    project = profile["project"]
    sa = profile["service_account"]
    findings = []

    def check(url: str, resource: str, fix: str, body: dict | None = None) -> dict:
        response = (
            session.post(url, json=body, timeout=30)
            if body is not None
            else session.get(url, timeout=30)
        )
        if response.status_code in {403, 404}:
            findings.append(Finding(resource, False, fix))
            return {}
        response.raise_for_status()
        return response.json()

    def role_check(
        resource: str, policy_url: str, role: str, project_policy: dict | None = None
    ) -> None:
        fix = f"Grant {role} to serviceAccount:{sa} on {resource} in IaC (Pulumi IAMMember: role={role!r}, member='serviceAccount:{sa}')"
        policy = check(policy_url, resource, fix)
        policies = [policy, project_policy or {}]
        ok = any(
            binding.get("role") == role
            and f"serviceAccount:{sa}" in binding.get("members", [])
            and not binding.get("condition")
            for p in policies
            for binding in p.get("bindings", [])
        )
        findings.append(Finding(f"{resource}: {role}", ok, fix))

    for api in ("batch", "compute", "logging", "artifactregistry", "secretmanager"):
        service = f"{api}.googleapis.com"
        data = check(
            f"https://serviceusage.googleapis.com/v1/projects/{project}/services/{service}",
            service,
            f"gcloud services enable {service} --project={project}",
        )
        if data:
            findings.append(
                Finding(
                    service,
                    data.get("state") == "ENABLED",
                    f"gcloud services enable {service} --project={project}",
                )
            )
    account = check(
        f"https://iam.googleapis.com/v1/projects/-/serviceAccounts/{sa}",
        sa,
        f"Create service account {sa} in IaC",
    )
    if account:
        findings.append(
            Finding(sa, not account.get("disabled", False), f"Enable service account {sa}")
        )
    policy = check(
        f"https://cloudresourcemanager.googleapis.com/v1/projects/{project}:getIamPolicy",
        "project IAM",
        "Grant permission to inspect project IAM",
        {},
    )
    for role in ("roles/batch.agentReporter", "roles/logging.logWriter"):
        ok = any(
            b["role"] == role
            and f"serviceAccount:{sa}" in b.get("members", [])
            and not b.get("condition")
            for b in policy.get("bindings", [])
        )
        findings.append(
            Finding(
                role,
                ok,
                f"gcloud projects add-iam-policy-binding {project} --member=serviceAccount:{sa} --role={role}",
            )
        )
    resource = f"projects/-/serviceAccounts/{sa}"
    permissions = check(
        f"https://iam.googleapis.com/v1/{resource}:testIamPermissions",
        "caller actAs",
        f"Grant roles/iam.serviceAccountUser on {sa}",
        {"permissions": ["iam.serviceAccounts.actAs"]},
    )
    findings.append(
        Finding(
            "iam.serviceAccounts.actAs",
            permissions.get("permissions") == ["iam.serviceAccounts.actAs"],
            f"Grant roles/iam.serviceAccountUser on {sa}",
        )
    )
    for name, reference in profile.get("secrets", {}).items():
        version = secret_ref(project, reference)
        data = check(
            f"https://secretmanager.googleapis.com/v1/{version}",
            name,
            f"Create an enabled version for {version}",
        )
        if data:
            findings.append(Finding(version, data.get("state") == "ENABLED", f"Enable {version}"))
        secret = version.rsplit("/versions/", 1)[0]
        role_check(
            secret,
            f"https://secretmanager.googleapis.com/v1/{secret}:getIamPolicy",
            "roles/secretmanager.secretAccessor",
            policy if secret.startswith(f"projects/{project}/") else None,
        )
    region, image_project, repository, _ = _registry_parts(profile["image"])
    repo = f"projects/{image_project}/locations/{region}/repositories/{repository}"
    role_check(
        repo,
        f"https://artifactregistry.googleapis.com/v1/{repo}:getIamPolicy",
        "roles/artifactregistry.reader",
        policy if image_project == project else None,
    )
    cache_url = profile.get("cache_url")
    if cache_url is None:
        from dvc.exceptions import NotDvcRepoError
        from dvc.fs import get_fs_config
        from dvc.repo import Repo

        try:
            with Repo() as local_repo:
                remote = local_repo.config.get("core", {}).get("remote")
                cache_url = get_fs_config(local_repo.config, name=remote)["url"] if remote else None
        except NotDvcRepoError:
            cache_url = None
    if cache_url and cache_url.startswith("gs://"):
        bucket = cache_url[5:].split("/", 1)[0]
        role_check(
            bucket,
            f"https://storage.googleapis.com/storage/v1/b/{bucket}/iam",
            "roles/storage.objectUser",
        )
    else:
        findings.append(
            Finding(
                "cache storage IAM",
                False,
                "Configure a gs:// DVC remote or profile cache_url for IAM verification; cross-cloud remotes require their own credentials",
            )
        )
    network = (
        profile.get("subnetwork")
        or profile.get("network")
        or f"projects/{project}/global/networks/default"
    )
    network_url = (
        network
        if network.startswith("https://")
        else f"https://compute.googleapis.com/compute/v1/{network}"
    )
    network_data = check(network_url, network, f"Create/configure network {network} in IaC")
    if network_data:
        findings.append(Finding(network, True))
    findings.append(ensure_repository(profile["image"], create=not check_only))
    return findings
