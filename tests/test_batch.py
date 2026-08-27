"""Batch packaging: pure spec builders + command builder + CLI surface.

The boto3 orchestration (``push_image`` / ``bootstrap`` / ``submit``) is a
thin wrapper over these builders; asserting on the builders' shape catches
regressions without spinning up a mocked AWS.
"""
from __future__ import annotations

from click.testing import CliRunner

from dvx.batch import (
    ECR_LIFECYCLE_POLICY,
    compute_environment_spec,
    job_definition_spec,
    push_commands,
    run_command,
    submit_overrides,
)
from dvx.cli import cli


# ── job definition ──────────────────────────────────────────────────────────

def test_job_definition_spec_defaults():
    """DVX defaults: ARM64 / 16 vCPU / 64 GiB / 100 GiB ephemeral, per
    ``specs/done/batch-executor.md``. `PYTHONFAULTHANDLER=1` in the env
    surfaces mute SIGSEGVs as tracebacks in CloudWatch."""
    assert job_definition_spec(
        image="123.dkr.ecr.us-east-1.amazonaws.com/dvx:abc1234",
        execution_role_arn="arn:aws:iam::123:role/dvx-batch-execution",
    ) == {
        "jobDefinitionName": "dvx",
        "type": "container",
        "platformCapabilities": ["FARGATE"],
        "containerProperties": {
            "image": "123.dkr.ecr.us-east-1.amazonaws.com/dvx:abc1234",
            "runtimePlatform": {
                "operatingSystemFamily": "LINUX",
                "cpuArchitecture": "ARM64",
            },
            "resourceRequirements": [
                {"type": "VCPU", "value": "16"},
                {"type": "MEMORY", "value": "65536"},
            ],
            "ephemeralStorage": {"sizeInGiB": 100},
            "executionRoleArn": "arn:aws:iam::123:role/dvx-batch-execution",
            "networkConfiguration": {"assignPublicIp": "ENABLED"},
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {"awslogs-group": "/dvx/batch"},
            },
            "environment": [
                {"name": "PYTHONFAULTHANDLER", "value": "1"},
            ],
        },
        "retryStrategy": {"attempts": 2},
    }


def test_job_definition_spec_x86_64():
    spec = job_definition_spec(
        image="img",
        arch="X86_64",
        execution_role_arn="arn",
    )
    assert spec["containerProperties"]["runtimePlatform"] == {
        "operatingSystemFamily": "LINUX",
        "cpuArchitecture": "X86_64",
    }


def test_job_definition_spec_extra_environment():
    """Extra env vars merge with the default ``PYTHONFAULTHANDLER``; keys
    are sorted so the resulting spec is stable across dict iteration."""
    spec = job_definition_spec(
        image="img",
        execution_role_arn="arn",
        environment={"AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "SK"},
    )
    assert spec["containerProperties"]["environment"] == [
        {"name": "AWS_ACCESS_KEY_ID", "value": "AK"},
        {"name": "AWS_SECRET_ACCESS_KEY", "value": "SK"},
        {"name": "PYTHONFAULTHANDLER", "value": "1"},
    ]


def test_job_definition_spec_env_override():
    """Caller-supplied ``PYTHONFAULTHANDLER`` overrides the default."""
    spec = job_definition_spec(
        image="img",
        execution_role_arn="arn",
        environment={"PYTHONFAULTHANDLER": "0"},
    )
    assert spec["containerProperties"]["environment"] == [
        {"name": "PYTHONFAULTHANDLER", "value": "0"},
    ]


# ── ECR lifecycle policy ────────────────────────────────────────────────────

def test_ecr_lifecycle_policy():
    """Self-maintaining pruning: keep 4 tags, expire untagged after 7d."""
    assert ECR_LIFECYCLE_POLICY == {
        "rules": [
            {
                "rulePriority": 1,
                "description": "expire untagged (superseded buildx manifests) after 7 days",
                "selection": {
                    "tagStatus": "untagged",
                    "countType": "sinceImagePushed",
                    "countUnit": "days",
                    "countNumber": 7,
                },
                "action": {"type": "expire"},
            },
            {
                "rulePriority": 2,
                "description": "keep the 4 most recent tags",
                "selection": {
                    "tagStatus": "tagged",
                    "tagPatternList": ["*"],
                    "countType": "imageCountMoreThan",
                    "countNumber": 4,
                },
                "action": {"type": "expire"},
            },
        ],
    }


# ── docker push commands ────────────────────────────────────────────────────

def test_push_commands_default():
    ref = "123.dkr.ecr.us-east-1.amazonaws.com/dvx:abc1234"
    assert push_commands(ref, dockerfile="Dockerfile") == [
        [
            "docker", "build", "-t", ref, "--provenance=false", "--sbom=false",
            "--platform", "linux/arm64",
            "-f", "Dockerfile", ".",
        ],
        ["docker", "push", ref],
    ]


def test_push_commands_no_build():
    ref = "123.dkr.ecr.us-east-1.amazonaws.com/dvx:abc1234"
    assert push_commands(ref, build=False) == [["docker", "push", ref]]


def test_push_commands_no_platform_custom_context():
    ref = "123.dkr.ecr.us-east-1.amazonaws.com/dvx:abc1234"
    assert push_commands(ref, platform=None, context="wt/x") == [
        [
            "docker", "build", "-t", ref, "--provenance=false", "--sbom=false",
            "wt/x",
        ],
        ["docker", "push", ref],
    ]


# ── compute environment ─────────────────────────────────────────────────────

def test_compute_environment_spec_spot():
    assert compute_environment_spec(
        subnets=["subnet-1", "subnet-2"],
        security_group_ids=["sg-1"],
    ) == {
        "computeEnvironmentName": "dvx-spot",
        "type": "MANAGED",
        "state": "ENABLED",
        "computeResources": {
            "type": "FARGATE_SPOT",
            "maxvCpus": 16,
            "subnets": ["subnet-1", "subnet-2"],
            "securityGroupIds": ["sg-1"],
        },
    }


def test_compute_environment_spec_on_demand():
    assert compute_environment_spec(
        spot=False,
        subnets=["subnet-1"],
        security_group_ids=["sg-1"],
    ) == {
        "computeEnvironmentName": "dvx-od",
        "type": "MANAGED",
        "state": "ENABLED",
        "computeResources": {
            "type": "FARGATE",
            "maxvCpus": 16,
            "subnets": ["subnet-1"],
            "securityGroupIds": ["sg-1"],
        },
    }


# ── run command builder ─────────────────────────────────────────────────────

def test_run_command_defaults():
    """Default container command: ``dvx run --commit never --push each -v``
    — no git writes from the container, every stage flushed to remote for
    Spot-reclaim resilience, verbose progress on stderr for CloudWatch."""
    assert run_command() == ["run", "--commit", "never", "--push", "each", "-v"]


def test_run_command_targets():
    assert run_command(("a.dvc", "b.dvc")) == [
        "run", "--commit", "never", "--push", "each", "-v", "a.dvc", "b.dvc",
    ]


def test_run_command_force_and_jobs():
    assert run_command(("target",), force=True, jobs=8) == [
        "run", "--commit", "never", "--push", "each", "--force", "-j", "8", "-v", "target",
    ]


def test_run_command_no_verbose():
    assert run_command(verbose=False) == ["run", "--commit", "never", "--push", "each"]


def test_run_command_commit_and_push_overrides():
    """``--commit`` / ``--push`` overrides propagate; the batch defaults
    (``never``/``each``) can be swapped for e.g. a container that DOES
    have git push access."""
    assert run_command(commit="auto", push="end") == [
        "run", "--commit", "auto", "--push", "end", "-v",
    ]


# ── submit overrides ────────────────────────────────────────────────────────

def test_submit_overrides_minimal():
    """Just a command; no resource / env overrides."""
    assert submit_overrides(["run", "-v"]) == {"command": ["run", "-v"]}


def test_submit_overrides_resources():
    """Override vcpus + memory to size a specific submission smaller
    than the job definition's default."""
    assert submit_overrides(
        ["run", "-v"], vcpus=4, memory_mib=16384,
    ) == {
        "command": ["run", "-v"],
        "resourceRequirements": [
            {"type": "VCPU", "value": "4"},
            {"type": "MEMORY", "value": "16384"},
        ],
    }


def test_submit_overrides_environment():
    """Env vars are sorted for stable specs."""
    assert submit_overrides(
        ["run"],
        environment={"Z": "z", "A": "a"},
    ) == {
        "command": ["run"],
        "environment": [
            {"name": "A", "value": "a"},
            {"name": "Z", "value": "z"},
        ],
    }


# ── CLI surface ─────────────────────────────────────────────────────────────

def test_batch_group_registered():
    """`dvx batch` shows the three subcommands and their one-line docs."""
    runner = CliRunner()
    result = runner.invoke(cli, ["batch", "--help"])
    assert result.exit_code == 0
    # Extract "  <name>  <first line of docstring>" pairs from Commands section.
    lines = result.output.split("\n")
    start = lines.index("Commands:") + 1
    entries = []
    for line in lines[start:]:
        s = line.strip()
        if not s:
            break
        name, _, _ = s.partition(" ")
        entries.append(name)
    assert sorted(entries) == ["bootstrap", "push", "submit"]


def test_batch_push_help():
    """`dvx batch push` accepts the documented option set."""
    runner = CliRunner()
    result = runner.invoke(cli, ["batch", "push", "--help"])
    assert result.exit_code == 0
    # Parse "  --name" entries from the Options section.
    opts = _parse_options(result.output)
    assert opts == {"--no-build", "--context", "--dockerfile", "--platform", "--help"}


def test_batch_bootstrap_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["batch", "bootstrap", "--help"])
    assert result.exit_code == 0
    opts = _parse_options(result.output)
    assert opts == {
        "--arch", "--env", "--ephemeral", "--image",
        "--max-vcpus", "--memory", "--on-demand", "--vcpus",
        "--help",
    }


def test_batch_submit_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["batch", "submit", "--help"])
    assert result.exit_code == 0
    opts = _parse_options(result.output)
    assert opts == {
        "--commit", "--env", "--force", "--jobs", "--memory", "--job-name",
        "--on-demand", "--push", "--vcpus", "--watch",
        "--help",
    }


def test_batch_push_env_parse_error():
    """Malformed `-e KEY` (no `=`) surfaces a click.BadParameter."""
    runner = CliRunner()
    result = runner.invoke(cli, [
        "batch", "bootstrap", "-i", "img", "-e", "MISSING_EQUALS",
    ])
    assert result.exit_code != 0
    assert "expected NAME=VALUE" in result.output


# ── helpers ─────────────────────────────────────────────────────────────────

def _parse_options(help_output: str) -> set[str]:
    """Extract the ``--name`` tokens from a Click ``--help``'s Options section.

    Click renders each option as ``  -X, --long-name TYPE  <help>`` or
    ``  --long-name TYPE  <help>``. Grab every ``--word`` that appears at
    the option-column position (indented, possibly after a ``-X,`` short form)
    of a line in the Options section.
    """
    import re
    # Find the Options section, then look for the first `--word` on each line.
    if "Options:" in help_output:
        section = help_output.split("Options:", 1)[1]
    else:
        section = help_output
    seen = []
    for line in section.split("\n"):
        # Option lines start with whitespace + a `-` (short or long form).
        if not re.match(r"^\s+-", line):
            continue
        m = re.search(r"(--[a-z0-9-]+)", line)
        if m:
            seen.append(m.group(1))
    return set(seen)
