"""Tests for dvx CLI commands.

Assertions parse CLI output into structured forms and assert exact
equality / set equality / regex match. Avoid bare ``in result.output``.
"""

import os
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dvx.cli import cli


# ────────────────────────────────────────────────────────────────────────────
# CLI output parsers
# ────────────────────────────────────────────────────────────────────────────


@dataclass
class ClickHelp:
    """Structured ``--help`` output. Click renders to a fixed shape.

    ``description`` is the indented prose paragraph(s) after ``Usage:``;
    ``options`` and ``commands`` are the names parsed from their
    respective sections.
    """
    usage: str
    description: str  # collapsed multi-line description (newlines → spaces)
    options: list[str] = field(default_factory=list)        # ["--help", "--force", ...]
    commands: list[str] = field(default_factory=list)        # ["cache", "run", ...]


def parse_click_help(output: str) -> ClickHelp:
    """Parse a Click ``--help`` output into structured form.

    Click's format:
        Usage: <usage line>

          <description paragraph, indented 2 spaces>

        Options:
          --name [args]   <description>
          ...

        Commands:
          name  <description>
          ...
    """
    lines = output.split("\n")
    usage = ""
    desc_lines: list[str] = []
    options: list[str] = []
    commands: list[str] = []
    section: str | None = None  # "desc" | "options" | "commands"
    for line in lines:
        if line.startswith("Usage:"):
            usage = line[len("Usage:"):].strip()
            section = "desc"
            continue
        if line == "Options:":
            section = "options"
            continue
        if line == "Commands:":
            section = "commands"
            continue
        if section == "desc":
            stripped = line.strip()
            if stripped:
                desc_lines.append(stripped)
        elif section == "options":
            m = re.match(r"^  (-{1,2}[\w-]+(?:, -{1,2}[\w-]+)*)", line)
            if m:
                # Take the long form (last comma-separated name) for stable IDs.
                options.append(m.group(1).split(",")[-1].strip())
        elif section == "commands":
            m = re.match(r"^  (\S+)", line)
            if m:
                commands.append(m.group(1))
    return ClickHelp(
        usage=usage,
        description=" ".join(desc_lines),
        options=options,
        commands=commands,
    )


@dataclass
class Version:
    dvx: str  # full version string
    dvc: str


_VERSION_RE = re.compile(r"^DVX version: (\S+)\nDVC version: (\S+)\n?$")


def parse_version(output: str) -> Version:
    m = _VERSION_RE.match(output)
    assert m is not None, f"unexpected version output:\n{output!r}"
    return Version(dvx=m.group(1), dvc=m.group(2))


@dataclass(frozen=True)
class StatusItem:
    icon: str  # ✓ | ✗ | ? | ⚠
    name: str  # e.g. "stale.txt.dvc"
    reason: str  # the parenthesized cause


@dataclass
class StatusOutput:
    """Parsed ``dvx status`` output.

    ``groups`` is the count per heading ("Stale (1):"); empty when ``-G``
    flattens the output. ``items`` is every per-stage line. ``summary``
    is the trailing ``Fresh: X, Stale: Y, Missing: Z`` counts.
    """
    groups: dict[str, int] = field(default_factory=dict)
    items: list[StatusItem] = field(default_factory=list)
    summary: dict[str, int] = field(default_factory=dict)


_STATUS_GROUP_RE = re.compile(r"^([A-Z][a-z]+) \((\d+)\):$")
_STATUS_ITEM_RE = re.compile(r"^ *([✓✗⚠?]) (\S+)(?: \((.*)\))?$")
_STATUS_SUMMARY_RE = re.compile(r"^([A-Z][a-z]+: \d+(?:, [A-Z][a-z]+: \d+)*)$")
_STATUS_PAIR_RE = re.compile(r"([A-Z][a-z]+): (\d+)")


def parse_status(output: str) -> StatusOutput:
    r = StatusOutput()
    for line in output.split("\n"):
        if (m := _STATUS_GROUP_RE.match(line)):
            r.groups[m.group(1)] = int(m.group(2))
        elif (m := _STATUS_ITEM_RE.match(line)):
            r.items.append(StatusItem(
                icon=m.group(1), name=m.group(2), reason=m.group(3) or "",
            ))
        elif _STATUS_SUMMARY_RE.match(line):
            r.summary = {k: int(v) for k, v in _STATUS_PAIR_RE.findall(line)}
    return r


@pytest.fixture
def runner():
    """Create a Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def temp_dvc_repo(tmp_path):
    """Create a temporary DVC repository."""
    # Initialize git repo
    subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=tmp_path,
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=tmp_path,
        capture_output=True,
        check=True,
    )

    # Initialize DVC
    subprocess.run(["dvc", "init"], cwd=tmp_path, capture_output=True, check=True)

    return tmp_path


def test_cli_help(runner):
    """Test CLI shows help."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0

    help = parse_click_help(result.output)
    assert help.usage == "cli [OPTIONS] COMMAND [ARGS]..."
    assert help.description.startswith("DVX - Minimal data version control.")
    # Top-level CLI exposes these commands. Order is alphabetical via Click.
    assert set(help.commands) >= {
        "add", "cache", "cat", "checkout", "diff", "init", "pull", "push",
        "root", "run", "status", "version",
    }


def test_cli_version(runner):
    """Test version command."""
    result = runner.invoke(cli, ["version"])
    assert result.exit_code == 0
    version = parse_version(result.output)
    # DVX uses setuptools-scm; version starts with a digit. DVC is pinned via uv.
    assert re.match(r"^\d", version.dvx), version.dvx
    assert re.match(r"^\d+\.\d+\.\d+", version.dvc), version.dvc


def test_cache_help(runner):
    """Test cache subcommand help."""
    result = runner.invoke(cli, ["cache", "--help"])
    assert result.exit_code == 0

    help = parse_click_help(result.output)
    assert help.usage == "cli cache [OPTIONS] COMMAND [ARGS]..."
    assert help.description.startswith("Manage DVC cache and inspect cached files.")
    assert set(help.commands) == {"dir", "md5", "path"}


def test_cache_md5(runner, tmp_path):
    """Test cache md5 command."""
    os.chdir(tmp_path)

    # Create .dvc file
    expected_hash = "abc123def456"
    dvc_content = {
        "outs": [
            {
                "md5": expected_hash,
                "size": 100,
                "path": "data.txt",
            }
        ]
    }
    dvc_file = tmp_path / "data.txt.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    result = runner.invoke(cli, ["cache", "md5", "data.txt"])
    assert result.exit_code == 0
    assert result.output.strip() == expected_hash


def test_cache_path(runner, tmp_path):
    """Test cache path command."""
    os.chdir(tmp_path)

    # Create .dvc directory structure
    dvc_dir = tmp_path / ".dvc"
    dvc_dir.mkdir()

    # Create .dvc file
    md5_hash = "abc123def456789"
    dvc_content = {
        "outs": [
            {
                "md5": md5_hash,
                "size": 100,
                "path": "data.txt",
            }
        ]
    }
    dvc_file = tmp_path / "data.txt.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    result = runner.invoke(cli, ["cache", "path", "data.txt"])
    assert result.exit_code == 0
    # DVC cache path structure: .dvc/cache/files/md5/<first2>/<rest>
    expected_path = f".dvc/cache/files/md5/{md5_hash[:2]}/{md5_hash[2:]}"
    assert result.output.strip() == expected_path


def test_root_command(runner, temp_dvc_repo):
    """Test root command."""
    os.chdir(temp_dvc_repo)

    result = runner.invoke(cli, ["root"])
    assert result.exit_code == 0
    # Root command outputs the repo root path - could be "." or absolute path
    output_path = result.output.strip()
    assert output_path in (".", str(temp_dvc_repo))


def test_run_help(runner):
    """Test run command help."""
    result = runner.invoke(cli, ["run", "--help"])
    assert result.exit_code == 0
    help = parse_click_help(result.output)
    assert help.usage == "cli run [OPTIONS] [TARGETS]..."
    assert help.description.startswith("Execute artifact computations from .dvc files.")
    # The `run` command exposes these flags. Exact set match guards
    # against unintended additions / removals to the CLI surface.
    assert set(help.options) == {
        "--force", "--force-upstream", "--cached", "--jobs", "--commit",
        "--dry-run", "--no-provenance", "--push", "--no-cache-push",
        "--no-pull-deps", "--no-prune-fresh", "--verbose", "--help",
    }


def test_run_no_dvc_files(runner, tmp_path):
    """Test run command with no .dvc files."""
    os.chdir(tmp_path)

    result = runner.invoke(cli, ["run"])
    assert result.exit_code != 0
    # ClickException renders as "Error: <msg>" + trailing newlines.
    assert result.output.rstrip().split("\n") == [
        "Error: No .dvc files found.",
        "Specify targets or run from a directory with .dvc files.",
    ]


def test_run_dry_run(runner, tmp_path):
    """Test run command with --dry-run."""
    os.chdir(tmp_path)

    # Create a simple .dvc file with computation
    dvc_content = {
        "outs": [
            {
                "md5": "",
                "size": 0,
                "path": "output.txt",
            }
        ],
        "meta": {
            "computation": {
                "cmd": "echo hello > output.txt",
            }
        },
    }
    dvc_file = tmp_path / "output.txt.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    result = runner.invoke(cli, ["run", "--dry-run"])
    assert result.exit_code == 0
    # The Summary block (echoed via click) appears before the stderr
    # writes from the executor in CliRunner's captured output. Assert
    # the full literal layout — both blocks present with exact counts
    # and the per-stage "would run" status line.
    assert result.output.split("\n") == [
        "",
        "Summary:",
        "  Total: 1",
        "  Executed: 1",
        "  Skipped: 0",
        "Execution plan: 1 levels, 1 computations",
        "",
        "Dry run - showing what would execute:",
        "  output.txt: would run",
        "",
    ]


def test_cat_missing_cache(runner, tmp_path):
    """Test cat command with missing cache file."""
    os.chdir(tmp_path)

    # Create .dvc directory
    dvc_dir = tmp_path / ".dvc"
    dvc_dir.mkdir()

    # Create .dvc file
    dvc_content = {
        "outs": [
            {
                "md5": "abc123",
                "size": 100,
                "path": "data.txt",
            }
        ]
    }
    dvc_file = tmp_path / "data.txt.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump(dvc_content, f)

    result = runner.invoke(cli, ["cat", "data.txt"])
    assert result.exit_code != 0
    # ClickException prefixes "Error: " on stderr; captured into result.output.
    assert result.output.startswith("Error: Cache file not found"), result.output


def test_init_command(runner, tmp_path):
    """Test init command."""
    os.chdir(tmp_path)

    # Initialize git first
    subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)

    result = runner.invoke(cli, ["init"])
    assert result.exit_code == 0
    # DVC's "Initialized DVC repository." goes to the OS stderr stream
    # directly (DVC's logger bypasses Click capture); ``result.output``
    # contains only DVX's click.echo.
    assert result.output == "Initialized DVX repository.\n"
    assert (tmp_path / ".dvc").exists()


def test_add_command(runner, temp_dvc_repo):
    """Test add command."""
    os.chdir(temp_dvc_repo)

    # Create a file to track
    data_file = temp_dvc_repo / "data.txt"
    data_file.write_text("test data\n")

    result = runner.invoke(cli, ["add", "data.txt"])
    assert result.exit_code == 0

    # Should create .dvc file
    assert (temp_dvc_repo / "data.txt.dvc").exists()


def test_status_command(runner, temp_dvc_repo):
    """Test status command."""
    os.chdir(temp_dvc_repo)

    result = runner.invoke(cli, ["status"])
    # Should succeed even with no tracked files
    assert result.exit_code == 0


def test_diff_help(runner):
    """Test diff command help."""
    result = runner.invoke(cli, ["diff", "--help"])
    assert result.exit_code == 0
    help = parse_click_help(result.output)
    assert help.usage == "cli diff [OPTIONS] [cmd...] <path>"
    assert help.description.startswith("Diff DVC-tracked files between commits.")


def test_add_recursive_flag(runner, temp_dvc_repo):
    """Test add command with --recursive flag."""
    from dvx.cache import _hash_single_file

    os.chdir(temp_dvc_repo)

    # Create dep file
    dep_file = temp_dvc_repo / "input.txt"
    dep_file.write_text("input data\n")
    dep_hash = _hash_single_file(dep_file)

    # Create dep .dvc with WRONG hash (stale)
    dep_dvc = temp_dvc_repo / "input.txt.dvc"
    dvc_content = {
        "outs": [{"md5": "wrong_hash_123", "size": 10, "hash": "md5", "path": "input.txt"}]
    }
    with open(dep_dvc, "w") as f:
        yaml.dump(dvc_content, f)

    # Create output file
    output_file = temp_dvc_repo / "output.txt"
    output_file.write_text("output\n")

    # Create output .dvc with dep
    output_dvc = temp_dvc_repo / "output.txt.dvc"
    output_content = {
        "outs": [{"md5": "placeholder", "size": 7, "path": "output.txt"}],
        "meta": {
            "computation": {
                "cmd": "cat input.txt > output.txt",
                "deps": {"input.txt": "wrong_hash_123"},
            }
        },
    }
    with open(output_dvc, "w") as f:
        yaml.dump(output_content, f)

    # Without -r, should fail with stale dep error
    stale_hash = "wrong_hash_123"
    result = runner.invoke(cli, ["add", "output.txt"])
    assert result.exit_code == 1
    expected_lines = [
        "Error: Failed to add output.txt: Cannot add output.txt: 1 stale dep(s):",
        f"  input.txt: .dvc={stale_hash[:8]}... file={dep_hash[:8]}...",
        "Run `dvx add` on deps first, or use --recursive",
    ]
    assert result.output.strip().split("\n") == expected_lines

    # With -r, should succeed
    result = runner.invoke(cli, ["add", "-r", "output.txt"])
    assert result.exit_code == 0

    # Verify dep .dvc was updated with correct hash
    with open(dep_dvc) as f:
        dep_result = yaml.safe_load(f)
    assert dep_result["outs"][0]["md5"] == dep_hash


def test_status_shows_fresh_and_stale(runner, temp_dvc_repo):
    """Test status command shows correct freshness indicators."""
    from dvx.cache import _hash_single_file

    os.chdir(temp_dvc_repo)

    # Create a fresh file (hash matches .dvc)
    fresh_file = temp_dvc_repo / "fresh.txt"
    fresh_file.write_text("fresh data\n")
    fresh_hash = _hash_single_file(fresh_file)

    fresh_dvc = temp_dvc_repo / "fresh.txt.dvc"
    with open(fresh_dvc, "w") as f:
        yaml.dump({
            "outs": [{"md5": fresh_hash, "size": 11, "hash": "md5", "path": "fresh.txt"}]
        }, f)

    # Create a stale file (hash doesn't match .dvc)
    stale_file = temp_dvc_repo / "stale.txt"
    stale_file.write_text("stale data\n")
    stale_actual_hash = _hash_single_file(stale_file)

    stale_dvc = temp_dvc_repo / "stale.txt.dvc"
    wrong_hash = "00000000000000000000000000000000"
    with open(stale_dvc, "w") as f:
        yaml.dump({
            "outs": [{"md5": wrong_hash, "size": 11, "hash": "md5", "path": "stale.txt"}]
        }, f)

    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0

    # Parse output lines
    lines = result.output.strip().split("\n")

    # Find the stale.txt line - should have ✗ and show data changed
    stale_lines = [l.lstrip() for l in lines if "stale.txt" in l]
    assert len(stale_lines) == 1
    stale_line = stale_lines[0]
    assert stale_line.startswith("✗")
    assert "data changed" in stale_line


def test_status_json_output(runner, temp_dvc_repo):
    """Test status command with --json flag."""
    import json
    from dvx.cache import _hash_single_file

    os.chdir(temp_dvc_repo)

    # Create a file and track it
    data_file = temp_dvc_repo / "data.txt"
    data_file.write_text("test data\n")
    file_hash = _hash_single_file(data_file)

    dvc_file = temp_dvc_repo / "data.txt.dvc"
    with open(dvc_file, "w") as f:
        yaml.dump({
            "outs": [{"md5": file_hash, "size": 10, "hash": "md5", "path": "data.txt"}]
        }, f)

    result = runner.invoke(cli, ["status", "--json", "-v"])
    assert result.exit_code == 0

    assert json.loads(result.output) == [
        {"path": "data.txt.dvc", "status": "fresh", "reason": None}
    ]


def test_status_dep_changed(runner, temp_dvc_repo):
    """Test status shows dep changed vs data changed."""
    from dvx.cache import _hash_single_file

    os.chdir(temp_dvc_repo)

    # Create dep file and .dvc (fresh)
    dep_file = temp_dvc_repo / "input.txt"
    dep_file.write_text("input\n")
    dep_hash = _hash_single_file(dep_file)

    dep_dvc = temp_dvc_repo / "input.txt.dvc"
    with open(dep_dvc, "w") as f:
        yaml.dump({
            "outs": [{"md5": dep_hash, "size": 6, "hash": "md5", "path": "input.txt"}]
        }, f)

    # Create output file (fresh data)
    output_file = temp_dvc_repo / "output.txt"
    output_file.write_text("output\n")
    output_hash = _hash_single_file(output_file)

    # Create output .dvc with WRONG dep hash (dep changed scenario)
    output_dvc = temp_dvc_repo / "output.txt.dvc"
    with open(output_dvc, "w") as f:
        yaml.dump({
            "outs": [{"md5": output_hash, "size": 7, "hash": "md5", "path": "output.txt"}],
            "meta": {
                "computation": {
                    "cmd": "cat input.txt > output.txt",
                    "deps": {"input.txt": "old_wrong_hash"},
                }
            }
        }, f)

    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0

    status = parse_status(result.output)
    by_name = {item.name: item for item in status.items}
    # output.txt.dvc is stale with dep-changed reason (not data-changed).
    output_item = by_name["output.txt.dvc"]
    assert output_item == StatusItem(
        icon="✗", name="output.txt.dvc", reason="dep changed: input.txt",
    )


def test_run_discovers_dvc_files_recursively(runner, tmp_path):
    """Test that `dvx run` with no targets finds .dvc files in subdirectories."""
    os.chdir(tmp_path)

    # Create .dvc files in nested subdirectories
    sub1 = tmp_path / "sub1"
    sub1.mkdir()
    sub2 = tmp_path / "sub1" / "sub2"
    sub2.mkdir()

    for d, name in [(tmp_path, "top.txt"), (sub1, "mid.txt"), (sub2, "deep.txt")]:
        dvc_content = {
            "outs": [{"md5": "", "size": 0, "path": name}],
            "meta": {"computation": {"cmd": f"echo {name} > {name}"}},
        }
        dvc_file = d / f"{name}.dvc"
        with open(dvc_file, "w") as f:
            yaml.dump(dvc_content, f)

    # Also create a .dvc/config dir to make sure .dvc/ directory files are excluded
    dvc_dir = tmp_path / ".dvc"
    dvc_dir.mkdir()
    spurious = dvc_dir / "something.dvc"
    spurious.write_text("should be ignored")

    result = runner.invoke(cli, ["run", "--dry-run"])
    assert result.exit_code == 0

    # Parse "<artifact>: would run" lines from dry-run output.
    discovered = re.findall(r"^  (\S+): would run$", result.output, re.M)
    assert set(discovered) == {"top.txt", "sub1/mid.txt", "sub1/sub2/deep.txt"}


def test_status_transitive_staleness(runner, tmp_path):
    """dvx status shows transitively stale stages with ⚠ icon."""
    os.chdir(tmp_path)

    # Create .dvc dir
    (tmp_path / ".dvc").mkdir()
    (tmp_path / ".dvc" / "cache" / "files" / "md5").mkdir(parents=True)

    # Stage A: a raw input file with wrong hash → directly stale
    input_file = tmp_path / "input.txt"
    input_file.write_text("data\n")

    output_a = tmp_path / "step_a.txt"
    output_a.write_text("result_a\n")

    from dvx.run.hash import compute_md5
    a_md5 = compute_md5(output_a)

    dvc_a = {
        "outs": [{"md5": a_md5, "size": output_a.stat().st_size, "path": "step_a.txt"}],
        "meta": {"computation": {
            "cmd": "cat input.txt > step_a.txt",
            "deps": {"input.txt": "00000000000000000000000000000000"},  # Wrong hash → stale
        }},
    }
    with open(tmp_path / "step_a.txt.dvc", "w") as f:
        yaml.dump(dvc_a, f)

    # Stage B: depends on step_a.txt, output matches → directly fresh
    output_b = tmp_path / "step_b.txt"
    output_b.write_text("result_b\n")
    b_md5 = compute_md5(output_b)

    dvc_b = {
        "outs": [{"md5": b_md5, "size": output_b.stat().st_size, "path": "step_b.txt"}],
        "meta": {"computation": {
            "cmd": "cat step_a.txt > step_b.txt",
            "deps": {"step_a.txt": a_md5},  # Matches current → directly fresh
        }},
    }
    with open(tmp_path / "step_b.txt.dvc", "w") as f:
        yaml.dump(dvc_b, f)

    result = runner.invoke(cli, ["status", "-v"])
    assert result.exit_code == 0

    status = parse_status(result.output)
    by_name = {item.name: item for item in status.items}
    # step_a is directly stale (✗ icon, "data changed" reason).
    assert by_name["step_a.txt.dvc"].icon == "✗"
    # step_b is transitively stale (⚠ icon, "upstream stale: <ancestor>" reason).
    step_b = by_name["step_b.txt.dvc"]
    assert step_b.icon == "⚠"
    assert step_b.reason == "upstream stale: step_a.txt"


@pytest.fixture
def mixed_status_repo(tmp_path):
    """Repo with one stale, one missing, one fresh .dvc file."""
    os.chdir(tmp_path)
    (tmp_path / ".dvc").mkdir()

    from dvx.run.hash import compute_md5

    # Fresh
    f = tmp_path / "fresh.txt"
    f.write_text("fresh\n")
    with open(tmp_path / "fresh.txt.dvc", "w") as fp:
        yaml.dump({"outs": [{"md5": compute_md5(f), "size": f.stat().st_size, "path": "fresh.txt"}]}, fp)

    # Stale
    s = tmp_path / "stale.txt"
    s.write_text("stale\n")
    with open(tmp_path / "stale.txt.dvc", "w") as fp:
        yaml.dump({"outs": [{"md5": "0" * 32, "size": 5, "path": "stale.txt"}]}, fp)

    # Missing
    with open(tmp_path / "missing.txt.dvc", "w") as fp:
        yaml.dump({"outs": [{"md5": "1" * 32, "size": 10, "path": "missing.txt"}]}, fp)

    return tmp_path


def test_status_grouped_by_default(runner, mixed_status_repo):
    """Default output groups stale / missing under headers."""
    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0

    status = parse_status(result.output)
    # Stale group appears before Missing per GROUP_ORDER; dict preserves
    # insertion order so comparing the list of group keys captures that.
    assert list(status.groups.keys()) == ["Stale", "Missing"]
    assert status.groups == {"Stale": 1, "Missing": 1}
    # Items are the per-stage lines. Fresh is hidden by default → not listed.
    assert sorted(item.name for item in status.items) == ["missing.txt.dvc", "stale.txt.dvc"]
    assert status.summary == {"Fresh": 1, "Stale": 1, "Missing": 1}


def test_status_no_group(runner, mixed_status_repo):
    """-G disables grouping; no headers."""
    result = runner.invoke(cli, ["status", "-G"])
    assert result.exit_code == 0
    status = parse_status(result.output)
    assert status.groups == {}
    assert sorted(item.name for item in status.items) == ["missing.txt.dvc", "stale.txt.dvc"]


def test_status_omit_missing(runner, mixed_status_repo):
    """-x missing hides missing paths."""
    result = runner.invoke(cli, ["status", "-x", "missing"])
    assert result.exit_code == 0
    status = parse_status(result.output)
    assert status.groups == {"Stale": 1}
    assert [item.name for item in status.items] == ["stale.txt.dvc"]


def test_status_omit_prefix(runner, mixed_status_repo):
    """-x m (prefix) also hides missing."""
    result = runner.invoke(cli, ["status", "-x", "m"])
    assert result.exit_code == 0
    assert [item.name for item in parse_status(result.output).items] == ["stale.txt.dvc"]


def test_status_include_only(runner, mixed_status_repo):
    """-s stale shows only stale, hides missing even though not omitted."""
    result = runner.invoke(cli, ["status", "-s", "stale"])
    assert result.exit_code == 0
    assert [item.name for item in parse_status(result.output).items] == ["stale.txt.dvc"]


def test_status_include_prefix_comma_sep(runner, mixed_status_repo):
    """-s s,m accepts comma-separated prefixes."""
    result = runner.invoke(cli, ["status", "-s", "s,m"])
    assert result.exit_code == 0
    names = sorted(item.name for item in parse_status(result.output).items)
    assert names == ["missing.txt.dvc", "stale.txt.dvc"]


def test_status_unknown_status(runner, mixed_status_repo):
    """Unknown status name is rejected."""
    result = runner.invoke(cli, ["status", "-s", "bogus"])
    assert result.exit_code != 0
    # Click renders ``Usage:`` + ``Try 'cli status --help' for help.`` +
    # ``Error: Invalid value: unknown status 'bogus' (...)`` on validation
    # failure. Assert the full layout.
    assert result.output.rstrip().split("\n") == [
        "Usage: cli status [OPTIONS] [TARGETS]...",
        "Try 'cli status --help' for help.",
        "",
        "Error: Invalid value: unknown status 'bogus' "
        "(expected one of ['fresh', 'stale', 'missing', 'error', 'transitive'])",
    ]


def test_status_json_respects_filter(runner, mixed_status_repo):
    """JSON output respects -s filter."""
    import json
    result = runner.invoke(cli, ["status", "-s", "stale", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    statuses = {r["status"] for r in data}
    assert statuses == {"stale"}


def test_status_summary_includes_all_counts(runner, mixed_status_repo):
    """Summary line reflects full unfiltered set even when filtered."""
    result = runner.invoke(cli, ["status", "-s", "stale"])
    assert result.exit_code == 0
    summary = result.output.strip().split("\n")[-1]
    # Full counts: 1 fresh, 1 stale, 1 missing
    assert "Fresh: 1" in summary
    assert "Stale: 1" in summary
    assert "Missing: 1" in summary
