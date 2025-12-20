"""DVX CLI - minimal data version control.

This CLI wraps DVC commands, exposing only the core data versioning
functionality, plus DVX-specific enhancements like cache introspection
and parallel pipeline execution.
"""

import os
import sys

import click

from dvx import Repo


@click.group()
@click.version_option()
@click.option("-C", "--directory", default=".", help="Run as if dvx was started in this path.")
@click.option("-q", "--quiet", count=True, help="Decrease verbosity.")
@click.option("-v", "--verbose", count=True, help="Increase verbosity.")
@click.pass_context
def cli(ctx, directory, quiet, verbose):
    """DVX - Minimal data version control.

    DVX is a lightweight wrapper around DVC focused on data versioning.
    It provides add, push, pull, checkout and other core operations,
    without experiments, metrics, params, or plots.

    DVX adds enhanced features like cache introspection (cache path, cache md5),
    cat for viewing cached files, and parallel pipeline execution.
    """
    ctx.ensure_object(dict)
    ctx.obj["directory"] = directory
    ctx.obj["quiet"] = quiet
    ctx.obj["verbose"] = verbose

    if directory != ".":
        os.chdir(directory)


# =============================================================================
# Init
# =============================================================================


@cli.command()
@click.option("--no-scm", is_flag=True, help="Initialize without git integration.")
@click.option("-f", "--force", is_flag=True, help="Force initialization.")
def init(no_scm, force):
    """Initialize a DVX repository.

    Creates a .dvc directory and prepares the repository for tracking data.
    """
    try:
        repo = Repo.init(no_scm=no_scm, force=force)
        repo.close()
        click.echo("Initialized DVX repository.")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Add
# =============================================================================


@cli.command()
@click.argument("targets", nargs=-1, required=True)
@click.option("-f", "--force", is_flag=True, help="Override existing cache entry.")
def add(targets, force):
    """Track file(s) or directory(ies) with DVX.

    Creates .dvc files and adds data to the cache.
    Safe for parallel execution (no global locking).
    """
    from dvx.cache import add_to_cache

    for target in targets:
        try:
            md5, size, is_dir = add_to_cache(target, force=force)
            click.echo(f"Added {target} ({md5[:8]}...)")
        except Exception as e:
            raise click.ClickException(f"Failed to add {target}: {e}") from e


# =============================================================================
# Push
# =============================================================================


@cli.command()
@click.argument("targets", nargs=-1)
@click.option("-a", "--all-branches", is_flag=True, help="Push for all branches.")
@click.option("-A", "--all-commits", is_flag=True, help="Push for all commits.")
@click.option("-j", "--jobs", type=int, help="Number of parallel jobs.")
@click.option("-r", "--remote", help="Remote storage to push to.")
@click.option("-T", "--all-tags", is_flag=True, help="Push for all tags.")
@click.option("--glob", is_flag=True, help="Enable globbing for targets.")
def push(targets, all_branches, all_commits, jobs, remote, all_tags, glob):
    """Upload tracked data to remote storage."""
    try:
        with Repo() as repo:
            pushed = repo.push(
                targets=list(targets) if targets else None,
                jobs=jobs,
                remote=remote,
                all_branches=all_branches,
                all_tags=all_tags,
                all_commits=all_commits,
                glob=glob,
            )
            click.echo(f"{pushed} file(s) pushed.")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Pull
# =============================================================================


@cli.command()
@click.argument("targets", nargs=-1)
@click.option("-a", "--all-branches", is_flag=True, help="Pull for all branches.")
@click.option("-A", "--all-commits", is_flag=True, help="Pull for all commits.")
@click.option("-f", "--force", is_flag=True, help="Force pull, overwriting local files.")
@click.option("-j", "--jobs", type=int, help="Number of parallel jobs.")
@click.option("-r", "--remote", help="Remote storage to pull from.")
@click.option("-T", "--all-tags", is_flag=True, help="Pull for all tags.")
@click.option("--glob", is_flag=True, help="Enable globbing for targets.")
def pull(targets, all_branches, all_commits, force, jobs, remote, all_tags, glob):
    """Download tracked data from remote storage."""
    try:
        with Repo() as repo:
            result = repo.pull(
                targets=list(targets) if targets else None,
                jobs=jobs,
                remote=remote,
                all_branches=all_branches,
                all_tags=all_tags,
                all_commits=all_commits,
                force=force,
                glob=glob,
            )
            stats = result.get("stats", {}) if isinstance(result, dict) else {}
            fetched = stats.get("fetched", 0)
            added = stats.get("added", 0)
            click.echo(f"{fetched} file(s) fetched, {added} file(s) added.")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Fetch
# =============================================================================


@cli.command()
@click.argument("targets", nargs=-1)
@click.option("-a", "--all-branches", is_flag=True, help="Fetch for all branches.")
@click.option("-A", "--all-commits", is_flag=True, help="Fetch for all commits.")
@click.option("-j", "--jobs", type=int, help="Number of parallel jobs.")
@click.option("-r", "--remote", help="Remote storage to fetch from.")
@click.option("-T", "--all-tags", is_flag=True, help="Fetch for all tags.")
def fetch(targets, all_branches, all_commits, jobs, remote, all_tags):
    """Download tracked data to cache (without checkout)."""
    try:
        with Repo() as repo:
            fetched = repo.fetch(
                targets=list(targets) if targets else None,
                jobs=jobs,
                remote=remote,
                all_branches=all_branches,
                all_tags=all_tags,
                all_commits=all_commits,
            )
            click.echo(f"{fetched} file(s) fetched.")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Checkout
# =============================================================================


@cli.command()
@click.argument("targets", nargs=-1)
@click.option("-f", "--force", is_flag=True, help="Force checkout, overwriting local changes.")
@click.option("-R", "--recursive", is_flag=True, help="Checkout all subdirectories.")
@click.option("--relink", is_flag=True, help="Recreate links or copies from cache.")
def checkout(targets, force, recursive, relink):
    """Checkout data files from cache to workspace."""
    try:
        with Repo() as repo:
            repo.checkout(
                targets=list(targets) if targets else None,
                force=force,
                recursive=recursive,
                relink=relink,
            )
            click.echo("Checkout complete.")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Status - check freshness of artifacts
# =============================================================================


def _expand_targets(targets):
    """Expand targets: directories become all .dvc files under them, files get .dvc added if needed."""
    from pathlib import Path

    expanded = []
    for target in targets:
        p = Path(target)
        if p.suffix == ".dvc":
            # Already a .dvc file
            expanded.append(p)
        elif p.is_dir():
            # First check if this directory is itself a tracked output (has .dvc file)
            dvc_path = Path(str(p) + ".dvc")
            if dvc_path.exists():
                expanded.append(dvc_path)
            else:
                # Recursively find all .dvc files under this directory
                expanded.extend(sorted(p.glob("**/*.dvc")))
        else:
            # Try adding .dvc extension
            dvc_path = Path(str(p) + ".dvc")
            if dvc_path.exists():
                expanded.append(dvc_path)
            elif p.exists():
                # Path exists but no .dvc - could be a file inside a tracked dir
                expanded.append(p)
            else:
                # Neither exists - try .dvc version anyway (will error later with useful message)
                expanded.append(dvc_path)
    return expanded


def _check_one_target(target, with_deps=True, detailed=False):
    """Check freshness of a single target. Returns dict with status info."""
    from pathlib import Path

    from dvx.run.dvc_files import (
        find_parent_dvc_dir,
        get_freshness_details,
        is_output_fresh,
        read_dir_manifest,
        read_dvc_file,
    )
    from dvx.run.hash import compute_md5

    target = Path(target)

    # Handle both .dvc path and output path
    if target.suffix == ".dvc":
        dvc_path = target
        output_path = Path(str(target)[:-4])  # Strip .dvc suffix
    else:
        output_path = target
        dvc_path = Path(str(target) + ".dvc")

    info = read_dvc_file(dvc_path)
    if info is None:
        # Check if this is a file inside a tracked directory
        parent_result = find_parent_dvc_dir(target)
        if parent_result is not None:
            parent_dir, relpath = parent_result
            parent_info = read_dvc_file(parent_dir)
            if parent_info and parent_info.md5:
                # Look up expected hash from manifest
                manifest = read_dir_manifest(parent_info.md5)
                expected_hash = manifest.get(relpath)
                if expected_hash:
                    # Check if file exists and compute its hash
                    if not target.exists():
                        result = {
                            "path": str(target),
                            "status": "missing",
                            "reason": f"file missing (inside tracked dir {parent_dir.name}/)",
                        }
                        if detailed:
                            result["output_expected"] = expected_hash
                            result["parent_dir"] = str(parent_dir)
                        return result

                    try:
                        actual_hash = compute_md5(target)
                    except Exception as e:
                        return {
                            "path": str(target),
                            "status": "error",
                            "reason": f"hash error: {e}",
                        }

                    if actual_hash == expected_hash:
                        result = {
                            "path": str(target),
                            "status": "fresh",
                            "reason": None,
                        }
                        if detailed:
                            result["output_expected"] = expected_hash
                            result["output_actual"] = actual_hash
                            result["parent_dir"] = str(parent_dir)
                        return result
                    else:
                        result = {
                            "path": str(target),
                            "status": "stale",
                            "reason": f"hash mismatch (inside tracked dir {parent_dir.name}/)",
                        }
                        if detailed:
                            result["output_expected"] = expected_hash
                            result["output_actual"] = actual_hash
                            result["parent_dir"] = str(parent_dir)
                        return result
                else:
                    return {
                        "path": str(target),
                        "status": "error",
                        "reason": f"file not in manifest of tracked dir {parent_dir.name}/",
                    }

        return {
            "path": str(target),
            "status": "error",
            "reason": "dvc file not found or invalid",
        }

    if detailed:
        # Use detailed freshness check for structured output
        details = get_freshness_details(output_path, check_deps=with_deps, info=info)
        result = {
            "path": str(target),
            "status": "fresh" if details.fresh else ("missing" if "missing" in details.reason else "stale"),
            "reason": details.reason if not details.fresh else None,
        }
        if details.output_expected:
            result["output_expected"] = details.output_expected
        if details.output_expected_commit:
            result["output_expected_commit"] = details.output_expected_commit
        if details.output_actual:
            result["output_actual"] = details.output_actual
        if details.changed_deps:
            result["changed_deps"] = details.changed_deps
        return result
    else:
        # Simple freshness check
        fresh, reason = is_output_fresh(output_path, check_deps=with_deps, info=info)

        if fresh:
            return {"path": str(target), "status": "fresh", "reason": None}
        elif "missing" in reason:
            return {"path": str(target), "status": "missing", "reason": reason}
        else:
            return {"path": str(target), "status": "stale", "reason": reason}


@cli.command()
@click.argument("targets", nargs=-1)
@click.option("-d", "--with-deps", is_flag=True, default=True, help="Check upstream dependencies.")
@click.option("-j", "--jobs", type=int, default=None, help="Number of parallel workers.")
@click.option("-v", "--verbose", is_flag=True, help="Show all files including fresh.")
@click.option("--json", "as_json", is_flag=True, help="Output results as JSON.")
@click.option("-y", "--yaml", "as_yaml", is_flag=True, help="Output detailed results as YAML (includes before/after hashes).")
def status(targets, with_deps, jobs, verbose, as_json, as_yaml):
    """Check freshness status of artifacts.

    By default, only shows stale/missing files (like git status).
    Use -v/--verbose to show all files including fresh ones.
    Use -y/--yaml for detailed output with before/after hashes for changed deps.

    Examples:
        dvx status                   # Check all .dvc files
        dvx status output.dvc        # Check specific target
        dvx status data/             # Check all .dvc files under data/
        dvx status -j 4              # Use 4 parallel workers
        dvx status --json            # Output as JSON
        dvx status -y                # Detailed YAML with hashes
    """
    import json as json_module
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from functools import partial
    from pathlib import Path

    # Find targets - expand directories to .dvc files
    if targets:
        target_list = _expand_targets(targets)
    else:
        # Default: all .dvc files in current directory tree (excluding .dvc/ directory)
        target_list = [
            p for p in Path(".").glob("**/*.dvc") if p.is_file() and ".dvc/" not in str(p)
        ]

    if not target_list:
        click.echo("No .dvc files found")
        return

    # Use detailed mode for YAML output
    detailed = as_yaml
    results = []
    check_fn = partial(_check_one_target, with_deps=with_deps, detailed=detailed)

    if jobs is None or jobs == 1:
        # Sequential
        for target in target_list:
            results.append(check_fn(target))
    else:
        # Parallel
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            futures = {executor.submit(check_fn, t): t for t in target_list}
            for future in as_completed(futures):
                results.append(future.result())

    results.sort(key=lambda r: r["path"])
    stale_count = sum(1 for r in results if r["status"] == "stale")
    missing_count = sum(1 for r in results if r["status"] == "missing")
    fresh_count = sum(1 for r in results if r["status"] == "fresh")
    error_count = sum(1 for r in results if r["status"] == "error")

    if as_yaml:
        import yaml
        # Filter to non-fresh unless verbose
        if not verbose:
            results = [r for r in results if r["status"] != "fresh"]
        # Convert to dict keyed by path for nicer YAML
        yaml_data = {}
        for r in results:
            path = r.pop("path")
            # Remove None values for cleaner output
            yaml_data[path] = {k: v for k, v in r.items() if v is not None}
        click.echo(yaml.dump(yaml_data, default_flow_style=False, sort_keys=False))
    elif as_json:
        click.echo(json_module.dumps(results, indent=2))
    else:
        # By default, only show non-fresh files (like git status)
        for r in results:
            if r["status"] == "fresh" and not verbose:
                continue
            icon = {"fresh": "✓", "stale": "✗", "missing": "?", "error": "!"}.get(
                r["status"], "?"
            )
            line = f"{icon} {r['path']}"
            if r.get("reason"):
                line += f" ({r['reason']})"
            click.echo(line)

        # Summary line
        click.echo(f"\nFresh: {fresh_count}, Stale: {stale_count}")


# =============================================================================
# Diff - content diff for DVC-tracked files
# =============================================================================


from dataclasses import dataclass
from enum import Enum


class CacheStatus(Enum):
    """Status of cache lookup for a .dvc file."""
    OK = "ok"                    # Cache file exists
    NOT_TRACKED = "not_tracked"  # .dvc file doesn't exist at this revision
    CACHE_MISSING = "cache_missing"  # .dvc file exists but cache file is missing


@dataclass
class CacheResult:
    """Result of looking up a cache path."""
    status: CacheStatus
    path: str | None = None
    md5: str | None = None
    error: str | None = None

    @property
    def exists(self) -> bool:
        return self.status == CacheStatus.OK


def _normalize_path(path: str) -> tuple[str, str]:
    """Normalize path to (data_path, dvc_path) tuple."""
    if path.endswith(os.sep):
        path = path[:-1]
    if path.endswith(".dvc"):
        dvc_path = path
        data_path = path[:-4]
    else:
        data_path = path
        dvc_path = path + ".dvc"
    return data_path, dvc_path


def _find_parent_dvc_file(path: str) -> tuple[str, str] | None:
    """Find the parent .dvc file for a path inside a DVC-tracked directory.

    Returns (parent_dvc_path, relpath_within_dir) or None if not found.
    """
    parts = path.split(os.sep)
    for i in range(len(parts) - 1, 0, -1):
        parent = os.sep.join(parts[:i])
        parent_dvc = parent + ".dvc"
        if os.path.exists(parent_dvc):
            rel = os.sep.join(parts[i:])
            return (parent_dvc, rel)
    return None


def _get_file_md5_from_manifest(manifest_path: str, rel: str) -> str | None:
    """Get the MD5 hash of a file from a directory manifest."""
    import json

    if not os.path.exists(manifest_path):
        return None
    try:
        with open(manifest_path) as f:
            entries = json.load(f)
        for entry in entries:
            if entry.get("relpath") == rel:
                return entry.get("md5")
        return None
    except (json.JSONDecodeError, KeyError):
        return None


def _find_dvc_root() -> str:
    """Find the root directory of the DVC repository."""
    from dvc.repo import Repo as DVCRepo

    return DVCRepo.find_root()


def _get_cache_path_for_ref(
    dvc_path: str,
    ref: str | None,
    file_in_dir: str | None = None,
) -> CacheResult:
    """Get cache path for a .dvc file at a specific git ref.

    If file_in_dir is provided, dvc_path should be a directory .dvc file and
    this will return the cache path for the specific file within that directory.

    Returns a CacheResult with:
    - status=OK if cache file exists
    - status=NOT_TRACKED if .dvc file doesn't exist at this ref
    - status=CACHE_MISSING if .dvc file exists but cache is missing
    """
    import subprocess

    import yaml

    try:
        root_dir = _find_dvc_root()

        if ref:
            # Read .dvc file content directly from git
            result = subprocess.run(
                ["git", "show", f"{ref}:{dvc_path}"],
                capture_output=True,
                text=True,
                check=False,
                cwd=root_dir,
            )
            if result.returncode != 0:
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"{dvc_path} not tracked at {ref}")

            loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
            dvc_content = yaml.load(result.stdout, Loader=loader)  # noqa: S506
            outs = dvc_content.get("outs", [])
            if not outs:
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"No outputs in {dvc_path} at {ref}")

            md5 = outs[0].get("md5", "")
            if not md5:
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"No md5 hash in {dvc_path} at {ref}")

            # For directories, md5 ends with .dir - strip for prefix, keep for suffix
            md5_base = md5.replace(".dir", "")
            suffix = ".dir" if md5.endswith(".dir") else ""
            cache_path = os.path.join(
                root_dir, ".dvc", "cache", "files", "md5", md5_base[:2], md5_base[2:] + suffix
            )

            # If looking for a file within a directory, look it up in the manifest
            if file_in_dir and suffix == ".dir":
                if not os.path.exists(cache_path):
                    return CacheResult(
                        CacheStatus.CACHE_MISSING,
                        md5=md5,
                        error=f"Directory manifest missing from cache: {md5}",
                    )
                file_md5 = _get_file_md5_from_manifest(cache_path, file_in_dir)
                if file_md5:
                    file_cache_path = os.path.join(
                        root_dir, ".dvc", "cache", "files", "md5", file_md5[:2], file_md5[2:]
                    )
                    if os.path.exists(file_cache_path):
                        return CacheResult(CacheStatus.OK, path=file_cache_path, md5=file_md5)
                    return CacheResult(
                        CacheStatus.CACHE_MISSING,
                        md5=file_md5,
                        error=f"File missing from cache: {file_md5}",
                    )
                return CacheResult(
                    CacheStatus.NOT_TRACKED,
                    error=f"{file_in_dir} not found in directory manifest",
                )

            # Check if cache file exists
            if os.path.exists(cache_path):
                return CacheResult(CacheStatus.OK, path=cache_path, md5=md5)
            return CacheResult(CacheStatus.CACHE_MISSING, md5=md5, error=f"Cache file missing: {md5}")
        else:
            # Read from current working tree
            if not os.path.exists(dvc_path):
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"{dvc_path} does not exist")

            loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
            with open(dvc_path) as f:
                dvc_content = yaml.load(f, Loader=loader)  # noqa: S506

            outs = dvc_content.get("outs", [])
            if not outs:
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"No outputs in {dvc_path}")

            md5 = outs[0].get("md5", "")
            if not md5:
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"No md5 hash in {dvc_path}")

            # For directories, md5 ends with .dir
            md5_base = md5.replace(".dir", "")
            suffix = ".dir" if md5.endswith(".dir") else ""
            cache_path = os.path.join(
                root_dir, ".dvc", "cache", "files", "md5", md5_base[:2], md5_base[2:] + suffix
            )

            # If looking for a file within a directory
            if file_in_dir and suffix == ".dir":
                if not os.path.exists(cache_path):
                    return CacheResult(
                        CacheStatus.CACHE_MISSING,
                        md5=md5,
                        error=f"Directory manifest missing from cache: {md5}",
                    )
                file_md5 = _get_file_md5_from_manifest(cache_path, file_in_dir)
                if file_md5:
                    file_cache_path = os.path.join(
                        root_dir, ".dvc", "cache", "files", "md5", file_md5[:2], file_md5[2:]
                    )
                    if os.path.exists(file_cache_path):
                        return CacheResult(CacheStatus.OK, path=file_cache_path, md5=file_md5)
                    return CacheResult(
                        CacheStatus.CACHE_MISSING,
                        md5=file_md5,
                        error=f"File missing from cache: {file_md5}",
                    )
                return CacheResult(
                    CacheStatus.NOT_TRACKED,
                    error=f"{file_in_dir} not found in directory manifest",
                )

            # Check if cache file exists
            if os.path.exists(cache_path):
                return CacheResult(CacheStatus.OK, path=cache_path, md5=md5)
            return CacheResult(CacheStatus.CACHE_MISSING, md5=md5, error=f"Cache file missing: {md5}")

    except Exception as e:
        return CacheResult(CacheStatus.NOT_TRACKED, error=str(e))


def _run_diff(
    path1: str | None,
    path2: str | None,
    color: bool | None = None,
    unified: int | None = None,
    ignore_whitespace: bool = False,
) -> int:
    """Run diff on two paths."""
    import subprocess

    args = ["diff"]

    if ignore_whitespace:
        args.append("-w")
    if unified is not None:
        args.extend(["-U", str(unified)])
    if color is True:
        args.append("--color=always")
    elif color is False:
        args.append("--color=never")

    args.append(path1 or "/dev/null")
    args.append(path2 or "/dev/null")

    result = subprocess.run(args, check=False)
    return result.returncode


def _run_pipeline_diff(
    path1: str | None,
    path2: str | None,
    cmds: list[str],
    color: bool | None = None,
    unified: int | None = None,
    ignore_whitespace: bool = False,
    verbose: bool = False,
    shell: bool = True,
    shell_executable: str | None = None,
    both: bool = False,
    pipefail: bool = False,
) -> int:
    """Run diff with preprocessing pipeline using dffs."""
    from dffs import join_pipelines

    diff_args = []
    if ignore_whitespace:
        diff_args.append("-w")
    if unified is not None:
        diff_args.extend(["-U", str(unified)])
    if color is True:
        diff_args.append("--color=always")
    elif color is False:
        diff_args.append("--color=never")

    cmd, *sub_cmds = cmds

    if path1 is None:
        cmds1 = ["cat /dev/null"]
    else:
        cmds1 = [f"{cmd} {path1}", *sub_cmds]

    if path2 is None:
        cmds2 = ["cat /dev/null"]
    else:
        cmds2 = [f"{cmd} {path2}", *sub_cmds]

    return join_pipelines(
        base_cmd=["diff", *diff_args],
        cmds1=cmds1,
        cmds2=cmds2,
        verbose=verbose,
        shell=shell,
        executable=shell_executable,
        both=both,
        pipefail=pipefail,
    )


def _compute_dir_manifest(dir_path: str) -> dict[str, tuple[str, int]]:
    """Compute MD5 hashes and sizes for all files in a directory."""
    import hashlib

    manifest = {}
    dir_path = os.path.abspath(dir_path)
    for root, _dirs, files in os.walk(dir_path):
        for filename in files:
            filepath = os.path.join(root, filename)
            rel = os.path.relpath(filepath, dir_path)
            try:
                hasher = hashlib.md5()  # noqa: S324
                with open(filepath, "rb") as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        hasher.update(chunk)
                size = os.path.getsize(filepath)
                manifest[rel] = (hasher.hexdigest(), size)
            except OSError:
                pass  # Skip files we can't read
    return manifest


def _get_cache_file_size(md5: str) -> int | None:
    """Get size of a file in the cache by its MD5 hash."""
    try:
        root_dir = _find_dvc_root()
        cache_path = os.path.join(root_dir, ".dvc", "cache", "files", "md5", md5[:2], md5[2:])
        if os.path.exists(cache_path):
            return os.path.getsize(cache_path)
    except Exception:
        pass
    return None


def _diff_directory(path1: str | None, path2: str | None, data_path: str, after: str | None) -> int:
    """Compare directory manifests.

    DVC stores directory contents as JSON manifests in the cache.
    This function diffs those manifests to show which files changed.
    """
    import json

    def load_manifest(path: str | None) -> dict[str, tuple[str, int | None]] | None:
        """Load directory manifest, returning {relpath: (md5, size)} dict."""
        if not path or not os.path.exists(path):
            return {}
        # Skip if it's a directory (working tree) - we only read manifest files
        if os.path.isdir(path):
            return None  # Signal that we can't read this as manifest
        try:
            with open(path) as f:
                entries = json.load(f)
                # Look up sizes from cache for each file
                result = {}
                for e in entries:
                    md5 = e["md5"]
                    size = _get_cache_file_size(md5)
                    result[e["relpath"]] = (md5, size)
                return result
        except (json.JSONDecodeError, KeyError):
            return {}

    manifest1 = load_manifest(path1)
    manifest2 = load_manifest(path2)

    # If one side is a working tree directory, compute hashes for it
    if manifest2 is None and path2 and os.path.isdir(path2):
        manifest2 = _compute_dir_manifest(path2)

    if manifest1 is None and path1 and os.path.isdir(path1):
        manifest1 = _compute_dir_manifest(path1)

    # Handle empty manifests
    manifest1 = manifest1 or {}
    manifest2 = manifest2 or {}

    # Compare manifests - output diff-style with full paths relative to data_path
    all_paths = sorted(set(manifest1) | set(manifest2))
    if not all_paths:
        return 0

    has_diff = False

    for rel in all_paths:
        entry1 = manifest1.get(rel)  # (md5, size) or None
        entry2 = manifest2.get(rel)  # (md5, size) or None
        full_path = os.path.join(data_path, rel)

        if entry1 is None:
            # Added
            md5_2, size_2 = entry2
            click.secho(f"+ {full_path}  {md5_2}  {size_2 if size_2 is not None else '?'}", fg="green")
            has_diff = True
        elif entry2 is None:
            # Removed
            md5_1, size_1 = entry1
            click.secho(f"- {full_path}  {md5_1}  {size_1 if size_1 is not None else '?'}", fg="red")
            has_diff = True
        else:
            md5_1, size_1 = entry1
            md5_2, size_2 = entry2
            if md5_1 != md5_2:
                # Modified - show both old and new
                click.secho(f"- {full_path}  {md5_1}  {size_1 if size_1 is not None else '?'}", fg="red")
                click.secho(f"+ {full_path}  {md5_2}  {size_2 if size_2 is not None else '?'}", fg="green")
                has_diff = True

    return 1 if has_diff else 0


@cli.command()
@click.option("-b", "--both", is_flag=True, help="Merge stderr into stdout in pipeline commands.")
@click.option("-c/-C", "--color/--no-color", default=None, help="Force or prevent colorized output.")
@click.option("-P", "--pipefail", is_flag=True, help="Check all pipeline commands for errors (like bash's `set -o pipefail`); default only checks last command.")
@click.option("-r", "--refspec", help="<commit1>..<commit2> or <commit> (compare to worktree).")
@click.option("-R", "--ref", help="Shorthand for -r <ref>^..<ref> (compare commit to parent).")
@click.option("-e", "--shell-executable", help="Shell to use for executing commands.")
@click.option("-S", "--no-shell", is_flag=True, help="Don't use shell for subprocess execution.")
@click.option("-s", "--summary", is_flag=True, help="Show summary of changes (files and hashes) instead of content diff.")
@click.option("-U", "--unified", type=int, help="Number of lines of context.")
@click.option("-v", "--verbose", is_flag=True, help="Log intermediate commands to stderr.")
@click.option("-w", "--ignore-whitespace", is_flag=True, help="Ignore whitespace differences.")
@click.option("-x", "--exec-cmd", "exec_cmds", multiple=True, help="Command to execute before diffing.")
@click.argument("args", nargs=-1, metavar="[cmd...] <path>")
@click.pass_context
def diff(
    ctx,
    both,
    color,
    pipefail,
    refspec,
    ref,
    shell_executable,
    no_shell,
    summary,
    unified,
    verbose,
    ignore_whitespace,
    exec_cmds,
    args,
):
    """Diff DVC-tracked files between commits.

    By default, shows actual content differences. Use -s/--summary to show
    a summary of which files changed (with hashes) instead.

    Examples:

    \b
      dvx diff data.csv
        Show content diff of data.csv between HEAD and worktree.

    \b
      dvx diff -r HEAD^..HEAD data.csv
        Show content diff between previous and current commit.

    \b
      dvx diff -s
        Show summary of all changed files with hashes.

    \b
      dvx diff -R abc123 wc -l data.csv
        Compare line count of data.csv at commit abc123 vs its parent.
    """
    import json as json_module

    # Handle summary mode (shows file/hash changes via DVC)
    if summary:
        # Parse refspec for summary mode
        if refspec and ref:
            raise click.UsageError("Specify -r/--refspec or -R/--ref, not both")

        if ref:
            a_rev = f"{ref}^"
            b_rev = ref
        elif refspec and ".." in refspec:
            a_rev, b_rev = refspec.split("..", 1)
        elif refspec:
            a_rev = refspec
            b_rev = None
        else:
            a_rev = None
            b_rev = None

        try:
            with Repo() as repo:
                d = repo.diff(
                    a_rev=a_rev,
                    b_rev=b_rev,
                    targets=list(args) if args else None,
                )
                if any(d.values()):
                    click.echo(json_module.dumps(d, indent=2))
                else:
                    click.echo("No changes.")
        except Exception as e:
            raise click.ClickException(str(e)) from e
        return

    # Content diff mode - need a target path
    remaining = list(exec_cmds) + list(args)

    if not remaining:
        raise click.UsageError("Must specify [cmd...] <path> (or use -s/--summary)")

    *cmds, target = remaining
    data_path, dvc_path = _normalize_path(target)

    # Parse refspec
    if refspec and ref:
        raise click.UsageError("Specify -r/--refspec or -R/--ref, not both")

    if ref:
        refspec = f"{ref}^..{ref}"
    elif not refspec:
        refspec = "HEAD"

    # Split refspec into before/after
    if ".." in refspec:
        before, after = refspec.split("..", 1)
    else:
        before = refspec
        after = None  # Compare to working tree

    # Check if this is a file inside a DVC-tracked directory
    parent_dvc_info = _find_parent_dvc_file(data_path)

    # Get cache paths with better error handling
    result1 = _get_cache_path_for_ref(dvc_path, before)
    if result1.status == CacheStatus.NOT_TRACKED and parent_dvc_info:
        # Try looking up as file inside directory
        parent_dvc, rel = parent_dvc_info
        result1 = _get_cache_path_for_ref(parent_dvc, before, file_in_dir=rel)

    if after is None:
        # Compare to working tree - use the actual file if it exists
        if os.path.exists(data_path):
            path2 = data_path
            result2 = None  # Using actual file, not cache
        else:
            result2 = _get_cache_path_for_ref(dvc_path, None)
            if result2.status == CacheStatus.NOT_TRACKED and parent_dvc_info:
                parent_dvc, rel = parent_dvc_info
                result2 = _get_cache_path_for_ref(parent_dvc, None, file_in_dir=rel)
            path2 = result2.path if result2 and result2.exists else None
    else:
        result2 = _get_cache_path_for_ref(dvc_path, after)
        if result2.status == CacheStatus.NOT_TRACKED and parent_dvc_info:
            parent_dvc, rel = parent_dvc_info
            result2 = _get_cache_path_for_ref(parent_dvc, after, file_in_dir=rel)
        path2 = result2.path if result2.exists else None

    # Check for cache missing errors (distinct from "file doesn't exist at revision")
    if result1.status == CacheStatus.CACHE_MISSING:
        raise click.ClickException(
            f"Cache missing for '{before}': {result1.error}\n"
            "Run 'dvc pull' to fetch from remote."
        )
    if result2 is not None and result2.status == CacheStatus.CACHE_MISSING:
        after_ref = after or "working tree"
        raise click.ClickException(
            f"Cache missing for '{after_ref}': {result2.error}\n"
            "Run 'dvc pull' to fetch from remote."
        )

    # Extract path1 (None means file doesn't exist at that revision - legitimate add/delete)
    path1 = result1.path if result1.exists else None

    if result1.status == CacheStatus.NOT_TRACKED and (result2 is None or result2.status == CacheStatus.NOT_TRACKED):
        raise click.ClickException(f"Could not find {dvc_path} at either revision")

    # Check if it's a directory (cache paths for dirs end with .dir)
    is_dir = (path1 and path1.endswith(".dir")) or (path2 and path2.endswith(".dir"))
    if is_dir:
        ctx.exit(_diff_directory(path1, path2, data_path, after))

    # Run diff
    if cmds:
        returncode = _run_pipeline_diff(
            path1,
            path2,
            cmds,
            color=color,
            unified=unified,
            ignore_whitespace=ignore_whitespace,
            verbose=verbose,
            shell=not no_shell,
            shell_executable=shell_executable,
            both=both,
            pipefail=pipefail,
        )
    else:
        returncode = _run_diff(
            path1,
            path2,
            color=color,
            unified=unified,
            ignore_whitespace=ignore_whitespace,
        )

    ctx.exit(returncode)


# =============================================================================
# GC
# =============================================================================


@cli.command()
@click.option("-a", "--all-branches", is_flag=True, help="Keep cache for all branches.")
@click.option("-A", "--all-commits", is_flag=True, help="Keep cache for all commits.")
@click.option("-c", "--cloud", is_flag=True, help="Also gc remote storage.")
@click.option("-f", "--force", is_flag=True, help="Force gc without confirmation.")
@click.option("-j", "--jobs", type=int, help="Number of parallel jobs.")
@click.option("-n", "--dry", is_flag=True, help="Dry run - show what would be removed.")
@click.option("-r", "--remote", help="Remote storage to gc.")
@click.option("-T", "--all-tags", is_flag=True, help="Keep cache for all tags.")
@click.option("-w", "--workspace", is_flag=True, help="Keep only cache for current workspace.")
def gc(all_branches, all_commits, cloud, force, jobs, dry, remote, all_tags, workspace):
    """Garbage collect unused cache files."""
    if not any([workspace, all_branches, all_tags, all_commits]):
        raise click.ClickException(
            "One of -w/--workspace, -a/--all-branches, -T/--all-tags, "
            "-A/--all-commits is required."
        )
    try:
        with Repo() as repo:
            result = repo.gc(
                workspace=workspace,
                all_branches=all_branches,
                all_tags=all_tags,
                all_commits=all_commits,
                cloud=cloud,
                remote=remote,
                force=force,
                jobs=jobs,
                dry=dry,
            )
            click.echo(f"Removed {result.get('deleted', 0)} file(s).")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Remove
# =============================================================================


@cli.command()
@click.argument("targets", nargs=-1, required=True)
@click.option("-f", "--force", is_flag=True, help="Force removal.")
@click.option("--outs", is_flag=True, help="Also remove the output files.")
def remove(targets, force, outs):
    """Stop tracking file(s) with DVX."""
    try:
        with Repo() as repo:
            repo.remove(list(targets), force=force, outs=outs)
            click.echo(f"Removed {len(targets)} target(s).")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Move
# =============================================================================


@cli.command()
@click.argument("src")
@click.argument("dst")
def move(src, dst):
    """Move a DVX-tracked file or directory."""
    try:
        with Repo() as repo:
            repo.move(src, dst)
            click.echo(f"Moved {src} -> {dst}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Import
# =============================================================================


@cli.command("import")
@click.argument("url")
@click.argument("path")
@click.option("-o", "--out", help="Output path.")
@click.option("--rev", help="Git revision in the source repo.")
def import_cmd(url, path, out, rev):
    """Import a file from another DVC/DVX repository."""
    try:
        with Repo() as repo:
            repo.imp(url=url, path=path, out=out, rev=rev)
            click.echo(f"Imported {path} from {url}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


@cli.command("import-url")
@click.argument("url")
@click.option("-o", "--out", help="Output path.")
def import_url(url, out):
    """Import a file from a URL."""
    try:
        with Repo() as repo:
            repo.imp_url(url=url, out=out)
            click.echo(f"Imported {url}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Get (download without tracking)
# =============================================================================


@cli.command()
@click.argument("url")
@click.argument("path")
@click.option("-o", "--out", help="Output path.")
@click.option("--rev", help="Git revision in the source repo.")
def get(url, path, out, rev):
    """Download a file from a DVC/DVX repository (without tracking)."""
    try:
        Repo.get(url=url, path=path, out=out, rev=rev)
        click.echo(f"Downloaded {path} from {url}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


@cli.command("get-url")
@click.argument("url")
@click.option("-o", "--out", help="Output path.")
def get_url(url, out):
    """Download a file from a URL (without tracking)."""
    try:
        Repo.get_url(url=url, out=out)
        click.echo(f"Downloaded {url}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Cache subcommands
# =============================================================================


@cli.group()
def cache():
    """Manage DVC cache and inspect cached files."""


@cache.command("dir")
@click.argument("value", required=False)
@click.option("-u", "--unset", is_flag=True, help="Unset cache directory.")
def cache_dir(value, unset):
    """Get or set the cache directory location."""
    from dvc.cli import main as dvc_main

    if value is None and not unset:
        # Get current value - delegate to dvc
        sys.exit(dvc_main(["cache", "dir"]))
    else:
        args = ["cache", "dir"]
        if unset:
            args.append("--unset")
        if value:
            args.append(value)
        sys.exit(dvc_main(args))


@cache.command("path")
@click.argument("target")
@click.option("-r", "--rev", metavar="<rev>", help="Git revision.")
@click.option("--remote", metavar="<name>", help="Get remote blob URL instead.")
@click.option("--absolute", is_flag=True, help="Output absolute path (default is relative).")
def cache_path(target, rev, remote, absolute):
    """Get the cache path for a DVC-tracked file.

    TARGET can be:
    - a .dvc file or path to a tracked file (adds .dvc if needed)
    - a file inside a DVC-tracked directory
    - an MD5 hash (32 hex chars) to get path directly

    Examples:
        dvx cache path data.txt.dvc
        dvx cache path data.txt
        dvx cache path data.txt --remote myremote
        dvx cache path data.txt -r HEAD~1
        dvx cache path tracked_dir/file.txt
        dvx cache path d8e8fca2dc0f896fd7cb4cb0031ba249
    """
    from dvx.cache import get_cache_path

    try:
        path = get_cache_path(target, rev=rev, remote=remote, absolute=absolute)
        click.echo(path)
    except Exception as e:
        raise click.ClickException(str(e)) from e


@cache.command("md5")
@click.argument("target")
@click.option("-r", "--rev", metavar="<rev>", help="Git revision.")
def cache_md5(target, rev):
    """Get the MD5 hash for a DVC-tracked file.

    TARGET can be:
    - a .dvc file or path to a tracked file (adds .dvc if needed)
    - a file inside a DVC-tracked directory

    Examples:
        dvx cache md5 data.txt.dvc
        dvx cache md5 data.txt
        dvx cache md5 data.txt -r HEAD~1
        dvx cache md5 tracked_dir/file.txt
    """
    from dvx.cache import get_hash

    try:
        md5 = get_hash(target, rev=rev)
        click.echo(md5)
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Cat - view cached file contents
# =============================================================================


@cli.command()
@click.argument("target")
@click.option("-r", "--rev", metavar="<rev>", help="Git revision.")
def cat(target, rev):
    """Display contents of a DVC-tracked file from cache.

    TARGET can be:
    - a .dvc file or path to a tracked file (adds .dvc if needed)
    - a file inside a DVC-tracked directory
    - an MD5 hash (32 hex chars) to read directly from cache

    Examples:
        dvx cat data.txt.dvc
        dvx cat data.txt
        dvx cat data.txt -r HEAD~1
        dvx cat tracked_dir/file.txt
        dvx cat d8e8fca2dc0f896fd7cb4cb0031ba249
    """
    from dvx.cache import get_cache_path

    try:
        cache_path = get_cache_path(target, rev=rev, absolute=True)
        if not os.path.exists(cache_path):
            raise click.ClickException(f"Cache file not found: {cache_path}")

        with open(cache_path, "rb") as f:
            while chunk := f.read(65536):
                sys.stdout.buffer.write(chunk)
    except click.ClickException:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Root - show repo root
# =============================================================================


@cli.command()
def root():
    """Show the root directory of the DVX repository."""
    from dvc.repo import Repo as DVCRepo

    try:
        root_dir = DVCRepo.find_root()
        # Output relative to current directory
        rel = os.path.relpath(root_dir)
        click.echo(rel)
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Config (delegate to dvc)
# =============================================================================


@cli.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def config(ctx, args):
    """Configure DVX/DVC settings.

    This delegates to `dvc config`. Run `dvc config --help` for options.
    """
    from dvc.cli import main as dvc_main

    sys.exit(dvc_main(["config", *args]))


# =============================================================================
# Remote (delegate to dvc)
# =============================================================================


@cli.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def remote(ctx, args):
    """Manage remote storage.

    This delegates to `dvc remote`. Run `dvc remote --help` for options.
    """
    from dvc.cli import main as dvc_main

    sys.exit(dvc_main(["remote", *args]))


# =============================================================================
# Run - execute artifact computations
# =============================================================================


@cli.command("run")
@click.argument("targets", nargs=-1, type=click.Path())
@click.option("-f", "--force", is_flag=True, help="Force re-run all computations.")
@click.option("--force-upstream", multiple=True, metavar="<pattern>", help="Force re-run upstream artifacts matching pattern.")
@click.option("--cached", multiple=True, metavar="<pattern>", help="Use cached value for artifacts matching pattern.")
@click.option("-j", "--jobs", type=int, help="Number of parallel jobs (default: CPU count).")
@click.option("-n", "--dry-run", is_flag=True, help="Show execution plan without running.")
@click.option("--no-provenance", is_flag=True, help="Don't include provenance in .dvc files.")
@click.option("-v", "--verbose", is_flag=True, help="Show detailed output.")
def run_cmd(targets, force, force_upstream, cached, jobs, dry_run, no_provenance, verbose):
    """Execute artifact computations from .dvc files.

    Run computations defined in .dvc files, respecting dependencies and
    executing in parallel where possible. Skips fresh (up-to-date) artifacts.

    If no targets specified, runs all *.dvc files in current directory.

    Examples:
        dvx run                    # Run all .dvc files
        dvx run output.dvc         # Run specific target
        dvx run -j 4               # Use 4 parallel workers
        dvx run --dry-run          # Show what would run
        dvx run --force            # Force re-run all
    """
    from pathlib import Path

    from dvx.run.executor import ExecutionConfig, run

    # Find targets
    target_paths = list(targets) if targets else []
    if not target_paths:
        # Default: find all .dvc files in current directory
        target_paths = list(Path(".").glob("*.dvc"))
        if not target_paths:
            raise click.ClickException(
                "No .dvc files found in current directory.\n"
                "Specify targets or run from a directory with .dvc files."
            )

    config = ExecutionConfig(
        max_workers=jobs,
        dry_run=dry_run,
        force=force,
        force_patterns=list(force_upstream) if force_upstream else [],
        cached_patterns=list(cached) if cached else [],
        provenance=not no_provenance,
        verbose=verbose,
    )

    try:
        results = run([Path(t) for t in target_paths], config, output=sys.stderr)

        # Print summary
        total = len(results)
        executed = sum(1 for r in results if r.success and not r.skipped)
        skipped = sum(1 for r in results if r.skipped)
        failed = sum(1 for r in results if not r.success)

        click.echo("")
        click.echo("Summary:")
        click.echo(f"  Total: {total}")
        click.echo(f"  Executed: {executed}")
        click.echo(f"  Skipped: {skipped}")
        if failed:
            click.echo(f"  Failed: {failed}")
            sys.exit(1)

    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Version
# =============================================================================


@cli.command()
def version():
    """Show DVX and DVC versions."""
    import dvc

    try:
        from dvx._version import __version__ as dvx_version
    except ImportError:
        dvx_version = "dev"

    click.echo(f"DVX version: {dvx_version}")
    click.echo(f"DVC version: {dvc.__version__}")


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
