"""DVX status command - check freshness of artifacts."""

from pathlib import Path

import click


def _expand_targets(targets):
    """Expand targets: directories become all .dvc files under them, files get .dvc added if needed."""
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


def _mark_transitive_staleness(results: list[dict], target_list: list) -> None:
    """Mark fresh stages as transitively stale if an ancestor is stale.

    Modifies results in-place, changing status to "transitive" for stages
    whose upstream deps are stale.
    """
    from dvx.run.dvc_files import read_dvc_file

    # Build a map of output_path → result for quick lookup
    result_map: dict[str, dict] = {}
    for r in results:
        path = r["path"]
        # Normalize: strip .dvc suffix for lookup
        key = path[:-4] if path.endswith(".dvc") else path
        result_map[key] = r

    # Build reverse dep graph: for each dep, which stages depend on it
    dependents: dict[str, list[str]] = {}
    for target in target_list:
        target_path = Path(target)
        if target_path.suffix == ".dvc":
            output_path = Path(str(target_path)[:-4])
        else:
            output_path = target_path

        info = read_dvc_file(target_path)
        if info is None or not info.cmd:
            continue

        output_key = str(output_path)
        # All deps (both file and git)
        for dep_path in list(info.deps.keys()) + list(info.git_deps.keys()):
            dependents.setdefault(dep_path, []).append(output_key)

    # BFS from stale stages to mark descendants
    stale_keys = {
        (r["path"][:-4] if r["path"].endswith(".dvc") else r["path"])
        for r in results
        if r["status"] in ("stale", "missing")
    }

    from collections import deque
    queue = deque(stale_keys)
    visited = set(stale_keys)
    while queue:
        current = queue.popleft()
        for dependent in dependents.get(current, []):
            if dependent not in visited:
                visited.add(dependent)
                if dependent in result_map and result_map[dependent]["status"] == "fresh":
                    result_map[dependent]["status"] = "transitive"
                    # Find nearest stale ancestor for the reason
                    result_map[dependent]["reason"] = f"upstream stale: {current}"
                queue.append(dependent)


STATUS_NAMES = ["fresh", "stale", "missing", "error", "transitive"]
GROUP_ORDER = ["stale", "missing", "transitive", "error", "fresh"]


def _resolve_status_list(value: str | None) -> set[str] | None:
    """Resolve a comma-separated list of status names (with prefix matching) to a set.

    Returns None if value is None/empty. Raises click.BadParameter on ambiguous or unknown prefixes.
    """
    if not value:
        return None
    result = set()
    for raw in value.split(","):
        token = raw.strip().lower()
        if not token:
            continue
        matches = [s for s in STATUS_NAMES if s.startswith(token)]
        if not matches:
            raise click.BadParameter(f"unknown status {token!r} (expected one of {STATUS_NAMES})")
        if len(matches) > 1:
            raise click.BadParameter(f"ambiguous status prefix {token!r}: matches {matches}")
        result.add(matches[0])
    return result


@click.command()
@click.argument("targets", nargs=-1)
@click.option("-d", "--with-deps", is_flag=True, default=True, help="Check upstream dependencies.")
@click.option("-G", "--no-group", is_flag=True, help="Don't group output by status.")
@click.option("-j", "--jobs", type=int, default=None, help="Number of parallel workers.")
@click.option("-N", "--no-transitive", is_flag=True, help="Hide transitively stale stages.")
@click.option("-s", "--status", "status_filter", default=None, help="Show only these statuses (comma-sep, prefix-matched, e.g. 's,m').")
@click.option("-v", "--verbose", is_flag=True, help="Show all files including fresh.")
@click.option("-x", "--omit", default=None, help="Exclude these statuses (comma-sep, prefix-matched, e.g. 'm').")
@click.option("--json", "as_json", is_flag=True, help="Output results as JSON.")
@click.option("-y", "--yaml", "as_yaml", is_flag=True, help="Output detailed results as YAML (includes before/after hashes).")
def status(targets, with_deps, no_group, jobs, no_transitive, status_filter, verbose, omit, as_json, as_yaml):
    """Check freshness status of artifacts.

    By default, only shows stale/missing files (like git status), grouped by status.
    Use -v/--verbose to also include fresh files.
    Use -s/--status to include only specific statuses (e.g. -s stale,missing).
    Use -x/--omit to exclude specific statuses (e.g. -x missing).
    Status names support prefix matching: 's' → stale, 'm' → missing, etc.
    Use -G/--no-group to flatten output (paths sorted, no per-status sections).
    Use -y/--yaml for detailed output with before/after hashes for changed deps.

    Examples:
        dvx status                   # Check all .dvc files
        dvx status output.dvc        # Check specific target
        dvx status data/             # Check all .dvc files under data/
        dvx status -j 4              # Use 4 parallel workers
        dvx status --json            # Output as JSON
        dvx status -y                # Detailed YAML with hashes
        dvx status -x m              # Hide missing files
        dvx status -s s,t            # Show only stale and transitive
    """
    import json as json_module
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from functools import partial

    include = _resolve_status_list(status_filter)
    exclude = _resolve_status_list(omit) or set()

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

    # Mark transitively stale stages (unless disabled)
    if not no_transitive:
        _mark_transitive_staleness(results, target_list)

    results.sort(key=lambda r: r["path"])

    # Counts from the full, unfiltered set (for the summary line)
    counts = {s: sum(1 for r in results if r["status"] == s) for s in STATUS_NAMES}

    # Compute the visible set. Precedence: -s overrides default; -v adds fresh to default;
    # -x always subtracts.
    if include is not None:
        visible = set(include)
    else:
        visible = set(STATUS_NAMES) if verbose else {"stale", "missing", "error", "transitive"}
    visible -= exclude

    filtered = [r for r in results if r["status"] in visible]

    if as_yaml:
        import yaml
        yaml_data = {}
        for r in filtered:
            path = r.pop("path")
            yaml_data[path] = {k: v for k, v in r.items() if v is not None}
        click.echo(yaml.dump(yaml_data, default_flow_style=False, sort_keys=False))
        return

    if as_json:
        click.echo(json_module.dumps(filtered, indent=2))
        return

    status_style = {
        "fresh": ("✓", "green"),
        "stale": ("✗", "red"),
        "missing": ("?", "magenta"),
        "error": ("!", "red"),
        "transitive": ("⚠", "yellow"),
    }

    def _render(r):
        icon, color = status_style.get(r["status"], ("?", "red"))
        styled_icon = click.style(icon, fg=color)
        line = f"{styled_icon} {r['path']}"
        if r.get("reason"):
            line += click.style(f" ({r['reason']})", fg="bright_black")
        return line

    if no_group:
        for r in filtered:
            click.echo(_render(r))
    else:
        first = True
        for s in GROUP_ORDER:
            if s not in visible:
                continue
            group = [r for r in filtered if r["status"] == s]
            if not group:
                continue
            if not first:
                click.echo()
            first = False
            _, color = status_style[s]
            header = click.style(f"{s.capitalize()} ({len(group)}):", fg=color, bold=True)
            click.echo(header)
            for r in group:
                click.echo(f"  {_render(r)}")

    # Summary line (always reflects the full set, not filtered)
    parts = [f"Fresh: {counts['fresh']}", f"Stale: {counts['stale']}"]
    if counts["missing"]:
        parts.append(f"Missing: {counts['missing']}")
    if counts["transitive"]:
        parts.append(f"Transitively stale: {counts['transitive']}")
    if counts["error"]:
        parts.append(f"Error: {counts['error']}")
    click.echo(f"\n{', '.join(parts)}")


# Export the command
cmd = status
