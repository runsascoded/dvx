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


@click.command()
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


# Export the command
cmd = status
