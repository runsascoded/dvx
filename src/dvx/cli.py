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
@click.option("--no-commit", is_flag=True, help="Don't put files/directories into cache.")
@click.option("--glob", is_flag=True, help="Enable globbing for targets.")
@click.option("-o", "--out", metavar="<path>", help="Destination path to put files to.")
@click.option("--to-remote", is_flag=True, help="Upload directly to remote storage.")
@click.option("-r", "--remote", metavar="<name>", help="Remote storage to upload to.")
@click.option("-f", "--force", is_flag=True, help="Override local file or folder if exists.")
def add(targets, no_commit, glob, out, to_remote, remote, force):
    """Track file(s) or directory(ies) with DVX.

    Creates .dvc files and adds data to the cache.
    """
    # Validation
    if to_remote or out:
        if len(targets) != 1:
            raise click.ClickException("--to-remote/--out can't be used with multiple targets")
        if glob:
            raise click.ClickException("--glob can't be used with --to-remote/--out")
        if no_commit:
            raise click.ClickException("--no-commit can't be used with --to-remote/--out")
    else:
        if remote:
            raise click.ClickException("--remote can't be used without --to-remote")

    try:
        with Repo() as repo:
            repo.add(
                list(targets),
                no_commit=no_commit,
                glob=glob,
                out=out,
                remote=remote,
                to_remote=to_remote,
                force=force,
            )
    except Exception as e:
        raise click.ClickException(str(e)) from e


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
# Status
# =============================================================================


@cli.command()
@click.argument("targets", nargs=-1)
@click.option("-c", "--cloud", is_flag=True, help="Check status against remote storage.")
@click.option("-r", "--remote", help="Remote storage to check against.")
@click.option("-a", "--all-branches", is_flag=True, help="Check all branches.")
@click.option("-A", "--all-commits", is_flag=True, help="Check all commits.")
@click.option("-T", "--all-tags", is_flag=True, help="Check all tags.")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
def status(targets, cloud, remote, all_branches, all_commits, all_tags, as_json):
    """Show status of tracked files."""
    import json

    try:
        with Repo() as repo:
            st = repo.status(
                targets=list(targets) if targets else None,
                cloud=cloud,
                remote=remote,
                all_branches=all_branches,
                all_tags=all_tags,
                all_commits=all_commits,
            )
            if as_json:
                click.echo(json.dumps(st, indent=2))
            elif not st:
                click.echo("No changes.")
            else:
                click.echo(json.dumps(st, indent=2))
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Diff
# =============================================================================


@cli.command()
@click.argument("a_rev", required=False)
@click.argument("b_rev", required=False)
@click.option("-t", "--targets", multiple=True, help="Specific files to diff.")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
def diff(a_rev, b_rev, targets, as_json):
    """Show changes between revisions or workspace.

    Examples:
        dvx diff              # workspace vs HEAD
        dvx diff HEAD~1       # HEAD~1 vs workspace
        dvx diff HEAD~1 HEAD  # HEAD~1 vs HEAD
    """
    import json

    try:
        with Repo() as repo:
            d = repo.diff(
                a_rev=a_rev,
                b_rev=b_rev,
                targets=list(targets) if targets else None,
            )
            if as_json or any(d.values()):
                click.echo(json.dumps(d, indent=2))
            else:
                click.echo("No changes.")
    except Exception as e:
        raise click.ClickException(str(e)) from e


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

    TARGET is a .dvc file or path to a tracked file (adds .dvc if needed).

    Examples:
        dvx cache path data.txt.dvc
        dvx cache path data.txt
        dvx cache path data.txt --remote myremote
        dvx cache path data.txt -r HEAD~1
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

    TARGET is a .dvc file or path to a tracked file (adds .dvc if needed).

    Examples:
        dvx cache md5 data.txt.dvc
        dvx cache md5 data.txt
        dvx cache md5 data.txt -r HEAD~1
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

    TARGET is a .dvc file or path to a tracked file (adds .dvc if needed).

    Examples:
        dvx cat data.txt.dvc
        dvx cat data.txt
        dvx cat data.txt -r HEAD~1
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
