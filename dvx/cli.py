"""DVX CLI - minimal data version control.

This CLI wraps DVC commands, exposing only the core data versioning
functionality.
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
    without pipelines, experiments, metrics, params, or plots.
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
@click.option("--no-commit", is_flag=True, help="Don't auto-commit to git.")
@click.option("--glob", is_flag=True, help="Enable globbing for targets.")
def add(targets, no_commit, glob):
    """Track file(s) or directory(ies) with DVX.

    Creates .dvc files and adds data to the cache.
    """
    try:
        with Repo() as repo:
            repo.add(list(targets), no_commit=no_commit, glob=glob)
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
def checkout(targets, force):
    """Checkout data files from cache to workspace."""
    try:
        with Repo() as repo:
            repo.checkout(targets=list(targets) if targets else None, force=force)
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
def status(targets, cloud, remote, all_branches, all_commits, all_tags):
    """Show status of tracked files."""
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
            if not st:
                click.echo("No changes.")
            else:
                import json
                click.echo(json.dumps(st, indent=2))
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Diff
# =============================================================================


@cli.command()
@click.argument("a_rev", required=False)
@click.argument("b_rev", required=False)
@click.option("--targets", "-t", multiple=True, help="Specific files to diff.")
def diff(a_rev, b_rev, targets):
    """Show changes between revisions or workspace.

    Examples:
        dvx diff              # workspace vs HEAD
        dvx diff HEAD~1       # HEAD~1 vs workspace
        dvx diff HEAD~1 HEAD  # HEAD~1 vs HEAD
    """
    try:
        with Repo() as repo:
            d = repo.diff(
                a_rev=a_rev,
                b_rev=b_rev,
                targets=list(targets) if targets else None,
            )
            if not any(d.values()):
                click.echo("No changes.")
            else:
                import json
                click.echo(json.dumps(d, indent=2))
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
@click.option("-r", "--remote", help="Remote storage to gc.")
@click.option("-T", "--all-tags", is_flag=True, help="Keep cache for all tags.")
@click.option("-w", "--workspace", is_flag=True, help="Keep only cache for current workspace.")
def gc(all_branches, all_commits, cloud, force, jobs, remote, all_tags, workspace):
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
            )
            click.echo(f"Removed {result.get('deleted', 0)} file(s).")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Remove
# =============================================================================


@cli.command()
@click.argument("targets", nargs=-1, required=True)
@click.option("--outs", is_flag=True, help="Also remove the output files.")
def remove(targets, outs):
    """Stop tracking file(s) with DVX."""
    try:
        with Repo() as repo:
            repo.remove(list(targets), outs=outs)
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
# Cache (delegate to dvc)
# =============================================================================


@cli.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def cache(ctx, args):
    """Manage DVC cache.

    This delegates to `dvc cache`. Run `dvx cache --help` for options.
    """
    from dvc.cli import main as dvc_main

    sys.exit(dvc_main(["cache", *args]))


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
