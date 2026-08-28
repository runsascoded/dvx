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
@click.option("-r", "--recursive", is_flag=True, help="Auto-add stale deps first (depth-first).")
def add(targets, force, recursive):
    """Track file(s) or directory(ies) with DVX.

    Creates .dvc files and adds data to the cache.
    Safe for parallel execution (no global locking).

    If deps are stale (file hash != .dvc hash), errors by default.
    Use --recursive to auto-add stale deps first.
    """
    from dvx.cache import add_to_cache

    for target in targets:
        try:
            md5, size, is_dir = add_to_cache(target, force=force, recursive=recursive)
            click.echo(f"Added {target} ({md5[:8]}...)")
        except Exception as e:
            raise click.ClickException(f"Failed to add {target}: {e}") from e


# =============================================================================
# Transfer commands (push, pull, fetch) - from transfer module
# =============================================================================

from .transfer import fetch, pull, push

cli.add_command(push)
cli.add_command(pull)
cli.add_command(fetch)


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
# Status - from status module
# =============================================================================

from .status import status

cli.add_command(status)


# =============================================================================
# Diff - from diff module
# =============================================================================

from .diff import diff

cli.add_command(diff)


# =============================================================================
# GC
# =============================================================================


def _gc_delete(deletable, force, dry, skipped=None):
    """Shared print/confirm/delete tail for dvx-native gc paths.

    ``skipped`` (from ``--safe``) is reported but never deleted.
    """
    from dvx.gc import format_size

    if skipped:
        skipped_size = sum(s for _, s, _ in skipped)
        click.echo(
            f"⚠ Skipping {len(skipped)} blob(s) ({format_size(skipped_size)}) "
            "not present in any checked remote — push first or gc without --safe:"
        )
        for md5, size, _path in sorted(skipped, key=lambda x: x[1], reverse=True)[:10]:
            click.echo(f"  {md5[:12]}...  {format_size(size)}")
        if len(skipped) > 10:
            click.echo(f"  ... and {len(skipped) - 10} more")

    if not deletable:
        click.echo("Nothing to delete.")
        return

    total_size = sum(s for _, s, _ in deletable)
    click.echo(f"Would delete {len(deletable)} blob(s) ({format_size(total_size)}):")
    for md5, size, _path in sorted(deletable, key=lambda x: x[1], reverse=True):
        click.echo(f"  {md5[:12]}...  {format_size(size)}")

    if dry:
        return

    if not force:
        click.confirm(f"\nDelete {len(deletable)} cached blob(s)?", abort=True)

    deleted = 0
    freed = 0
    for md5, size, path in deletable:
        try:
            path.unlink()
            deleted += 1
            freed += size
            # Remove empty parent dir
            try:
                path.parent.rmdir()
            except OSError:
                pass
        except OSError as e:
            click.echo(f"  ⚠ {md5[:12]}...: {e}", err=True)

    click.echo(f"\nDeleted {deleted} blob(s), freed {format_size(freed)}.")


@cli.command()
@click.argument("targets", nargs=-1)
@click.option("-a", "--all-branches", is_flag=True, help="Keep cache for all branches.")
@click.option("-A", "--all-commits", is_flag=True, help="Keep cache for all commits.")
@click.option("-c", "--cloud", is_flag=True, help="Also gc remote storage.")
@click.option("-f", "--force", is_flag=True, help="Force gc without confirmation.")
@click.option("-j", "--jobs", type=int, help="Number of parallel jobs.")
@click.option("-k", "--keep", type=int, help="Keep the N most recent versions per artifact.")
@click.option("-n", "--dry", is_flag=True, help="Dry run - show what would be removed.")
@click.option("-o", "--older-than", help="Delete versions older than duration (e.g. 30d, 1w, 24h).")
@click.option("-r", "--remote", help="Remote to gc / (with --safe) require membership in.")
@click.option("-s", "--safe", is_flag=True, help="Only delete blobs present in a remote (skip + report the rest).")
@click.option("--any-remote", is_flag=True, help="With --safe: membership in ANY configured remote suffices.")
@click.option("-T", "--all-tags", is_flag=True, help="Keep cache for all tags.")
@click.option("-w", "--workspace", is_flag=True, help="Keep only cache for current workspace.")
def gc(targets, all_branches, all_commits, cloud, force, jobs, keep, dry, older_than, remote, safe, any_remote, all_tags, workspace):
    """Garbage collect unused cache files.

    With --keep N or --older-than, uses version-aware retention: walks git
    history to find all versions per artifact, keeps those matching the
    policy, deletes the rest from local cache.

    With --safe, only blobs present in a remote (recoverable) are deleted;
    unpushed blobs are reported and skipped. Composes with all retention
    policies.

    Without --keep/--older-than/--safe, delegates to DVC's gc (requires -w,
    -a, -T, or -A).

    Examples:
        dvx gc -w                     # keep only workspace-referenced blobs
        dvx gc -w --safe              # …deleting only what the remote can restore
        dvx gc --keep 5               # keep 5 most recent versions per artifact
        dvx gc --older-than 30d       # delete versions older than 30 days
        dvx gc --keep 3 -a            # keep 3 newest, considering all branches
        dvx gc --dry --keep 5         # show what would be deleted
        dvx gc data.parquet.dvc       # GC specific artifact
    """
    safe_remotes = None
    if safe:
        from dvx.gc import cache_link_types

        if "symlink" in cache_link_types():
            click.echo(
                "⚠ cache.type includes 'symlink': workspace links will dangle "
                "when cache objects are deleted.",
                err=True,
            )
        if any_remote:
            from dvx.cli.cache.comm import _known_remotes
            names = _known_remotes()["names"]
            if not names:
                raise click.ClickException("--any-remote: no remotes configured")
            safe_remotes = list(names)
        else:
            safe_remotes = [remote]

    # Version-aware GC (--keep or --older-than)
    if keep is not None or older_than is not None:
        from dvx.gc import compute_gc_plan, partition_by_remote

        try:
            _keep_hashes, _delete_hashes, deletable = compute_gc_plan(
                keep=keep,
                older_than=older_than,
                all_branches=all_branches,
                targets=list(targets) if targets else None,
            )
        except ValueError as e:
            raise click.ClickException(str(e)) from e

        skipped = None
        if safe:
            try:
                deletable, skipped = partition_by_remote(deletable, safe_remotes, fresh=True)
            except Exception as e:
                raise click.ClickException(f"--safe: remote check failed: {e}") from e

        _gc_delete(deletable, force, dry, skipped=skipped)
        return

    if not any([workspace, all_branches, all_tags, all_commits]):
        raise click.ClickException(
            "One of -w/--workspace, -a/--all-branches, -T/--all-tags, "
            "-A/--all-commits is required (or use --keep/--older-than)."
        )

    if safe:
        # Native safe GC: keep = referenced objects (with `.dir` manifests
        # expanded), deletable = local − keep, then delete only blobs a
        # remote can restore. Per-object skipping needs the dvx-native
        # deletion path — DVC's gc is all-or-nothing.
        if all_tags:
            raise click.ClickException("--all-tags is not supported with --safe yet.")

        from dvx.comm import local_objects, ref_objects
        from dvx.gc import partition_by_remote

        refspec = (
            "--all-commits" if all_commits
            else "--all-branches" if all_branches
            else "workspace"
        )
        try:
            rs = ref_objects(refspec, remotes=safe_remotes)
        except Exception as e:
            raise click.ClickException(str(e)) from e
        if rs.unexpandable:
            raise click.ClickException(
                "Cannot expand referenced directory manifest(s) — inner blobs "
                "are unknown and deletion would risk data loss: "
                + ", ".join(rs.unexpandable)
                + ". Fetch the manifests (e.g. `dvx pull`) and retry."
            )

        from pathlib import Path as _Path

        from dvc.repo import Repo as DVCRepo
        root = _Path(DVCRepo.find_root())
        cache_md5 = root / ".dvc" / "cache" / "files" / "md5"
        deletable = []
        for key, size in local_objects().items():
            if key in rs.objects:
                continue
            h = key[:-4] if key.endswith(".dir") else key
            p = cache_md5 / h[:2] / (h[2:] + (".dir" if key.endswith(".dir") else ""))
            deletable.append((key, size, p))

        try:
            deletable, skipped = partition_by_remote(deletable, safe_remotes, fresh=True)
        except Exception as e:
            raise click.ClickException(f"--safe: remote check failed: {e}") from e

        _gc_delete(deletable, force, dry, skipped=skipped)
        return

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
# External data commands (import, get) - from external module
# =============================================================================

from .external import get, get_url, import_cmd, import_url, update

cli.add_command(import_cmd)
cli.add_command(import_url)
cli.add_command(get)
cli.add_command(get_url)
cli.add_command(update)


# =============================================================================
# Cache subcommands (from cli.cache package)
# =============================================================================

from .cache import cache

cli.add_command(cache)


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
# Run - from run_cmd module
# =============================================================================

from .run_cmd import run_cmd

cli.add_command(run_cmd)


# =============================================================================
# DAG - from dag module
# =============================================================================

from .dag import dag

cli.add_command(dag)


# =============================================================================
# Batch - from batch_cmd module (AWS Batch packaging / submit)
# =============================================================================

from .batch_cmd import batch

cli.add_command(batch)


# =============================================================================
# Shell Integration
# =============================================================================


@cli.command("shell-integration")
@click.argument("shell", type=click.Choice(["bash", "zsh", "fish"]), required=False)
def shell_integration(shell):
    """Output shell aliases for dvx commands.

    Usage:
        # Bash/Zsh: Add to your ~/.bashrc or ~/.zshrc:
        eval "$(dvx shell-integration bash)"

        # Fish: Add to your ~/.config/fish/config.fish:
        dvx shell-integration fish | source

        # Or save to a file and source it:
        dvx shell-integration bash > ~/.dvx-aliases.sh
        echo 'source ~/.dvx-aliases.sh' >> ~/.bashrc
    """
    from pathlib import Path

    # Auto-detect shell if not specified
    if not shell:
        shell_env = os.environ.get("SHELL", "")
        if "fish" in shell_env:
            shell = "fish"
        elif "zsh" in shell_env:
            shell = "zsh"
        else:
            shell = "bash"

    # Get the shell directory (in the dvx package, not cli subpackage)
    shell_dir = Path(__file__).parent.parent / "shell"
    # zsh uses bash aliases
    shell_file = shell_dir / f"dvx.{shell if shell != 'zsh' else 'bash'}"

    if shell_file.exists():
        click.echo(shell_file.read_text())
    else:
        raise click.ClickException(f"Shell integration file not found: {shell_file}")


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
