"""DVX transfer commands - push, pull, fetch."""

import click

from dvx import Repo


@click.command()
@click.argument("targets", nargs=-1)
@click.option("-a", "--all-branches", is_flag=True, help="Push for all branches.")
@click.option("-A", "--all-commits", is_flag=True, help="Push for all commits.")
@click.option("-j", "--jobs", type=int, help="Number of parallel jobs.")
@click.option("-n", "--dry-run", is_flag=True, help="Show what would be pushed without pushing.")
@click.option("-r", "--remote", help="Remote storage to push to.")
@click.option("-T", "--all-tags", is_flag=True, help="Push for all tags.")
@click.option("-V", "--verify", is_flag=True, help="Verify remote has correct data after push.")
@click.option("--glob", is_flag=True, help="Enable globbing for targets.")
def push(targets, all_branches, all_commits, jobs, dry_run, remote, all_tags, verify, glob):
    """Upload tracked data to remote storage.

    Use --verify to check that remote has the correct data after pushing.
    """
    from dvx.cache import _format_size, check_remote_cache, get_transfer_status

    if dry_run:
        status = get_transfer_status(
            targets=list(targets) if targets else None,
            remote=remote,
            direction="push",
            glob_pattern=glob,
        )
        missing = status["missing"]
        cached = status["cached"]
        errors = status["errors"]

        if missing:
            click.echo(f"Would push {len(missing)} file(s) ({_format_size(status['total_missing_size'])}):")
            for path, md5, size in missing:
                click.echo(f"  {path}  ({_format_size(size)})  {md5[:8]}...")
        else:
            click.echo("Nothing to push (all files already in remote).")

        if cached:
            click.echo(f"\nAlready in remote: {len(cached)} file(s) ({_format_size(status['total_cached_size'])})")

        if errors:
            click.echo(f"\nErrors ({len(errors)}):")
            for path, err in errors:
                click.echo(f"  {path}: {err}", err=True)
        return

    # Get list of hashes to push (for verification)
    hashes_to_verify = []
    if verify:
        status = get_transfer_status(
            targets=list(targets) if targets else None,
            remote=remote,
            direction="push",
            glob_pattern=glob,
        )
        hashes_to_verify = [(path, md5) for path, md5, _size in status["missing"]]

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

            # Verify after push
            if verify and hashes_to_verify:
                click.echo("\nVerifying remote...")
                verified = 0
                failed = []
                for path, md5 in hashes_to_verify:
                    if check_remote_cache(md5, remote):
                        verified += 1
                    else:
                        failed.append((path, md5))

                if failed:
                    click.echo(f"Verification failed for {len(failed)} file(s):", err=True)
                    for path, md5 in failed:
                        click.echo(f"  {path}  {md5[:8]}...", err=True)
                    raise click.ClickException("Verification failed")
                else:
                    click.echo(f"Verified {verified} file(s) in remote.")
    except click.ClickException:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e


@click.command()
@click.argument("targets", nargs=-1)
@click.option("-a", "--all-branches", is_flag=True, help="Pull for all branches.")
@click.option("-A", "--all-commits", is_flag=True, help="Pull for all commits.")
@click.option("-f", "--force", is_flag=True, help="Force pull, overwriting local files.")
@click.option("-j", "--jobs", type=int, help="Number of parallel jobs.")
@click.option("-n", "--dry-run", is_flag=True, help="Show what would be pulled without pulling.")
@click.option("-r", "--remote", help="Remote storage to pull from.")
@click.option("-R", "--ref", help="Pull files as they existed at a specific git ref (fetch to cache only, no checkout).")
@click.option("-T", "--all-tags", is_flag=True, help="Pull for all tags.")
@click.option("--glob", is_flag=True, help="Enable globbing for targets.")
def pull(targets, all_branches, all_commits, force, jobs, dry_run, remote, ref, all_tags, glob):
    """Download tracked data from remote storage.

    By default, pulls files for current worktree state. Use -R/--ref to pull
    files as they existed at a specific git ref (to cache only, no checkout).

    Examples:
        dvx pull                     # Pull current worktree files
        dvx pull -R HEAD~5           # Pull files as of 5 commits ago
        dvx pull -R v1.0 data/       # Pull data/ files from tag v1.0
    """
    from dvx.cache import (
        _format_size,
        get_transfer_status,
        get_transfer_status_at_ref,
        pull_hashes,
    )

    # Ref-specific pull mode
    if ref:
        status = get_transfer_status_at_ref(
            ref=ref,
            targets=list(targets) if targets else None,
            remote=remote,
        )
        missing = status["missing"]
        cached = status["cached"]
        errors = status["errors"]

        if dry_run:
            if missing:
                click.echo(f"Would pull {len(missing)} file(s) ({_format_size(status['total_missing_size'])}) from {ref}:")
                for path, md5, size in missing:
                    click.echo(f"  {path}  ({_format_size(size)})  {md5[:8]}...")
            else:
                click.echo(f"Nothing to pull for {ref} (all files already cached locally).")

            if cached:
                click.echo(f"\nAlready cached: {len(cached)} file(s) ({_format_size(status['total_cached_size'])})")

            if errors:
                click.echo(f"\nErrors ({len(errors)}):")
                for path, err in errors:
                    click.echo(f"  {path}: {err}", err=True)
            return

        # Actually pull the missing hashes
        if missing:
            hashes = [md5 for _path, md5, _size in missing]
            try:
                fetched = pull_hashes(hashes, remote=remote, jobs=jobs)
                click.echo(f"{fetched} file(s) fetched from {ref}.")
            except Exception as e:
                raise click.ClickException(str(e)) from e
        else:
            click.echo(f"Nothing to pull for {ref} (all files already cached locally).")

        if errors:
            click.echo(f"\nErrors ({len(errors)}):")
            for path, err in errors:
                click.echo(f"  {path}: {err}", err=True)
        return

    # Standard pull mode (current worktree)
    if dry_run:
        status = get_transfer_status(
            targets=list(targets) if targets else None,
            remote=remote,
            direction="pull",
            glob_pattern=glob,
        )
        missing = status["missing"]
        cached = status["cached"]
        errors = status["errors"]

        if missing:
            click.echo(f"Would pull {len(missing)} file(s) ({_format_size(status['total_missing_size'])}):")
            for path, md5, size in missing:
                click.echo(f"  {path}  ({_format_size(size)})  {md5[:8]}...")
        else:
            click.echo("Nothing to pull (all files already cached locally).")

        if cached:
            click.echo(f"\nAlready cached: {len(cached)} file(s) ({_format_size(status['total_cached_size'])})")

        if errors:
            click.echo(f"\nErrors ({len(errors)}):")
            for path, err in errors:
                click.echo(f"  {path}: {err}", err=True)
        return

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


@click.command()
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
