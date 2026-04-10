"""DVX transfer commands - push, pull, fetch."""

import shutil
from pathlib import Path

import click

from dvx import Repo


def _pull_targets(
    targets: list[str],
    remote: str | None = None,
    jobs: int | None = None,
    force: bool = False,
):
    """Pull specific targets by resolving their .dvc files.

    Bypasses DVC's pipeline-based pull (which requires dvc.yaml) and
    instead reads MD5 hashes from .dvc files, fetches from remote cache,
    and links/copies to the output path.
    """
    from dvx.cache import get_cache_path_from_hash, pull_hashes
    from dvx.run.dvc_files import read_dvc_file

    # Resolve targets to (output_path, md5) pairs
    to_pull: list[tuple[Path, str]] = []
    for target in targets:
        target_path = Path(target)

        # Expand directories: find all .dvc files under them
        if target_path.is_dir():
            dvc_files = sorted(target_path.glob("**/*.dvc"))
            for dvc_file in dvc_files:
                if ".dvc/" in str(dvc_file):
                    continue
                info = read_dvc_file(dvc_file)
                if info and info.md5:
                    out = Path(str(dvc_file)[:-4])
                    to_pull.append((out, info.md5))
            continue

        # Try as output path or .dvc path
        info = read_dvc_file(target_path)
        if info and info.md5:
            if target_path.suffix == ".dvc":
                out = Path(str(target_path)[:-4])
            else:
                out = target_path
            to_pull.append((out, info.md5))
        else:
            click.echo(f"  ⚠ {target}: no .dvc file found or no hash", err=True)

    if not to_pull:
        click.echo("Nothing to pull.")
        return

    # Fetch hashes from remote
    hashes = [md5 for _, md5 in to_pull]
    fetched = pull_hashes(hashes, remote=remote, jobs=jobs)

    # Checkout: link/copy from cache to output path
    checked_out = 0
    for out_path, md5 in to_pull:
        cache_path = Path(get_cache_path_from_hash(md5, absolute=True))
        if not cache_path.exists():
            click.echo(f"  ⚠ {out_path}: not in local cache after fetch", err=True)
            continue

        if out_path.exists() and not force:
            # Check if already matches
            from dvx.run.hash import compute_md5
            try:
                if compute_md5(out_path) == md5:
                    continue  # Already up to date
            except (FileNotFoundError, ValueError):
                pass

        # Copy from cache (DVC cache files are read-only, so copy, not link)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(cache_path), str(out_path))
        checked_out += 1

    click.echo(f"{fetched} file(s) fetched, {checked_out} file(s) checked out.")


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
    from dvx.cache import _format_size, check_remote_cache_batch, get_transfer_status

    if dry_run:
        status = get_transfer_status(
            targets=list(targets) if targets else None,
            remote=remote,
            direction="push",
            glob_pattern=glob,
            jobs=jobs,
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
            jobs=jobs,
            progress=False,  # Don't show progress twice
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

            # Verify after push (parallel batch check)
            if verify and hashes_to_verify:
                click.echo("\nVerifying remote...")
                all_hashes = [md5 for _, md5 in hashes_to_verify]
                cache_status = check_remote_cache_batch(all_hashes, remote, jobs=jobs)

                verified = 0
                failed = []
                for path, md5 in hashes_to_verify:
                    if cache_status.get(md5, False):
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

    if targets:
        # Targeted pull: resolve .dvc files, fetch hashes, checkout
        _pull_targets(list(targets), remote=remote, jobs=jobs, force=force)
    else:
        try:
            with Repo() as repo:
                result = repo.pull(
                    targets=None,
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
