"""DVX run command - execute artifact computations."""

import sys
from pathlib import Path

import click


@click.command("run")
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


# Export the command
cmd = run_cmd
