#!/usr/bin/env python3
"""CLI for dvx run - execute artifact computations from .dvc files.

/// script
requires-python = ">=3.10"
dependencies = [
    "click>=8.0",
]
///
"""

import sys
from pathlib import Path

import click

from dvx.run.executor import ExecutionConfig, run


@click.command()
@click.argument('targets', nargs=-1, type=click.Path(path_type=Path))
@click.option(
    '-n', '--dry-run',
    is_flag=True,
    help='Show execution plan without running.',
)
@click.option(
    '-j', '--jobs',
    type=int,
    default=None,
    metavar='N',
    help='Number of parallel jobs (default: CPU count).',
)
@click.option(
    '-f', '--force',
    is_flag=True,
    help='Force re-run all computations (ignore freshness).',
)
@click.option(
    '--force-upstream',
    multiple=True,
    metavar='PATTERN',
    help='Force re-run upstream artifacts matching pattern.',
)
@click.option(
    '--cached',
    multiple=True,
    metavar='PATTERN',
    help='Use cached value for artifacts matching pattern.',
)
@click.option(
    '--no-provenance',
    is_flag=True,
    help='Do not include provenance metadata in .dvc files.',
)
@click.option(
    '-v', '--verbose',
    is_flag=True,
    help='Enable verbose output.',
)
def main(
    targets: tuple[Path, ...],
    dry_run: bool,
    jobs: int | None,
    force: bool,
    force_upstream: tuple[str, ...],
    cached: tuple[str, ...],
    no_provenance: bool,
    verbose: bool,
):
    """Execute artifact computations from .dvc files.

    Reads .dvc files, builds dependency graph from computation blocks,
    and executes stale computations in parallel.

    TARGETS are .dvc files or output paths. If no targets specified,
    searches for *.dvc files in current directory.

    \b
    Examples:
        # Run all .dvc files in current directory
        dvx run

        # Run specific artifacts
        dvx run output.parquet.dvc

        # Run with glob pattern
        dvx run normalized/*.dvc

        # Dry run - show what would execute
        dvx run --dry-run

        # Force re-run everything
        dvx run --force

        # Force re-run specific upstream pattern
        dvx run --force-upstream "*/raw/*" output.dvc

        # Use cached value for pattern (skip even if stale)
        dvx run --cached "*/external/*" output.dvc

        # Parallel execution with 4 workers
        dvx run -j 4
    """
    # Find targets
    target_list = list(targets)
    if not target_list:
        # Default: find all .dvc files in current directory
        target_list = list(Path('.').glob('*.dvc'))
        if not target_list:
            click.echo("No .dvc files found in current directory", err=True)
            click.echo("Specify targets or run from a directory with .dvc files", err=True)
            sys.exit(1)

    config = ExecutionConfig(
        max_workers=jobs,
        dry_run=dry_run,
        force=force,
        force_patterns=list(force_upstream),
        cached_patterns=list(cached),
        provenance=not no_provenance,
        verbose=verbose,
    )

    try:
        results = run(target_list, config, output=sys.stderr)

        # Print summary
        total = len(results)
        executed = sum(1 for r in results if r.success and not r.skipped)
        skipped = sum(1 for r in results if r.skipped)
        failed = sum(1 for r in results if not r.success)

        click.echo("", err=True)
        click.echo("Summary:", err=True)
        click.echo(f"  Total: {total}", err=True)
        click.echo(f"  Executed: {executed}", err=True)
        click.echo(f"  Skipped: {skipped}", err=True)
        if failed:
            click.echo(f"  Failed: {failed}", err=True)
            sys.exit(1)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
