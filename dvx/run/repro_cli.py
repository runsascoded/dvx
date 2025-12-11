#!/usr/bin/env -S uv run
"""CLI for dvx repro - reproduce artifacts from .dvc files.

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

from dvx.run.repro import ReproConfig, repro, status


@click.group()
def cli():
    """DVX repro - reproduce artifacts from .dvc file provenance."""


@cli.command()
@click.argument("targets", nargs=-1, type=click.Path(exists=True, path_type=Path))
@click.option(
    "--force", "-f",
    is_flag=True,
    help="Force recomputation even if fresh.",
)
@click.option(
    "--force-upstream",
    multiple=True,
    metavar="PATTERN",
    help="Force recomputation of upstream artifacts matching pattern.",
)
@click.option(
    "--cached",
    multiple=True,
    metavar="PATTERN",
    help="Use cached value for artifacts matching pattern, even if stale.",
)
@click.option(
    "--dry-run", "-n",
    is_flag=True,
    help="Show what would be run without executing.",
)
@click.option(
    "--jobs", "-j",
    type=int,
    default=1,
    help="Number of parallel jobs (default: 1).",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Enable verbose output.",
)
def run(
    targets: tuple[Path, ...],
    force: bool,
    force_upstream: tuple[str, ...],
    cached: tuple[str, ...],
    dry_run: bool,
    jobs: int,
    verbose: bool,
):
    """Reproduce artifacts from .dvc files.

    Reads .dvc files, builds dependency graph from computation blocks,
    and re-runs stale computations in topological order.

    TARGETS are .dvc files or output paths to reproduce.

    Examples:

        \b
        # Reproduce a single artifact
        dvx-repro run output.parquet.dvc

        \b
        # Reproduce all normalized data
        dvx-repro run s3/ctbk/normalized/*.dvc

        \b
        # Force recomputation
        dvx-repro run --force output.dvc

        \b
        # Force recompute specific upstream pattern
        dvx-repro run --force-upstream "*/normalized/*" output.dvc

        \b
        # Use cached raw data even if stale
        dvx-repro run --cached "*/raw/*" output.dvc

        \b
        # Dry run - show what would be executed
        dvx-repro run --dry-run output.dvc
    """
    if not targets:
        click.echo("Error: No targets specified", err=True)
        sys.exit(1)

    config = ReproConfig(
        force=force,
        force_upstream=list(force_upstream),
        cached=list(cached),
        dry_run=dry_run,
        jobs=jobs,
        verbose=verbose,
    )

    results = repro(list(targets), config, output=sys.stderr)

    # Print summary
    total = len(results)
    executed = sum(1 for r in results if r.success and not r.skipped)
    skipped = sum(1 for r in results if r.skipped)
    failed = sum(1 for r in results if not r.success)

    if verbose or dry_run:
        click.echo("\nSummary:", err=True)
        click.echo(f"  Total artifacts: {total}", err=True)
        if dry_run:
            click.echo(f"  Would execute: {executed}", err=True)
        else:
            click.echo(f"  Executed: {executed}", err=True)
        click.echo(f"  Skipped (up-to-date): {skipped}", err=True)
        if failed:
            click.echo(f"  Failed: {failed}", err=True)

    if failed > 0:
        sys.exit(1)


@cli.command()
@click.argument("targets", nargs=-1, type=click.Path(exists=True, path_type=Path))
@click.option(
    "--json", "json_output",
    is_flag=True,
    help="Output as JSON.",
)
def check(targets: tuple[Path, ...], json_output: bool):
    """Check freshness status of artifacts.

    Shows which artifacts are fresh (up-to-date) or stale (need recomputation).

    Examples:

        \b
        # Check status of all artifacts
        dvx-repro check *.dvc

        \b
        # Check specific artifact
        dvx-repro check output.parquet.dvc
    """
    if not targets:
        click.echo("Error: No targets specified", err=True)
        sys.exit(1)

    result = status(list(targets))

    if json_output:
        import json
        output = {path: {"fresh": fresh, "reason": reason} for path, (fresh, reason) in result.items()}
        click.echo(json.dumps(output, indent=2))
    else:
        for path, (fresh, reason) in result.items():
            status_icon = "✓" if fresh else "✗"
            click.echo(f"{status_icon} {path}: {reason}")


def main():
    cli()


if __name__ == "__main__":
    main()
