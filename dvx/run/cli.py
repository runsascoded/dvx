#!/usr/bin/env -S uv run
"""CLI for dvc-run parallel execution engine.

/// script
requires-python = ">=3.10"
dependencies = [
    "click>=8.0",
    "pyyaml>=6.0",
]
///
"""

import sys
from pathlib import Path

import click

from dvx.run.dag import DAG
from dvx.run.executor import ParallelExecutor
from dvx.run.parser import DVCYamlParser


@click.command()
@click.argument("stages", nargs=-1)
@click.option(
    "-d",
    "--dry-run",
    is_flag=True,
    help="Show execution plan without running stages",
)
@click.option(
    "-j",
    "--jobs",
    type=int,
    default=None,
    help="Number of parallel jobs (default: CPU count)",
)
@click.option(
    "-f",
    "--file",
    "dvc_yaml",
    type=click.Path(exists=True, path_type=Path),
    default="dvc.yaml",
    help="Path to dvc.yaml file",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Enable verbose output",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force re-run all stages (ignore freshness checks)",
)
@click.option(
    "--no-provenance",
    is_flag=True,
    help="Do not include provenance metadata in .dvc files",
)
@click.option(
    "--dot",
    "dot_output",
    type=click.Path(path_type=Path),
    help="Export DAG as GraphViz DOT format to file",
)
@click.option(
    "--svg",
    "svg_output",
    type=click.Path(path_type=Path),
    help="Export DAG as SVG to file (requires graphviz)",
)
@click.option(
    "--mermaid",
    "mermaid_output",
    type=click.Path(path_type=Path),
    help="Export DAG as Mermaid diagram to file",
)
def main(
    stages: tuple[str, ...],
    dry_run: bool,
    jobs: int | None,
    dvc_yaml: Path,
    verbose: bool,
    force: bool,
    no_provenance: bool,
    dot_output: Path | None,
    svg_output: Path | None,
    mermaid_output: Path | None,
):
    """Execute DVC pipeline stages in parallel.

    dvc-run reads your dvc.yaml file, builds a dependency graph, and executes
    independent stages in parallel. Each output gets a .dvc file with its hash
    and optional provenance metadata.

    Examples:

        \b
        # Run pipeline with default parallelism
        dvc-run

        \b
        # Limit to 4 parallel jobs
        dvc-run -j 4

        \b
        # Show execution plan without running
        dvc-run --dry-run

        \b
        # Force re-run all stages
        dvc-run --force

        \b
        # Run without provenance metadata
        dvc-run --no-provenance
    """
    try:
        # Parse dvc.yaml
        if verbose:
            click.echo(f"Parsing {dvc_yaml}...", err=True)

        parser = DVCYamlParser(dvc_yaml)
        all_stages = parser.parse()

        if not all_stages:
            click.echo("No stages found in dvc.yaml", err=True)
            sys.exit(1)

        if verbose:
            click.echo(f"Found {len(all_stages)} stage(s)", err=True)

        # Build DAG
        dag = DAG(all_stages)

        # Filter to selected stages if specified
        if stages:
            try:
                dag = dag.filter_to_targets(list(stages))
                if verbose:
                    click.echo(
                        f"Filtered to {len(dag.stages)} stage(s) "
                        f"(targets + dependencies)",
                        err=True,
                    )
            except ValueError as e:
                click.echo(f"Error: {e}", err=True)
                sys.exit(1)

        # Check for cycles
        cycle = dag.check_cycles()
        if cycle:
            click.echo(
                f"Error: Circular dependency detected: {' -> '.join(cycle)}",
                err=True,
            )
            sys.exit(1)

        # Export visualizations if requested
        if dot_output or svg_output or mermaid_output:
            from dvx.run.viz import DAGVisualizer

            viz = DAGVisualizer(dag)

            if dot_output:
                viz.to_dot_file(dot_output)
                click.echo(f"Exported DOT to {dot_output}", err=True)

            if svg_output:
                try:
                    viz.to_svg(svg_output)
                    click.echo(f"Exported SVG to {svg_output}", err=True)
                except RuntimeError as e:
                    click.echo(f"Error: {e}", err=True)
                    sys.exit(1)

            if mermaid_output:
                mermaid_output.write_text(viz.to_mermaid())
                click.echo(f"Exported Mermaid to {mermaid_output}", err=True)

            # If only exporting visualizations (no execution), exit
            if dry_run or (not stages and (dot_output or svg_output or mermaid_output)):
                return

        # Execute
        executor = ParallelExecutor(
            dag=dag,
            max_workers=jobs,
            dry_run=dry_run,
            output=sys.stderr,
            force=force,
            provenance=not no_provenance,
        )

        results = executor.execute()

        if not dry_run:
            # Print summary
            total = len(results)
            succeeded = sum(1 for r in results if r.success and not r.skipped)
            skipped = sum(1 for r in results if r.skipped)
            failed = sum(1 for r in results if not r.success)

            click.echo("\nSummary:", err=True)
            click.echo(f"  Total stages: {total}", err=True)
            click.echo(f"  Executed: {succeeded}", err=True)
            click.echo(f"  Skipped (up-to-date): {skipped}", err=True)
            if failed:
                click.echo(f"  Failed: {failed}", err=True)

            # Count .dvc files created
            dvc_files_created = sum(
                len(r.dvc_files) for r in results
                if r.dvc_files
            )
            if dvc_files_created:
                click.echo(f"  .dvc files created: {dvc_files_created}", err=True)

            if failed > 0:
                sys.exit(1)

    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo("\nInterrupted", err=True)
        sys.exit(130)


if __name__ == "__main__":
    main()
