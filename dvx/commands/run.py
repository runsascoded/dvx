import sys

from dvx.cli import formatter
from dvx.cli.command import CmdBase
from dvx.log import logger

logger = logger.getChild(__name__)


class CmdRun(CmdBase):
    def run(self):
        from pathlib import Path

        from dvx.run.dag import DAG
        from dvx.run.executor import ParallelExecutor
        from dvx.run.parser import DVCYamlParser

        args = self.args
        dvc_yaml = Path(args.file)

        try:
            # Parse dvc.yaml
            if args.verbose:
                logger.info(f"Parsing {dvc_yaml}...")

            parser = DVCYamlParser(dvc_yaml)
            all_stages = parser.parse()

            if not all_stages:
                logger.error("No stages found in dvc.yaml")
                return 1

            if args.verbose:
                logger.info(f"Found {len(all_stages)} stage(s)")

            # Build DAG
            dag = DAG(all_stages)

            # Filter to selected stages if specified
            if args.targets:
                try:
                    dag = dag.filter_to_targets(list(args.targets))
                    if args.verbose:
                        logger.info(
                            f"Filtered to {len(dag.stages)} stage(s) "
                            f"(targets + dependencies)"
                        )
                except ValueError as e:
                    logger.error(str(e))
                    return 1

            # Check for cycles
            cycle = dag.check_cycles()
            if cycle:
                logger.error(f"Circular dependency detected: {' -> '.join(cycle)}")
                return 1

            # Export visualizations if requested
            if args.dot or args.svg or args.mermaid:
                from dvx.run.viz import DAGVisualizer

                viz = DAGVisualizer(dag)

                if args.dot:
                    viz.to_dot_file(Path(args.dot))
                    logger.info(f"Exported DOT to {args.dot}")

                if args.svg:
                    try:
                        viz.to_svg(Path(args.svg))
                        logger.info(f"Exported SVG to {args.svg}")
                    except RuntimeError as e:
                        logger.error(str(e))
                        return 1

                if args.mermaid:
                    Path(args.mermaid).write_text(viz.to_mermaid())
                    logger.info(f"Exported Mermaid to {args.mermaid}")

                # If only exporting visualizations (no execution), exit
                if args.dry_run:
                    return 0

            # Execute
            executor = ParallelExecutor(
                dag=dag,
                max_workers=args.jobs,
                dry_run=args.dry_run,
                output=sys.stderr,
                force=args.force,
                provenance=not args.no_provenance,
            )

            results = executor.execute()

            if not args.dry_run:
                # Print summary
                total = len(results)
                succeeded = sum(1 for r in results if r.success and not r.skipped)
                skipped = sum(1 for r in results if r.skipped)
                failed = sum(1 for r in results if not r.success)

                logger.info("Summary:")
                logger.info(f"  Total stages: {total}")
                logger.info(f"  Executed: {succeeded}")
                logger.info(f"  Skipped (up-to-date): {skipped}")
                if failed:
                    logger.info(f"  Failed: {failed}")

                # Count .dvc files created
                dvc_files_created = sum(
                    len(r.dvc_files) for r in results if r.dvc_files
                )
                if dvc_files_created:
                    logger.info(f"  .dvc files created: {dvc_files_created}")

                if failed > 0:
                    return 1

            return 0

        except FileNotFoundError as e:
            logger.error(str(e))
            return 1
        except ValueError as e:
            logger.error(str(e))
            return 1
        except RuntimeError as e:
            logger.error(str(e))
            return 1


def add_parser(subparsers, parent_parser):
    RUN_HELP = "Execute pipeline stages in parallel."

    parser = subparsers.add_parser(
        "run",
        parents=[parent_parser],
        description=RUN_HELP,
        help=RUN_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "targets",
        nargs="*",
        help="Stage names to run (runs all if not specified).",
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        default=False,
        help="Show execution plan without running stages.",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=None,
        metavar="<number>",
        help="Number of parallel jobs (default: CPU count).",
    )
    parser.add_argument(
        "-f",
        "--file",
        default="dvc.yaml",
        metavar="<path>",
        help="Path to dvc.yaml file.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force re-run all stages (ignore freshness checks).",
    )
    parser.add_argument(
        "--no-provenance",
        action="store_true",
        default=False,
        help="Do not include provenance metadata in .dvc files.",
    )
    parser.add_argument(
        "--dot",
        metavar="<path>",
        help="Export DAG as GraphViz DOT format to file.",
    )
    parser.add_argument(
        "--svg",
        metavar="<path>",
        help="Export DAG as SVG to file (requires graphviz).",
    )
    parser.add_argument(
        "--mermaid",
        metavar="<path>",
        help="Export DAG as Mermaid diagram to file.",
    )
    parser.set_defaults(func=CmdRun)
