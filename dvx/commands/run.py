"""DVX run command - execute artifact computations from .dvc files."""

import sys
from pathlib import Path

from dvx.cli import formatter
from dvx.cli.command import CmdBase
from dvx.log import logger

logger = logger.getChild(__name__)


class CmdRun(CmdBase):
    def run(self):
        from dvx.run.executor import ExecutionConfig, run

        args = self.args

        # Find targets
        targets = list(args.targets) if args.targets else []
        if not targets:
            # Default: find all .dvc files in current directory
            targets = list(Path(".").glob("*.dvc"))
            if not targets:
                logger.error("No .dvc files found in current directory")
                logger.error("Specify targets or run from a directory with .dvc files")
                return 1

        config = ExecutionConfig(
            max_workers=args.jobs,
            dry_run=args.dry_run,
            force=args.force,
            force_patterns=list(args.force_upstream) if args.force_upstream else [],
            cached_patterns=list(args.cached) if args.cached else [],
            provenance=not args.no_provenance,
            verbose=args.verbose,
        )

        try:
            results = run(targets, config, output=sys.stderr)

            # Print summary
            total = len(results)
            executed = sum(1 for r in results if r.success and not r.skipped)
            skipped = sum(1 for r in results if r.skipped)
            failed = sum(1 for r in results if not r.success)

            logger.info("")
            logger.info("Summary:")
            logger.info("  Total: %s", total)
            logger.info("  Executed: %s", executed)
            logger.info("  Skipped: %s", skipped)
            if failed:
                logger.info("  Failed: %s", failed)
                return 1

            return 0

        except Exception as e:
            logger.exception(str(e))
            return 1


def add_parser(subparsers, parent_parser):
    RUN_HELP = "Execute artifact computations from .dvc files."

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
        type=Path,
        help=".dvc files or output paths to run (runs all *.dvc if not specified).",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        default=False,
        help="Show execution plan without running.",
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
        "--force",
        action="store_true",
        default=False,
        help="Force re-run all computations (ignore freshness).",
    )
    parser.add_argument(
        "--force-upstream",
        action="append",
        metavar="<pattern>",
        help="Force re-run upstream artifacts matching pattern.",
    )
    parser.add_argument(
        "--cached",
        action="append",
        metavar="<pattern>",
        help="Use cached value for artifacts matching pattern.",
    )
    parser.add_argument(
        "--no-provenance",
        action="store_true",
        default=False,
        help="Do not include provenance metadata in .dvc files.",
    )
    parser.set_defaults(func=CmdRun)
