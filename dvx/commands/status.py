"""DVX status command - check freshness status of artifacts."""

import sys
from functools import partial
from pathlib import Path

from dvx.cli import formatter
from dvx.cli.command import CmdBase
from dvx.log import logger
from dvx.ui import ui
from dvx.utils.threadpool import ThreadPoolExecutor

logger = logger.getChild(__name__)


def _check_one_target(target, with_deps: bool) -> dict:
    """Check freshness of a single target. Runs in thread pool."""
    from dvx.run.dvc_files import get_dvc_file_path, is_output_fresh, read_dvc_file

    target_path = Path(target)
    # If target already ends in .dvc, use it directly; otherwise add .dvc suffix
    if target_path.suffix == ".dvc":
        dvc_path = target_path
    else:
        dvc_path = get_dvc_file_path(target_path)

    if not dvc_path.exists():
        return {
            "path": str(target),
            "status": "missing",
            "reason": "dvc file not found",
        }

    dvc_data = read_dvc_file(dvc_path)
    if not dvc_data:
        return {
            "path": str(target),
            "status": "error",
            "reason": "failed to read dvc file",
        }

    # Get output path from .dvc file
    output_path = dvc_path.parent / dvc_data.path

    # Check freshness (pass pre-parsed info to avoid re-reading .dvc file)
    is_fresh, reason = is_output_fresh(output_path, check_deps=with_deps, info=dvc_data)
    return {
        "path": str(dvc_path),
        "status": "fresh" if is_fresh else "stale",
        "reason": reason if not is_fresh else None,
    }


class CmdStatus(CmdBase):
    def run(self):
        args = self.args

        # Find targets
        targets = list(args.targets) if args.targets else []
        if not targets:
            # Default: find all .dvc files recursively (like git status)
            # Filter to files only (exclude .dvc directory itself)
            targets = [p for p in Path(".").glob("**/*.dvc") if p.is_file()]
            if not targets:
                logger.warning("No .dvc files found")
                return 0

        results = []
        total = len(targets)
        completed = 0

        # Use thread pool for parallel checking
        max_workers = getattr(args, "jobs", None)  # None = ThreadPoolExecutor default
        show_progress = not args.quiet and not args.json and sys.stderr.isatty()

        with ThreadPoolExecutor(max_workers=max_workers, cancel_on_error=False) as executor:
            if show_progress:
                ui.write(f"Checking {total} artifacts ({executor._max_workers} workers)...", stderr=True)
            # Use imap_unordered for lazy evaluation and better memory usage
            check_fn = partial(_check_one_target, with_deps=args.with_deps)
            for result in executor.imap_unordered(check_fn, targets):
                results.append(result)
                completed += 1

                if show_progress:
                    # Simple progress: overwrite line with count
                    pct = int(100 * completed / total)
                    ui.write(f"\rChecking artifacts: {completed}/{total} ({pct}%)", end="", stderr=True)

        if show_progress:
            ui.write("", stderr=True)  # Newline after progress

        # Sort results by path for consistent output
        results.sort(key=lambda r: r["path"])

        # Count statuses
        stale_count = sum(1 for r in results if r["status"] == "stale")

        # Output results
        if args.json:
            ui.write_json(results)
        else:
            fresh_count = sum(1 for r in results if r["status"] == "fresh")
            other_count = len(results) - fresh_count - stale_count

            # By default, only show non-fresh files (like git status hides unchanged files)
            show_all = getattr(args, "verbose", False)

            for r in results:
                # Skip fresh files unless -v/--verbose
                if r["status"] == "fresh" and not show_all:
                    continue

                status_icon = {
                    "fresh": "✓",
                    "stale": "✗",
                    "missing": "?",
                    "error": "!",
                }.get(r["status"], "?")

                line = f"{status_icon} {r['path']}"
                if r.get("reason"):
                    line += f" ({r['reason']})"
                ui.write(line)

            if not args.quiet:
                ui.write("")
                ui.write(f"Fresh: {fresh_count}, Stale: {stale_count}", end="")
                if other_count:
                    ui.write(f", Other: {other_count}")
                else:
                    ui.write("")

        # Return 1 if any stale, 0 if all fresh
        return 1 if stale_count > 0 else 0


def add_parser(subparsers, parent_parser):
    STATUS_HELP = "Check freshness status of artifacts."

    parser = subparsers.add_parser(
        "status",
        parents=[parent_parser],
        description=STATUS_HELP,
        help=STATUS_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "targets",
        nargs="*",
        type=Path,
        help=".dvc files or output paths to check (checks all *.dvc if not specified).",
    )
    parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="Check upstream dependencies as well.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output results as JSON.",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=None,
        metavar="N",
        help="Number of parallel workers (default: CPU count).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Show all files including fresh (default: only stale/missing).",
    )
    parser.set_defaults(func=CmdStatus)
