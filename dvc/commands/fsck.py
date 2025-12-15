"""DVX fsck command - verify and rebuild artifact hash cache."""

import sys
from pathlib import Path

from dvc.cli import formatter
from dvc.cli.command import CmdBase
from dvc.log import logger
from dvc.ui import ui
from dvc.utils.threadpool import ThreadPoolExecutor

logger = logger.getChild(__name__)


def _hash_one_artifact(dvc_path: Path) -> dict:
    """Hash a single artifact and update the cache. Runs in thread pool."""
    from dvc.run.dvc_files import read_dvc_file
    from dvc.run.hash import compute_md5
    from dvc.run.status import get_artifact_hash_cached

    dvc_data = read_dvc_file(dvc_path)
    if not dvc_data:
        return {
            "path": str(dvc_path),
            "status": "error",
            "reason": "failed to read dvc file",
        }

    # Get output path from .dvc file
    output_path = dvc_path.parent / dvc_data.path

    if not output_path.exists():
        return {
            "path": str(dvc_path),
            "status": "missing",
            "reason": "output file not found",
        }

    # Compute hash (this will populate the cache)
    try:
        actual_hash, _size, was_cached = get_artifact_hash_cached(output_path, compute_md5)
        expected_hash = dvc_data.md5

        if actual_hash == expected_hash:
            return {
                "path": str(dvc_path),
                "status": "ok",
                "hash": actual_hash,
                "cached": was_cached,
            }
        return {
            "path": str(dvc_path),
            "status": "mismatch",
            "expected": expected_hash,
            "actual": actual_hash,
        }
    except Exception as e:
        return {
            "path": str(dvc_path),
            "status": "error",
            "reason": str(e),
        }


class CmdFsck(CmdBase):
    def run(self):
        from dvc.run.status import ArtifactStatusDB

        args = self.args

        # Find targets
        targets = list(args.targets) if args.targets else []
        if not targets:
            # Default: find all .dvc files recursively
            targets = list(Path(".").glob("**/*.dvc"))
            if not targets:
                logger.warning("No .dvc files found")
                return 0

        # Filter to only .dvc files
        dvc_files = [Path(t) for t in targets if str(t).endswith(".dvc")]
        if not dvc_files:
            logger.warning("No .dvc files in targets")
            return 0

        total = len(dvc_files)
        completed = 0
        results = []

        # Use thread pool for parallel hashing
        max_workers = getattr(args, "jobs", None)  # None = ThreadPoolExecutor default
        show_progress = not args.quiet and sys.stderr.isatty()

        if args.clear_cache:
            db = ArtifactStatusDB()
            db.clear()
            ui.write("Cleared hash cache", stderr=True)

        with ThreadPoolExecutor(max_workers=max_workers, cancel_on_error=False) as executor:
            if show_progress:
                ui.write(
                    f"Verifying {total} artifacts ({executor._max_workers} workers)...", stderr=True
                )
            # Use imap_unordered for lazy evaluation and better memory usage
            for result in executor.imap_unordered(_hash_one_artifact, dvc_files):
                results.append(result)
                completed += 1

                if show_progress:
                    pct = int(100 * completed / total)
                    ui.write(f"\rVerifying: {completed}/{total} ({pct}%)", end="", stderr=True)

        if show_progress:
            ui.write("", stderr=True)  # Newline after progress

        # Sort results by path
        results.sort(key=lambda r: r["path"])

        # Count and report
        ok_count = sum(1 for r in results if r["status"] == "ok")
        mismatch_count = sum(1 for r in results if r["status"] == "mismatch")
        missing_count = sum(1 for r in results if r["status"] == "missing")
        error_count = sum(1 for r in results if r["status"] == "error")

        if args.json:
            ui.write_json(results)
        else:
            # Only show problems by default
            for r in results:
                if r["status"] == "ok" and not args.verbose:
                    continue

                status_icon = {
                    "ok": "✓",
                    "mismatch": "✗",
                    "missing": "?",
                    "error": "!",
                }.get(r["status"], "?")

                line = f"{status_icon} {r['path']}"
                if r["status"] == "mismatch":
                    line += f" (expected {r['expected'][:8]}..., got {r['actual'][:8]}...)"
                elif r.get("reason"):
                    line += f" ({r['reason']})"
                ui.write(line)

            if not args.quiet:
                ui.write("")
                ui.write(
                    f"OK: {ok_count}, Mismatch: {mismatch_count}, Missing: {missing_count}, Error: {error_count}"
                )

        # Return 1 if any problems
        return 1 if (mismatch_count + error_count) > 0 else 0


def add_parser(subparsers, parent_parser):
    FSCK_HELP = "Verify artifact hashes and rebuild the hash cache."

    parser = subparsers.add_parser(
        "fsck",
        parents=[parent_parser],
        description=FSCK_HELP,
        help=FSCK_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "targets",
        nargs="*",
        type=Path,
        help=".dvc files to verify (defaults to all **/*.dvc).",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        default=False,
        help="Clear the hash cache before verifying (forces full rehash).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output results as JSON.",
    )
    parser.set_defaults(func=CmdFsck)
