import os
import sys

from dvx.cli import completion, formatter
from dvx.cli.command import CmdBase
from dvx.cli.utils import append_doc_link
from dvx.log import logger
from dvx.ui import ui

logger = logger.getChild(__name__)


def _get_output_from_target(repo, target, rev=None):
    """Load a .dvc file and return its output(s)."""
    from dvx.dvcfile import SingleStageFile
    from dvx.repo.brancher import switch

    # Normalize path - add .dvc if not present
    if not target.endswith(".dvc"):
        target = target + ".dvc"

    path = os.path.abspath(target)

    if rev:
        with switch(repo, rev):
            dvcfile = SingleStageFile(repo, path, verify=False)
            stage = dvcfile.stage
            return stage.outs
    else:
        dvcfile = SingleStageFile(repo, path, verify=False)
        stage = dvcfile.stage
        return stage.outs


class CmdCat(CmdBase):
    def run(self):
        from dvx.exceptions import DvcException

        try:
            outs = _get_output_from_target(self.repo, self.args.target, self.args.rev)
            if not outs:
                ui.error_write(f"No outputs found in {self.args.target}")
                return 1

            for out in outs:
                if not out.hash_info or not out.hash_info.value:
                    ui.error_write(f"No hash found for {out}")
                    continue

                cache_path = out.cache_path
                if not os.path.exists(cache_path):
                    ui.error_write(f"Cache file not found: {cache_path}")
                    return 1

                # Read and output the file contents
                # Use binary mode and write to stdout directly for proper handling
                with open(cache_path, "rb") as f:
                    # Write in chunks to handle large files
                    while True:
                        chunk = f.read(65536)
                        if not chunk:
                            break
                        sys.stdout.buffer.write(chunk)

            return 0
        except DvcException as exc:
            ui.error_write(str(exc))
            return 1


def add_parser(subparsers, parent_parser):
    CAT_HELP = "Display contents of a DVC-tracked file from the cache."
    cat_parser = subparsers.add_parser(
        "cat",
        parents=[parent_parser],
        description=append_doc_link(CAT_HELP, "cat"),
        help=CAT_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    cat_parser.add_argument(
        "target",
        help="Path to .dvc file (or data file, .dvc extension is optional).",
    ).complete = completion.FILE
    cat_parser.add_argument(
        "-r",
        "--rev",
        help="Git revision (e.g. HEAD~1, branch name, commit hash).",
        metavar="<rev>",
    )
    cat_parser.set_defaults(func=CmdCat)
