"""Content diff for DVC-tracked files.

Shows actual content differences between versions of DVC-tracked files,
optionally passing content through preprocessing commands before diffing.
"""

import os
import subprocess
from typing import Optional, Tuple

import click

from dvx.log import logger
from dvx.ui import ui

logger = logger.getChild(__name__)


def _normalize_path(path: str) -> tuple[str, str]:
    """Normalize path to (data_path, dvc_path) tuple."""
    if path.endswith(os.sep):
        path = path[:-1]
    if path.endswith(".dvc"):
        dvc_path = path
        data_path = path[:-4]
    else:
        data_path = path
        dvc_path = path + ".dvc"
    return data_path, dvc_path


def _get_cache_path_for_ref(repo, dvc_path: str, ref: Optional[str]) -> Optional[str]:
    """Get cache path for a .dvc file at a specific git ref."""
    from dvx.dvcfile import SingleStageFile
    from dvx.repo.brancher import switch

    abs_path = os.path.abspath(dvc_path)

    try:
        if ref:
            with switch(repo, ref):
                dvcfile = SingleStageFile(repo, abs_path, verify=False)
                stage = dvcfile.stage
                if not stage.outs:
                    return None
                out = stage.outs[0]
                if not out.hash_info or not out.hash_info.value:
                    return None
                return out.cache_path
        else:
            dvcfile = SingleStageFile(repo, abs_path, verify=False)
            stage = dvcfile.stage
            if not stage.outs:
                return None
            out = stage.outs[0]
            if not out.hash_info or not out.hash_info.value:
                return None
            return out.cache_path
    except Exception as e:
        logger.debug(f"Failed to get cache path for {dvc_path} at {ref}: {e}")
        return None


def _run_diff(
    path1: Optional[str],
    path2: Optional[str],
    color: Optional[bool] = None,
    unified: Optional[int] = None,
    ignore_whitespace: bool = False,
) -> int:
    """Run diff on two paths."""
    args = ["diff"]

    if ignore_whitespace:
        args.append("-w")
    if unified is not None:
        args.extend(["-U", str(unified)])
    if color is True:
        args.append("--color=always")
    elif color is False:
        args.append("--color=never")

    args.append(path1 or "/dev/null")
    args.append(path2 or "/dev/null")

    result = subprocess.run(args, capture_output=False)
    return result.returncode


def _run_pipeline_diff(
    path1: Optional[str],
    path2: Optional[str],
    cmds: list[str],
    color: Optional[bool] = None,
    unified: Optional[int] = None,
    ignore_whitespace: bool = False,
    verbose: bool = False,
    shell: bool = True,
    shell_executable: Optional[str] = None,
    both: bool = False,
) -> int:
    """Run diff with preprocessing pipeline using dffs."""
    try:
        from dffs import join_pipelines
    except ImportError:
        ui.error_write(
            "Pipeline diffing requires the 'dffs' package. "
            "Install with: pip install dffs"
        )
        return 1

    diff_args = []
    if ignore_whitespace:
        diff_args.append("-w")
    if unified is not None:
        diff_args.extend(["-U", str(unified)])
    if color is True:
        diff_args.append("--color=always")
    elif color is False:
        diff_args.append("--color=never")

    cmd, *sub_cmds = cmds

    if path1 is None:
        cmds1 = ["cat /dev/null"]
    else:
        cmds1 = [f"{cmd} {path1}", *sub_cmds]

    if path2 is None:
        cmds2 = ["cat /dev/null"]
    else:
        cmds2 = [f"{cmd} {path2}", *sub_cmds]

    return join_pipelines(
        base_cmd=["diff", *diff_args],
        cmds1=cmds1,
        cmds2=cmds2,
        verbose=verbose,
        shell=shell,
        executable=shell_executable,
        both=both,
    )


def _diff_directory(path1, path2, data_path, after):
    """Compare directory contents by MD5."""
    import json
    from dvc_data.hashfile.hash import file_md5

    dir_json1 = {}
    dir_json2 = {}

    if path1 and os.path.exists(path1):
        with open(path1, "r") as f:
            obj = json.load(f)
            dir_json1 = {e["relpath"]: e["md5"] for e in obj}

    if path2:
        if path2 == data_path and after is None and os.path.isdir(path2):
            # Working tree directory - compute hashes
            for filename in os.listdir(path2):
                filepath = os.path.join(path2, filename)
                if os.path.isfile(filepath):
                    md5, _ = file_md5(filepath)
                    dir_json2[filename] = md5
        elif os.path.exists(path2):
            with open(path2, "r") as f:
                dir_json2 = {e["relpath"]: e["md5"] for e in json.load(f)}

    # Compare and print differences
    all_paths = sorted(set(dir_json1) | set(dir_json2))
    has_diff = False
    for relpath in all_paths:
        md5_1 = dir_json1.get(relpath)
        md5_2 = dir_json2.get(relpath)
        if md5_1 != md5_2:
            has_diff = True
            click.echo(f"{relpath}: {md5_1} -> {md5_2}")

    return 1 if has_diff else 0


def _show_summary(ctx, refspec, ref, targets):
    """Show summary of file/hash changes (like old dvx diff)."""
    from dvx.repo import Repo
    from dvx.commands.diff import CmdDiff

    # Parse refspec
    if refspec and ref:
        raise click.UsageError("Specify -r/--refspec or -R/--ref, not both")

    if ref:
        # -R <ref> means compare <ref>^ to <ref>
        a_rev = f"{ref}^"
        b_rev = ref
    elif refspec and ".." in refspec:
        a_rev, b_rev = refspec.split("..", 1)
    elif refspec:
        a_rev = refspec
        b_rev = None
    else:
        a_rev = None
        b_rev = None

    try:
        repo = Repo()
    except Exception as e:
        raise click.ClickException(f"Not a DVX repository: {e}")

    with repo:
        diff_result = repo.diff(a_rev, b_rev, targets=targets or None)

    CmdDiff._show_diff(diff_result)
    ctx.exit(0)


@click.command("diff")
@click.option("-b", "--both", is_flag=True, help="Merge stderr into stdout in pipeline commands.")
@click.option("-c/-C", "--color/--no-color", default=None, help="Force or prevent colorized output.")
@click.option("-r", "--refspec", help="<commit1>..<commit2> or <commit> (compare to worktree).")
@click.option("-R", "--ref", help="Shorthand for -r <ref>^..<ref> (compare commit to parent).")
@click.option("-e", "--shell-executable", help="Shell to use for executing commands.")
@click.option("-S", "--no-shell", is_flag=True, help="Don't use shell for subprocess execution.")
@click.option("-s", "--summary", is_flag=True, help="Show summary of changes (files and hashes) instead of content diff.")
@click.option("-U", "--unified", type=int, help="Number of lines of context.")
@click.option("-v", "--verbose", is_flag=True, help="Log intermediate commands to stderr.")
@click.option("-w", "--ignore-whitespace", is_flag=True, help="Ignore whitespace differences.")
@click.option("-x", "--exec-cmd", "exec_cmds", multiple=True, help="Command to execute before diffing.")
@click.argument("args", nargs=-1, metavar="[cmd...] <path>")
@click.pass_context
def diff(
    ctx,
    both: bool,
    color: Optional[bool],
    refspec: Optional[str],
    ref: Optional[str],
    shell_executable: Optional[str],
    no_shell: bool,
    summary: bool,
    unified: Optional[int],
    verbose: bool,
    ignore_whitespace: bool,
    exec_cmds: Tuple[str, ...],
    args: Tuple[str, ...],
):
    """Diff DVC-tracked files between commits.

    By default, shows actual content differences. Use -s/--summary to show
    a summary of which files changed (with hashes) instead.

    Examples:

    \b
      dvx diff data.csv
        Show content diff of data.csv between HEAD and worktree.

    \b
      dvx diff -r HEAD^..HEAD data.csv
        Show content diff between previous and current commit.

    \b
      dvx diff -s
        Show summary of all changed files with hashes.

    \b
      dvx diff -R abc123 wc -l data.csv
        Compare line count of data.csv at commit abc123 vs its parent.
    """
    from dvx.repo import Repo

    # Handle summary mode (shows file/hash changes, not content)
    if summary:
        return _show_summary(ctx, refspec, ref, args)

    # Combine exec_cmds and args
    remaining = list(exec_cmds) + list(args)

    if not remaining:
        raise click.UsageError("Must specify [cmd...] <path> (or use -s/--summary)")

    *cmds, target = remaining
    data_path, dvc_path = _normalize_path(target)

    # Parse refspec
    if refspec and ref:
        raise click.UsageError("Specify -r/--refspec or -R/--ref, not both")

    if ref:
        refspec = f"{ref}^..{ref}"
    elif not refspec:
        refspec = "HEAD"

    # Split refspec into before/after
    if ".." in refspec:
        before, after = refspec.split("..", 1)
    else:
        before = refspec
        after = None  # Compare to working tree

    # Get repo
    try:
        repo = Repo()
    except Exception as e:
        raise click.ClickException(f"Not a DVX repository: {e}")

    # Get cache paths
    path1 = _get_cache_path_for_ref(repo, dvc_path, before)
    if after is None:
        # Compare to working tree - use the actual file if it exists
        if os.path.exists(data_path):
            path2 = data_path
        else:
            path2 = _get_cache_path_for_ref(repo, dvc_path, None)
    else:
        path2 = _get_cache_path_for_ref(repo, dvc_path, after)

    if path1 is None and path2 is None:
        raise click.ClickException(f"Could not find {dvc_path} at either revision")

    # Check if it's a directory
    is_dir = (path1 and os.path.isdir(path1)) or (path2 and os.path.isdir(path2))
    if is_dir:
        ctx.exit(_diff_directory(path1, path2, data_path, after))

    # Run diff
    if cmds:
        returncode = _run_pipeline_diff(
            path1,
            path2,
            cmds,
            color=color,
            unified=unified,
            ignore_whitespace=ignore_whitespace,
            verbose=verbose,
            shell=not no_shell,
            shell_executable=shell_executable,
            both=both,
        )
    else:
        returncode = _run_diff(
            path1,
            path2,
            color=color,
            unified=unified,
            ignore_whitespace=ignore_whitespace,
        )

    ctx.exit(returncode)


# Adapter to integrate Click command with DVX's argparse-based CLI
class CmdContentDiff:
    def __init__(self, args):
        self.args = args

    def do_run(self):
        # Build argv for click from argparse args
        argv = []

        if hasattr(self.args, 'both') and self.args.both:
            argv.append('--both')
        if hasattr(self.args, 'color') and self.args.color is True:
            argv.append('--color')
        elif hasattr(self.args, 'color') and self.args.color is False:
            argv.append('--no-color')
        if hasattr(self.args, 'refspec') and self.args.refspec:
            argv.extend(['-r', self.args.refspec])
        if hasattr(self.args, 'ref') and self.args.ref:
            argv.extend(['-R', self.args.ref])
        if hasattr(self.args, 'shell_executable') and self.args.shell_executable:
            argv.extend(['-s', self.args.shell_executable])
        if hasattr(self.args, 'no_shell') and self.args.no_shell:
            argv.append('--no-shell')
        if hasattr(self.args, 'unified') and self.args.unified is not None:
            argv.extend(['-U', str(self.args.unified)])
        if hasattr(self.args, 'verbose') and self.args.verbose:
            argv.append('--verbose')
        if hasattr(self.args, 'ignore_whitespace') and self.args.ignore_whitespace:
            argv.append('--ignore-whitespace')
        if hasattr(self.args, 'exec_cmds') and self.args.exec_cmds:
            for cmd in self.args.exec_cmds:
                argv.extend(['-x', cmd])
        if hasattr(self.args, 'args') and self.args.args:
            argv.extend(self.args.args)

        try:
            xdiff(argv, standalone_mode=False)
            return 0
        except click.ClickException as e:
            e.show()
            return 1
        except SystemExit as e:
            return e.code if isinstance(e.code, int) else 0


def add_parser(subparsers, parent_parser):
    from dvx.cli import formatter
    from dvx.cli.utils import append_doc_link

    DIFF_HELP = (
        "Show content differences in a DVC-tracked file between commits, "
        "optionally passing through preprocessing commands."
    )

    diff_parser = subparsers.add_parser(
        "xdiff",
        parents=[parent_parser],
        description=append_doc_link(DIFF_HELP, "xdiff"),
        help=DIFF_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )

    diff_parser.add_argument(
        "args",
        nargs="*",
        metavar="[cmd...] <path>",
        help="Optional preprocessing command(s) followed by target path.",
    )

    diff_parser.add_argument(
        "-r", "--refspec",
        metavar="<refspec>",
        help="Commit range: <commit1>..<commit2> or single <commit> (vs worktree).",
    )

    diff_parser.add_argument(
        "-R", "--ref",
        metavar="<ref>",
        help="Shorthand for -r <ref>^..<ref> (compare commit to its parent).",
    )

    diff_parser.add_argument(
        "-x", "--exec-cmd",
        action="append",
        dest="exec_cmds",
        metavar="<cmd>",
        help="Command to execute before diffing (can be repeated).",
    )

    diff_parser.add_argument(
        "-c", "--color",
        dest="color",
        default=None,
        action="store_true",
        help="Force colorized output.",
    )

    diff_parser.add_argument(
        "-C", "--no-color",
        dest="color",
        action="store_false",
        help="Disable colorized output.",
    )

    diff_parser.add_argument(
        "-U", "--unified",
        type=int,
        metavar="<n>",
        help="Number of lines of context.",
    )

    diff_parser.add_argument(
        "-w", "--ignore-whitespace",
        action="store_true",
        help="Ignore whitespace differences.",
    )

    diff_parser.add_argument(
        "-b", "--both",
        action="store_true",
        help="Merge stderr into stdout in pipeline commands.",
    )

    diff_parser.add_argument(
        "-s", "--shell-executable",
        metavar="<shell>",
        help="Shell to use for executing commands.",
    )

    diff_parser.add_argument(
        "-S", "--no-shell",
        action="store_true",
        help="Don't use shell for subprocess execution.",
    )

    diff_parser.set_defaults(func=CmdContentDiff)
