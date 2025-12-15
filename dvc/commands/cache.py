import argparse
import os

from dvc.cli import completion, formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.commands.config import CmdConfig
from dvc.ui import ui


class CmdCacheDir(CmdConfig):
    def run(self):
        if self.args.value is None and not self.args.unset:
            from dvc.config import ConfigError

            if self.args.level:
                conf = self.config.read(level=self.args.level)
            else:
                # Use merged config with default values
                conf = self.config
            try:
                self._check(conf, False, "cache", "dir")
                path = conf["cache"]["dir"]
            except ConfigError:
                if not self.config.dvc_dir or self.args.level:
                    raise
                path = os.path.join(self.config.dvc_dir, "cache")
            ui.write(path)
            return 0
        with self.config.edit(level=self.args.level) as conf:
            if self.args.unset:
                self._check(conf, False, "cache", "dir")
                del conf["cache"]["dir"]
            else:
                self._check(conf, False, "cache")
                conf["cache"]["dir"] = self.args.value
        return 0


class CmdCacheMigrate(CmdBase):
    def run(self):
        from dvc.cachemgr import migrate_2_to_3
        from dvc.repo.commit import commit_2_to_3

        migrate_2_to_3(self.repo, dry=self.args.dry)
        if self.args.dvc_files:
            commit_2_to_3(self.repo, dry=self.args.dry)
        return 0


def _get_output_from_target(repo, target, rev=None):
    """Load a .dvc file and return its output(s)."""
    from dvc.dvcfile import SingleStageFile
    from dvc.repo.brancher import switch

    # Normalize path - add .dvc if not present
    if not target.endswith(".dvc"):
        target = target + ".dvc"

    path = os.path.abspath(target)

    if rev:
        # For git refs, we need to get the file content from git
        with switch(repo, rev):
            dvcfile = SingleStageFile(repo, path, verify=False)
            stage = dvcfile.stage
            return stage.outs
    else:
        dvcfile = SingleStageFile(repo, path, verify=False)
        stage = dvcfile.stage
        return stage.outs


class CmdCachePath(CmdBase):
    def run(self):
        from dvc.exceptions import DvcException

        try:
            outs = _get_output_from_target(self.repo, self.args.target, self.args.rev)
            if not outs:
                ui.error_write(f"No outputs found in {self.args.target}")
                return 1

            for out in outs:
                if not out.hash_info or not out.hash_info.value:
                    ui.error_write(f"No hash found for {out}")
                    continue

                if self.args.remote:
                    # Get remote blob URL
                    remote_odb = self.repo.cloud.get_remote_odb(
                        name=self.args.remote, hash_name=out.hash_name
                    )
                    path = remote_odb.oid_to_path(out.hash_info.value)
                    url = remote_odb.fs.unstrip_protocol(path)
                    ui.write(url)
                else:
                    # Get local cache path
                    cache_path = out.cache_path
                    if self.args.relative:
                        cache_path = os.path.relpath(cache_path)
                    ui.write(cache_path)

            return 0
        except DvcException as exc:
            ui.error_write(str(exc))
            return 1


class CmdCacheMd5(CmdBase):
    def run(self):
        from dvc.exceptions import DvcException

        try:
            outs = _get_output_from_target(self.repo, self.args.target, self.args.rev)
            if not outs:
                ui.error_write(f"No outputs found in {self.args.target}")
                return 1

            for out in outs:
                if not out.hash_info or not out.hash_info.value:
                    ui.error_write(f"No hash found for {out}")
                    continue

                ui.write(out.hash_info.value)

            return 0
        except DvcException as exc:
            ui.error_write(str(exc))
            return 1


def add_parser(subparsers, parent_parser):
    from dvc.commands.config import parent_config_parser

    CACHE_HELP = "Manage cache settings."

    cache_parser = subparsers.add_parser(
        "cache",
        parents=[parent_parser],
        description=append_doc_link(CACHE_HELP, "cache"),
        help=CACHE_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )

    cache_subparsers = cache_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc cache CMD --help` for command-specific help.",
        required=True,
    )

    parent_cache_config_parser = argparse.ArgumentParser(
        add_help=False, parents=[parent_config_parser]
    )
    CACHE_DIR_HELP = "Configure cache directory location."

    cache_dir_parser = cache_subparsers.add_parser(
        "dir",
        parents=[parent_parser, parent_cache_config_parser],
        description=append_doc_link(CACHE_HELP, "cache/dir"),
        help=CACHE_DIR_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    cache_dir_parser.add_argument(
        "-u",
        "--unset",
        default=False,
        action="store_true",
        help="Unset option.",
    )
    cache_dir_parser.add_argument(
        "value",
        help=(
            "Path to cache directory. Relative paths are resolved relative "
            "to the current directory and saved to config relative to the "
            "config file location. If no path is provided, it returns the "
            "current cache directory."
        ),
        nargs="?",
    ).complete = completion.DIR
    cache_dir_parser.set_defaults(func=CmdCacheDir)

    CACHE_MIGRATE_HELP = "Migrate cached files to the DVC 3.0 cache location."
    cache_migrate_parser = cache_subparsers.add_parser(
        "migrate",
        parents=[parent_parser],
        description=append_doc_link(CACHE_HELP, "cache/migrate"),
        help=CACHE_MIGRATE_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    cache_migrate_parser.add_argument(
        "--dvc-files",
        help=("Migrate entries in all existing DVC files in the repository to the DVC 3.0 format."),
        action="store_true",
    )
    cache_migrate_parser.add_argument(
        "--dry",
        help=("Only print actions which would be taken without actually migrating any data."),
        action="store_true",
    )
    cache_migrate_parser.set_defaults(func=CmdCacheMigrate)

    # cache path subcommand
    CACHE_PATH_HELP = "Get the cache path for a DVC-tracked file."
    cache_path_parser = cache_subparsers.add_parser(
        "path",
        parents=[parent_parser],
        description=append_doc_link(CACHE_PATH_HELP, "cache/path"),
        help=CACHE_PATH_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    cache_path_parser.add_argument(
        "target",
        help="Path to .dvc file (or data file, .dvc extension is optional).",
    ).complete = completion.FILE
    cache_path_parser.add_argument(
        "-r",
        "--rev",
        help="Git revision (e.g. HEAD, branch name, commit hash).",
        metavar="<rev>",
    )
    cache_path_parser.add_argument(
        "--remote",
        help="Get the remote blob URL instead of local cache path.",
        metavar="<name>",
    )
    cache_path_parser.add_argument(
        "--relative",
        action="store_true",
        help="Output path relative to current directory.",
    )
    cache_path_parser.set_defaults(func=CmdCachePath)

    # cache md5 subcommand
    CACHE_MD5_HELP = "Get the MD5 hash for a DVC-tracked file."
    cache_md5_parser = cache_subparsers.add_parser(
        "md5",
        parents=[parent_parser],
        description=append_doc_link(CACHE_MD5_HELP, "cache/md5"),
        help=CACHE_MD5_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    cache_md5_parser.add_argument(
        "target",
        help="Path to .dvc file (or data file, .dvc extension is optional).",
    ).complete = completion.FILE
    cache_md5_parser.add_argument(
        "-r",
        "--rev",
        help="Git revision (e.g. HEAD, branch name, commit hash).",
        metavar="<rev>",
    )
    cache_md5_parser.set_defaults(func=CmdCacheMd5)
