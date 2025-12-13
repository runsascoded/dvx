"""Click-based CLI for DVX."""

import logging
import os
import sys
from functools import wraps

import click

from dvx import __version__
from dvx.log import logger

logger = logger.getChild(__name__)


class DvxContext:
    """Context object passed to all commands."""

    def __init__(self, cd: str = ".", quiet: int = 0, verbose: int = 0, wait_for_lock: bool = False):
        self.cd = cd
        self.quiet = quiet
        self.verbose = verbose
        self.wait_for_lock = wait_for_lock
        self._repo = None

    @property
    def repo(self):
        """Lazily initialize repo."""
        if self._repo is None:
            from dvx.repo import Repo
            self._repo = Repo(wait_for_lock=self.wait_for_lock)
        return self._repo


pass_context = click.make_pass_decorator(DvxContext, ensure=True)


def with_repo(f):
    """Decorator that ensures command runs within repo context."""
    @wraps(f)
    @pass_context
    def wrapper(ctx, *args, **kwargs):
        os.chdir(ctx.cd)
        with ctx.repo:
            return f(ctx, *args, **kwargs)
    return wrapper


def without_repo(f):
    """Decorator for commands that don't need a repo."""
    @wraps(f)
    @pass_context
    def wrapper(ctx, *args, **kwargs):
        os.chdir(ctx.cd)
        return f(ctx, *args, **kwargs)
    return wrapper


@click.group(invoke_without_command=True)
@click.option("-C", "--cd", default=".", help="Change to directory before executing.", metavar="<path>")
@click.option("-q", "--quiet", count=True, help="Be quiet.")
@click.option("-v", "--verbose", count=True, help="Be verbose.")
@click.option("--wait-for-lock", is_flag=True, help="Wait for lock instead of failing immediately.")
@click.version_option(__version__, "-V", "--version", prog_name="dvx")
@click.pass_context
def cli(ctx, cd: str, quiet: int, verbose: int, wait_for_lock: bool):
    """DVX - Minimal Data Version Control.

    Content-addressable storage for data files with Git integration.
    """
    ctx.ensure_object(DvxContext)
    ctx.obj = DvxContext(cd=cd, quiet=quiet, verbose=verbose, wait_for_lock=wait_for_lock)

    # Set up logging based on verbosity
    level = None
    if quiet:
        level = logging.CRITICAL
    elif verbose == 1:
        level = logging.DEBUG
    elif verbose > 1:
        level = logging.TRACE  # type: ignore

    if level is not None:
        from dvx.logger import set_loggers_level
        ctx.with_resource(set_loggers_level(level))

    # Enable UI output
    if sys.stdout and not sys.stdout.closed and not quiet:
        from dvx.ui import ui
        ui.enable()

    # Show help if no command
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# Import and register all commands
def register_commands():
    """Register all command modules."""

    # Each module should have a `cli_commands` list or a `register` function
    # For now, we'll register them individually as we convert them


# Simple commands converted to Click

@cli.command("version")
@click.pass_context
def version_cmd(ctx):
    """Display the DVX version and system/environment information."""
    from dvx.info import get_dvc_info
    from dvx.ui import ui

    os.chdir(ctx.obj.cd)
    dvc_info = get_dvc_info()
    ui.write(dvc_info, force=True)


cli.add_command(version_cmd, name="doctor")  # Alias


@cli.command("root")
@click.pass_context
def root_cmd(ctx):
    """Return the relative path to the root of the DVC project."""
    from dvx.repo import Repo
    from dvx.ui import ui
    from dvx.utils import relpath

    os.chdir(ctx.obj.cd)
    ui.write(relpath(Repo.find_root()))


@cli.command("init")
@click.option("--no-scm", is_flag=True, help="Initialize without SCM.")
@click.option("-f", "--force", is_flag=True, help="Force initialization.")
@click.option("--subdir", is_flag=True, help="Initialize in a subdirectory of a Git repo.")
@click.argument("directory", default=".", required=False)
@click.pass_context
def init_cmd(ctx, no_scm: bool, force: bool, subdir: bool, directory: str):
    """Initialize a DVC repository."""
    from dvx.exceptions import InitError
    from dvx.repo import Repo
    from dvx.ui import ui

    os.chdir(ctx.obj.cd)
    try:
        with Repo.init(directory, no_scm=no_scm, force=force, subdir=subdir) as repo:
            ui.write(f"Initialized DVC repository in {repo.root_dir}")
    except InitError as e:
        raise click.ClickException(str(e))


@cli.command("destroy")
@click.option("-f", "--force", is_flag=True, help="Force destruction without confirmation.")
@click.pass_context
def destroy_cmd(ctx, force: bool):
    """Remove DVC files and directories."""
    from dvx.repo import Repo

    os.chdir(ctx.obj.cd)

    if not force:
        if not click.confirm("This will destroy all DVC files. Are you sure?"):
            raise click.Abort()

    repo = Repo()
    with repo:
        repo.destroy()


@cli.command("unprotect")
@click.argument("targets", nargs=-1, required=True)
@click.pass_context
def unprotect_cmd(ctx, targets):
    """Unprotect tracked files or directories."""
    from dvx.repo import Repo

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        for target in targets:
            repo.unprotect(target)


@cli.command("add")
@click.argument("targets", nargs=-1, required=True)
@click.option("--no-commit", is_flag=True, help="Don't put files/directories into cache.")
@click.option("--glob", is_flag=True, help="Allow targets containing shell-style wildcards.")
@click.option("-o", "--out", metavar="<path>", help="Destination path to put files to.")
@click.option("--to-remote", is_flag=True, help="Upload directly to remote storage.")
@click.option("-r", "--remote", metavar="<name>", help="Remote storage to upload to.")
@click.option("--remote-jobs", type=int, metavar="<n>", help="Number of jobs for remote upload.")
@click.option("-f", "--force", is_flag=True, help="Override local file or folder if exists.")
@click.option("--no-relink", "relink", flag_value=False, default=True, help="Don't recreate links from cache.")
@click.pass_context
def add_cmd(ctx, targets, no_commit, glob, out, to_remote, remote, remote_jobs, force, relink):
    """Track data files or directories with DVC."""
    from dvx.exceptions import DvcException
    from dvx.repo import Repo

    os.chdir(ctx.obj.cd)

    # Validation
    if to_remote or out:
        if len(targets) != 1:
            raise click.ClickException("--to-remote/--out can't be used with multiple targets")
        if glob:
            raise click.ClickException("--glob can't be used with --to-remote/--out")
        if no_commit:
            raise click.ClickException("--no-commit can't be used with --to-remote/--out")
    else:
        if remote:
            raise click.ClickException("--remote can't be used without --to-remote")
        if remote_jobs:
            raise click.ClickException("--remote-jobs can't be used without --to-remote")

    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    try:
        with repo:
            repo.add(
                targets,
                no_commit=no_commit,
                glob=glob,
                out=out,
                remote=remote,
                to_remote=to_remote,
                remote_jobs=remote_jobs,
                force=force,
                relink=relink,
            )
    except (FileNotFoundError, DvcException) as e:
        raise click.ClickException(str(e))


@cli.command("checkout")
@click.argument("targets", nargs=-1)
@click.option("--summary", is_flag=True, help="Show summary of the changes.")
@click.option("-d", "--with-deps", is_flag=True, help="Checkout all dependencies of targets.")
@click.option("-R", "--recursive", is_flag=True, help="Checkout all subdirectories.")
@click.option("-f", "--force", is_flag=True, help="Do not prompt when removing files.")
@click.option("--relink", is_flag=True, help="Recreate links or copies from cache.")
@click.option("--allow-missing", is_flag=True, help="Ignore errors for missing files.")
@click.pass_context
def checkout_cmd(ctx, targets, summary, with_deps, recursive, force, relink, allow_missing):
    """Checkout data files from cache."""
    from dvx.exceptions import CheckoutError
    from dvx.repo import Repo
    from dvx.ui import ui
    from dvx.utils.humanize import get_summary

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)

    exc = None
    with repo:
        try:
            result = repo.checkout(
                targets=targets,
                with_deps=with_deps,
                force=force,
                relink=relink,
                recursive=recursive,
                allow_missing=allow_missing,
            )
        except CheckoutError as e:
            exc = e
            result = e.result

    if summary:
        stats = result.get("stats", {})
        msg = get_summary(stats.items()) if stats else "No changes."
        ui.write(msg)
    else:
        colors = {"modified": "yellow", "added": "green", "deleted": "red"}
        for state, color in colors.items():
            for entry in result.get(state, []):
                ui.write(f"[{color}]{state[0].upper()}", entry, styled=True, sep="\t")

    if exc:
        raise click.ClickException(str(exc))

    if relink:
        ui.write("Relinked successfully")


@cli.command("status")
@click.argument("targets", nargs=-1)
@click.option("-d", "--with-deps", is_flag=True, help="Check upstream dependencies.")
@click.option("--json", "as_json", is_flag=True, help="Output results as JSON.")
@click.pass_context
def status_cmd(ctx, targets, with_deps, as_json):
    """Check freshness status of artifacts."""
    from functools import partial
    from pathlib import Path

    from dvx.commands.status import _check_one_target
    from dvx.ui import ui
    from dvx.utils.threadpool import ThreadPoolExecutor

    os.chdir(ctx.obj.cd)

    # Find targets
    targets = list(targets) if targets else list(Path(".").glob("**/*.dvc"))
    if not targets:
        ui.write("No .dvc files found")
        return

    results = []
    with ThreadPoolExecutor(cancel_on_error=False) as executor:
        check_fn = partial(_check_one_target, with_deps=with_deps)
        for result in executor.imap_unordered(check_fn, targets):
            results.append(result)

    results.sort(key=lambda r: r["path"])
    stale_count = sum(1 for r in results if r["status"] == "stale")

    if as_json:
        ui.write_json(results)
    else:
        for r in results:
            icon = {"fresh": "✓", "stale": "✗", "missing": "?", "error": "!"}.get(r["status"], "?")
            line = f"{icon} {r['path']}"
            if r.get("reason"):
                line += f" ({r['reason']})"
            ui.write(line)

        fresh_count = sum(1 for r in results if r["status"] == "fresh")
        ui.write(f"\nFresh: {fresh_count}, Stale: {stale_count}")

    ctx.exit(1 if stale_count > 0 else 0)


@cli.command("remove")
@click.argument("targets", nargs=-1, required=True)
@click.option("-f", "--force", is_flag=True, help="Force removal.")
@click.option("--outs", is_flag=True, help="Remove outputs as well.")
@click.pass_context
def remove_cmd(ctx, targets, force, outs):
    """Remove DVC files and optionally their outputs."""
    from dvx.repo import Repo

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        repo.remove(targets, force=force, outs=outs)


@cli.command("move")
@click.argument("src")
@click.argument("dst")
@click.pass_context
def move_cmd(ctx, src, dst):
    """Rename or move a DVC-tracked file or directory."""
    from dvx.repo import Repo

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        repo.move(src, dst)


@cli.command("commit")
@click.argument("targets", nargs=-1)
@click.option("-f", "--force", is_flag=True, help="Force commit even if checksums differ.")
@click.option("-d", "--with-deps", is_flag=True, help="Commit dependencies as well.")
@click.option("-R", "--recursive", is_flag=True, help="Commit recursively.")
@click.pass_context
def commit_cmd(ctx, targets, force, with_deps, recursive):
    """Save changed data to cache and update DVC files."""
    from dvx.repo import Repo

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        repo.commit(
            targets,
            force=force,
            with_deps=with_deps,
            recursive=recursive,
        )


@cli.command("diff")
@click.argument("a_rev", required=False)
@click.argument("b_rev", required=False)
@click.option("--targets", "-t", multiple=True, help="Specific targets to diff.")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.option("--md", "as_md", is_flag=True, help="Output as Markdown.")
@click.option("--hide-missing", is_flag=True, help="Hide missing entries.")
@click.pass_context
def diff_cmd(ctx, a_rev, b_rev, targets, as_json, as_md, hide_missing):
    """Show changes between commits or cache and workspace."""
    from dvx.compare import show_diff
    from dvx.repo import Repo
    from dvx.ui import ui

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        diff_result = repo.diff(a_rev, b_rev, targets=targets or None)

    if as_json:
        ui.write_json(diff_result)
    elif as_md:
        from dvx.compare import show_md
        show_md(diff_result)
    else:
        show_diff(diff_result, hide_missing=hide_missing)


@cli.command("gc")
@click.option("-w", "--workspace", is_flag=True, help="Keep only data used in workspace.")
@click.option("-a", "--all-branches", is_flag=True, help="Keep data for all Git branches.")
@click.option("-T", "--all-tags", is_flag=True, help="Keep data for all Git tags.")
@click.option("-A", "--all-commits", is_flag=True, help="Keep data for all Git commits.")
@click.option("--all-experiments", is_flag=True, help="Keep data for all experiments.")
@click.option("-c", "--cloud", is_flag=True, help="Clean remote storage.")
@click.option("-r", "--remote", metavar="<name>", help="Remote to clean.")
@click.option("-f", "--force", is_flag=True, help="Force garbage collection.")
@click.option("-j", "--jobs", type=int, metavar="<n>", help="Number of parallel jobs.")
@click.option("-p", "--projects", multiple=True, metavar="<path>", help="Additional projects to consider.")
@click.option("-n", "--dry", is_flag=True, help="Dry run - show what would be removed.")
@click.pass_context
def gc_cmd(ctx, workspace, all_branches, all_tags, all_commits, all_experiments,
           cloud, remote, force, jobs, projects, dry):
    """Garbage collect unused data from cache."""
    from dvx.repo import Repo

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        repo.gc(
            workspace=workspace,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            all_experiments=all_experiments,
            cloud=cloud,
            remote=remote,
            force=force,
            jobs=jobs,
            repos=projects,
            dry=dry,
        )


# Data sync commands: pull, push, fetch
@cli.command("pull")
@click.argument("targets", nargs=-1)
@click.option("-r", "--remote", metavar="<name>", help="Remote storage to pull from.")
@click.option("-a", "--all-branches", is_flag=True, help="Pull for all branches.")
@click.option("-T", "--all-tags", is_flag=True, help="Pull for all tags.")
@click.option("-A", "--all-commits", is_flag=True, help="Pull for all commits.")
@click.option("-d", "--with-deps", is_flag=True, help="Pull dependencies as well.")
@click.option("-R", "--recursive", is_flag=True, help="Pull recursively.")
@click.option("-f", "--force", is_flag=True, help="Force pull.")
@click.option("-j", "--jobs", type=int, metavar="<n>", help="Number of parallel jobs.")
@click.option("--glob", is_flag=True, help="Allow glob patterns in targets.")
@click.option("--allow-missing", is_flag=True, help="Ignore missing files.")
@click.pass_context
def pull_cmd(ctx, targets, remote, all_branches, all_tags, all_commits, with_deps,
             recursive, force, jobs, glob, allow_missing):
    """Download tracked data from remote storage."""
    from dvx.repo import Repo
    from dvx.ui import ui

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        result = repo.pull(
            targets=targets,
            remote=remote,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            with_deps=with_deps,
            recursive=recursive,
            force=force,
            jobs=jobs,
            glob=glob,
            allow_missing=allow_missing,
        )
        ui.write(f"Downloaded {result.get('fetched', 0)} file(s)")


@cli.command("push")
@click.argument("targets", nargs=-1)
@click.option("-r", "--remote", metavar="<name>", help="Remote storage to push to.")
@click.option("-a", "--all-branches", is_flag=True, help="Push for all branches.")
@click.option("-T", "--all-tags", is_flag=True, help="Push for all tags.")
@click.option("-A", "--all-commits", is_flag=True, help="Push for all commits.")
@click.option("-d", "--with-deps", is_flag=True, help="Push dependencies as well.")
@click.option("-R", "--recursive", is_flag=True, help="Push recursively.")
@click.option("-j", "--jobs", type=int, metavar="<n>", help="Number of parallel jobs.")
@click.option("--glob", is_flag=True, help="Allow glob patterns in targets.")
@click.pass_context
def push_cmd(ctx, targets, remote, all_branches, all_tags, all_commits, with_deps,
             recursive, jobs, glob):
    """Upload tracked data to remote storage."""
    from dvx.repo import Repo
    from dvx.ui import ui

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        result = repo.push(
            targets=targets,
            remote=remote,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            with_deps=with_deps,
            recursive=recursive,
            jobs=jobs,
            glob=glob,
        )
        ui.write(f"Uploaded {result.get('uploaded', 0)} file(s)")


@cli.command("fetch")
@click.argument("targets", nargs=-1)
@click.option("-r", "--remote", metavar="<name>", help="Remote storage to fetch from.")
@click.option("-a", "--all-branches", is_flag=True, help="Fetch for all branches.")
@click.option("-T", "--all-tags", is_flag=True, help="Fetch for all tags.")
@click.option("-A", "--all-commits", is_flag=True, help="Fetch for all commits.")
@click.option("-d", "--with-deps", is_flag=True, help="Fetch dependencies as well.")
@click.option("-R", "--recursive", is_flag=True, help="Fetch recursively.")
@click.option("-j", "--jobs", type=int, metavar="<n>", help="Number of parallel jobs.")
@click.pass_context
def fetch_cmd(ctx, targets, remote, all_branches, all_tags, all_commits, with_deps,
              recursive, jobs):
    """Download data from remote storage to cache (without checkout)."""
    from dvx.repo import Repo
    from dvx.ui import ui

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        result = repo.fetch(
            targets=targets,
            remote=remote,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            with_deps=with_deps,
            recursive=recursive,
            jobs=jobs,
        )
        ui.write(f"Fetched {result.get('fetched', 0)} file(s)")


# Cache subcommands
@cli.group("cache")
def cache_group():
    """Manage cache settings."""


@cache_group.command("dir")
@click.argument("value", required=False)
@click.option("-u", "--unset", is_flag=True, help="Unset cache directory.")
@click.option("--global", "global_", is_flag=True, help="Use global config.")
@click.option("--system", is_flag=True, help="Use system config.")
@click.option("--local", is_flag=True, help="Use local config.")
@click.pass_context
def cache_dir_cmd(ctx, value, unset, global_, system, local):
    """Configure cache directory location."""
    from dvx.repo import Repo
    from dvx.ui import ui

    os.chdir(ctx.obj.cd)

    if value is None and not unset:
        # Get current value
        try:
            repo = Repo(uninitialized=True)
            path = repo.config.get("cache", {}).get("dir")
            if not path:
                path = os.path.join(repo.dvc_dir, "cache")
            ui.write(path)
        except Exception:
            ui.write(".dvc/cache")
    else:
        repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
        with repo:
            level = "global" if global_ else "system" if system else "local" if local else None
            with repo.config.edit(level=level) as conf:
                if unset:
                    if "cache" in conf and "dir" in conf["cache"]:
                        del conf["cache"]["dir"]
                else:
                    conf.setdefault("cache", {})["dir"] = value


@cache_group.command("path")
@click.argument("target")
@click.option("-r", "--rev", metavar="<rev>", help="Git revision.")
@click.option("--remote", metavar="<name>", help="Get remote blob URL.")
@click.option("--relative", is_flag=True, help="Output relative path.")
@click.pass_context
def cache_path_cmd(ctx, target, rev, remote, relative):
    """Get the cache path for a DVC-tracked file."""
    from dvx.commands.cache import _get_output_from_target
    from dvx.repo import Repo
    from dvx.ui import ui

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        outs = _get_output_from_target(repo, target, rev)
        if not outs:
            raise click.ClickException(f"No outputs found in {target}")

        for out in outs:
            if not out.hash_info or not out.hash_info.value:
                continue

            if remote:
                remote_odb = repo.cloud.get_remote_odb(name=remote, hash_name=out.hash_name)
                path = remote_odb.oid_to_path(out.hash_info.value)
                url = remote_odb.fs.unstrip_protocol(path)
                ui.write(url)
            else:
                cache_path = out.cache_path
                if relative:
                    cache_path = os.path.relpath(cache_path)
                ui.write(cache_path)


@cache_group.command("md5")
@click.argument("target")
@click.option("-r", "--rev", metavar="<rev>", help="Git revision.")
@click.pass_context
def cache_md5_cmd(ctx, target, rev):
    """Get the MD5 hash for a DVC-tracked file."""
    from dvx.commands.cache import _get_output_from_target
    from dvx.repo import Repo
    from dvx.ui import ui

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        outs = _get_output_from_target(repo, target, rev)
        if not outs:
            raise click.ClickException(f"No outputs found in {target}")

        for out in outs:
            if out.hash_info and out.hash_info.value:
                ui.write(out.hash_info.value)


# Cat command
@cli.command("cat")
@click.argument("target")
@click.option("-r", "--rev", metavar="<rev>", help="Git revision.")
@click.pass_context
def cat_cmd(ctx, target, rev):
    """Display contents of a DVC-tracked file from the cache."""
    import sys

    from dvx.commands.cache import _get_output_from_target
    from dvx.repo import Repo

    os.chdir(ctx.obj.cd)
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        outs = _get_output_from_target(repo, target, rev)
        if not outs:
            raise click.ClickException(f"No outputs found in {target}")

        for out in outs:
            if not out.hash_info or not out.hash_info.value:
                continue

            cache_path = out.cache_path
            if not os.path.exists(cache_path):
                raise click.ClickException(f"Cache file not found: {cache_path}")

            with open(cache_path, "rb") as f:
                while chunk := f.read(65536):
                    sys.stdout.buffer.write(chunk)


# Remote subcommands
@cli.group("remote")
def remote_group():
    """Manage remote storage settings."""


@remote_group.command("add")
@click.argument("name")
@click.argument("url")
@click.option("-d", "--default", is_flag=True, help="Set as default remote.")
@click.option("-f", "--force", is_flag=True, help="Force add even if exists.")
@click.option("--global", "global_", is_flag=True, help="Use global config.")
@click.option("--system", is_flag=True, help="Use system config.")
@click.option("--local", is_flag=True, help="Use local config.")
@click.pass_context
def remote_add_cmd(ctx, name, url, default, force, global_, system, local):
    """Add a remote storage."""
    from dvx.repo import Repo

    os.chdir(ctx.obj.cd)
    level = "global" if global_ else "system" if system else "local" if local else None
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        repo.config.set(f"remote.{name}.url", url, level=level, force=force)
        if default:
            repo.config.set("core.remote", name, level=level)


@remote_group.command("remove")
@click.argument("name")
@click.option("--global", "global_", is_flag=True, help="Use global config.")
@click.option("--system", is_flag=True, help="Use system config.")
@click.option("--local", is_flag=True, help="Use local config.")
@click.pass_context
def remote_remove_cmd(ctx, name, global_, system, local):
    """Remove a remote storage."""
    from dvx.repo import Repo

    os.chdir(ctx.obj.cd)
    level = "global" if global_ else "system" if system else "local" if local else None
    repo = Repo(wait_for_lock=ctx.obj.wait_for_lock)
    with repo:
        with repo.config.edit(level=level) as conf:
            if "remote" in conf and name in conf["remote"]:
                del conf["remote"][name]


@remote_group.command("list")
@click.option("--global", "global_", is_flag=True, help="Use global config.")
@click.option("--system", is_flag=True, help="Use system config.")
@click.option("--local", is_flag=True, help="Use local config.")
@click.pass_context
def remote_list_cmd(ctx, global_, system, local):
    """List all remotes."""
    from dvx.repo import Repo
    from dvx.ui import ui

    os.chdir(ctx.obj.cd)
    repo = Repo(uninitialized=True)
    remotes = repo.config.get("remote", {})
    for name, conf in remotes.items():
        url = conf.get("url", "")
        ui.write(f"{name}\t{url}")


# Register the xdiff command from content_diff module
from dvx.commands.content_diff import xdiff

cli.add_command(xdiff)


def main(argv=None):
    """Main entry point."""
    try:
        cli(argv, standalone_mode=False)
        return 0
    except click.ClickException as e:
        e.show()
        return e.exit_code
    except click.Abort:
        return 1
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 0
    except Exception as e:
        from dvx.exceptions import DvcException, NotDvcRepoError

        if isinstance(e, NotDvcRepoError):
            logger.exception("")
            return 253
        if isinstance(e, DvcException):
            logger.exception("")
            return 255
        logger.exception("unexpected error")
        return 255


if __name__ == "__main__":
    sys.exit(main())
