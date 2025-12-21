"""DVX diff command - content diff for DVC-tracked files."""

import os
from dataclasses import dataclass
from enum import Enum

import click

from dvx import Repo


class CacheStatus(Enum):
    """Status of cache lookup for a .dvc file."""
    OK = "ok"                    # Cache file exists
    NOT_TRACKED = "not_tracked"  # .dvc file doesn't exist at this revision
    CACHE_MISSING = "cache_missing"  # .dvc file exists but cache file is missing


@dataclass
class CacheResult:
    """Result of looking up a cache path."""
    status: CacheStatus
    path: str | None = None
    md5: str | None = None
    error: str | None = None

    @property
    def exists(self) -> bool:
        return self.status == CacheStatus.OK


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


def _find_parent_dvc_file(path: str) -> tuple[str, str] | None:
    """Find the parent .dvc file for a path inside a DVC-tracked directory.

    Returns (parent_dvc_path, relpath_within_dir) or None if not found.
    """
    parts = path.split(os.sep)
    for i in range(len(parts) - 1, 0, -1):
        parent = os.sep.join(parts[:i])
        parent_dvc = parent + ".dvc"
        if os.path.exists(parent_dvc):
            rel = os.sep.join(parts[i:])
            return (parent_dvc, rel)
    return None


def _get_file_md5_from_manifest(manifest_path: str, rel: str) -> str | None:
    """Get the MD5 hash of a file from a directory manifest."""
    import json

    if not os.path.exists(manifest_path):
        return None
    try:
        with open(manifest_path) as f:
            entries = json.load(f)
        for entry in entries:
            if entry.get("relpath") == rel:
                return entry.get("md5")
        return None
    except (json.JSONDecodeError, KeyError):
        return None


def _find_dvc_root() -> str:
    """Find the root directory of the DVC repository."""
    from dvc.repo import Repo as DVCRepo

    return DVCRepo.find_root()


def _get_cache_path_for_ref(
    dvc_path: str,
    ref: str | None,
    file_in_dir: str | None = None,
) -> CacheResult:
    """Get cache path for a .dvc file at a specific git ref.

    If file_in_dir is provided, dvc_path should be a directory .dvc file and
    this will return the cache path for the specific file within that directory.

    Returns a CacheResult with:
    - status=OK if cache file exists
    - status=NOT_TRACKED if .dvc file doesn't exist at this ref
    - status=CACHE_MISSING if .dvc file exists but cache is missing
    """
    import subprocess

    import yaml

    try:
        root_dir = _find_dvc_root()

        if ref:
            # Read .dvc file content directly from git
            result = subprocess.run(
                ["git", "show", f"{ref}:{dvc_path}"],
                capture_output=True,
                text=True,
                check=False,
                cwd=root_dir,
            )
            if result.returncode != 0:
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"{dvc_path} not tracked at {ref}")

            loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
            dvc_content = yaml.load(result.stdout, Loader=loader)  # noqa: S506
            outs = dvc_content.get("outs", [])
            if not outs:
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"No outputs in {dvc_path} at {ref}")

            md5 = outs[0].get("md5", "")
            if not md5:
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"No md5 hash in {dvc_path} at {ref}")

            # For directories, md5 ends with .dir - strip for prefix, keep for suffix
            md5_base = md5.replace(".dir", "")
            suffix = ".dir" if md5.endswith(".dir") else ""
            cache_path = os.path.join(
                root_dir, ".dvc", "cache", "files", "md5", md5_base[:2], md5_base[2:] + suffix
            )

            # If looking for a file within a directory, look it up in the manifest
            if file_in_dir and suffix == ".dir":
                if not os.path.exists(cache_path):
                    return CacheResult(
                        CacheStatus.CACHE_MISSING,
                        md5=md5,
                        error=f"Directory manifest missing from cache: {md5}",
                    )
                file_md5 = _get_file_md5_from_manifest(cache_path, file_in_dir)
                if file_md5:
                    file_cache_path = os.path.join(
                        root_dir, ".dvc", "cache", "files", "md5", file_md5[:2], file_md5[2:]
                    )
                    if os.path.exists(file_cache_path):
                        return CacheResult(CacheStatus.OK, path=file_cache_path, md5=file_md5)
                    return CacheResult(
                        CacheStatus.CACHE_MISSING,
                        md5=file_md5,
                        error=f"File missing from cache: {file_md5}",
                    )
                return CacheResult(
                    CacheStatus.NOT_TRACKED,
                    error=f"{file_in_dir} not found in directory manifest",
                )

            # Check if cache file exists
            if os.path.exists(cache_path):
                return CacheResult(CacheStatus.OK, path=cache_path, md5=md5)
            return CacheResult(CacheStatus.CACHE_MISSING, md5=md5, error=f"Cache file missing: {md5}")
        else:
            # Read from current working tree
            if not os.path.exists(dvc_path):
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"{dvc_path} does not exist")

            loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
            with open(dvc_path) as f:
                dvc_content = yaml.load(f, Loader=loader)  # noqa: S506

            outs = dvc_content.get("outs", [])
            if not outs:
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"No outputs in {dvc_path}")

            md5 = outs[0].get("md5", "")
            if not md5:
                return CacheResult(CacheStatus.NOT_TRACKED, error=f"No md5 hash in {dvc_path}")

            # For directories, md5 ends with .dir
            md5_base = md5.replace(".dir", "")
            suffix = ".dir" if md5.endswith(".dir") else ""
            cache_path = os.path.join(
                root_dir, ".dvc", "cache", "files", "md5", md5_base[:2], md5_base[2:] + suffix
            )

            # If looking for a file within a directory
            if file_in_dir and suffix == ".dir":
                if not os.path.exists(cache_path):
                    return CacheResult(
                        CacheStatus.CACHE_MISSING,
                        md5=md5,
                        error=f"Directory manifest missing from cache: {md5}",
                    )
                file_md5 = _get_file_md5_from_manifest(cache_path, file_in_dir)
                if file_md5:
                    file_cache_path = os.path.join(
                        root_dir, ".dvc", "cache", "files", "md5", file_md5[:2], file_md5[2:]
                    )
                    if os.path.exists(file_cache_path):
                        return CacheResult(CacheStatus.OK, path=file_cache_path, md5=file_md5)
                    return CacheResult(
                        CacheStatus.CACHE_MISSING,
                        md5=file_md5,
                        error=f"File missing from cache: {file_md5}",
                    )
                return CacheResult(
                    CacheStatus.NOT_TRACKED,
                    error=f"{file_in_dir} not found in directory manifest",
                )

            # Check if cache file exists
            if os.path.exists(cache_path):
                return CacheResult(CacheStatus.OK, path=cache_path, md5=md5)
            return CacheResult(CacheStatus.CACHE_MISSING, md5=md5, error=f"Cache file missing: {md5}")

    except Exception as e:
        return CacheResult(CacheStatus.NOT_TRACKED, error=str(e))


def _run_diff(
    path1: str | None,
    path2: str | None,
    color: bool | None = None,
    unified: int | None = None,
    ignore_whitespace: bool = False,
) -> int:
    """Run diff on two paths."""
    import subprocess

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

    result = subprocess.run(args, check=False)
    return result.returncode


def _run_pipeline_diff(
    path1: str | None,
    path2: str | None,
    cmds: list[str],
    color: bool | None = None,
    unified: int | None = None,
    ignore_whitespace: bool = False,
    verbose: bool = False,
    shell: bool = True,
    shell_executable: str | None = None,
    both: bool = False,
    pipefail: bool = False,
) -> int:
    """Run diff with preprocessing pipeline using dffs."""
    from dffs import join_pipelines

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
        pipefail=pipefail,
    )


def _compute_dir_manifest(dir_path: str) -> dict[str, tuple[str, int]]:
    """Compute MD5 hashes and sizes for all files in a directory."""
    import hashlib

    manifest = {}
    dir_path = os.path.abspath(dir_path)
    for root, _dirs, files in os.walk(dir_path):
        for filename in files:
            filepath = os.path.join(root, filename)
            rel = os.path.relpath(filepath, dir_path)
            try:
                hasher = hashlib.md5()  # noqa: S324
                with open(filepath, "rb") as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        hasher.update(chunk)
                size = os.path.getsize(filepath)
                manifest[rel] = (hasher.hexdigest(), size)
            except OSError:
                pass  # Skip files we can't read
    return manifest


def _get_cache_file_size(md5: str) -> int | None:
    """Get size of a file in the cache by its MD5 hash."""
    try:
        root_dir = _find_dvc_root()
        cache_path = os.path.join(root_dir, ".dvc", "cache", "files", "md5", md5[:2], md5[2:])
        if os.path.exists(cache_path):
            return os.path.getsize(cache_path)
    except Exception:
        pass
    return None


def _diff_directory(path1: str | None, path2: str | None, data_path: str, after: str | None) -> int:
    """Compare directory manifests.

    DVC stores directory contents as JSON manifests in the cache.
    This function diffs those manifests to show which files changed.
    """
    import json

    def load_manifest(path: str | None) -> dict[str, tuple[str, int | None]] | None:
        """Load directory manifest, returning {relpath: (md5, size)} dict."""
        if not path or not os.path.exists(path):
            return {}
        # Skip if it's a directory (working tree) - we only read manifest files
        if os.path.isdir(path):
            return None  # Signal that we can't read this as manifest
        try:
            with open(path) as f:
                entries = json.load(f)
                # Look up sizes from cache for each file
                result = {}
                for e in entries:
                    md5 = e["md5"]
                    size = _get_cache_file_size(md5)
                    result[e["relpath"]] = (md5, size)
                return result
        except (json.JSONDecodeError, KeyError):
            return {}

    manifest1 = load_manifest(path1)
    manifest2 = load_manifest(path2)

    # If one side is a working tree directory, compute hashes for it
    if manifest2 is None and path2 and os.path.isdir(path2):
        manifest2 = _compute_dir_manifest(path2)

    if manifest1 is None and path1 and os.path.isdir(path1):
        manifest1 = _compute_dir_manifest(path1)

    # Handle empty manifests
    manifest1 = manifest1 or {}
    manifest2 = manifest2 or {}

    # Compare manifests - output diff-style with full paths relative to data_path
    all_paths = sorted(set(manifest1) | set(manifest2))
    if not all_paths:
        return 0

    has_diff = False

    for rel in all_paths:
        entry1 = manifest1.get(rel)  # (md5, size) or None
        entry2 = manifest2.get(rel)  # (md5, size) or None
        full_path = os.path.join(data_path, rel)

        if entry1 is None:
            # Added
            md5_2, size_2 = entry2
            click.secho(f"+ {full_path}  {md5_2}  {size_2 if size_2 is not None else '?'}", fg="green")
            has_diff = True
        elif entry2 is None:
            # Removed
            md5_1, size_1 = entry1
            click.secho(f"- {full_path}  {md5_1}  {size_1 if size_1 is not None else '?'}", fg="red")
            has_diff = True
        else:
            md5_1, size_1 = entry1
            md5_2, size_2 = entry2
            if md5_1 != md5_2:
                # Modified - show both old and new
                click.secho(f"- {full_path}  {md5_1}  {size_1 if size_1 is not None else '?'}", fg="red")
                click.secho(f"+ {full_path}  {md5_2}  {size_2 if size_2 is not None else '?'}", fg="green")
                has_diff = True

    return 1 if has_diff else 0


@click.command()
@click.option("-b", "--both", is_flag=True, help="Merge stderr into stdout in pipeline commands.")
@click.option("-c/-C", "--color/--no-color", default=None, help="Force or prevent colorized output.")
@click.option("-P", "--pipefail", is_flag=True, help="Check all pipeline commands for errors (like bash's `set -o pipefail`); default only checks last command.")
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
    both,
    color,
    pipefail,
    refspec,
    ref,
    shell_executable,
    no_shell,
    summary,
    unified,
    verbose,
    ignore_whitespace,
    exec_cmds,
    args,
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
    import json as json_module

    # Handle summary mode (shows file/hash changes via DVC)
    if summary:
        # Parse refspec for summary mode
        if refspec and ref:
            raise click.UsageError("Specify -r/--refspec or -R/--ref, not both")

        if ref:
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
            with Repo() as repo:
                d = repo.diff(
                    a_rev=a_rev,
                    b_rev=b_rev,
                    targets=list(args) if args else None,
                )
                if any(d.values()):
                    click.echo(json_module.dumps(d, indent=2))
                else:
                    click.echo("No changes.")
        except Exception as e:
            raise click.ClickException(str(e)) from e
        return

    # Content diff mode - need a target path
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

    # Check if this is a file inside a DVC-tracked directory
    parent_dvc_info = _find_parent_dvc_file(data_path)

    # Get cache paths with better error handling
    result1 = _get_cache_path_for_ref(dvc_path, before)
    if result1.status == CacheStatus.NOT_TRACKED and parent_dvc_info:
        # Try looking up as file inside directory
        parent_dvc, rel = parent_dvc_info
        result1 = _get_cache_path_for_ref(parent_dvc, before, file_in_dir=rel)

    if after is None:
        # Compare to working tree - use the actual file if it exists
        if os.path.exists(data_path):
            path2 = data_path
            result2 = None  # Using actual file, not cache
        else:
            result2 = _get_cache_path_for_ref(dvc_path, None)
            if result2.status == CacheStatus.NOT_TRACKED and parent_dvc_info:
                parent_dvc, rel = parent_dvc_info
                result2 = _get_cache_path_for_ref(parent_dvc, None, file_in_dir=rel)
            path2 = result2.path if result2 and result2.exists else None
    else:
        result2 = _get_cache_path_for_ref(dvc_path, after)
        if result2.status == CacheStatus.NOT_TRACKED and parent_dvc_info:
            parent_dvc, rel = parent_dvc_info
            result2 = _get_cache_path_for_ref(parent_dvc, after, file_in_dir=rel)
        path2 = result2.path if result2.exists else None

    # Check for cache missing errors (distinct from "file doesn't exist at revision")
    if result1.status == CacheStatus.CACHE_MISSING:
        raise click.ClickException(
            f"Cache missing for '{before}': {result1.error}\n"
            "Run 'dvc pull' to fetch from remote."
        )
    if result2 is not None and result2.status == CacheStatus.CACHE_MISSING:
        after_ref = after or "working tree"
        raise click.ClickException(
            f"Cache missing for '{after_ref}': {result2.error}\n"
            "Run 'dvc pull' to fetch from remote."
        )

    # Extract path1 (None means file doesn't exist at that revision - legitimate add/delete)
    path1 = result1.path if result1.exists else None

    if result1.status == CacheStatus.NOT_TRACKED and (result2 is None or result2.status == CacheStatus.NOT_TRACKED):
        raise click.ClickException(f"Could not find {dvc_path} at either revision")

    # Check if it's a directory (cache paths for dirs end with .dir)
    is_dir = (path1 and path1.endswith(".dir")) or (path2 and path2.endswith(".dir"))
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
            pipefail=pipefail,
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


# Export the command
cmd = diff
