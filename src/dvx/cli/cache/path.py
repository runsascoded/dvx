"""Cache path subcommand."""

import click


@click.command("path")
@click.argument("target")
@click.option("-r", "--rev", metavar="<rev>", help="Git revision.")
@click.option("--remote", metavar="<name>", help="Get remote blob URL instead.")
@click.option("--absolute", is_flag=True, help="Output absolute path (default is relative).")
def cmd(target, rev, remote, absolute):
    """Get the cache path for a DVC-tracked file.

    TARGET can be:
    - a .dvc file or path to a tracked file (adds .dvc if needed)
    - a file inside a DVC-tracked directory
    - an MD5 hash (32 hex chars) to get path directly

    Examples:
        dvx cache path data.txt.dvc
        dvx cache path data.txt
        dvx cache path data.txt --remote myremote
        dvx cache path data.txt -r HEAD~1
        dvx cache path tracked_dir/file.txt
        dvx cache path d8e8fca2dc0f896fd7cb4cb0031ba249
    """
    from dvx.cache import get_cache_path

    try:
        path = get_cache_path(target, rev=rev, remote=remote, absolute=absolute)
        click.echo(path)
    except Exception as e:
        raise click.ClickException(str(e)) from e
