"""Cache md5 subcommand."""

import click


@click.command("md5")
@click.argument("target")
@click.option("-r", "--rev", metavar="<rev>", help="Git revision.")
def cmd(target, rev):
    """Get the MD5 hash for a DVC-tracked file.

    TARGET can be:
    - a .dvc file or path to a tracked file (adds .dvc if needed)
    - a file inside a DVC-tracked directory

    Examples:
        dvx cache md5 data.txt.dvc
        dvx cache md5 data.txt
        dvx cache md5 data.txt -r HEAD~1
        dvx cache md5 tracked_dir/file.txt
    """
    from dvx.cache import get_hash

    try:
        md5 = get_hash(target, rev=rev)
        click.echo(md5)
    except Exception as e:
        raise click.ClickException(str(e)) from e
