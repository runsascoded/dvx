"""Cache dir subcommand."""

import sys

import click


@click.command("dir")
@click.argument("value", required=False)
@click.option("-u", "--unset", is_flag=True, help="Unset cache directory.")
def cmd(value, unset):
    """Get or set the cache directory location."""
    from dvc.cli import main as dvc_main

    if value is None and not unset:
        # Get current value - delegate to dvc
        sys.exit(dvc_main(["cache", "dir"]))
    else:
        args = ["cache", "dir"]
        if unset:
            args.append("--unset")
        if value:
            args.append(value)
        sys.exit(dvc_main(args))
