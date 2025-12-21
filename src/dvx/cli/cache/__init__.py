"""Cache group - manage DVC cache and inspect cached files."""

import click

from . import dir, md5, path


@click.group()
def cache():
    """Manage DVC cache and inspect cached files."""


# Register subcommands
cache.add_command(dir.cmd)
cache.add_command(md5.cmd)
cache.add_command(path.cmd)
