"""DVX external data commands - import and get from external sources."""

import click

from dvx import Repo


# =============================================================================
# Import (download and track)
# =============================================================================


@click.command("import")
@click.argument("url")
@click.argument("path")
@click.option("-o", "--out", help="Output path.")
@click.option("--rev", help="Git revision in the source repo.")
def import_cmd(url, path, out, rev):
    """Import a file from another DVC/DVX repository."""
    try:
        with Repo() as repo:
            repo.imp(url=url, path=path, out=out, rev=rev)
            click.echo(f"Imported {path} from {url}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


@click.command("import-url")
@click.argument("url")
@click.option("-o", "--out", help="Output path.")
def import_url(url, out):
    """Import a file from a URL."""
    try:
        with Repo() as repo:
            repo.imp_url(url=url, out=out)
            click.echo(f"Imported {url}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Get (download without tracking)
# =============================================================================


@click.command()
@click.argument("url")
@click.argument("path")
@click.option("-o", "--out", help="Output path.")
@click.option("--rev", help="Git revision in the source repo.")
def get(url, path, out, rev):
    """Download a file from a DVC/DVX repository (without tracking)."""
    try:
        Repo.get(url=url, path=path, out=out, rev=rev)
        click.echo(f"Downloaded {path} from {url}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


@click.command("get-url")
@click.argument("url")
@click.option("-o", "--out", help="Output path.")
def get_url(url, out):
    """Download a file from a URL (without tracking)."""
    try:
        Repo.get_url(url=url, out=out)
        click.echo(f"Downloaded {url}")
    except Exception as e:
        raise click.ClickException(str(e)) from e
