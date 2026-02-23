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
@click.option("-F", "--fs-config", multiple=True, help="Filesystem config (key=value).")
@click.option("-N", "--no-download", is_flag=True, help="Track metadata only (no download).")
@click.option("-o", "--out", help="Output path.")
@click.option("-V", "--version-aware", is_flag=True, help="Track S3 version IDs.")
def import_url(url, fs_config, no_download, out, version_aware):
    """Import a file from a URL.

    Use --no-download to track metadata (ETag, size) without downloading.
    Use --fs-config allow_anonymous_login=true for public buckets.
    """
    fs_config_dict = dict(kv.split("=", 1) for kv in fs_config) if fs_config else None
    try:
        with Repo() as repo:
            repo.imp_url(
                url=url,
                out=out,
                no_download=no_download,
                fs_config=fs_config_dict,
                version_aware=version_aware,
            )
            action = "Tracked" if no_download else "Imported"
            click.echo(f"{action} {url}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


# =============================================================================
# Update (re-check source metadata)
# =============================================================================


@click.command()
@click.option("-N", "--no-download", is_flag=True, help="Update metadata only.")
@click.option("-r", "--recursive", is_flag=True, help="Update targets recursively.")
@click.argument("targets", nargs=-1, required=True)
def update(no_download, recursive, targets):
    """Update imported data from external sources.

    Re-checks source ETags and optionally re-downloads if changed.
    """
    try:
        with Repo() as repo:
            repo.update(
                targets=list(targets),
                no_download=no_download,
                recursive=recursive,
            )
            for target in targets:
                click.echo(f"Updated {target}")
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
