"""``dvx cache comm`` — set accounting between local cache, remote(s), and
referenced objects. See ``specs/done/cache-comm-remote-audit.md``.

A LOCATION is one of:

- ``local`` — the local cache (``.dvc/cache``)
- a remote name (from ``.dvc/config``) — objects listed from that remote
- a ref-set: ``HEAD``, ``workspace``, any git rev, ``--all-commits``,
  ``--all-branches`` — objects referenced by ``.dvc`` files there, with
  ``.dir`` manifests expanded

Default (no args): ``local <default-remote> HEAD`` — the disk-triage case
("what's safe to delete locally?").
"""

from __future__ import annotations

import sys

import click


def _known_remotes() -> dict[str, "str | None"]:
    """Remote names from dvc config → name (default remote resolvable)."""
    try:
        from dvc.repo import Repo as DVCRepo

        with DVCRepo() as repo:
            remotes = dict(repo.config.get("remote", {}))
            core = repo.config.get("core", {})
            default = core.get("remote")
            return {"names": remotes, "default": default}  # type: ignore[return-value]
    except Exception:
        return {"names": {}, "default": None}  # type: ignore[return-value]


@click.command("comm")
@click.option("-F", "--fresh", is_flag=True, help="Refetch remote listings (ignore .dvc/tmp/comm/ cache).")
@click.option("-j", "--json", "as_json", is_flag=True, help="Machine-readable full breakdown.")
@click.option("-o", "--only", metavar="PATTERN", help="Emit just the object list for one membership pattern (e.g. 'local,s3,!HEAD'): one `md5 size` per line on stdout; summary to stderr.")
@click.argument("locations", nargs=-1)
def comm(fresh: bool, as_json: bool, only: str | None, locations: tuple[str, ...]) -> None:
    """Venn-style membership accounting across cache LOCATIONS.

    \b
    Examples:
        dvx cache comm                       # local vs default remote vs HEAD
        dvx cache comm local s3 workspace
        dvx cache comm -o 'local,s3,!HEAD'   # deletable-locally object list
    """
    from dvx.comm import (
        compute_comm,
        local_objects,
        parse_only_pattern,
        ref_objects,
        remote_objects,
    )

    remotes_info = _known_remotes()
    remote_names = remotes_info["names"]  # type: ignore[index]

    if not locations:
        default_remote = remotes_info["default"]  # type: ignore[index]
        if default_remote is None and remote_names:
            default_remote = next(iter(remote_names))
        # A remote literally named "local" collides with the `local` keyword;
        # spell it `remote:local` so both columns resolve correctly.
        remote_label = None
        if default_remote:
            remote_label = f"remote:{default_remote}" if default_remote == "local" else default_remote
        locations = ("local",) + ((remote_label,) if remote_label else ()) + ("HEAD",)

    labels = list(locations)

    def _remote_name(loc: str) -> "str | None":
        """Resolve a location token to a remote name (None = not a remote).

        `remote:NAME` is the explicit form (needed when a remote is named
        `local`, `HEAD`, or `workspace` — the keywords win bare).
        """
        if loc.startswith("remote:"):
            name = loc.split(":", 1)[1]
            if name not in remote_names:
                raise click.ClickException(f"unknown remote: {name}")
            return name
        if loc in ("local", "workspace", "HEAD", "--all-commits", "--all-branches"):
            return None
        return loc if loc in remote_names else None

    remote_name_list = [n for n in (_remote_name(loc) for loc in labels) if n]
    if "local" in remote_names and "local" in labels:
        click.echo(
            "⚠ a remote is named 'local'; the bare token means the local cache — "
            "use 'remote:local' for the remote.",
            err=True,
        )

    maps = []
    unexpandable: list[str] = []
    try:
        for loc in labels:
            rname = _remote_name(loc)
            if rname is not None:
                maps.append(remote_objects(rname, fresh=fresh))
            elif loc == "local":
                maps.append(local_objects())
            else:
                # Ref-set: workspace / HEAD / rev / --all-commits / --all-branches
                rs = ref_objects(loc, remotes=remote_name_list or [None])
                unexpandable.extend(rs.unexpandable)
                maps.append(rs.objects)
    except Exception as e:
        raise click.ClickException(str(e)) from e

    rows = compute_comm(maps)

    from dvx.gc import format_size

    def fmt_size(row) -> str:
        if row.count == 0:
            return "–"
        s = format_size(row.size) if row.size else ("?" if row.unknown_sizes else "0 B")
        if row.unknown_sizes and row.size:
            s += " +?"
        return s

    if only:
        try:
            pattern = parse_only_pattern(only, labels)
        except ValueError as e:
            raise click.ClickException(str(e)) from e
        match = next(r for r in rows if r.pattern == pattern)
        # Summary to stderr, object list to stdout (pipe-able).
        print(
            f"{match.count} object(s), {format_size(match.size)}"
            + (f" (+{match.unknown_sizes} unknown-size)" if match.unknown_sizes else ""),
            file=sys.stderr,
        )
        best = {}
        for m in maps:
            for k, v in m.items():
                if v is not None and k not in best:
                    best[k] = v
        for key in match.keys:
            size = best.get(key)
            click.echo(f"{key} {size if size is not None else '?'}")
        return

    if as_json:
        import json as _json
        out = {
            "locations": labels,
            "rows": [
                {
                    "pattern": {label: p for label, p in zip(labels, r.pattern)},
                    "objects": r.count,
                    "size": r.size,
                    "unknown_sizes": r.unknown_sizes,
                }
                for r in rows
            ],
            "unexpandable_dirs": unexpandable,
        }
        click.echo(_json.dumps(out, indent=2))
        return

    # Table: one column per location, then counts + sizes.
    widths = [max(len(label), 5) for label in labels]
    header = "  ".join(label.center(w) for label, w in zip(labels, widths))
    click.echo(f"{header}  {'objects':>9}  {'size':>10}")
    for r in rows:
        marks = "  ".join(("✓" if p else "–").center(w) for p, w in zip(r.pattern, widths))
        count = f"{r.count:,}" if r.count else "–"
        click.echo(f"{marks}  {count:>9}  {fmt_size(r):>10}")

    if unexpandable:
        click.echo(
            f"⚠ {len(unexpandable)} dir manifest(s) unexpandable (children unknown, "
            f"treated conservatively): {', '.join(unexpandable[:5])}"
            + ("…" if len(unexpandable) > 5 else ""),
            err=True,
        )


cmd = comm
