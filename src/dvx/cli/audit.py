"""DVX audit command — blob classification, lineage, and cache analysis."""

import json

import click


def _format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    if size_bytes >= 1_000_000_000:
        return f"{size_bytes / 1_000_000_000:.1f} GB"
    if size_bytes >= 1_000_000:
        return f"{size_bytes / 1_000_000:.1f} MB"
    if size_bytes >= 1_000:
        return f"{size_bytes / 1_000:.1f} KB"
    return f"{size_bytes} B"


def _print_summary(summary):
    """Print workspace audit summary."""
    from dvx.audit.model import BlobKind, Reproducibility

    click.echo(f"Blobs in workspace:       {summary.total_count}")

    input_count = summary.count_by_kind(BlobKind.INPUT)
    input_size = summary.size_by_kind(BlobKind.INPUT)
    if input_count:
        click.echo(f"  Input:                  {input_count} ({_format_size(input_size)})")

    gen_count = summary.count_by_kind(BlobKind.GENERATED)
    gen_size = summary.size_by_kind(BlobKind.GENERATED)
    if gen_count:
        repro = summary.reproducible_count
        repro_str = f", {repro} reproducible" if repro else ""
        click.echo(f"  Generated:              {gen_count} ({_format_size(gen_size)}{repro_str})")

    foreign_count = summary.count_by_kind(BlobKind.FOREIGN)
    click.echo(f"  Foreign:                {foreign_count}")

    click.echo()
    click.echo(
        f"Local cache:              {summary.cached_count} of {summary.total_count} "
        f"({_format_size(summary.cached_size)})"
    )
    if summary.missing_count:
        click.echo(
            f"  Missing:                {summary.missing_count} "
            f"({_format_size(summary.missing_size)})"
        )


def _print_artifact(blob):
    """Print per-artifact lineage."""
    from dvx.audit.model import BlobKind, Reproducibility

    click.echo(f"Path:    {blob.path}")
    if blob.md5:
        click.echo(f"MD5:     {blob.md5}")
    click.echo(f"Size:    {_format_size(blob.size)}")

    kind_str = blob.kind.value.capitalize()
    if blob.kind == BlobKind.GENERATED:
        repro_str = blob.reproducible.value.replace("_", " ")
        kind_str = f"{kind_str} ({repro_str})"
    click.echo(f"Type:    {kind_str}")

    if blob.cmd:
        click.echo(f"Command: {blob.cmd}")

    data_deps = {k: v for k, v in blob.deps.items()}
    code_deps = {k: v for k, v in blob.git_deps.items()}
    total_deps = len(data_deps) + len(code_deps)
    if total_deps:
        click.echo()
        click.echo(f"Dependencies ({len(data_deps)} data + {len(code_deps)} code):")
        for dep_path, dep_md5 in sorted(data_deps.items()):
            click.echo(f"  [data] {dep_path}  ({dep_md5[:8]}...)")
        for dep_path, dep_sha in sorted(code_deps.items()):
            click.echo(f"  [code] {dep_path}  (git: {dep_sha[:6]})")

    click.echo()
    local_str = "yes" if blob.in_local_cache else "no"
    remote_str = "not checked"
    if blob.in_remote_cache is True:
        remote_str = "yes"
    elif blob.in_remote_cache is False:
        remote_str = "no"
    click.echo(f"Cache:   local={local_str}  remote={remote_str}")


def _print_orphans(orphans):
    """Print orphaned cache blobs."""
    if not orphans:
        click.echo("No orphaned blobs found.")
        return

    total_size = sum(size for _, size in orphans)
    click.echo(f"{len(orphans)} orphaned blob(s) ({_format_size(total_size)}):")
    for md5, size in orphans:
        click.echo(f"  {md5}  ({_format_size(size)})")


def _format_graph(summary):
    """Format audit as Graphviz DOT with kind-based coloring."""
    from dvx.audit.model import BlobKind, Reproducibility

    lines = ["digraph DVX_Audit {"]
    lines.append("  rankdir=TB;")
    lines.append("  node [shape=box, style=filled];")
    lines.append("")

    for blob in summary.blobs:
        escaped = blob.path.replace('"', '\\"')
        label = escaped
        if blob.is_dir:
            label += "/"

        if blob.kind == BlobKind.INPUT:
            color = "palegreen"
        elif blob.kind == BlobKind.GENERATED:
            if blob.reproducible == Reproducibility.REPRODUCIBLE:
                color = "lightblue"
            else:
                color = "steelblue1"
        elif blob.kind == BlobKind.FOREIGN:
            color = "lightgray"
        else:
            color = "lightyellow"

        style = 'style="filled,dashed"' if blob.kind == BlobKind.FOREIGN else "style=filled"
        lines.append(f'  "{escaped}" [label="{label}", fillcolor={color}, {style}];')

    lines.append("")

    # Add edges from deps
    blob_paths = {b.path for b in summary.blobs}
    for blob in summary.blobs:
        escaped_path = blob.path.replace('"', '\\"')
        for dep_path in sorted(blob.deps.keys()):
            escaped_dep = dep_path.replace('"', '\\"')
            if dep_path not in blob_paths:
                # External dep node (not a tracked blob)
                lines.append(
                    f'  "{escaped_dep}" [label="{escaped_dep}", '
                    f'style=dashed, color=gray, fontcolor=gray];'
                )
                blob_paths.add(dep_path)
            lines.append(f'  "{escaped_dep}" -> "{escaped_path}";')

    lines.append("")
    lines.append("  // Legend")
    lines.append('  subgraph cluster_legend {')
    lines.append('    label="Legend"; style=rounded;')
    lines.append('    legend_input [label="Input", fillcolor=palegreen, style=filled];')
    lines.append('    legend_gen [label="Generated\\n(reproducible)", fillcolor=lightblue, style=filled];')
    lines.append('    legend_foreign [label="Foreign", fillcolor=lightgray, style="filled,dashed"];')
    lines.append('    legend_input -> legend_gen -> legend_foreign [style=invis];')
    lines.append("  }")
    lines.append("}")
    return "\n".join(lines)


@click.command("audit")
@click.argument("targets", nargs=-1)
@click.option("--json", "output_json", is_flag=True, help="Machine-readable JSON output.")
@click.option("-g", "--graph", is_flag=True, help="Output DOT dependency graph colored by kind.")
@click.option("-j", "--jobs", type=int, default=None, help="Parallel workers for remote checks.")
@click.option("-o", "--orphans", is_flag=True, help="List unreferenced cache blobs.")
@click.option("-r", "--remote", default=None, help="Also check remote cache.")
@click.option("-S", "--snapshot", type=click.Path(exists=True), help="Load from snapshot directory (testing).")
def audit(targets, output_json, graph, jobs, orphans, remote, snapshot):
    """Audit workspace blobs: classification, lineage, and cache analysis.

    With no arguments, prints a workspace summary.
    With a path argument, shows per-artifact lineage.
    With --orphans, lists unreferenced cache blobs.
    With --graph, outputs a DOT dependency graph colored by blob kind.

    \b
    Examples:
        dvx audit                          # workspace summary
        dvx audit some/artifact            # per-artifact lineage
        dvx audit --orphans                # find orphans
        dvx audit --json                   # JSON output
        dvx audit --graph | dot -Tsvg      # colored DAG
        dvx audit -r myremote              # also check remote cache
        dvx audit -S tmp/crashes-snapshot  # from snapshot
    """
    from pathlib import Path

    from dvx.audit.scan import audit_artifact, find_orphans, scan_workspace

    view = None
    if snapshot:
        from dvx.audit.repo_view import SnapshotRepoView
        view = SnapshotRepoView.load(Path(snapshot))

    check_remote = remote is not None

    if orphans:
        orphan_list = find_orphans(view=view)
        if output_json:
            click.echo(json.dumps(
                [{"md5": md5, "size": size} for md5, size in orphan_list],
                indent=2,
            ))
        else:
            _print_orphans(orphan_list)
        return

    target_list = list(targets) if targets else None

    # Per-artifact mode: single target
    if target_list and len(target_list) == 1 and not graph:
        blob = audit_artifact(
            target_list[0],
            remote=remote,
            check_remote=check_remote,
            view=view,
        )
        if blob is None:
            raise click.ClickException(f"No .dvc file found for: {target_list[0]}")
        if output_json:
            click.echo(json.dumps(blob.to_dict(), indent=2))
        else:
            _print_artifact(blob)
        return

    # Workspace summary / graph mode
    summary = scan_workspace(
        targets=target_list,
        remote=remote,
        check_remote=check_remote,
        view=view,
    )

    if not summary.blobs:
        click.echo("No .dvc files found in workspace.", err=True)
        return

    if output_json:
        click.echo(json.dumps(summary.to_dict(), indent=2))
    elif graph:
        click.echo(_format_graph(summary))
    else:
        _print_summary(summary)
