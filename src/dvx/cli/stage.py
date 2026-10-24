"""SDK-backed stage-graph manipulation: the ``dvx stage`` command group.

Thin CLI over :mod:`dvx.sdk` so the authoring/manipulation layer is usable
without writing Python:

- ``dvx stage subset <targets>`` — the dependency-ordered slice (targets plus
  their transitive ancestors) you'd rebuild or submit to produce ``targets``.
- ``dvx stage resources <path>`` — set or clear a stage's resource request,
  re-emitting its ``.dvc`` while preserving committed outs and dep hashes.

``dvx stage`` is a separate group from the ``dvx dag`` visualizer because
``dvx dag`` takes a positional ``targets`` argument (``dvx dag out.dvc``), so it
can't also be a subcommand group without breaking that syntax.
"""

import click

from dvx.sdk import Dag, Resources


def _strip_dvc(target: str) -> str:
    """Normalize a user-supplied target to its output path (drop a ``.dvc``)."""
    return target[:-4] if target.endswith(".dvc") else target


@click.group()
def stage() -> None:
    """Author / manipulate the `.dvc` stage graph (SDK-backed)."""


@stage.command("subset")
@click.argument("targets", nargs=-1, required=True)
@click.option("-d", "--dvc", "as_dvc", is_flag=True, help="Print `.dvc` file paths instead of output paths.")
def subset_cmd(targets: tuple[str, ...], as_dvc: bool) -> None:
    """Print TARGETS plus their transitive ancestors, in dependency order.

    The stage slice you'd rebuild (or submit) to produce TARGETS — upstream
    stages first. Only computed stages are listed; raw inputs are not stages.
    """
    dag = Dag.from_dvc()
    try:
        sub = dag.subset([_strip_dvc(t) for t in targets])
    except KeyError as e:
        # KeyError's str is the repr'd message, e.g. '"not a stage ...: \'x\'"'.
        raise click.ClickException(e.args[0]) from e
    for artifact in sub.ordered():
        click.echo(f"{artifact.path}.dvc" if as_dvc else artifact.path)


@stage.command("resources")
@click.argument("path")
@click.option("-c", "--cpus", type=int, help="Set the CPU count.")
@click.option("-m", "--mem-gb", type=float, help="Set the memory request, in GB.")
def resources_cmd(path: str, cpus: int | None, mem_gb: float | None) -> None:
    """Set the resource request on a single stage's `.dvc`.

    Loads PATH's stage, merges the given values into
    `meta.computation.resources`, and re-emits the `.dvc` — preserving its
    committed outs and dependency hashes.

    Re-emit is *additive*: it updates fields but cannot remove them (the
    rewrite merges onto the existing file to keep its comments / key order), so
    there is no "clear" — edit the `.dvc` directly to drop a request.
    """
    if cpus is None and mem_gb is None:
        raise click.ClickException("specify --cpus and/or --mem-gb")
    target = _strip_dvc(path)
    stages = [a for a in Dag.from_dvc([target]).artifacts if a.path == target]
    if not stages:
        raise click.ClickException(f"not a computed stage: {target!r}")
    (artifact,) = stages
    merged = dict(artifact.computation.resources)
    merged.update(Resources(cpus=cpus, mem_gb=mem_gb).to_dict())
    artifact.computation.resources = merged

    dag = Dag()
    dag.add(artifact)
    (written,) = dag.to_dvc()
    click.echo(str(written))
