"""Python SDK for authoring dvx DAGs.

A thin, composable layer over the artifact model in :mod:`dvx.run.artifact`
(``Artifact`` / ``Computation`` / ``delayed`` / ``materialize``). It adds the
two pieces the low-level API lacks for ergonomic authoring:

- :class:`Resources` — a typed, reusable resource spec, so one value can be
  defined once and applied to many stages (``resources=big``) instead of
  repeating a raw ``{"mem_gb": ..., "cpus": ...}`` dict.
- :class:`Dag` — an explicit container with ``validate()`` (unique paths,
  resolvable deps, no cycles) and a batch ``to_dvc()`` that emits the whole
  stage family at once.

Everything lowers to the ``.dvc`` graph dvx already understands — this is a
config-construction / manipulation layer, not a runtime. Placement/executor
hints and a rendered execution manifest ("run lock") are deferred; see
``specs/dag-sdk-design.md``.

Example (the reproc pattern, DRY):

    from dvx.sdk import Dag, Resources

    big = Resources(cpus=16, mem_gb=64)          # defined once
    dag = Dag()
    prev = None
    for ym in months:
        s = dag.stage(
            path=f"normalized/{ym}",
            cmd=f"app normalize {ym}",
            deps=[f"raw/{ym}"] + ([prev] if prev else []),
            resources=big,                        # one value, many stages
        )
        prev = s
    dag.to_dvc()                                   # emit/update the .dvc graph
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dvx.run.artifact import Artifact, Computation, delayed, materialize

__all__ = [
    "Artifact",
    "Computation",
    "Dag",
    "Resources",
    "delayed",
    "materialize",
]

# Resource keys dvx's scheduler/executors understand (mirrors
# dvx.run.dvc_files._RESOURCE_KEYS). The SDK *produces* exactly these so it
# feeds the existing resource-aware scheduling rather than modeling a parallel
# set — see specs/dag-sdk-design.md ("Decided").
_RESOURCE_KEYS = ("mem_gb", "cpus")


@dataclass(frozen=True)
class Resources:
    """A reusable per-stage resource spec.

    Rendered to the ``{mem_gb, cpus}`` dict that
    ``Computation.resources`` carries and dvx's resource-aware scheduling
    (and the Batch executors) consume. Frozen so one instance can be shared
    across many stages without aliasing surprises.
    """

    cpus: int | None = None
    mem_gb: float | None = None

    def to_dict(self) -> dict[str, float]:
        """Render to the resource dict dvx consumes (omitting unset fields)."""
        d: dict[str, float] = {}
        if self.mem_gb is not None:
            d["mem_gb"] = self.mem_gb
        if self.cpus is not None:
            d["cpus"] = self.cpus
        return d


def _dep_key(dep: Artifact | str | Path) -> str:
    """Normalized path key for a dependency (intra-DAG edge identity)."""
    path = dep.path if isinstance(dep, Artifact) else dep
    return str(Path(path))


@dataclass
class Dag:
    """A collection of stages authored in Python, lowered to ``.dvc`` files.

    Build stages with :meth:`stage` (or :meth:`add` an existing
    :class:`Artifact`), then :meth:`validate` and :meth:`to_dvc`. The DAG is a
    construction/manipulation surface: an ordinary list of ``Artifact`` you can
    filter, re-target, or generate in a loop.
    """

    def __init__(self) -> None:
        self.artifacts: list[Artifact] = []
        self._by_path: dict[str, Artifact] = {}

    def add(self, artifact: Artifact) -> Artifact:
        """Add an existing :class:`Artifact`; returns it for chaining.

        Raises ``ValueError`` on a duplicate output path.
        """
        key = _dep_key(artifact)
        if key in self._by_path:
            raise ValueError(f"duplicate stage path: {key}")
        self._by_path[key] = artifact
        self.artifacts.append(artifact)
        return artifact

    def stage(
        self,
        path: str | Path,
        cmd: str,
        deps: list[Artifact | str | Path] | None = None,
        *,
        git_deps: list[Artifact | str | Path] | None = None,
        git_log_deps: list[Artifact | str | Path] | None = None,
        resources: Resources | dict[str, float] | None = None,
        params: dict | None = None,
    ) -> Artifact:
        """Create, register, and return a stage (an :class:`Artifact`).

        ``resources`` accepts a :class:`Resources` or a raw dict; a
        ``Resources`` is rendered via :meth:`Resources.to_dict`.
        """
        res = resources.to_dict() if isinstance(resources, Resources) else dict(resources or {})
        artifact = Artifact(
            path=str(path),
            computation=Computation(
                cmd=cmd,
                deps=list(deps or []),
                git_deps=list(git_deps or []),
                git_log_deps=list(git_log_deps or []),
                resources=res,
                params=dict(params or {}),
            ),
        )
        return self.add(artifact)

    def _internal_edges(self) -> dict[str, list[str]]:
        """Map each in-DAG stage path → the in-DAG stage paths it depends on.

        A dependency is an internal edge when it names another stage's output
        path (whether declared as an ``Artifact`` or a bare path string);
        dependencies on raw inputs outside the DAG are not edges here.
        """
        edges: dict[str, list[str]] = {}
        for artifact in self.artifacts:
            key = _dep_key(artifact)
            deps = artifact.computation.deps if artifact.computation else []
            edges[key] = [
                _dep_key(dep) for dep in deps if _dep_key(dep) in self._by_path
            ]
        return edges

    def validate(self) -> None:
        """Check the DAG is well-formed: resolvable deps, acyclic.

        Raises ``ValueError`` with the offending stage(s). Uniqueness of output
        paths is enforced at :meth:`add` time. An ``Artifact`` dependency must
        be registered in this DAG (a bare-path dep may be a raw input and is
        not required to be).
        """
        for artifact in self.artifacts:
            deps = artifact.computation.deps if artifact.computation else []
            for dep in deps:
                if isinstance(dep, Artifact) and _dep_key(dep) not in self._by_path:
                    raise ValueError(
                        f"stage {_dep_key(artifact)!r} depends on unregistered "
                        f"artifact {_dep_key(dep)!r}; add it to the Dag"
                    )
        self._toposort()  # raises on cycle

    def _toposort(self) -> list[str]:
        """Return stage paths in dependency order; raise ``ValueError`` on cycle."""
        edges = self._internal_edges()
        WHITE, GREY, BLACK = 0, 1, 2
        color = dict.fromkeys(edges, WHITE)
        order: list[str] = []

        def visit(node: str, stack: list[str]) -> None:
            color[node] = GREY
            for dep in edges[node]:
                if color[dep] == GREY:
                    cycle = stack[stack.index(dep):] + [dep]
                    raise ValueError("cycle in Dag: " + " -> ".join(cycle))
                if color[dep] == WHITE:
                    visit(dep, [*stack, dep])
            color[node] = BLACK
            order.append(node)

        for node in edges:
            if color[node] == WHITE:
                visit(node, [node])
        return order

    def to_dvc(self) -> list[Path]:
        """Validate, then write every stage's ``.dvc`` file (prep phase).

        Writes in dependency order and returns the written ``.dvc`` paths.
        Placeholder stages (no output hash yet) are filled by the first
        ``dvx run``.
        """
        self.validate()
        order = self._toposort()
        return [self._by_path[key].write_dvc() for key in order]
