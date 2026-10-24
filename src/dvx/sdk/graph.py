"""Pure dependency-graph algorithms, shared across dvx.

The single source of truth for transitive closure (ancestors/descendants) and
topological ordering over a string-keyed adjacency map. Both the authoring
layer (:class:`dvx.sdk.Dag`, over its stage→stage edges) and the ``dvx dag``
visualizer (:mod:`dvx.cli.dag`, over its full dep graph including raw inputs)
build on these, instead of each re-implementing the walks.

An *adjacency map* ``adj`` maps a node to the nodes reachable in one step.
Edges to keys absent from the map are followed as dead ends (``adj.get(x,
())``), so a caller may include edges to nodes outside the map (e.g. external
dependencies with no ``.dvc`` file) — the closure stops there rather than
erroring.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping


def transitive_closure(adj: Mapping[str, Iterable[str]], start: str) -> set[str]:
    """Return every node reachable from ``start`` via ``adj`` (excluding ``start``).

    Iterative DFS; safe on cycles (a visited set bounds the walk) and on edges
    that point outside ``adj`` (treated as leaves).
    """
    seen: set[str] = set()
    stack = list(adj.get(start, ()))
    while stack:
        node = stack.pop()
        if node not in seen:
            seen.add(node)
            stack.extend(adj.get(node, ()))
    return seen


def toposort(
    deps: Mapping[str, Iterable[str]],
    nodes: Iterable[str] | None = None,
    *,
    cycle_prefix: str = "cycle: ",
) -> list[str]:
    """Topologically order ``nodes`` so each node follows the deps it depends on.

    ``deps[node]`` lists the nodes ``node`` depends on (its upstreams). Ordering
    is deterministic: among nodes with no remaining unordered dep, the
    lexicographically smallest is emitted first. Only edges among ``nodes`` are
    considered (a dep outside ``nodes`` — e.g. a raw input or external file —
    does not gate ordering).

    Args:
        deps: upstream adjacency map.
        nodes: the node set to order (default: ``deps`` keys).
        cycle_prefix: prepended to the ``a -> b -> a`` cycle path in the raised
            ``ValueError`` message, so each caller can name its own structure.

    Raises:
        ValueError: if the nodes contain a cycle (message is
            ``f"{cycle_prefix}{a} -> {b} -> {a}"``).
    """
    node_set = set(deps) if nodes is None else set(nodes)
    WHITE, GREY, BLACK = 0, 1, 2
    color = dict.fromkeys(node_set, WHITE)
    order: list[str] = []

    def visit(node: str, stack: list[str]) -> None:
        color[node] = GREY
        # Deterministic: visit upstreams in sorted order.
        for dep in sorted(d for d in deps.get(node, ()) if d in node_set):
            if color[dep] == GREY:
                cycle = stack[stack.index(dep):] + [dep]
                raise ValueError(cycle_prefix + " -> ".join(cycle))
            if color[dep] == WHITE:
                visit(dep, [*stack, dep])
        color[node] = BLACK
        order.append(node)

    for node in sorted(node_set):
        if color[node] == WHITE:
            visit(node, [node])
    return order
