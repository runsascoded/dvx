"""DAG visualization for DVX artifacts.

Build and display dependency graphs from .dvc files with meta.computation.deps.
"""

import json
import os
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import click

from dvx.run.dvc_files import read_dir_manifest, read_dvc_file


@dataclass
class DagNode:
    """A node in the dependency DAG."""

    path: str  # Path to the artifact (without .dvc suffix)
    dvc_path: str  # Path to the .dvc file
    md5: str | None = None
    size: int | None = None
    cmd: str | None = None
    deps: dict[str, str] = field(default_factory=dict)  # {path: md5}
    is_dir: bool = False


@dataclass
class DependencyGraph:
    """A dependency graph built from .dvc files."""

    nodes: dict[str, DagNode] = field(default_factory=dict)  # {path: node}
    edges: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))  # {from: {to, ...}}
    reverse_edges: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))  # {to: {from, ...}}

    def add_node(self, node: DagNode) -> None:
        """Add a node to the graph."""
        self.nodes[node.path] = node
        # Add edges from deps to this node
        for dep_path in node.deps:
            self.edges[dep_path].add(node.path)
            self.reverse_edges[node.path].add(dep_path)

    def get_ancestors(self, path: str) -> set[str]:
        """Get all ancestors (dependencies) of a node."""
        ancestors = set()
        to_visit = list(self.reverse_edges.get(path, []))
        while to_visit:
            dep = to_visit.pop()
            if dep not in ancestors:
                ancestors.add(dep)
                to_visit.extend(self.reverse_edges.get(dep, []))
        return ancestors

    def get_descendants(self, path: str) -> set[str]:
        """Get all descendants (dependents) of a node."""
        descendants = set()
        to_visit = list(self.edges.get(path, []))
        while to_visit:
            dep = to_visit.pop()
            if dep not in descendants:
                descendants.add(dep)
                to_visit.extend(self.edges.get(dep, []))
        return descendants

    def get_roots(self) -> set[str]:
        """Get nodes with no dependencies (roots/sources)."""
        return {path for path, node in self.nodes.items() if not node.deps}

    def get_leaves(self) -> set[str]:
        """Get nodes with no dependents (leaves/sinks)."""
        all_deps = set()
        for node in self.nodes.values():
            all_deps.update(node.deps.keys())
        return set(self.nodes.keys()) - all_deps

    def topological_sort(self) -> list[str]:
        """Return nodes in topological order (deps before dependents)."""
        in_degree = defaultdict(int)
        for path in self.nodes:
            in_degree[path] = len(self.reverse_edges.get(path, set()) & set(self.nodes.keys()))

        # Start with roots (in_degree == 0)
        queue = [p for p, d in in_degree.items() if d == 0]
        result = []

        while queue:
            queue.sort()  # Deterministic ordering
            node = queue.pop(0)
            result.append(node)
            for dependent in self.edges.get(node, []):
                if dependent in self.nodes:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

        return result


def find_dvc_files(root: Path, pattern: str = "**/*.dvc") -> list[Path]:
    """Find all .dvc files under a directory."""
    return sorted(root.glob(pattern))


def build_graph(root: Path, targets: list[str] | None = None) -> DependencyGraph:
    """Build a dependency graph from .dvc files.

    Args:
        root: Root directory to search for .dvc files
        targets: Optional list of specific targets to include (with ancestors)

    Returns:
        DependencyGraph with all nodes and edges
    """
    graph = DependencyGraph()

    # Find all .dvc files
    dvc_files = find_dvc_files(root)

    for dvc_path in dvc_files:
        # Skip .dvc files in .dvc directory (cache, etc.)
        if ".dvc/" in str(dvc_path) or "/.dvc/" in str(dvc_path):
            continue

        info = read_dvc_file(dvc_path)
        if info is None:
            continue

        # Get artifact path (relative to root)
        artifact_path = str(dvc_path.relative_to(root))[:-4]  # Remove .dvc suffix

        node = DagNode(
            path=artifact_path,
            dvc_path=str(dvc_path.relative_to(root)),
            md5=info.md5,
            size=info.size,
            cmd=info.cmd,
            deps=info.deps or {},
            is_dir=info.is_dir,
        )
        graph.add_node(node)

    # If targets specified, filter to just those and their ancestors
    if targets:
        # Normalize target paths
        normalized_targets = set()
        for t in targets:
            # Remove .dvc suffix if present
            if t.endswith(".dvc"):
                t = t[:-4]
            normalized_targets.add(t)

        # Find all ancestors
        relevant = set(normalized_targets)
        for t in normalized_targets:
            if t in graph.nodes:
                relevant.update(graph.get_ancestors(t))

        # Filter graph to relevant nodes
        filtered = DependencyGraph()
        for path in relevant:
            if path in graph.nodes:
                filtered.add_node(graph.nodes[path])

        return filtered

    return graph


def format_ascii(graph: DependencyGraph, show_cmd: bool = False) -> str:
    """Format graph as ASCII tree.

    Shows nodes in topological order (deps before dependents), grouped by
    directory prefix for readability.
    """
    lines = []
    sorted_nodes = graph.topological_sort()

    # Group by directory prefix for cleaner output
    by_dir: dict[str, list[str]] = defaultdict(list)
    for path in sorted_nodes:
        dir_path = os.path.dirname(path) or "."
        by_dir[dir_path].append(path)

    # Sort directories, then nodes within each directory
    for dir_path in sorted(by_dir.keys()):
        paths = sorted(by_dir[dir_path])
        for path in paths:
            node = graph.nodes[path]
            hash_str = f"  {node.md5[:8]}..." if node.md5 else ""
            dir_marker = "/" if node.is_dir else ""
            dep_count = len(node.deps)
            dep_str = f"  ({dep_count} deps)" if dep_count > 0 else ""
            lines.append(f"{path}{dir_marker}{hash_str}{dep_str}")
            if show_cmd and node.cmd:
                lines.append(f"  cmd: {node.cmd}")

    return "\n".join(lines)


def format_dot(
    graph: DependencyGraph,
    show_cmd: bool = False,
    cluster_dirs: bool = True,
) -> str:
    """Format graph as Graphviz DOT with optional directory clustering."""
    lines = ["digraph DVX {"]
    lines.append("  rankdir=TB;")
    lines.append("  node [shape=box];")
    lines.append("  compound=true;")  # Allow edges to clusters
    lines.append("")

    # Build directory info: {dir_path: set of rel_paths}
    dir_contents: dict[str, set[str]] = {}
    dir_nodes: set[str] = set()

    if cluster_dirs:
        for path, node in graph.nodes.items():
            if node.is_dir:
                dir_nodes.add(path)
                # Try to read manifest from cache
                if node.md5:
                    manifest = read_dir_manifest(node.md5)
                    if manifest:
                        dir_contents[path] = set(manifest.keys())
                    else:
                        dir_contents[path] = set()
                else:
                    dir_contents[path] = set()

    # Collect all dep paths
    all_deps: set[str] = set()
    for node in graph.nodes.values():
        all_deps.update(node.deps.keys())

    # Build mapping: full_file_path -> (dir_path, rel_path)
    # First from manifests, then by detecting deps that match dir_path/... pattern
    file_to_dir: dict[str, tuple[str, str]] = {}

    # From manifests (if available)
    for dir_path, rel_paths in dir_contents.items():
        for rel_path in rel_paths:
            full_path = f"{dir_path}/{rel_path}"
            file_to_dir[full_path] = (dir_path, rel_path)

    # From deps that match directory prefix (fallback when manifests aren't cached)
    for dep in all_deps:
        if dep in file_to_dir or dep in graph.nodes:
            continue
        # Check if dep is inside any tracked directory
        for dir_path in dir_nodes:
            if dep.startswith(f"{dir_path}/"):
                rel_path = dep[len(dir_path) + 1:]
                file_to_dir[dep] = (dir_path, rel_path)
                dir_contents[dir_path].add(rel_path)
                break

    # External deps: deps that aren't nodes and aren't files inside tracked dirs
    external_deps = all_deps - set(graph.nodes.keys()) - set(file_to_dir.keys())

    # Add directory clusters
    for dir_path in sorted(dir_nodes):
        rel_paths = dir_contents[dir_path]
        if not rel_paths:
            continue  # No files inside, skip cluster

        escaped_dir = dir_path.replace('"', '\\"').replace("/", "_").replace(".", "_")
        cluster_name = f"cluster_{escaped_dir}"

        lines.append(f"  subgraph {cluster_name} {{")
        lines.append(f'    label="{dir_path}/";')
        lines.append("    style=filled; fillcolor=lightyellow;")

        # Add files inside directory
        for rel_path in sorted(rel_paths):
            full_path = f"{dir_path}/{rel_path}"
            escaped_full = full_path.replace('"', '\\"')
            # Show just filename as label
            lines.append(f'    "{escaped_full}" [label="{rel_path}"];')

        lines.append("  }")
        lines.append("")

    # Track which directories became clusters (have files inside)
    clustered_dirs = {d for d in dir_nodes if dir_contents.get(d)}

    # Add regular (non-directory or empty directory) nodes
    for path, node in sorted(graph.nodes.items()):
        if path in clustered_dirs:
            continue  # Already handled as cluster

        escaped_path = path.replace('"', '\\"')
        label = escaped_path
        if node.is_dir:
            label += "/"
        if show_cmd and node.cmd:
            cmd_short = node.cmd[:40] + "..." if len(node.cmd) > 40 else node.cmd
            cmd_short = cmd_short.replace('"', '\\"')
            label += f"\\n{cmd_short}"

        style = "style=filled,fillcolor=lightblue" if node.cmd else ""
        lines.append(f'  "{escaped_path}" [label="{label}"{", " + style if style else ""}];')

    # Add external dep nodes (ghost nodes - deps without .dvc files)
    if external_deps:
        lines.append("")
        lines.append("  // External dependencies (no .dvc file)")
        for dep in sorted(external_deps):
            escaped_dep = dep.replace('"', '\\"')
            lines.append(f'  "{escaped_dep}" [label="{escaped_dep}", style=dashed, color=gray];')

    lines.append("")

    # Add edges
    for path, node in sorted(graph.nodes.items()):
        escaped_path = path.replace('"', '\\"')

        # Check if target node is a directory cluster
        target_is_cluster = path in clustered_dirs

        for dep in sorted(node.deps.keys()):
            escaped_dep = dep.replace('"', '\\"')

            # Determine edge attributes for source (ltail) and target (lhead)
            attrs = []
            source_node = escaped_dep
            target_node = escaped_path

            # Check if dep is a file inside a tracked directory
            if dep in file_to_dir:
                # Source is a file inside a directory cluster
                dir_path, rel_path = file_to_dir[dep]
                source_node = f"{dir_path}/{rel_path}".replace('"', '\\"')
            elif dep in clustered_dirs:
                # Source is a directory cluster - pick first file, use ltail
                first_file = sorted(dir_contents[dep])[0]
                source_node = f"{dep}/{first_file}".replace('"', '\\"')
                escaped_cluster = dep.replace("/", "_").replace(".", "_")
                attrs.append(f"ltail=cluster_{escaped_cluster}")

            # If target is a directory cluster, use lhead
            if target_is_cluster:
                first_file = sorted(dir_contents[path])[0]
                target_node = f"{path}/{first_file}".replace('"', '\\"')
                escaped_cluster = path.replace("/", "_").replace(".", "_")
                attrs.append(f"lhead=cluster_{escaped_cluster}")

            attr_str = f" [{', '.join(attrs)}]" if attrs else ""
            lines.append(f'  "{source_node}" -> "{target_node}"{attr_str};')

    lines.append("}")
    return "\n".join(lines)


def format_mermaid(graph: DependencyGraph, show_cmd: bool = False) -> str:
    """Format graph as Mermaid diagram."""
    lines = ["flowchart TD"]

    # Create node IDs (mermaid doesn't like slashes in IDs)
    node_ids = {}
    for i, path in enumerate(sorted(graph.nodes.keys())):
        node_ids[path] = f"n{i}"

    # Add nodes
    for path, node in sorted(graph.nodes.items()):
        nid = node_ids[path]
        label = path
        if node.is_dir:
            label += "/"
        if show_cmd and node.cmd:
            cmd_short = node.cmd[:30] + "..." if len(node.cmd) > 30 else node.cmd
            label += f"<br/><i>{cmd_short}</i>"

        # Escape special chars
        label = label.replace('"', "'")
        lines.append(f'  {nid}["{label}"]')

    lines.append("")

    # Add edges
    for path, node in sorted(graph.nodes.items()):
        nid = node_ids[path]
        for dep in sorted(node.deps.keys()):
            if dep in node_ids:
                dep_id = node_ids[dep]
                lines.append(f"  {dep_id} --> {nid}")

    return "\n".join(lines)


def format_json(graph: DependencyGraph) -> str:
    """Format graph as JSON."""
    data = {
        "nodes": {},
        "edges": [],
    }

    for path, node in sorted(graph.nodes.items()):
        data["nodes"][path] = {
            "md5": node.md5,
            "size": node.size,
            "cmd": node.cmd,
            "deps": node.deps,
            "is_dir": node.is_dir,
        }

    for from_path, to_paths in sorted(graph.edges.items()):
        for to_path in sorted(to_paths):
            if from_path in graph.nodes or to_path in graph.nodes:
                data["edges"].append({"from": from_path, "to": to_path})

    return json.dumps(data, indent=2)


def format_html(graph: DependencyGraph) -> str:
    """Format graph as interactive HTML with D3.js force-directed layout."""
    # Build graph data for D3
    nodes = []
    node_index = {}
    for i, (path, node) in enumerate(sorted(graph.nodes.items())):
        node_index[path] = i
        nodes.append({
            "id": path,
            "md5": node.md5[:8] if node.md5 else None,
            "size": node.size,
            "cmd": node.cmd,
            "deps": list(node.deps.keys()),
            "is_dir": node.is_dir,
            "dep_count": len(node.deps),
        })

    links = []
    for from_path, to_paths in graph.edges.items():
        for to_path in to_paths:
            if from_path in node_index and to_path in node_index:
                links.append({
                    "source": node_index[from_path],
                    "target": node_index[to_path],
                })

    graph_data = json.dumps({"nodes": nodes, "links": links})

    return f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>DVX Dependency Graph</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
        #container {{ display: flex; height: 100vh; }}
        #sidebar {{
            width: 350px; padding: 15px; background: #f5f5f5;
            border-right: 1px solid #ddd; overflow-y: auto;
        }}
        #graph {{ flex: 1; }}
        svg {{ width: 100%; height: 100%; }}
        .node {{ cursor: pointer; }}
        .node circle {{ stroke: #fff; stroke-width: 2px; }}
        .node text {{ font-size: 10px; pointer-events: none; }}
        .node.root circle {{ fill: #4CAF50; }}
        .node.leaf circle {{ fill: #2196F3; }}
        .node.middle circle {{ fill: #FF9800; }}
        .node.highlighted circle {{ stroke: #f00; stroke-width: 3px; }}
        .node.ancestor circle {{ opacity: 1; }}
        .node.descendant circle {{ opacity: 1; }}
        .node.dimmed circle {{ opacity: 0.2; }}
        .node.dimmed text {{ opacity: 0.2; }}
        .link {{ stroke: #999; stroke-opacity: 0.6; fill: none; }}
        .link.highlighted {{ stroke: #f00; stroke-opacity: 1; stroke-width: 2px; }}
        .link.dimmed {{ stroke-opacity: 0.1; }}
        #search {{ width: 100%; padding: 8px; margin-bottom: 10px; border: 1px solid #ccc; border-radius: 4px; }}
        #stats {{ margin-bottom: 15px; font-size: 13px; color: #666; }}
        #details {{ background: #fff; padding: 10px; border-radius: 4px; border: 1px solid #ddd; }}
        #details h3 {{ margin-bottom: 8px; font-size: 14px; word-break: break-all; }}
        #details p {{ font-size: 12px; margin: 4px 0; color: #555; }}
        #details .cmd {{ font-family: monospace; background: #eee; padding: 4px; border-radius: 2px; word-break: break-all; }}
        #details .deps {{ margin-top: 8px; }}
        #details .dep {{ font-size: 11px; color: #666; cursor: pointer; padding: 2px 0; }}
        #details .dep:hover {{ color: #2196F3; }}
        .legend {{ margin-top: 15px; font-size: 12px; }}
        .legend-item {{ display: flex; align-items: center; margin: 4px 0; }}
        .legend-dot {{ width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; }}
        #controls {{ margin: 10px 0; }}
        #controls button {{ padding: 5px 10px; margin-right: 5px; cursor: pointer; }}
    </style>
</head>
<body>
    <div id="container">
        <div id="sidebar">
            <h2 style="margin-bottom: 15px;">DVX DAG</h2>
            <input type="text" id="search" placeholder="Search nodes...">
            <div id="controls">
                <button onclick="resetView()">Reset View</button>
                <button onclick="resetHighlight()">Clear Highlight</button>
            </div>
            <div id="stats"></div>
            <div id="details">
                <p style="color: #999;">Click a node to see details</p>
            </div>
            <div class="legend">
                <div class="legend-item"><div class="legend-dot" style="background: #4CAF50;"></div> Root (no deps)</div>
                <div class="legend-item"><div class="legend-dot" style="background: #2196F3;"></div> Leaf (no dependents)</div>
                <div class="legend-item"><div class="legend-dot" style="background: #FF9800;"></div> Intermediate</div>
            </div>
        </div>
        <div id="graph"></div>
    </div>

    <script>
        const data = {graph_data};

        // Compute node types
        const hasDependent = new Set();
        data.links.forEach(l => hasDependent.add(l.source));
        data.nodes.forEach((n, i) => {{
            n.index = i;
            if (n.dep_count === 0) n.type = 'root';
            else if (!hasDependent.has(i)) n.type = 'leaf';
            else n.type = 'middle';
        }});

        // Stats
        const roots = data.nodes.filter(n => n.type === 'root').length;
        const leaves = data.nodes.filter(n => n.type === 'leaf').length;
        document.getElementById('stats').innerHTML =
            `<strong>${{data.nodes.length}}</strong> nodes, <strong>${{data.links.length}}</strong> edges<br>` +
            `${{roots}} roots, ${{leaves}} leaves`;

        // D3 setup
        const container = document.getElementById('graph');
        const width = container.clientWidth;
        const height = container.clientHeight;

        const svg = d3.select('#graph').append('svg')
            .attr('viewBox', [0, 0, width, height]);

        const g = svg.append('g');

        // Zoom
        const zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => g.attr('transform', event.transform));
        svg.call(zoom);

        // Arrow marker
        svg.append('defs').append('marker')
            .attr('id', 'arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 20)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#999');

        // Force simulation
        const simulation = d3.forceSimulation(data.nodes)
            .force('link', d3.forceLink(data.links).id(d => d.index).distance(80))
            .force('charge', d3.forceManyBody().strength(-200))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(30));

        // Links
        const link = g.append('g')
            .selectAll('path')
            .data(data.links)
            .join('path')
            .attr('class', 'link')
            .attr('marker-end', 'url(#arrow)');

        // Nodes
        const node = g.append('g')
            .selectAll('g')
            .data(data.nodes)
            .join('g')
            .attr('class', d => `node ${{d.type}}`)
            .call(d3.drag()
                .on('start', dragstarted)
                .on('drag', dragged)
                .on('end', dragended));

        node.append('circle')
            .attr('r', d => 6 + Math.min(d.dep_count, 10));

        node.append('text')
            .attr('dx', 12)
            .attr('dy', 4)
            .text(d => d.id.split('/').pop());

        node.on('click', (event, d) => showDetails(d));

        // Simulation tick
        simulation.on('tick', () => {{
            link.attr('d', d => {{
                const dx = d.target.x - d.source.x;
                const dy = d.target.y - d.source.y;
                return `M${{d.source.x}},${{d.source.y}}L${{d.target.x}},${{d.target.y}}`;
            }});
            node.attr('transform', d => `translate(${{d.x}},${{d.y}})`);
        }});

        function dragstarted(event) {{
            if (!event.active) simulation.alphaTarget(0.3).restart();
            event.subject.fx = event.subject.x;
            event.subject.fy = event.subject.y;
        }}

        function dragged(event) {{
            event.subject.fx = event.x;
            event.subject.fy = event.y;
        }}

        function dragended(event) {{
            if (!event.active) simulation.alphaTarget(0);
            event.subject.fx = null;
            event.subject.fy = null;
        }}

        function showDetails(d) {{
            // Highlight ancestors and descendants
            const ancestors = new Set();
            const descendants = new Set();

            function findAncestors(idx) {{
                data.links.forEach(l => {{
                    if (l.target.index === idx && !ancestors.has(l.source.index)) {{
                        ancestors.add(l.source.index);
                        findAncestors(l.source.index);
                    }}
                }});
            }}

            function findDescendants(idx) {{
                data.links.forEach(l => {{
                    if (l.source.index === idx && !descendants.has(l.target.index)) {{
                        descendants.add(l.target.index);
                        findDescendants(l.target.index);
                    }}
                }});
            }}

            findAncestors(d.index);
            findDescendants(d.index);

            node.classed('highlighted', n => n.index === d.index)
                .classed('ancestor', n => ancestors.has(n.index))
                .classed('descendant', n => descendants.has(n.index))
                .classed('dimmed', n => n.index !== d.index && !ancestors.has(n.index) && !descendants.has(n.index));

            link.classed('highlighted', l =>
                    (l.source.index === d.index || ancestors.has(l.source.index)) &&
                    (l.target.index === d.index || descendants.has(l.target.index)) ||
                    ancestors.has(l.target.index) && (ancestors.has(l.source.index) || l.target.index === d.index) ||
                    descendants.has(l.source.index) && (descendants.has(l.target.index) || l.source.index === d.index))
                .classed('dimmed', l =>
                    !ancestors.has(l.source.index) && !ancestors.has(l.target.index) &&
                    !descendants.has(l.source.index) && !descendants.has(l.target.index) &&
                    l.source.index !== d.index && l.target.index !== d.index);

            // Show details
            let html = `<h3>${{d.id}}${{d.is_dir ? '/' : ''}}</h3>`;
            if (d.md5) html += `<p><strong>Hash:</strong> ${{d.md5}}...</p>`;
            if (d.size) html += `<p><strong>Size:</strong> ${{d.size.toLocaleString()}} bytes</p>`;
            if (d.cmd) html += `<p><strong>Command:</strong></p><p class="cmd">${{d.cmd}}</p>`;
            if (d.deps.length > 0) {{
                html += `<div class="deps"><strong>Dependencies (${{d.deps.length}}):</strong>`;
                d.deps.forEach(dep => {{
                    html += `<div class="dep" onclick="focusNode('${{dep}}')">${{dep}}</div>`;
                }});
                html += `</div>`;
            }}
            html += `<p style="margin-top: 8px; color: #999;"><strong>Ancestors:</strong> ${{ancestors.size}} | <strong>Descendants:</strong> ${{descendants.size}}</p>`;
            document.getElementById('details').innerHTML = html;
        }}

        function focusNode(id) {{
            const n = data.nodes.find(n => n.id === id);
            if (n) {{
                showDetails(n);
                svg.transition().duration(500).call(
                    zoom.transform,
                    d3.zoomIdentity.translate(width/2 - n.x, height/2 - n.y)
                );
            }}
        }}

        function resetHighlight() {{
            node.classed('highlighted ancestor descendant dimmed', false);
            link.classed('highlighted dimmed', false);
            document.getElementById('details').innerHTML = '<p style="color: #999;">Click a node to see details</p>';
        }}

        function resetView() {{
            svg.transition().duration(500).call(zoom.transform, d3.zoomIdentity);
        }}

        // Search
        document.getElementById('search').addEventListener('input', (e) => {{
            const query = e.target.value.toLowerCase();
            if (!query) {{
                node.classed('dimmed', false);
                link.classed('dimmed', false);
                return;
            }}
            node.classed('dimmed', d => !d.id.toLowerCase().includes(query));
            link.classed('dimmed', true);
        }});
    </script>
</body>
</html>'''


@click.command("dag")
@click.argument("targets", nargs=-1)
@click.option("-c", "--cmd", is_flag=True, help="Show commands in output.")
@click.option("--dot", "output_format", flag_value="dot", help="Output in Graphviz DOT format.")
@click.option("--html", "output_format", flag_value="html", help="Output as interactive HTML with D3.js.")
@click.option("--json", "output_format", flag_value="json", help="Output in JSON format.")
@click.option("--md", "output_format", flag_value="md", help="Output as Mermaid in Markdown block.")
@click.option("--mermaid", "output_format", flag_value="mermaid", help="Output in Mermaid format.")
@click.option("-O", "--open", "open_browser", is_flag=True, help="Open HTML output in browser.")
@click.option("-o", "--outs", is_flag=True, help="Show output paths instead of .dvc paths.")
@click.option("-s", "--stats", is_flag=True, help="Show graph statistics.")
def dag(targets, cmd, output_format, open_browser, outs, stats):
    """Visualize DVX dependency graph.

    Builds a DAG from .dvc files with meta.computation.deps and displays
    it in various formats.

    Examples:

    \b
        dvx dag                    # ASCII list of all artifacts
        dvx dag output.dvc         # Show ancestors of specific target
        dvx dag --dot              # Graphviz DOT format
        dvx dag --mermaid          # Mermaid diagram
        dvx dag --json             # JSON for web apps
        dvx dag --html             # Interactive HTML with D3.js
        dvx dag --html -O          # Open in browser
        dvx dag -s                 # Show statistics
    """
    from dvc.repo import Repo as DVCRepo

    try:
        root = Path(DVCRepo.find_root())
    except Exception:
        root = Path.cwd()

    # Build the graph
    target_list = list(targets) if targets else None
    graph = build_graph(root, target_list)

    if not graph.nodes:
        click.echo("No .dvc files with dependencies found.", err=True)
        return

    # Show statistics if requested
    if stats:
        click.echo(f"Nodes: {len(graph.nodes)}")
        click.echo(f"Edges: {sum(len(e) for e in graph.edges.values())}")
        click.echo(f"Roots (no deps): {len(graph.get_roots())}")
        click.echo(f"Leaves (no dependents): {len(graph.get_leaves())}")
        nodes_with_cmd = sum(1 for n in graph.nodes.values() if n.cmd)
        click.echo(f"Nodes with cmd: {nodes_with_cmd}")
        if not output_format:
            return

    # Format output
    if output_format == "dot":
        output = format_dot(graph, show_cmd=cmd)
    elif output_format == "html":
        output = format_html(graph)
        if open_browser:
            import tempfile
            import webbrowser
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
                f.write(output)
                webbrowser.open(f'file://{f.name}')
                click.echo(f"Opened {f.name} in browser", err=True)
                return
    elif output_format == "mermaid":
        if len(graph.nodes) > 100:
            click.echo(
                f"Warning: {len(graph.nodes)} nodes may exceed Mermaid size limits. "
                "Consider --html or filtering with targets.",
                err=True,
            )
        output = format_mermaid(graph, show_cmd=cmd)
    elif output_format == "md":
        if len(graph.nodes) > 100:
            click.echo(
                f"Warning: {len(graph.nodes)} nodes may exceed Mermaid size limits. "
                "Consider --html or filtering with targets.",
                err=True,
            )
        output = "```mermaid\n" + format_mermaid(graph, show_cmd=cmd) + "\n```"
    elif output_format == "json":
        output = format_json(graph)
    else:
        output = format_ascii(graph, show_cmd=cmd)

    click.echo(output)
