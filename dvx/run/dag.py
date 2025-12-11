"""Dependency graph builder for DVC stages."""

from collections import defaultdict, deque

from dvx.run.stage import Stage


class DAG:
    """Directed acyclic graph of stage dependencies."""

    def __init__(self, stages: list[Stage]):
        self.stages = {stage.name: stage for stage in stages}
        self.graph: dict[str, set[str]] = defaultdict(set)
        self.reverse_graph: dict[str, set[str]] = defaultdict(set)
        self._build_graph()

    def _build_graph(self):
        """Build dependency graph from stage definitions.

        For each stage, determine which other stages it depends on by
        checking if any of its dependencies are outputs of other stages.
        """
        # Build output-to-stage mapping
        output_to_stage: dict[str, str] = {}
        for stage_name, stage in self.stages.items():
            for output in stage.get_output_files():
                output_to_stage[output] = stage_name

        # Build dependency edges
        for stage_name, stage in self.stages.items():
            for dep in stage.get_dependency_files():
                # If this dependency is an output of another stage, add edge
                if dep in output_to_stage:
                    producer_stage = output_to_stage[dep]
                    # Edge: producer -> consumer (stage depends on producer)
                    self.graph[producer_stage].add(stage_name)
                    self.reverse_graph[stage_name].add(producer_stage)

    def get_dependencies(self, stage_name: str) -> set[str]:
        """Get all stages that this stage depends on."""
        return self.reverse_graph.get(stage_name, set())

    def get_dependents(self, stage_name: str) -> set[str]:
        """Get all stages that depend on this stage."""
        return self.graph.get(stage_name, set())

    def check_cycles(self) -> list[str] | None:
        """Check for cycles in the dependency graph.

        Returns:
            None if no cycles, otherwise a list of stage names forming a cycle
        """
        visited = set()
        rec_stack = set()

        def visit(node: str, path: list[str]) -> list[str] | None:
            if node in rec_stack:
                # Found cycle - return the cyclic portion
                cycle_start = path.index(node)
                return path[cycle_start:] + [node]

            if node in visited:
                return None

            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in self.graph.get(node, set()):
                cycle = visit(neighbor, path.copy())
                if cycle:
                    return cycle

            rec_stack.remove(node)
            return None

        for stage_name in self.stages:
            if stage_name not in visited:
                cycle = visit(stage_name, [])
                if cycle:
                    return cycle

        return None

    def topological_sort(self) -> list[list[str]]:
        """Perform topological sort and return stages grouped by execution level.

        Returns:
            List of levels, where each level is a list of stage names that can
            be executed in parallel.

        Raises:
            ValueError: If graph contains cycles
        """
        cycle = self.check_cycles()
        if cycle:
            raise ValueError(f"Circular dependency detected: {' -> '.join(cycle)}")

        # Kahn's algorithm for topological sort
        in_degree = {stage: len(self.get_dependencies(stage)) for stage in self.stages}
        levels = []
        queue = deque([stage for stage, degree in in_degree.items() if degree == 0])

        while queue:
            # All stages with in-degree 0 can run in parallel at this level
            current_level = list(queue)
            levels.append(current_level)
            queue.clear()

            # Process all stages in current level
            for stage_name in current_level:
                # Reduce in-degree for dependent stages
                for dependent in self.get_dependents(stage_name):
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

        # Verify all stages were processed
        if sum(len(level) for level in levels) != len(self.stages):
            raise ValueError("Failed to process all stages - possible cycle")

        return levels

    def filter_to_targets(self, target_stages: list[str]) -> 'DAG':
        """Create a new DAG containing only target stages and their dependencies.

        Args:
            target_stages: List of stage names to include

        Returns:
            New DAG with filtered stages

        Raises:
            ValueError: If any target stage doesn't exist
        """
        # Validate all targets exist
        missing = set(target_stages) - set(self.stages.keys())
        if missing:
            raise ValueError(f"Stage(s) not found: {', '.join(sorted(missing))}")

        # Collect all stages needed (targets + transitive dependencies)
        needed_stages = set()
        to_process = list(target_stages)

        while to_process:
            stage_name = to_process.pop()
            if stage_name in needed_stages:
                continue

            needed_stages.add(stage_name)

            # Add all dependencies
            deps = self.get_dependencies(stage_name)
            to_process.extend(deps)

        # Create new DAG with filtered stages
        filtered_stage_objs = [
            self.stages[name]
            for name in needed_stages
        ]

        return DAG(filtered_stage_objs)
