"""Stage model representing a DVC pipeline stage."""

from dataclasses import dataclass, field


@dataclass
class Stage:
    """Represents a single DVC pipeline stage."""

    name: str
    cmd: str
    deps: list[str] = field(default_factory=list)
    outs: list[str] = field(default_factory=list)
    desc: str | None = None

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, Stage):
            return False
        return self.name == other.name

    def get_dependency_files(self) -> set[str]:
        """Get all file dependencies for this stage."""
        return set(self.deps)

    def get_output_files(self) -> set[str]:
        """Get all output files for this stage."""
        return set(self.outs)
