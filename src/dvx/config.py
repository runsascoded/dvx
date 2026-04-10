"""DVX configuration file loading.

Loads run configuration from ``.dvx/config.yml`` or ``dvx.yml`` in the
repo root. Supports global defaults and per-stage overrides.

Example config::

    # .dvx/config.yml
    run:
      commit: auto       # auto | always | never
      push: end          # never | each | end

      stages:
        njsp/data/refresh.dvc:
          push: each     # push immediately after this stage
        api/d1-import.dvc:
          commit: never  # don't commit for this stage

Priority: CLI flags > env vars > config file > defaults.
"""

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class StageConfig:
    """Per-stage configuration overrides."""

    commit: str | None = None  # "auto", "always", "never", or None (inherit)
    push: str | None = None  # "never", "each", "end", or None (inherit)


@dataclass
class RunConfig:
    """Configuration for ``dvx run``."""

    commit: str = "auto"  # "auto" | "always" | "never"
    push: str = "never"  # "never" | "each" | "end"
    stages: dict[str, StageConfig] = field(default_factory=dict)

    def get_stage_config(self, dvc_path: str) -> StageConfig:
        """Get merged config for a specific stage.

        Tries exact match, then without .dvc suffix, then with .dvc suffix.
        """
        # Normalize: try with and without .dvc suffix
        candidates = [dvc_path]
        if dvc_path.endswith(".dvc"):
            candidates.append(dvc_path[:-4])
        else:
            candidates.append(dvc_path + ".dvc")

        for candidate in candidates:
            if candidate in self.stages:
                return self.stages[candidate]

        return StageConfig()

    def should_commit(self, dvc_path: str) -> str:
        """Get effective commit strategy for a stage."""
        stage = self.get_stage_config(dvc_path)
        return stage.commit if stage.commit is not None else self.commit

    def should_push(self, dvc_path: str) -> str:
        """Get effective push strategy for a stage."""
        stage = self.get_stage_config(dvc_path)
        return stage.push if stage.push is not None else self.push


def load_config(root: Path | None = None) -> RunConfig:
    """Load DVX configuration from the repo root.

    Searches for ``.dvx/config.yml`` or ``dvx.yml``.

    Args:
        root: Repo root directory (default: walk up from cwd to find .dvc/)

    Returns:
        RunConfig (defaults if no config file found)
    """
    if root is None:
        # Walk up to find repo root (directory containing .dvc/)
        cwd = Path.cwd()
        for parent in [cwd, *cwd.parents]:
            if (parent / ".dvc").is_dir():
                root = parent
                break
        if root is None:
            return RunConfig()

    # Try config file locations
    candidates = [
        root / ".dvx" / "config.yml",
        root / "dvx.yml",
    ]

    for config_path in candidates:
        if config_path.exists():
            return _parse_config(config_path)

    return RunConfig()


def _parse_config(config_path: Path) -> RunConfig:
    """Parse a DVX config file."""
    with open(config_path) as f:
        data = yaml.safe_load(f)

    if not data or not isinstance(data, dict):
        return RunConfig()

    run_data = data.get("run", {})
    if not isinstance(run_data, dict):
        return RunConfig()

    # Parse per-stage overrides
    stages = {}
    stages_data = run_data.get("stages", {})
    if isinstance(stages_data, dict):
        for stage_path, stage_data in stages_data.items():
            if isinstance(stage_data, dict):
                stages[stage_path] = StageConfig(
                    commit=stage_data.get("commit"),
                    push=stage_data.get("push"),
                )

    return RunConfig(
        commit=str(run_data.get("commit", "auto")),
        push=str(run_data.get("push", "never")),
        stages=stages,
    )
