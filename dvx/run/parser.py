"""Parser for dvc.yaml pipeline files."""

from pathlib import Path
from typing import Any

from ruamel.yaml import YAML

from dvx.run.stage import Stage


class DVCYamlParser:
    """Parse dvc.yaml files and extract stage definitions."""

    def __init__(self, dvc_yaml_path: Path = Path("dvc.yaml")):
        self.dvc_yaml_path = dvc_yaml_path

    def parse(self) -> list[Stage]:
        """Parse dvc.yaml and return list of Stage objects.

        Handles both simple stages and foreach/matrix stages.
        Foreach stages are expanded into multiple concrete stages.

        Returns:
            List of Stage objects, one per stage in dvc.yaml

        Raises:
            FileNotFoundError: If dvc.yaml doesn't exist
            ValueError: If dvc.yaml is malformed
        """
        if not self.dvc_yaml_path.exists():
            raise FileNotFoundError(f"dvc.yaml not found at {self.dvc_yaml_path}")

        yaml = YAML(typ="safe")
        with open(self.dvc_yaml_path) as f:
            data = yaml.load(f)

        if not data or "stages" not in data:
            raise ValueError("dvc.yaml must contain 'stages' section")

        stages = []
        for stage_name, stage_config in data["stages"].items():
            if "foreach" in stage_config:
                # Expand foreach stage into multiple concrete stages
                stages.extend(self._expand_foreach(stage_name, stage_config))
            else:
                stages.append(self._parse_stage(stage_name, stage_config))

        return stages

    def _expand_foreach(self, name: str, config: dict[str, Any]) -> list[Stage]:
        """Expand a foreach stage into multiple concrete stages.

        Supports both list and dict foreach values:
        - List: foreach: [a, b, c] -> stages named "name@a", "name@b", "name@c"
        - Dict: foreach: {k1: v1, k2: v2} -> stages with ${item.key} and ${item.value}

        Args:
            name: Base stage name
            config: Stage configuration with 'foreach' and 'do' keys

        Returns:
            List of expanded Stage objects
        """
        foreach_items = config.get("foreach", [])
        do_config = config.get("do", {})

        if not do_config:
            raise ValueError(f"Stage '{name}' has 'foreach' but missing 'do' block")

        stages = []
        for item in foreach_items:
            if isinstance(foreach_items, dict):
                # Dict foreach: item is the key, value is foreach_items[item]
                item_key = item
                item_value = foreach_items[item]
                expanded_name = f"{name}@{item_key}"
                substitutions = {
                    "item": str(item_value),
                    "item.key": str(item_key),
                    "item.value": str(item_value),
                }
            else:
                # List foreach: item is the value
                expanded_name = f"{name}@{item}"
                substitutions = {"item": str(item)}

            expanded_config = self._substitute_vars(do_config, substitutions)
            stage = self._parse_stage(expanded_name, expanded_config)
            stages.append(stage)

        return stages

    def _substitute_vars(self, config: dict[str, Any], subs: dict[str, str]) -> dict[str, Any]:
        """Substitute ${var} placeholders in a config dict.

        Args:
            config: Configuration dict (possibly nested)
            subs: Substitution mapping {var_name: value}

        Returns:
            New config dict with substitutions applied
        """
        def substitute(value: Any) -> Any:
            if isinstance(value, str):
                result = value
                for var, replacement in subs.items():
                    result = result.replace(f"${{{var}}}", replacement)
                return result
            elif isinstance(value, list):
                return [substitute(v) for v in value]
            elif isinstance(value, dict):
                return {k: substitute(v) for k, v in value.items()}
            else:
                return value

        return substitute(config)

    def _parse_stage(self, name: str, config: dict[str, Any]) -> Stage:
        """Parse a single stage configuration.

        Args:
            name: Stage name
            config: Stage configuration dict

        Returns:
            Stage object

        Raises:
            ValueError: If stage config is invalid
        """
        if "cmd" not in config:
            raise ValueError(f"Stage '{name}' missing required 'cmd' field")

        # Extract command (can be string or list)
        cmd = config["cmd"]
        if isinstance(cmd, list):
            cmd = " && ".join(cmd)

        # Extract dependencies
        deps = config.get("deps", [])
        if isinstance(deps, dict):
            # Handle params files and other dependency types
            deps = list(deps.values())
        elif not isinstance(deps, list):
            deps = [deps]

        # Extract outputs
        outs = config.get("outs", [])
        if isinstance(outs, dict):
            outs = list(outs.values())
        elif not isinstance(outs, list):
            outs = [outs]

        # Extract description
        desc = config.get("desc")

        return Stage(
            name=name,
            cmd=cmd,
            deps=deps,
            outs=outs,
            desc=desc,
        )
