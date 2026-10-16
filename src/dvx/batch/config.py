"""Named execution profiles, loaded without contacting a cloud provider."""

from pathlib import Path

import yaml


def load_batch_config(root: Path | None = None) -> dict:
    directories = [root] if root is not None else [Path.cwd(), *Path.cwd().parents]
    for directory in directories:
        for relative in (".dvx/config.yml", "dvx.yml"):
            path = directory / relative
            if not path.is_file():
                continue
            data = yaml.safe_load(path.read_text()) or {}
            if not isinstance(data, dict) or not isinstance(data.get("batch", {}), dict):
                raise ValueError(f"{path}: batch configuration must be a mapping")
            return data.get("batch", {})
    return {}


def load_profile(name: str = "default", root: Path | None = None) -> dict:
    profiles = load_batch_config(root).get("profiles", {})
    if not isinstance(profiles, dict) or name not in profiles:
        raise ValueError(f"Batch profile {name!r} not found in .dvx/config.yml")
    profile = profiles[name]
    if not isinstance(profile, dict):
        raise ValueError(f"Batch profile {name!r} must be a mapping")
    return dict(profile)
