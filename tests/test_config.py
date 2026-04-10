"""Tests for dvx.config module."""

from pathlib import Path

import yaml

from dvx.config import RunConfig, StageConfig, load_config, _parse_config


def test_default_config():
    """Default RunConfig has sensible defaults."""
    config = RunConfig()
    assert config.commit == "auto"
    assert config.push == "never"
    assert config.stages == {}


def test_stage_config_override():
    """Per-stage config overrides global defaults."""
    config = RunConfig(
        commit="auto",
        push="never",
        stages={
            "deploy.dvc": StageConfig(push="each"),
            "import.dvc": StageConfig(commit="never"),
        },
    )

    assert config.should_commit("deploy.dvc") == "auto"
    assert config.should_push("deploy.dvc") == "each"

    assert config.should_commit("import.dvc") == "never"
    assert config.should_push("import.dvc") == "never"

    # Unmatched stage inherits global
    assert config.should_commit("other.dvc") == "auto"
    assert config.should_push("other.dvc") == "never"


def test_stage_config_path_normalization():
    """Stage config matches with or without .dvc suffix."""
    config = RunConfig(
        stages={"njsp/data/refresh.dvc": StageConfig(push="each")},
    )

    # Match with suffix
    assert config.should_push("njsp/data/refresh.dvc") == "each"
    # Match without suffix
    assert config.should_push("njsp/data/refresh") == "each"


def test_parse_config_file(tmp_path):
    """Parse a .dvx/config.yml file."""
    config_file = tmp_path / "config.yml"
    config_data = {
        "run": {
            "commit": "always",
            "push": "end",
            "stages": {
                "deploy.dvc": {"push": "each"},
                "import.dvc": {"commit": "never"},
            },
        }
    }
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = _parse_config(config_file)

    assert config.commit == "always"
    assert config.push == "end"
    assert config.should_push("deploy.dvc") == "each"
    assert config.should_commit("import.dvc") == "never"


def test_load_config_dvx_dir(tmp_path):
    """load_config finds .dvx/config.yml."""
    dvx_dir = tmp_path / ".dvx"
    dvx_dir.mkdir()
    config_file = dvx_dir / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump({"run": {"push": "end"}}, f)

    # Also need .dvc dir for repo root detection
    (tmp_path / ".dvc").mkdir()

    config = load_config(tmp_path)
    assert config.push == "end"


def test_load_config_dvx_yml(tmp_path):
    """load_config finds dvx.yml in repo root."""
    config_file = tmp_path / "dvx.yml"
    with open(config_file, "w") as f:
        yaml.dump({"run": {"commit": "always"}}, f)

    (tmp_path / ".dvc").mkdir()

    config = load_config(tmp_path)
    assert config.commit == "always"


def test_load_config_missing(tmp_path):
    """load_config returns defaults when no config file exists."""
    (tmp_path / ".dvc").mkdir()

    config = load_config(tmp_path)
    assert config.commit == "auto"
    assert config.push == "never"


def test_load_config_empty_file(tmp_path):
    """load_config handles empty config file."""
    dvx_dir = tmp_path / ".dvx"
    dvx_dir.mkdir()
    (dvx_dir / "config.yml").write_text("")
    (tmp_path / ".dvc").mkdir()

    config = load_config(tmp_path)
    assert config.commit == "auto"
    assert config.push == "never"
