"""Tests for dvc.yaml parser."""

import tempfile
from pathlib import Path

import pytest

from dvx.run.parser import DVCYamlParser


def test_parse_simple_stages():
    """Test parsing simple stage definitions."""
    yaml_content = """
stages:
  stage_a:
    cmd: echo "test" > output.txt
    deps:
      - input.txt
    outs:
      - output.txt

  stage_b:
    cmd: cat input.txt
    deps:
      - input.txt
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        f.flush()
        yaml_path = Path(f.name)

    try:
        parser = DVCYamlParser(yaml_path)
        stages = parser.parse()

        assert len(stages) == 2

        stage_a = next(s for s in stages if s.name == "stage_a")
        assert stage_a.cmd == 'echo "test" > output.txt'
        assert stage_a.deps == ["input.txt"]
        assert stage_a.outs == ["output.txt"]

        stage_b = next(s for s in stages if s.name == "stage_b")
        assert stage_b.cmd == "cat input.txt"
        assert stage_b.deps == ["input.txt"]
        assert stage_b.outs == []
    finally:
        yaml_path.unlink()


def test_parse_missing_file():
    """Test error handling for missing dvc.yaml."""
    parser = DVCYamlParser(Path("/nonexistent/dvc.yaml"))

    with pytest.raises(FileNotFoundError):
        parser.parse()


def test_parse_missing_cmd():
    """Test error handling for stage without cmd."""
    yaml_content = """
stages:
  bad_stage:
    deps:
      - input.txt
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        f.flush()
        yaml_path = Path(f.name)

    try:
        parser = DVCYamlParser(yaml_path)
        with pytest.raises(ValueError, match="missing required 'cmd' field"):
            parser.parse()
    finally:
        yaml_path.unlink()


def test_parse_list_cmd():
    """Test parsing stages with list-style commands."""
    yaml_content = """
stages:
  multi_cmd:
    cmd:
      - echo "step 1"
      - echo "step 2"
      - echo "step 3"
    outs:
      - output.txt
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        f.flush()
        yaml_path = Path(f.name)

    try:
        parser = DVCYamlParser(yaml_path)
        stages = parser.parse()

        assert len(stages) == 1
        assert stages[0].cmd == 'echo "step 1" && echo "step 2" && echo "step 3"'
    finally:
        yaml_path.unlink()


def test_parse_no_stages():
    """Test error handling for yaml without stages section."""
    yaml_content = """
vars:
  - data_dir: /data
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        f.flush()
        yaml_path = Path(f.name)

    try:
        parser = DVCYamlParser(yaml_path)
        with pytest.raises(ValueError, match="must contain 'stages' section"):
            parser.parse()
    finally:
        yaml_path.unlink()


def test_parse_foreach_list():
    """Test parsing foreach stages with list values."""
    yaml_content = """
stages:
  process:
    foreach:
      - 202501
      - 202502
      - 202503
    do:
      cmd: process ${item}
      deps:
        - input/${item}.csv
      outs:
        - output/${item}.parquet
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        f.flush()
        yaml_path = Path(f.name)

    try:
        parser = DVCYamlParser(yaml_path)
        stages = parser.parse()

        assert len(stages) == 3

        # Check stage names follow DVC convention: name@item
        stage_names = {s.name for s in stages}
        assert stage_names == {"process@202501", "process@202502", "process@202503"}

        # Check variable substitution
        stage_1 = next(s for s in stages if s.name == "process@202501")
        assert stage_1.cmd == "process 202501"
        assert stage_1.deps == ["input/202501.csv"]
        assert stage_1.outs == ["output/202501.parquet"]
    finally:
        yaml_path.unlink()


def test_parse_foreach_dict():
    """Test parsing foreach stages with dict values."""
    yaml_content = """
stages:
  train:
    foreach:
      small: data/small.csv
      large: data/large.csv
    do:
      cmd: train --name ${item.key} --input ${item.value}
      deps:
        - ${item.value}
      outs:
        - models/${item.key}.pkl
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        f.flush()
        yaml_path = Path(f.name)

    try:
        parser = DVCYamlParser(yaml_path)
        stages = parser.parse()

        assert len(stages) == 2

        stage_names = {s.name for s in stages}
        assert stage_names == {"train@small", "train@large"}

        # Check key/value substitution
        stage_small = next(s for s in stages if s.name == "train@small")
        assert stage_small.cmd == "train --name small --input data/small.csv"
        assert stage_small.deps == ["data/small.csv"]
        assert stage_small.outs == ["models/small.pkl"]
    finally:
        yaml_path.unlink()


def test_parse_foreach_missing_do():
    """Test error handling for foreach without do block."""
    yaml_content = """
stages:
  bad_foreach:
    foreach:
      - item1
      - item2
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        f.flush()
        yaml_path = Path(f.name)

    try:
        parser = DVCYamlParser(yaml_path)
        with pytest.raises(ValueError, match="missing 'do' block"):
            parser.parse()
    finally:
        yaml_path.unlink()


def test_parse_mixed_stages():
    """Test parsing a mix of simple and foreach stages."""
    yaml_content = """
stages:
  setup:
    cmd: mkdir -p output
    outs:
      - output

  process:
    foreach:
      - a
      - b
    do:
      cmd: process ${item}
      deps:
        - output
        - input/${item}.txt
      outs:
        - output/${item}.out

  finalize:
    cmd: cat output/*.out > final.txt
    deps:
      - output/a.out
      - output/b.out
    outs:
      - final.txt
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        f.flush()
        yaml_path = Path(f.name)

    try:
        parser = DVCYamlParser(yaml_path)
        stages = parser.parse()

        # 1 setup + 2 process (expanded) + 1 finalize = 4
        assert len(stages) == 4

        stage_names = {s.name for s in stages}
        assert stage_names == {"setup", "process@a", "process@b", "finalize"}
    finally:
        yaml_path.unlink()
