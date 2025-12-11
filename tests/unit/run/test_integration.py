"""Integration tests for dvc-run CLI."""

import subprocess
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def dvc_project():
    """Create a temporary DVC project."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir)

        # Initialize git
        subprocess.run(["git", "init"], cwd=project_dir, check=True, capture_output=True)
        subprocess.run(
            ["git", "config", "user.email", "test@example.com"],
            cwd=project_dir,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Test User"],
            cwd=project_dir,
            check=True,
            capture_output=True,
        )

        # Initialize DVC
        subprocess.run(["dvc", "init"], cwd=project_dir, check=True, capture_output=True)

        yield project_dir


def test_simple_pipeline(dvc_project):
    """Test running a simple pipeline."""
    dvc_yaml = dvc_project / "dvc.yaml"
    dvc_yaml.write_text("""
stages:
  stage_a:
    cmd: echo "test" > output.txt
    outs:
      - output.txt
""")

    # Run dvc-run
    result = subprocess.run(
        ["dvc-run"],
        cwd=dvc_project,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert (dvc_project / "output.txt").exists()
    assert (dvc_project / "output.txt").read_text().strip() == "test"


def test_parallel_execution(dvc_project):
    """Test parallel execution of independent stages."""
    dvc_yaml = dvc_project / "dvc.yaml"
    dvc_yaml.write_text("""
stages:
  stage_a:
    cmd: echo "a" > a.txt
    outs:
      - a.txt

  stage_b:
    cmd: echo "b" > b.txt
    outs:
      - b.txt

  stage_c:
    cmd: cat a.txt b.txt > c.txt
    deps:
      - a.txt
      - b.txt
    outs:
      - c.txt
""")

    result = subprocess.run(
        ["dvc-run"],
        cwd=dvc_project,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert (dvc_project / "a.txt").exists()
    assert (dvc_project / "b.txt").exists()
    assert (dvc_project / "c.txt").exists()
    assert (dvc_project / "c.txt").read_text() == "a\nb\n"


def test_dry_run(dvc_project):
    """Test dry-run mode."""
    dvc_yaml = dvc_project / "dvc.yaml"
    dvc_yaml.write_text("""
stages:
  stage_a:
    cmd: echo "test" > output.txt
    outs:
      - output.txt
""")

    result = subprocess.run(
        ["dvc-run", "--dry-run"],
        cwd=dvc_project,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Dry run" in result.stderr
    assert not (dvc_project / "output.txt").exists()


def test_incremental_execution(dvc_project):
    """Test that stages are skipped when up-to-date."""
    dvc_yaml = dvc_project / "dvc.yaml"
    dvc_yaml.write_text("""
stages:
  stage_a:
    cmd: echo "test" > output.txt
    outs:
      - output.txt
""")

    # First run
    result1 = subprocess.run(
        ["dvc-run"],
        cwd=dvc_project,
        capture_output=True,
        text=True,
    )
    assert result1.returncode == 0
    assert "Executed: 1" in result1.stderr

    # Second run - should skip
    result2 = subprocess.run(
        ["dvc-run"],
        cwd=dvc_project,
        capture_output=True,
        text=True,
    )
    assert result2.returncode == 0
    assert "Skipped (up-to-date): 1" in result2.stderr
    assert "Executed: 0" in result2.stderr


def test_failing_stage(dvc_project):
    """Test that failing stages cause proper error."""
    dvc_yaml = dvc_project / "dvc.yaml"
    dvc_yaml.write_text("""
stages:
  stage_a:
    cmd: exit 1
    outs:
      - output.txt
""")

    result = subprocess.run(
        ["dvc-run"],
        cwd=dvc_project,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "failed" in result.stderr.lower()


def test_long_running_parallel_stages(dvc_project):
    """Test parallel execution with long-running stages to verify no lock contention.

    This test creates 4 independent stages that each sleep for 3 seconds.
    If they run in parallel, total time should be ~3s.
    If they run serially (due to lock contention), it would be ~12s.
    """
    import time

    dvc_yaml = dvc_project / "dvc.yaml"
    dvc_yaml.write_text("""
stages:
  slow_a:
    cmd: sleep 3 && echo "A" > output_a.txt
    outs:
      - output_a.txt
  slow_b:
    cmd: sleep 3 && echo "B" > output_b.txt
    outs:
      - output_b.txt
  slow_c:
    cmd: sleep 3 && echo "C" > output_c.txt
    outs:
      - output_c.txt
  slow_d:
    cmd: sleep 3 && echo "D" > output_d.txt
    outs:
      - output_d.txt
""")

    start_time = time.time()
    result = subprocess.run(
        ["dvc-run"],
        cwd=dvc_project,
        capture_output=True,
        text=True,
    )
    elapsed = time.time() - start_time

    assert result.returncode == 0
    assert "Executed: 4" in result.stderr

    # Verify outputs were created
    assert (dvc_project / "output_a.txt").read_text().strip() == "A"
    assert (dvc_project / "output_b.txt").read_text().strip() == "B"
    assert (dvc_project / "output_c.txt").read_text().strip() == "C"
    assert (dvc_project / "output_d.txt").read_text().strip() == "D"

    # Verify true parallelism: should complete in ~3s, not ~12s
    # Allow some overhead for process spawning, but should be well under 6s
    assert elapsed < 6.0, f"Took {elapsed:.1f}s - stages may have run serially"
    print(f"✓ 4 stages completed in {elapsed:.1f}s (expected ~3s for parallel)")


def test_dot_export(dvc_project):
    """Test DOT export."""
    dvc_yaml = dvc_project / "dvc.yaml"
    dvc_yaml.write_text("""
stages:
  stage_a:
    cmd: echo "test" > output.txt
    outs:
      - output.txt
""")

    dot_file = dvc_project / "pipeline.dot"
    result = subprocess.run(
        ["dvc-run", "--dot", str(dot_file)],
        cwd=dvc_project,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert dot_file.exists()
    content = dot_file.read_text()
    assert "digraph pipeline" in content
    assert "stage_a" in content


def test_mermaid_export(dvc_project):
    """Test Mermaid export."""
    dvc_yaml = dvc_project / "dvc.yaml"
    dvc_yaml.write_text("""
stages:
  stage_a:
    cmd: echo "a" > a.txt
    outs:
      - a.txt

  stage_b:
    cmd: cat a.txt > b.txt
    deps:
      - a.txt
    outs:
      - b.txt
""")

    mermaid_file = dvc_project / "pipeline.mmd"
    result = subprocess.run(
        ["dvc-run", "--mermaid", str(mermaid_file)],
        cwd=dvc_project,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert mermaid_file.exists()
    content = mermaid_file.read_text()
    assert "graph LR" in content
    assert "stage_a --> stage_b" in content
