"""E2E tests for S3 import-url --no-download, pull, and update.

Requires network access and dvc-s3. Uses a small public file from the
Materials Project S3 bucket (anonymous access).
"""

import os
import subprocess
from pathlib import Path

import pytest
import yaml


# Small public file (~14MB); change if it disappears
S3_URL = "s3://materialsproject-parsed/chgcars/mp-1775579.json.gz"
OUT_NAME = "mp-1775579.json.gz"

pytestmark = pytest.mark.skipif(
    os.environ.get("DVX_TEST_S3") != "1",
    reason="Set DVX_TEST_S3=1 to run S3 integration tests",
)


@pytest.fixture
def dvc_repo(tmp_path):
    """Create a temporary git+dvc repo."""
    subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=tmp_path, capture_output=True, check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=tmp_path, capture_output=True, check=True,
    )
    subprocess.run(["dvc", "init"], cwd=tmp_path, capture_output=True, check=True)
    return tmp_path


def _run_dvx(args, cwd):
    """Run dvx as subprocess, return (stdout, stderr, returncode)."""
    result = subprocess.run(
        ["dvx", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return result.stdout, result.stderr, result.returncode


def _load_dvc(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def test_import_url_no_download(dvc_repo):
    """import-url --no-download creates .dvc with ETag but no content md5."""
    stdout, stderr, rc = _run_dvx([
        "import-url", "--no-download",
        "--fs-config", "allow_anonymous_login=true",
        S3_URL, "-o", OUT_NAME,
    ], dvc_repo)
    assert rc == 0, f"import-url failed: {stderr}"
    assert "Tracked" in stdout

    dvc_file = dvc_repo / f"{OUT_NAME}.dvc"
    assert dvc_file.exists(), f"Expected {dvc_file}"

    dvc_data = _load_dvc(dvc_file)

    # deps should have etag
    deps = dvc_data.get("deps", [])
    assert len(deps) == 1
    assert deps[0].get("etag"), f"Missing etag in deps: {deps[0]}"
    assert S3_URL in deps[0]["path"]

    # outs should have null/missing md5 (not downloaded)
    outs = dvc_data.get("outs", [])
    assert len(outs) == 1
    assert not outs[0].get("md5"), f"Expected null md5 in outs, got: {outs[0].get('md5')}"

    # data file should NOT exist
    assert not (dvc_repo / OUT_NAME).exists()


def test_pull_after_no_download(dvc_repo):
    """pull after import-url --no-download fetches the file from S3."""
    # Setup: import without downloading
    _, stderr, rc = _run_dvx([
        "import-url", "--no-download",
        "--fs-config", "allow_anonymous_login=true",
        S3_URL, "-o", OUT_NAME,
    ], dvc_repo)
    assert rc == 0, f"import-url failed: {stderr}"

    # Pull should fetch from source
    stdout, stderr, rc = _run_dvx(["pull", OUT_NAME], dvc_repo)
    assert rc == 0, f"pull failed: {stderr}"

    # File should now exist
    data_file = dvc_repo / OUT_NAME
    assert data_file.exists(), "Data file not created by pull"
    assert data_file.stat().st_size > 0, "Data file is empty"


def test_update_no_download(dvc_repo):
    """update --no-download re-checks ETag without downloading."""
    # Setup: import without downloading
    _, stderr, rc = _run_dvx([
        "import-url", "--no-download",
        "--fs-config", "allow_anonymous_login=true",
        S3_URL, "-o", OUT_NAME,
    ], dvc_repo)
    assert rc == 0, f"import-url failed: {stderr}"

    dvc_file = dvc_repo / f"{OUT_NAME}.dvc"
    etag_before = _load_dvc(dvc_file)["deps"][0]["etag"]

    # Commit .dvc so update doesn't complain about uncommitted changes
    subprocess.run(
        ["git", "add", f"{OUT_NAME}.dvc", ".gitignore"],
        cwd=dvc_repo, capture_output=True, check=True,
    )
    subprocess.run(
        ["git", "commit", "-m", "track"],
        cwd=dvc_repo, capture_output=True, check=True,
    )

    # Update: re-check ETag
    stdout, stderr, rc = _run_dvx([
        "update", "--no-download", f"{OUT_NAME}.dvc",
    ], dvc_repo)
    assert rc == 0, f"update failed: {stderr}"

    # ETag should be unchanged (file hasn't changed on S3)
    etag_after = _load_dvc(dvc_file)["deps"][0]["etag"]
    assert etag_before == etag_after, f"ETag changed: {etag_before} -> {etag_after}"


def test_full_roundtrip(dvc_repo):
    """Full flow: import --no-download → pull → update --no-download."""
    # 1. Import (metadata only)
    _, stderr, rc = _run_dvx([
        "import-url", "--no-download",
        "--fs-config", "allow_anonymous_login=true",
        S3_URL, "-o", OUT_NAME,
    ], dvc_repo)
    assert rc == 0, f"import-url failed: {stderr}"

    dvc_file = dvc_repo / f"{OUT_NAME}.dvc"
    dvc_data = _load_dvc(dvc_file)
    assert not dvc_data["outs"][0].get("md5")
    etag = dvc_data["deps"][0]["etag"]
    assert etag

    # 2. Pull (downloads from source)
    _, stderr, rc = _run_dvx(["pull", OUT_NAME], dvc_repo)
    assert rc == 0, f"pull failed: {stderr}"

    data_file = dvc_repo / OUT_NAME
    assert data_file.exists(), "Data file not created by pull"
    assert data_file.stat().st_size > 0, "Data file is empty"

    # 3. Commit so update works
    subprocess.run(
        ["git", "add", f"{OUT_NAME}.dvc", ".gitignore"],
        cwd=dvc_repo, capture_output=True, check=True,
    )
    subprocess.run(
        ["git", "commit", "-m", "track"],
        cwd=dvc_repo, capture_output=True, check=True,
    )

    # 4. Update --no-download (re-check ETag)
    _, stderr, rc = _run_dvx([
        "update", "--no-download", f"{OUT_NAME}.dvc",
    ], dvc_repo)
    assert rc == 0, f"update failed: {stderr}"

    dvc_data = _load_dvc(dvc_file)
    assert dvc_data["deps"][0]["etag"] == etag, "ETag should be unchanged"
