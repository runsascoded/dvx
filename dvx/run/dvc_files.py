"""Read/write .dvc files for output tracking.

dvc-run uses individual .dvc files to track pipeline outputs rather than
a centralized dvc.lock file. This provides:
- Locality: hash and provenance info lives next to each artifact
- Git-friendly: small, independent files instead of one large lock file
- Tooling compatibility: works with existing .dvc-based workflows
"""

from dataclasses import dataclass
from pathlib import Path

import yaml

from dvx.run.hash import compute_md5


@dataclass
class DVCFileInfo:
    """Content of a .dvc file."""

    path: str
    md5: str
    size: int
    # Provenance (optional)
    stage: str | None = None
    cmd: str | None = None
    deps: dict[str, str] | None = None  # {path: md5}


def read_dvc_file(output_path: Path) -> DVCFileInfo | None:
    """Read .dvc file for an output.

    Args:
        output_path: Path to the output file/directory

    Returns:
        DVCFileInfo if .dvc file exists and is valid, None otherwise
    """
    dvc_path = Path(str(output_path) + ".dvc")

    if not dvc_path.exists():
        return None

    with open(dvc_path) as f:
        data = yaml.safe_load(f)

    if not data or "outs" not in data or not data["outs"]:
        return None

    out = data["outs"][0]
    meta = data.get("meta", {})

    return DVCFileInfo(
        path=out.get("path", str(output_path)),
        md5=out.get("md5", ""),
        size=out.get("size", 0),
        stage=meta.get("stage"),
        cmd=meta.get("cmd"),
        deps=meta.get("deps"),
    )


def write_dvc_file(
    output_path: Path,
    md5: str,
    size: int,
    stage: str | None = None,
    cmd: str | None = None,
    deps: dict[str, str] | None = None,
) -> Path:
    """Write .dvc file for an output.

    Args:
        output_path: Path to the output file/directory
        md5: MD5 hash of the output
        size: Size in bytes
        stage: Name of the stage that produced this (provenance)
        cmd: Command that was run (provenance)
        deps: {dep_path: md5} of inputs (provenance)

    Returns:
        Path to the created .dvc file
    """
    dvc_path = Path(str(output_path) + ".dvc")

    # Ensure parent directory exists
    dvc_path.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "outs": [{
            "md5": md5,
            "size": size,
            "path": str(output_path),
        }]
    }

    # Add provenance metadata
    if stage or cmd or deps:
        data["meta"] = {}
        if stage:
            data["meta"]["stage"] = stage
        if cmd:
            data["meta"]["cmd"] = cmd
        if deps:
            data["meta"]["deps"] = deps

    with open(dvc_path, "w") as f:
        yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    return dvc_path


def is_output_fresh(output_path: Path) -> tuple[bool, str]:
    """Check if output matches its .dvc file.

    Args:
        output_path: Path to the output file/directory

    Returns:
        Tuple of (is_fresh, reason)
    """
    info = read_dvc_file(output_path)
    if info is None:
        return False, "no .dvc file"

    path = Path(output_path)
    if not path.exists():
        return False, "output missing"

    try:
        current_md5 = compute_md5(path)
    except (FileNotFoundError, ValueError) as e:
        return False, f"hash error: {e}"

    if current_md5 != info.md5:
        return False, f"hash mismatch ({info.md5[:8]}... vs {current_md5[:8]}...)"

    return True, "up-to-date"


def get_dvc_file_path(output_path: Path) -> Path:
    """Get the .dvc file path for an output.

    Args:
        output_path: Path to the output file/directory

    Returns:
        Path to the .dvc file (may not exist)
    """
    return Path(str(output_path) + ".dvc")
