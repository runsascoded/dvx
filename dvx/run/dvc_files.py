"""Read/write .dvc files for output tracking.

DVX uses individual .dvc files to track pipeline outputs rather than
a centralized dvc.lock file. This provides:
- Locality: hash and provenance info lives next to each artifact
- Git-friendly: small, independent files instead of one large lock file
- Self-documenting: each artifact knows how it was produced

The DVX .dvc format extends standard DVC with a `computation` block:

```yaml
outs:
- md5: abc123...
  size: 12345
  path: output.parquet

computation:
  cmd: "python process.py --input data.csv"
  code_ref: "a1b2c3d4..."  # git SHA when computed
  deps:
    data.csv: def456...
    process.py: 789abc...
```
"""

import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from dvx.run.hash import compute_md5


def get_git_head_sha(repo_path: Path | None = None) -> str | None:
    """Get the current HEAD commit SHA.

    Args:
        repo_path: Path to git repository (default: current directory)

    Returns:
        Full SHA string, or None if not in a git repo
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


@dataclass
class DVCFileInfo:
    """Content of a .dvc file."""

    path: str
    md5: str
    size: int
    # Provenance via computation block (optional)
    cmd: str | None = None
    code_ref: str | None = None  # git SHA
    deps: dict[str, str] = field(default_factory=dict)  # {path: md5}
    # Legacy field for backward compatibility
    stage: str | None = None


def read_dvc_file(output_path: Path) -> DVCFileInfo | None:
    """Read .dvc file for an output.

    Handles both DVX format (computation block) and legacy format (meta block).

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

    # Try new computation block first, fall back to legacy meta block
    computation = data.get("computation", {})
    meta = data.get("meta", {})

    return DVCFileInfo(
        path=out.get("path", str(output_path)),
        md5=out.get("md5", ""),
        size=out.get("size", 0),
        # Prefer computation block, fall back to meta
        cmd=computation.get("cmd") or meta.get("cmd"),
        code_ref=computation.get("code_ref"),
        deps=computation.get("deps") or meta.get("deps") or {},
        stage=meta.get("stage"),  # Legacy only
    )


def write_dvc_file(
    output_path: Path,
    md5: str,
    size: int,
    cmd: str | None = None,
    code_ref: str | None = None,
    deps: dict[str, str] | None = None,
    stage: str | None = None,  # Legacy, deprecated
) -> Path:
    """Write .dvc file for an output with provenance.

    Args:
        output_path: Path to the output file/directory
        md5: MD5 hash of the output
        size: Size in bytes
        cmd: Command that was run (provenance)
        code_ref: Git SHA when computation was run (provenance)
        deps: {dep_path: md5} of inputs (provenance)
        stage: Deprecated, kept for backward compatibility

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

    # Add computation block for provenance
    if cmd or code_ref or deps:
        data["computation"] = {}
        if cmd:
            data["computation"]["cmd"] = cmd
        if code_ref:
            data["computation"]["code_ref"] = code_ref
        if deps:
            data["computation"]["deps"] = deps

    with open(dvc_path, "w") as f:
        yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    return dvc_path


def is_output_fresh(
    output_path: Path,
    check_deps: bool = True,
) -> tuple[bool, str]:
    """Check if output is fresh (up-to-date with its .dvc file and deps).

    Freshness is determined by:
    1. Output file exists
    2. Output hash matches .dvc file
    3. All dependency hashes match (if check_deps=True and deps recorded)

    Note: code_ref checking is not done here - that would require determining
    if relevant code changed between recorded SHA and current HEAD, which is
    more complex. For now, code_ref serves as documentation/audit trail.

    Args:
        output_path: Path to the output file/directory
        check_deps: Whether to verify dependency hashes (default: True)

    Returns:
        Tuple of (is_fresh, reason)
    """
    info = read_dvc_file(output_path)
    if info is None:
        return False, "no .dvc file"

    path = Path(output_path)
    if not path.exists():
        return False, "output missing"

    # Check output hash
    try:
        current_md5 = compute_md5(path)
    except (FileNotFoundError, ValueError) as e:
        return False, f"hash error: {e}"

    if current_md5 != info.md5:
        return False, f"output hash mismatch ({info.md5[:8]}... vs {current_md5[:8]}...)"

    # Check dependency hashes if requested and deps are recorded
    if check_deps and info.deps:
        for dep_path, recorded_md5 in info.deps.items():
            dep = Path(dep_path)
            if not dep.exists():
                return False, f"dep missing: {dep_path}"
            try:
                current_dep_md5 = compute_md5(dep)
            except (FileNotFoundError, ValueError) as e:
                return False, f"dep hash error ({dep_path}): {e}"

            if current_dep_md5 != recorded_md5:
                return False, f"dep changed: {dep_path}"

    return True, "up-to-date"


def get_dvc_file_path(output_path: Path) -> Path:
    """Get the .dvc file path for an output.

    Args:
        output_path: Path to the output file/directory

    Returns:
        Path to the .dvc file (may not exist)
    """
    return Path(str(output_path) + ".dvc")
