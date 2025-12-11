"""DVC-compatible MD5 hash computation."""

import hashlib
import json
from pathlib import Path


def compute_md5(file_path: Path) -> str:
    """Compute MD5 hash of a file (DVC-compatible).

    For files: MD5 of contents
    For directories: MD5 of {relpath: md5} JSON (DVC's .dir format)

    Args:
        file_path: Path to file or directory to hash

    Returns:
        Hexadecimal MD5 hash string

    Raises:
        FileNotFoundError: If file_path doesn't exist
        ValueError: If file_path is neither file nor directory
    """
    if not file_path.exists():
        raise FileNotFoundError(f"{file_path} not found")

    if file_path.is_file():
        return _hash_file(file_path)
    elif file_path.is_dir():
        return _hash_directory(file_path)
    else:
        raise ValueError(f"{file_path} is neither file nor directory")


def _hash_file(file_path: Path) -> str:
    """Hash contents of a single file.

    Args:
        file_path: Path to file

    Returns:
        MD5 hash of file contents
    """
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        # Read in 64KB chunks to handle large files efficiently
        for chunk in iter(lambda: f.read(65536), b''):
            md5.update(chunk)
    return md5.hexdigest()


def _hash_directory(dir_path: Path) -> str:
    """Hash a directory by hashing the JSON of {relpath: md5} mapping.

    This matches DVC's .dir format for directory outputs.

    Args:
        dir_path: Path to directory

    Returns:
        MD5 hash of the sorted {relpath: md5} JSON representation
    """
    file_hashes = {}

    # Recursively hash all files in directory
    for subfile in sorted(dir_path.rglob('*')):
        if subfile.is_file():
            rel_path = subfile.relative_to(dir_path)
            # Use forward slashes for cross-platform compatibility
            rel_path_str = str(rel_path).replace('\\', '/')
            file_hashes[rel_path_str] = _hash_file(subfile)

    # Hash the JSON representation (sorted for determinism)
    json_str = json.dumps(file_hashes, sort_keys=True, separators=(',', ':'))
    return hashlib.md5(json_str.encode()).hexdigest()


def compute_file_size(file_path: Path) -> int:
    """Get size of file or directory.

    For directories, returns sum of all file sizes.

    Args:
        file_path: Path to file or directory

    Returns:
        Size in bytes
    """
    if not file_path.exists():
        raise FileNotFoundError(f"{file_path} not found")

    if file_path.is_file():
        return file_path.stat().st_size
    elif file_path.is_dir():
        total = 0
        for subfile in file_path.rglob('*'):
            if subfile.is_file():
                total += subfile.stat().st_size
        return total
    else:
        raise ValueError(f"{file_path} is neither file nor directory")
