from dvc.fs.dvc import _DVCFileSystem as DVCFileSystem

from .data import get_url, open, read  # noqa: A004
from .scm import all_branches, all_commits, all_tags

__all__ = [
    "DVCFileSystem",
    "all_branches",
    "all_commits",
    "all_tags",
    "get_url",
    "open",
    "read",
]
