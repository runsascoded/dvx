"""
DVX
----
Minimal data version control - content-addressable storage for data files.
"""

import dvc.logger
from dvc.build import PKG  # noqa: F401
from dvc.version import __version__, version_tuple  # noqa: F401

dvc.logger.setup()
