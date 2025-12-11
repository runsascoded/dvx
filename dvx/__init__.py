"""
DVX
----
Minimal data version control - content-addressable storage for data files.
"""

import dvx.logger
from dvx.build import PKG  # noqa: F401
from dvx.version import __version__, version_tuple  # noqa: F401

dvx.logger.setup()
