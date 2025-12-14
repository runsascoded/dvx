"""This module provides an entrypoint to the dvc cli and parsing utils."""

import logging
from typing import Optional

from dvx.log import logger

# Workaround for CPython bug. See [1] and [2] for more info.
# [1] https://github.com/aws/aws-cli/blob/1.16.277/awscli/clidriver.py#L55
# [2] https://bugs.python.org/issue29288
"".encode("idna")


logger = logger.getChild(__name__)


class DvcParserError(Exception):
    """Base class for CLI parser errors."""

    def __init__(self):
        super().__init__("parser error")


# Re-export main from Click-based CLI
from .main import main  # noqa: E402, F401


def _log_unknown_exceptions() -> None:
    from dvx.info import get_dvc_info
    from dvx.ui import ui
    from dvx.utils import colorize

    logger.exception("unexpected error")
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Version info for developers:\n%s", get_dvc_info())

    q = colorize("Having any troubles?", "yellow")
    link = colorize("https://dvc.org/support", "blue")
    footer = f"\n{q} Hit us up at {link}, we are always happy to help!"
    ui.error_write(footer)


def _log_exceptions(exc: Exception) -> int | None:
    """Try to log some known exceptions, that are not DVCExceptions."""
    from dvx.utils import error_link, format_link

    if isinstance(exc, OSError):
        import errno

        if exc.errno == errno.EMFILE:
            logger.exception(
                (
                    "too many open files, please visit "
                    "%s to see how to handle this problem"
                ),
                error_link("many-files"),
                extra={"tb_only": True},
            )
        else:
            _log_unknown_exceptions()
        return None

    from dvx.fs import AuthError, ConfigError, RemoteMissingDepsError

    if isinstance(exc, RemoteMissingDepsError):
        from dvx import PKG

        proto = exc.protocol
        by_pkg = {
            "pip": f"pip install 'dvc[{proto}]'",
            "conda": f"conda install -c conda-forge dvc-{proto}",
        }

        if PKG in by_pkg:
            link = format_link("https://dvc.org/doc/install")
            cmd = by_pkg.get(PKG)
            hint = (
                "To install dvc with those dependencies, run:\n"
                "\n"
                f"\t{cmd}\n"
                "\n"
                f"See {link} for more info."
            )
        else:
            link = format_link("https://github.com/treeverse/dvc/issues")
            hint = f"\nPlease report this bug to {link}. Thank you!"

        logger.exception(
            "URL '%s' is supported but requires these missing dependencies: %s. %s",
            exc.url,
            exc.missing_deps,
            hint,
            extra={"tb_only": True},
        )
        return None

    if isinstance(exc, (AuthError, ConfigError)):
        link = format_link("https://man.dvc.org/remote/modify")
        logger.exception("configuration error")
        logger.exception(
            "%s\nLearn more about configuration settings at %s.",
            exc,
            link,
            extra={"tb_only": True},
        )
        return 251

    from dvc_data.hashfile.cache import DiskError

    if isinstance(exc, DiskError):
        from dvx.utils import relpath

        directory = relpath(exc.directory)
        logger.exception(
            (
                "Could not open pickled '%s' cache.\n"
                "Remove the '%s' directory and then retry this command."
                "\nSee %s for more information."
            ),
            exc.type,
            directory,
            error_link("pickle"),
            extra={"tb_only": True},
        )
        return None

    from dvc_data.hashfile.build import IgnoreInCollectedDirError

    if isinstance(exc, IgnoreInCollectedDirError):
        logger.exception("")
        return None

    _log_unknown_exceptions()
    return None
