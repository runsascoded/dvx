"""DVX CLI package.

This package organizes CLI commands into modules. The main CLI group
and entry point are in main.py, with subcommand groups in subpackages.
"""

# Re-export main entry point
from .main import cli, main

__all__ = ["cli", "main"]
