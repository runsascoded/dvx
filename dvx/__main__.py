"""Main entry point for DVC command line tool."""

import sys

from dvx.cli import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
