"""DVX - Minimal data version control.

DVX is a lightweight wrapper around DVC that provides only the core
data versioning functionality: add, push, pull, checkout, status, etc.

It intentionally excludes DVC's pipeline/experiment features (dvc.yaml,
dvc run, dvc exp, metrics, params, plots) to provide a simpler tool
focused purely on versioning data files.

Usage:
    from dvx import Repo

    repo = Repo()
    repo.add("data.csv")
    repo.push()
"""

from dvx.repo import Repo

__all__ = ["Repo"]
