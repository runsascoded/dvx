"""Test that dvc package is not installed - dvx is a fork, not a wrapper."""

import pytest


def test_import_dvc_fails():
    """Verify that 'import dvc' raises ImportError - dvx should not depend on dvc."""
    with pytest.raises(ImportError):
        import dvc  # noqa: F401
