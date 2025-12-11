# Contributing to DVX

Contributions are welcome! DVX is a fork of [DVC](https://github.com/iterative/dvc) focused on minimal data versioning.

## Development Setup

```bash
# Clone the repo
git clone https://github.com/runsascoded/dvx.git
cd dvx

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest
```

## Submitting Changes

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting: `pre-commit run --all-files`
5. Submit a pull request

## Code Style

This project uses:
- [ruff](https://github.com/astral-sh/ruff) for linting and formatting
- [mypy](https://mypy-lang.org/) for type checking
- [pre-commit](https://pre-commit.com/) for automated checks
