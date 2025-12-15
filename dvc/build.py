try:
    # file is created during dvc build
    from . import _build  # type: ignore[attr-defined, import]

    PKG: str | None = _build.PKG  # type: ignore[assignment]
except ImportError:
    PKG = None  # type: ignore[assignment]
