"""tars: adaptable, config-driven document ingestion for Databricks."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("tars")
except PackageNotFoundError:  # pragma: no cover - local/editable checkout
    __version__ = "0.0.0.dev0"

__all__ = ["__version__"]
