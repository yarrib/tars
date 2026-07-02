"""Plugin discovery and instantiation.

Plugins come from two places:

1. **Entry points** -- anything installed in the Python environment that
   declares an entry point in the ``tars.plugins`` group (built-in plugins
   register themselves this way via ``pyproject.toml``, and third-party
   packages can do the same to extend tars without forking it).
2. **In-process registration** -- via :func:`register` (a decorator) or
   :meth:`PluginRegistry.register`, useful for tests and for one-off
   plugins defined directly in a user's Databricks repo/notebook.

Config refers to plugins purely by name (e.g. ``source.autoloader`` or
``my_org.custom_classifier``), never by import path, which is what keeps
pipeline YAML declarative and decoupled from Python packaging details.
"""

from __future__ import annotations

from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # Deferred: tars.plugins.base imports tars.pipeline.context, and
    # eagerly importing it here would re-enter tars.plugins while it's
    # still being initialized (tars.plugins.__init__ imports base before
    # registry). Only needed for type hints, so it costs nothing at runtime.
    from tars.plugins.base import Plugin

_ENTRY_POINT_GROUP = "tars.plugins"


class PluginNotFoundError(KeyError):
    def __init__(self, name: str, known: list[str]):
        self.name = name
        self.known = known
        super().__init__(
            f"No plugin registered as {name!r}. Known plugins: {', '.join(sorted(known)) or '(none)'}"
        )


class PluginRegistry:
    """Resolves plugin names (as used in YAML config) to classes."""

    def __init__(self, *, load_entry_points: bool = True) -> None:
        self._plugins: dict[str, type[Plugin]] = {}
        self._loaded_entry_points = False
        self._auto_load_entry_points = load_entry_points

    def _ensure_entry_points_loaded(self) -> None:
        # Deferred to first real use (not __init__) so that constructing
        # `default_registry` at import time never eagerly imports plugin
        # modules -- those modules import tars.plugins.base, which would
        # re-enter tars.plugins mid-initialization.
        if self._auto_load_entry_points and not self._loaded_entry_points:
            self.load_entry_points()

    def load_entry_points(self) -> None:
        if self._loaded_entry_points:
            return
        for ep in entry_points(group=_ENTRY_POINT_GROUP):
            try:
                cls = ep.load()
            except Exception as exc:  # pragma: no cover - defensive
                raise ImportError(f"Failed to load plugin entry point {ep.name!r}: {exc}") from exc
            self._plugins[ep.name] = cls
        self._loaded_entry_points = True

    def register(self, name: str, cls: type[Plugin] | None = None):
        """Register a plugin class under `name`. Usable as a decorator:

            @registry.register("classifier.my_thing")
            class MyClassifier(Classifier): ...
        """
        if cls is not None:
            self._plugins[name] = cls
            return cls

        def decorator(plugin_cls: type[Plugin]) -> type[Plugin]:
            self._plugins[name] = plugin_cls
            return plugin_cls

        return decorator

    def get(self, name: str) -> type[Plugin]:
        self._ensure_entry_points_loaded()
        if name not in self._plugins:
            raise PluginNotFoundError(name, list(self._plugins))
        return self._plugins[name]

    def create(self, name: str, config: dict[str, Any] | None = None, **kwargs: Any) -> Plugin:
        return self.get(name)(config=config, **kwargs)

    def names(self, kind: str | None = None) -> list[str]:
        self._ensure_entry_points_loaded()
        if kind is None:
            return sorted(self._plugins)
        return sorted(n for n, cls in self._plugins.items() if getattr(cls, "kind", None) == kind)

    def __contains__(self, name: str) -> bool:
        self._ensure_entry_points_loaded()
        return name in self._plugins


#: Process-wide default registry. Most callers should use this; tests may
#: prefer a fresh `PluginRegistry(load_entry_points=False)` for isolation.
default_registry = PluginRegistry()

register = default_registry.register
