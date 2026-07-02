from __future__ import annotations

import pytest

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Classifier
from tars.plugins.registry import PluginNotFoundError, PluginRegistry


class _Dummy(Classifier):
    def classify(self, document: Document, context: PipelineContext) -> str | None:
        return "dummy"


def test_builtin_plugins_are_discoverable(registry: PluginRegistry) -> None:
    assert "source.autoloader" in registry
    assert "classifier.rule_based" in registry
    assert "parser.text" in registry
    assert "sink.volume" in registry


def test_names_filters_by_kind(registry: PluginRegistry) -> None:
    sources = registry.names("source")
    assert "source.api" in sources
    assert "parser.text" not in sources


def test_register_via_decorator_and_create() -> None:
    registry = PluginRegistry(load_entry_points=False)

    @registry.register("classifier.dummy")
    class Registered(_Dummy):
        pass

    instance = registry.create("classifier.dummy", {})
    assert isinstance(instance, Registered)
    assert instance.classify(Document(uri="x"), PipelineContext(pipeline_name="p")) == "dummy"


def test_register_via_call() -> None:
    registry = PluginRegistry(load_entry_points=False)
    registry.register("classifier.dummy", _Dummy)
    assert registry.get("classifier.dummy") is _Dummy


def test_unknown_plugin_raises() -> None:
    registry = PluginRegistry(load_entry_points=False)
    with pytest.raises(PluginNotFoundError):
        registry.get("does.not.exist")
