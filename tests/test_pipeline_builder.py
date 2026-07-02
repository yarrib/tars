from __future__ import annotations

import pytest

from tars.config.schema import PipelineConfig, PluginRef
from tars.pipeline.builder import build_pipeline
from tars.plugins.registry import PluginRegistry


@pytest.fixture
def registry() -> PluginRegistry:
    return PluginRegistry(load_entry_points=True)


def test_build_pipeline_wires_up_all_stages(registry: PluginRegistry) -> None:
    config = PipelineConfig(
        name="invoice_pipeline",
        source=PluginRef(name="source.api"),
        classifiers=[PluginRef(name="classifier.rule_based", config={"rules": []})],
        parser=PluginRef(name="parser.text"),
        sinks=[PluginRef(name="sink.volume", config={"path": "/tmp/tars-test-out"})],
    )

    built = build_pipeline(config, registry=registry)

    assert built.name == "invoice_pipeline"
    assert built.source is not None
    assert len(built.classifiers) == 1
    assert built.parser is not None
    assert len(built.sinks) == 1
    assert built.dead_letter_sink is None
    assert len(built._all_plugins) == 4


def test_build_pipeline_without_source_is_valid_for_router_dispatch(registry: PluginRegistry) -> None:
    config = PipelineConfig(
        name="invoice_pipeline",
        parser=PluginRef(name="parser.text"),
        sinks=[PluginRef(name="sink.volume", config={"path": "/tmp/tars-test-out"})],
    )

    built = build_pipeline(config, registry=registry)

    assert built.source is None


def test_build_pipeline_includes_dead_letter_sink(registry: PluginRegistry) -> None:
    config = PipelineConfig(
        name="p",
        parser=PluginRef(name="parser.text"),
        sinks=[PluginRef(name="sink.volume", config={"path": "/tmp/tars-test-out"})],
        on_error="dead_letter",
        dead_letter_sink=PluginRef(name="sink.volume", config={"path": "/tmp/tars-test-dlq"}),
    )

    built = build_pipeline(config, registry=registry)

    assert built.dead_letter_sink is not None
    assert built.on_error == "dead_letter"
