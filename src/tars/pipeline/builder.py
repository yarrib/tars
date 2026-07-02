"""Turns a validated `PipelineConfig` into instantiated plugin objects.

Kept separate from `PipelineRunner` so pipelines can be built once and run
many times (e.g. a long-lived streaming job), and so `tars validate` can
build (and thus type/config-check) every pipeline without executing it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from tars.config.schema import PipelineConfig
from tars.plugins.base import Classifier, Parser, Sink, Source
from tars.plugins.registry import PluginRegistry, default_registry


@dataclass
class BuiltPipeline:
    name: str
    source: Source | None
    classifiers: list[Classifier]
    parser: Parser | None
    sinks: list[Sink]
    dead_letter_sink: Sink | None
    on_error: str = "skip"
    _all_plugins: list = field(default_factory=list, repr=False)


def build_pipeline(config: PipelineConfig, *, registry: PluginRegistry | None = None) -> BuiltPipeline:
    registry = registry or default_registry

    source = registry.create(config.source.name, config.source.config) if config.source else None
    classifiers = [registry.create(ref.name, ref.config) for ref in config.classifiers]
    parser = registry.create(config.parser.name, config.parser.config) if config.parser else None
    sinks = [registry.create(ref.name, ref.config) for ref in config.sinks]
    dead_letter_sink = (
        registry.create(config.dead_letter_sink.name, config.dead_letter_sink.config)
        if config.dead_letter_sink
        else None
    )

    all_plugins = [p for p in [source, *classifiers, parser, *sinks, dead_letter_sink] if p is not None]

    return BuiltPipeline(
        name=config.name,
        source=source,
        classifiers=classifiers,
        parser=parser,
        sinks=sinks,
        dead_letter_sink=dead_letter_sink,
        on_error=config.on_error,
        _all_plugins=all_plugins,
    )
