"""Executes built pipelines against real Documents.

Two entry points cover the two ingestion topologies tars supports:

* `run_pipeline` -- a self-contained pipeline with its own `source`
  (simplest case: one file type, one landing zone, one destination).
* `run_router`   -- one shared source feeds the `DocumentRouter`, which
  classifies each Document and dispatches it into the matching
  pipeline's classify/parse/sink chain (that pipeline's own `source`, if
  declared, is not used in this mode).

Both funnel through `process_document`, which is also the extension point
tests use to exercise a pipeline against synthetic Documents without a
real source.
"""

from __future__ import annotations

import logging

from tars.config.schema import AppConfig
from tars.pipeline.builder import BuiltPipeline, build_pipeline
from tars.pipeline.context import Document, PipelineContext
from tars.plugins.registry import PluginRegistry, default_registry
from tars.router.router import DocumentRouter

logger = logging.getLogger(__name__)


class PipelineRunner:
    def __init__(self, app_config: AppConfig, *, registry: PluginRegistry | None = None) -> None:
        self.app_config = app_config
        self.registry = registry or default_registry
        self._built: dict[str, BuiltPipeline] = {}
        self._started: set[str] = set()

    def get_built(self, name: str) -> BuiltPipeline:
        if name not in self._built:
            if name not in self.app_config.pipelines:
                raise KeyError(f"Unknown pipeline: {name!r}")
            self._built[name] = build_pipeline(self.app_config.pipelines[name], registry=self.registry)
        return self._built[name]

    def _ensure_started(self, built: BuiltPipeline, context: PipelineContext) -> None:
        if built.name in self._started:
            return
        for plugin in built._all_plugins:
            plugin.setup(context)
        self._started.add(built.name)

    def _teardown(self, built: BuiltPipeline, context: PipelineContext) -> None:
        for plugin in built._all_plugins:
            plugin.teardown(context)
        self._started.discard(built.name)

    def process_document(
        self, built: BuiltPipeline, document: Document, context: PipelineContext
    ) -> None:
        try:
            for classifier in built.classifiers:
                doc_type = classifier.classify(document, context)
                if doc_type is not None:
                    document.doc_type = doc_type

            if built.parser is not None:
                document.extracted = built.parser.parse(document, context)

            for sink in built.sinks:
                sink.write(document, context)

            context.increment("processed")
        except Exception as exc:  # noqa: BLE001 - error routed per pipeline policy
            document.fail(str(exc))
            context.increment("failed")
            logger.warning("Pipeline %r failed on %s: %s", built.name, document.uri, exc)

            if built.on_error == "fail":
                raise
            if built.on_error == "dead_letter" and built.dead_letter_sink is not None:
                built.dead_letter_sink.write(document, context)

    def run_pipeline(self, name: str) -> PipelineContext:
        built = self.get_built(name)
        if built.source is None:
            raise RuntimeError(
                f"pipeline {name!r} has no `source`; run it via the router or call "
                "process_document directly with your own Documents"
            )
        context = PipelineContext(pipeline_name=name)
        self._ensure_started(built, context)
        try:
            for document in built.source.discover(context):
                self.process_document(built, document, context)
        finally:
            self._teardown(built, context)
        return context

    def run_router(self) -> PipelineContext:
        router_config = self.app_config.router
        if router_config.source is None:
            raise RuntimeError("router.source is not configured; router-driven ingestion needs it")

        source = self.registry.create(router_config.source.name, router_config.source.config)
        router = DocumentRouter(router_config, registry=self.registry)
        context = PipelineContext(pipeline_name="router")

        source.setup(context)
        try:
            for document in source.discover(context):
                context.increment("received")
                pipeline_name = router.route(document, context, strict=False)
                if pipeline_name is None:
                    context.increment("unrouted")
                    continue
                built = self.get_built(pipeline_name)
                self._ensure_started(built, context)
                self.process_document(built, document, context)
        finally:
            source.teardown(context)
            for name in list(self._started):
                self._teardown(self._built[name], context)
        return context
