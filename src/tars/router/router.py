"""Document router: the "classifier" stage that decides which declarative
pipeline should handle a given Document.

The router runs its configured classifier chain (first non-None answer
wins) to assign `document.doc_type`, then resolves a pipeline name via
`RouterConfig.resolve`, matching on exact doc_type first, then a `*`
wildcard route, then `default_pipeline`.
"""

from __future__ import annotations

import logging

from tars.config.schema import RouterConfig
from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Classifier
from tars.plugins.registry import PluginRegistry, default_registry

logger = logging.getLogger(__name__)


class UnroutableDocumentError(RuntimeError):
    def __init__(self, document: Document):
        self.document = document
        super().__init__(
            f"No pipeline could be resolved for document {document.uri!r} "
            f"(doc_type={document.doc_type!r}); add a route or a default_pipeline"
        )


class DocumentRouter:
    """Classifies Documents and dispatches them to a pipeline by name."""

    def __init__(self, config: RouterConfig, *, registry: PluginRegistry | None = None) -> None:
        self.config = config
        self.registry = registry or default_registry
        self._classifiers: list[Classifier] = [
            self.registry.create(ref.name, ref.config) for ref in config.classifiers
        ]

    def classify(self, document: Document, context: PipelineContext) -> str | None:
        for classifier in self._classifiers:
            doc_type = classifier.classify(document, context)
            if doc_type is not None:
                document.doc_type = doc_type
                return doc_type
        return document.doc_type

    def route(self, document: Document, context: PipelineContext, *, strict: bool = True) -> str | None:
        """Classify `document` (if not already classified) and return the
        target pipeline name, or None (or raise, if `strict`) if unroutable."""
        if document.doc_type is None:
            self.classify(document, context)

        pipeline_name = self.config.resolve(document.doc_type)
        if pipeline_name is None and strict:
            raise UnroutableDocumentError(document)
        if pipeline_name is None:
            logger.warning("Unroutable document %s (doc_type=%s)", document.uri, document.doc_type)
        return pipeline_name
