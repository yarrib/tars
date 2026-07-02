"""Abstract base classes every tars plugin implements.

tars has four plugin kinds, matching the four stages of a pipeline:

    Source  -- discovers/receives raw files (event-driven or API-driven)
    Classifier -- inspects a Document and assigns/refines its doc_type
    Parser  -- extracts structured content from a Document (e.g. via an LLM)
    Sink    -- persists the (possibly parsed) Document somewhere durable

Plugins are plain classes: `__init__(self, config: dict, **kwargs)` followed
by one required method (`discover`/`iter_events`, `classify`, `parse`,
`write`). Config validation is the plugin's own responsibility -- tars only
guarantees the raw, already-interpolated dict from YAML is handed over.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any

from tars.pipeline.context import Document, PipelineContext


class Plugin(ABC):
    """Common lifecycle shared by all plugin kinds."""

    #: Set by subclasses to a short, stable name used only for logging.
    kind: str = "plugin"

    def __init__(self, config: dict[str, Any] | None = None, **_: Any) -> None:
        self.config = config or {}

    def setup(self, context: PipelineContext) -> None:
        """Optional hook called once before a pipeline run starts."""

    def teardown(self, context: PipelineContext) -> None:
        """Optional hook called once after a pipeline run finishes."""


class Source(Plugin):
    """Produces Documents, either by polling/listening for events or via
    direct API invocation (`ApiSource.submit`)."""

    kind = "source"

    @abstractmethod
    def discover(self, context: PipelineContext) -> Iterator[Document]:
        """Yield Documents that are ready for processing.

        For streaming/event-driven sources this may block and yield
        Documents as events arrive; for batch sources it should discover
        everything currently available and return.
        """


class Classifier(Plugin):
    """Assigns or refines `Document.doc_type` so the router can dispatch it
    to the correct downstream pipeline/parser."""

    kind = "classifier"

    @abstractmethod
    def classify(self, document: Document, context: PipelineContext) -> str | None:
        """Return a doc_type label, or None if this classifier can't decide."""


class Parser(Plugin):
    """Extracts structured data from a Document's content."""

    kind = "parser"

    @abstractmethod
    def parse(self, document: Document, context: PipelineContext) -> dict[str, Any]:
        """Return a dict of extracted fields; also settable on
        `document.extracted` directly by the pipeline runner."""


class Sink(Plugin):
    """Persists a Document (and any extracted data) to a destination."""

    kind = "sink"

    @abstractmethod
    def write(self, document: Document, context: PipelineContext) -> None:
        """Persist the document. Raise to signal failure."""
