"""Push/API-driven source: documents arrive via direct calls to `submit()`
rather than being discovered by polling or a Spark stream.

This is what backs both:

* in-process API ingestion (call `source.submit(...)` directly from your
  own code -- e.g. a notebook widget, a custom webhook handler you already
  run), and
* the optional FastAPI wrapper (`create_app`) for a standalone HTTP
  ingestion endpoint, e.g. to receive cloud storage event notifications
  (S3 EventBridge, ADLS Event Grid) or direct file uploads.

`discover` drains whatever has been queued since the last call and
returns; callers that want to block/wait for events should loop.
"""

from __future__ import annotations

import base64
import queue
from collections.abc import Iterator
from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Source


class ApiSource(Source):
    kind = "source"

    def __init__(self, config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self.max_queue_size: int = self.config.get("max_queue_size", 10_000)
        self._queue: queue.Queue[Document] = queue.Queue(maxsize=self.max_queue_size)

    def submit(
        self,
        uri: str,
        content: bytes | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Document:
        """Enqueue a document for the next `discover()` call. Thread-safe."""
        document = Document(
            uri=uri,
            content=content,
            metadata={"source": "api", **(metadata or {})},
        )
        self._queue.put(document)
        return document

    def discover(self, context: PipelineContext) -> Iterator[Document]:
        drained = 0
        while True:
            try:
                document = self._queue.get_nowait()
            except queue.Empty:
                break
            context.increment("received")
            drained += 1
            yield document
        if drained == 0:
            return


def create_app(source: ApiSource | None = None):
    """Build a FastAPI app exposing `POST /ingest` for webhook-style
    ingestion. Requires the `tars[api]` extra (fastapi + uvicorn)."""
    try:
        from fastapi import FastAPI
        from pydantic import BaseModel
    except ImportError as exc:  # pragma: no cover - exercised only without extra installed
        raise ImportError(
            "create_app requires the `tars[api]` extra: pip install 'tars[api]'"
        ) from exc

    source = source or ApiSource()
    app = FastAPI(title="tars ingestion API")

    class IngestRequest(BaseModel):
        uri: str
        content_base64: str | None = None
        metadata: dict[str, Any] = {}

    @app.post("/ingest")
    def ingest(request: IngestRequest) -> dict[str, str]:
        content = base64.b64decode(request.content_base64) if request.content_base64 else None
        document = source.submit(request.uri, content=content, metadata=request.metadata)
        return {"doc_id": document.doc_id, "status": "queued"}

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
