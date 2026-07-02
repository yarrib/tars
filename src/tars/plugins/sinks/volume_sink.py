"""Sink that writes each Document's extracted data (and optionally raw
content) as files under a directory tree -- a Unity Catalog Volume path or
a local directory. Simplest possible durable sink; good default for
examples, tests, and dead-letter queues.

Config:

    sink:
      name: sink.volume
      config:
        path: /Volumes/main/docs/processed
        write_content: false   # also copy the raw bytes alongside the JSON
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Sink


class VolumeSink(Sink):
    kind = "sink"

    def __init__(self, config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        if "path" not in self.config:
            raise ValueError("sink.volume requires `path`")
        self.path = Path(self.config["path"])
        self.write_content: bool = self.config.get("write_content", False)

    def write(self, document: Document, context: PipelineContext) -> None:
        self.path.mkdir(parents=True, exist_ok=True)
        record = {
            "doc_id": document.doc_id,
            "uri": document.uri,
            "doc_type": document.doc_type,
            "ingested_at": document.ingested_at,
            "metadata": document.metadata,
            "extracted": document.extracted,
            "errors": document.errors,
            "pipeline_name": context.pipeline_name,
            "run_id": context.run_id,
        }
        (self.path / f"{document.doc_id}.json").write_text(json.dumps(record, default=str, indent=2))
        if self.write_content and document.content is not None:
            (self.path / f"{document.doc_id}.{document.extension or 'bin'}").write_bytes(document.content)
