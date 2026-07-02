"""Trivial parser: decodes `document.content` as text. Useful as a cheap
default/fallback and as a dependency-free baseline for tests -- no network
calls, no LLM.

Config:

    parser:
      name: parser.text
      config:
        encoding: utf-8   # optional, defaults to utf-8 with errors ignored
"""

from __future__ import annotations

from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Parser


class TextParser(Parser):
    kind = "parser"

    def __init__(self, config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self.encoding: str = self.config.get("encoding", "utf-8")

    def parse(self, document: Document, context: PipelineContext) -> dict[str, Any]:
        text = document.content.decode(self.encoding, errors="ignore") if document.content else ""
        return {"text": text, "length": len(text)}
