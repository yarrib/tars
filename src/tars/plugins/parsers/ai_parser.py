""""AI parse doc": extracts structured fields from a document's content
using Claude's tool-use (forced function-calling) to get reliable JSON
back, rather than parsing free text.

Config:

    parser:
      name: parser.ai
      config:
        model: claude-sonnet-5
        instructions: "Extract invoice header fields."
        fields:
          - name: vendor_name
            type: string
            description: "Name of the company issuing the invoice"
          - name: total_amount
            type: number
            description: "Total amount due"
          - name: line_items
            type: array
            description: "Line items, each with description/quantity/price"
            required: false

`fields` is the ergonomic declarative form; pass a full `json_schema`
instead if you need nested/complex structures it can't express.
"""

from __future__ import annotations

import os
from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Parser

_DEFAULT_INSTRUCTIONS = "Extract the requested fields from the document. Leave a field null if absent."


class AiParser(Parser):
    kind = "parser"

    def __init__(self, config: dict[str, Any] | None = None, *, client: Any = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self.fields: list[dict[str, Any]] | None = self.config.get("fields")
        self.json_schema: dict[str, Any] | None = self.config.get("json_schema")
        if not self.fields and not self.json_schema:
            raise ValueError("parser.ai requires either `fields` or `json_schema`")
        self.model: str = self.config.get("model", "claude-sonnet-5")
        self.instructions: str = self.config.get("instructions", _DEFAULT_INSTRUCTIONS)
        self.max_content_chars: int = self.config.get("max_content_chars", 20_000)
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            try:
                import anthropic
            except ImportError as exc:  # pragma: no cover - exercised only without extra
                raise ImportError(
                    "parser.ai requires the `tars[ai]` extra: pip install 'tars[ai]'"
                ) from exc
            api_key = self.config.get("api_key") or os.environ.get("ANTHROPIC_API_KEY")
            self._client = anthropic.Anthropic(api_key=api_key)
        return self._client

    def _build_schema(self) -> dict[str, Any]:
        if self.json_schema is not None:
            return self.json_schema
        properties: dict[str, Any] = {}
        required: list[str] = []
        for field in self.fields or []:
            properties[field["name"]] = {
                "type": field.get("type", "string"),
                "description": field.get("description", ""),
            }
            if field.get("required", True):
                required.append(field["name"])
        return {"type": "object", "properties": properties, "required": required}

    def _document_text(self, document: Document) -> str:
        if not document.content:
            return ""
        return document.content[: self.max_content_chars].decode("utf-8", errors="ignore")

    def parse(self, document: Document, context: PipelineContext) -> dict[str, Any]:
        tool = {
            "name": "extract_fields",
            "description": self.instructions,
            "input_schema": self._build_schema(),
        }
        response = self.client.messages.create(
            model=self.model,
            max_tokens=self.config.get("max_tokens", 1024),
            tools=[tool],
            tool_choice={"type": "tool", "name": "extract_fields"},
            messages=[
                {
                    "role": "user",
                    "content": f"Filename: {document.filename}\n\n{self._document_text(document)}",
                }
            ],
        )
        for block in response.content:
            if getattr(block, "type", None) == "tool_use":
                return dict(block.input)
        raise RuntimeError("parser.ai: model response did not include the expected tool_use block")
