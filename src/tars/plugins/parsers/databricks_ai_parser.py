"""Parser backed by Databricks' built-in `ai_parse_document()` SQL
function -- OCR + layout-aware parsing of PDFs/images into structured
text, tables, and elements, running on a Databricks-governed model with
no external API key.

For field-level structured extraction on top of the parsed text, set
`fields`/`json_schema` plus `endpoint`: the plugin then calls `ai_query()`
against a Databricks Model Serving endpoint (a pay-per-token Foundation
Model API endpoint, or your own fine-tuned/external model registered in
Unity Catalog) with a JSON response schema -- the same declarative shape
as `parser.ai`, but staying inside the Databricks-governed model boundary
instead of calling Anthropic directly.

Config:

    parser:
      name: parser.databricks_ai
      config:
        endpoint: databricks-claude-sonnet-5   # only needed if fields/json_schema given
        fields:
          - name: vendor_name
            type: string
          - name: total_amount
            type: number
"""

from __future__ import annotations

import json
from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Parser


class DatabricksAiParser(Parser):
    kind = "parser"

    def __init__(self, config: dict[str, Any] | None = None, *, spark: Any = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self.fields: list[dict[str, Any]] | None = self.config.get("fields")
        self.json_schema: dict[str, Any] | None = self.config.get("json_schema")
        self.endpoint: str | None = self.config.get("endpoint")
        if (self.fields or self.json_schema) and not self.endpoint:
            raise ValueError("parser.databricks_ai requires `endpoint` when `fields`/`json_schema` is set")
        self.max_content_chars: int = self.config.get("max_content_chars", 20_000)
        self._spark = spark

    def _get_spark(self) -> Any:
        if self._spark is not None:
            return self._spark
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:  # pragma: no cover - exercised only off-Databricks
            raise ImportError(
                "parser.databricks_ai requires pyspark and a Databricks runtime with AI "
                "Functions enabled; install the `tars[databricks]` extra and run on Databricks."
            ) from exc
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    def schema(self) -> dict[str, Any]:
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

    def parse(self, document: Document, context: PipelineContext) -> dict[str, Any]:
        if not document.content:
            return {}
        spark = self._get_spark()

        parsed_row = spark.sql(
            "SELECT ai_parse_document(:content) AS parsed",
            args={"content": document.content},
        ).collect()[0]
        parsed = parsed_row["parsed"]
        result: dict[str, Any] = {"parsed_document": parsed}

        if self.fields or self.json_schema:
            parsed_text = (
                parsed.get("document", {}).get("text")
                if isinstance(parsed, dict)
                else str(parsed)
            )
            text = (parsed_text or "")[: self.max_content_chars]
            schema_json = json.dumps(self.schema())
            extract_row = spark.sql(
                "SELECT ai_query(:endpoint, :prompt, responseFormat => :schema) AS extracted",
                args={
                    "endpoint": self.endpoint,
                    "prompt": f"Extract fields per this JSON schema:\n{schema_json}\n\nDocument:\n{text}",
                    "schema": schema_json,
                },
            ).collect()[0]
            extracted = extract_row["extracted"]
            result.update(extracted if isinstance(extracted, dict) else json.loads(extracted))

        return result
