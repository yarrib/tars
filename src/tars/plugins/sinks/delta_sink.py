"""Sink that writes processed Documents as rows in a Delta Lake table.

Buffers rows in memory and flushes in batches (on reaching `batch_size`,
and always on `teardown`) rather than issuing one Delta write per
document -- small, frequent Delta commits are expensive and fragment the
table.

Config:

    sink:
      name: sink.delta
      config:
        table: main.docs.invoices   # catalog.schema.table (Unity Catalog) or schema.table
        mode: append                 # append | overwrite
        batch_size: 100
"""

from __future__ import annotations

import json
from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Sink


class DeltaSink(Sink):
    kind = "sink"

    def __init__(self, config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        if "table" not in self.config:
            raise ValueError("sink.delta requires `table` (catalog.schema.table)")
        self.table: str = self.config["table"]
        self.mode: str = self.config.get("mode", "append")
        self.batch_size: int = self.config.get("batch_size", 100)
        self._buffer: list[dict[str, Any]] = []

    @staticmethod
    def _row(document: Document, context: PipelineContext) -> dict[str, Any]:
        return {
            "doc_id": document.doc_id,
            "uri": document.uri,
            "doc_type": document.doc_type,
            "ingested_at": document.ingested_at,
            "metadata_json": json.dumps(document.metadata, default=str),
            "extracted_json": json.dumps(document.extracted, default=str) if document.extracted else None,
            "pipeline_name": context.pipeline_name,
            "run_id": context.run_id,
        }

    def _get_spark(self):
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:  # pragma: no cover - exercised only off-Databricks
            raise ImportError(
                "sink.delta requires pyspark and a Databricks runtime; "
                "install the `tars[databricks]` extra and run inside a Databricks cluster/job."
            ) from exc
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    def write(self, document: Document, context: PipelineContext) -> None:
        self._buffer.append(self._row(document, context))
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        spark = self._get_spark()
        df = spark.createDataFrame(self._buffer)
        df.write.format("delta").mode(self.mode).saveAsTable(self.table)
        self._buffer = []

    def teardown(self, context: PipelineContext) -> None:
        self.flush()
