"""Classifier backed by Databricks' built-in `ai_classify()` SQL function.

Runs against a Databricks-governed foundation model (Mosaic AI Model
Serving) directly from Spark SQL -- no external API key, no extra
network egress, and it inherits Unity Catalog governance/audit logging.
Only available inside a Databricks runtime with AI Functions enabled.

Config:

    classifier:
      name: classifier.databricks_ai
      config:
        doc_types: [invoice, contract, resume]
        max_content_chars: 4000
"""

from __future__ import annotations

from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Classifier


class DatabricksAiClassifier(Classifier):
    kind = "classifier"

    def __init__(self, config: dict[str, Any] | None = None, *, spark: Any = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        doc_types = self.config.get("doc_types")
        if not doc_types:
            raise ValueError("classifier.databricks_ai requires `doc_types`")
        self.doc_types: list[str] = list(doc_types)
        self.max_content_chars: int = self.config.get("max_content_chars", 4000)
        self._spark = spark

    def _get_spark(self) -> Any:
        if self._spark is not None:
            return self._spark
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:  # pragma: no cover - exercised only off-Databricks
            raise ImportError(
                "classifier.databricks_ai requires pyspark and a Databricks runtime with AI "
                "Functions enabled; install the `tars[databricks]` extra and run on Databricks."
            ) from exc
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    def classify(self, document: Document, context: PipelineContext) -> str | None:
        if not document.content:
            return None
        text = document.content[: self.max_content_chars].decode("utf-8", errors="ignore")
        spark = self._get_spark()
        labels_sql = ", ".join(f"'{label}'" for label in self.doc_types)
        row = spark.sql(
            f"SELECT ai_classify(:text, array({labels_sql})) AS label",
            args={"text": text},
        ).collect()[0]
        label = row["label"]
        return label if label in self.doc_types else None
