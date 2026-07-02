"""Event-driven source backed by Databricks Auto Loader (`cloudFiles`).

Config:

    source:
      name: source.autoloader
      config:
        path: /Volumes/main/landing/raw_docs        # or s3://, abfss://, gs://
        file_format: binaryFile                      # cloudFiles.format
        schema_location: /Volumes/main/landing/_schema/raw_docs
        include_content: true                         # read bytes into Document.content
        max_files_per_trigger: 1000
        options:                                       # passed through verbatim
          cloudFiles.inferColumnTypes: "true"

Auto Loader is inherently a Spark Structured Streaming source; because
`Source.discover` is a pull-based generator, this plugin runs each call as
an `availableNow` micro-batch (the idiomatic pattern for scheduled
Databricks Jobs / Lakeflow declarative pipelines) rather than blocking
forever -- for continuous processing, schedule the owning Job on a short
interval, or wrap `discover` in `while True`.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Source


class AutoloaderSource(Source):
    kind = "source"

    def __init__(self, config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        if "path" not in self.config:
            raise ValueError("source.autoloader requires `path`")
        self.path: str = self.config["path"]
        self.file_format: str = self.config.get("file_format", "binaryFile")
        self.schema_location: str | None = self.config.get("schema_location")
        self.include_content: bool = self.config.get("include_content", True)
        self.max_files_per_trigger: int | None = self.config.get("max_files_per_trigger")
        self.extra_options: dict[str, str] = self.config.get("options", {})

    def cloud_files_options(self) -> dict[str, str]:
        """Build the `.option(...)` map for `spark.readStream.format("cloudFiles")`."""
        options = {"cloudFiles.format": self.file_format}
        if self.schema_location:
            options["cloudFiles.schemaLocation"] = self.schema_location
        if self.max_files_per_trigger:
            options["cloudFiles.maxFilesPerTrigger"] = str(self.max_files_per_trigger)
        options.update(self.extra_options)
        return options

    def _get_spark(self):
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:  # pragma: no cover - exercised only off-Databricks
            raise ImportError(
                "source.autoloader requires pyspark and a Databricks runtime; "
                "install the `tars[databricks]` extra and run inside a Databricks cluster/job."
            ) from exc
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    def discover(self, context: PipelineContext) -> Iterator[Document]:
        spark = self._get_spark()
        reader = spark.readStream.format("cloudFiles")
        for key, value in self.cloud_files_options().items():
            reader = reader.option(key, value)
        stream_df = reader.load(self.path)

        batches: list = []

        def collect_batch(batch_df, batch_id):  # noqa: ANN001 - pyspark callback signature
            batches.append(batch_df.collect())

        query = stream_df.writeStream.foreachBatch(collect_batch).trigger(availableNow=True).start()
        query.awaitTermination()

        for batch in batches:
            for row in batch:
                data = row.asDict()
                metadata = {k: v for k, v in data.items() if k != "content"}
                metadata["source"] = "autoloader"
                yield Document(
                    uri=data.get("path", ""),
                    content=data.get("content") if self.include_content else None,
                    metadata=metadata,
                )
