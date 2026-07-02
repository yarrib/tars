"""Source backed by Databricks Zerobus: a low-latency, gRPC-based direct
write API into Delta tables, meant for producers that can't wait for a
file to land in object storage (webhooks, edge services, IoT) and want
sub-second, exactly-once ingestion instead of `cloudFiles` micro-batch
latency.

Zerobus is a *write* path -- some external producer (e.g. `source.api`'s
webhook handler, or your own service) pushes rows directly into a Unity
Catalog table via the Zerobus Ingest SDK. `ZerobusSource` is the *read*
side within tars: it incrementally reads whatever has landed in that
table since the last checkpoint, exactly like `source.volume_listener`
does for a directory, so the rest of a pipeline (classify/parse/sink)
doesn't need to know or care which ingestion mechanism produced the row.

**Opinionated guidance**: Zerobus is tuned for high-frequency, small
records (a gRPC round trip per record) -- it is a poor transport for raw
multi-megabyte document bytes. Prefer `source.autoloader` for the actual
file content; reach for `source.zerobus` when the producer wants to push
a lightweight *pointer/event* record (a path + metadata, with the real
bytes already sitting in object storage) or a genuinely small inline
document (short text, a form submission) with sub-second latency that a
`cloudFiles` micro-batch trigger can't give you.

Config:

    source:
      name: source.zerobus
      config:
        table: main.landing.raw_documents
        uri_column: uri
        content_column: content
        timestamp_column: _zerobus_ingested_at
        include_content: true
        state_path: /Volumes/main/landing/_state/zerobus.json

Use `ZerobusPublisher` on the producer side to write rows this source can
then read; the exact `databricks-zerobus` SDK call shape may vary by
version, so treat `_ingest_via_sdk` as a starting point to adapt against
whatever version you pin, not a guaranteed-stable contract.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Source


class ZerobusSource(Source):
    kind = "source"

    def __init__(self, config: dict[str, Any] | None = None, *, spark: Any = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        if "table" not in self.config:
            raise ValueError("source.zerobus requires `table` (the Zerobus-written landing table)")
        self.table: str = self.config["table"]
        self.uri_column: str = self.config.get("uri_column", "uri")
        self.content_column: str = self.config.get("content_column", "content")
        self.timestamp_column: str = self.config.get("timestamp_column", "_zerobus_ingested_at")
        self.include_content: bool = self.config.get("include_content", True)
        state_path = self.config.get("state_path")
        self.state_path = Path(state_path) if state_path else Path(f".tars_checkpoint_{self.table}.json")
        self._spark = spark

    def _get_spark(self) -> Any:
        if self._spark is not None:
            return self._spark
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:  # pragma: no cover - exercised only off-Databricks
            raise ImportError(
                "source.zerobus requires pyspark and a Databricks runtime; install the "
                "`tars[databricks]` extra and run inside a Databricks cluster/job."
            ) from exc
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    def _load_checkpoint(self) -> str | None:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text()).get("last_timestamp")
        return None

    def _save_checkpoint(self, last_timestamp: Any) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps({"last_timestamp": str(last_timestamp)}))

    def discover(self, context: PipelineContext) -> Iterator[Document]:
        spark = self._get_spark()
        df = spark.read.table(self.table)

        checkpoint = self._load_checkpoint()
        if checkpoint is not None:
            df = df.where(f"{self.timestamp_column} > '{checkpoint}'")
        df = df.orderBy(self.timestamp_column)

        rows = df.collect()
        latest_timestamp = checkpoint
        for row in rows:
            data = row.asDict()
            latest_timestamp = data[self.timestamp_column]
            metadata = {k: v for k, v in data.items() if k not in (self.content_column,)}
            metadata["source"] = "zerobus"
            yield Document(
                uri=data[self.uri_column],
                content=data.get(self.content_column) if self.include_content else None,
                metadata=metadata,
            )
            context.increment("discovered")

        if latest_timestamp is not None and latest_timestamp != checkpoint:
            self._save_checkpoint(latest_timestamp)


class ZerobusPublisher:
    """Producer-side helper: pushes a single record into a Zerobus stream
    for the table `ZerobusSource` reads. Runs outside the pipeline itself
    (e.g. inside your webhook handler), not inside tars' own runner.
    """

    def __init__(self, table: str, *, workspace_url: str | None = None, endpoint: str | None = None) -> None:
        self.table = table
        self.workspace_url = workspace_url
        self.endpoint = endpoint
        self._stream: Any = None

    def _get_stream(self) -> Any:
        if self._stream is not None:
            return self._stream
        try:
            from databricks.zerobus import TableProperties, ZerobusSdk
        except ImportError as exc:  # pragma: no cover - exercised only with the SDK installed
            raise ImportError(
                "ZerobusPublisher requires the `databricks-zerobus` SDK: "
                "pip install databricks-zerobus. Check that package's current API "
                "against this method -- Zerobus is a young/fast-moving SDK and the "
                "exact constructor/stream shape may have changed since this was written."
            ) from exc
        sdk = ZerobusSdk(self.endpoint, self.workspace_url)
        self._stream = sdk.create_stream(TableProperties(table_name=self.table))
        return self._stream

    def publish(self, uri: str, content: bytes | None = None, **extra_columns: Any) -> None:
        stream = self._get_stream()
        record = {"uri": uri, "content": content, **extra_columns}
        stream.ingest_record(record)

    def close(self) -> None:
        if self._stream is not None:
            self._stream.close()
