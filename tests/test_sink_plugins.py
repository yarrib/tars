from __future__ import annotations

import json
from pathlib import Path

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.sinks.delta_sink import DeltaSink
from tars.plugins.sinks.volume_sink import VolumeSink


def test_volume_sink_writes_json_record(tmp_path: Path) -> None:
    sink = VolumeSink({"path": str(tmp_path)})
    doc = Document(uri="/x/a.txt", doc_type="invoice", extracted={"total": 42})
    context = PipelineContext(pipeline_name="p")

    sink.write(doc, context)

    record = json.loads((tmp_path / f"{doc.doc_id}.json").read_text())
    assert record["doc_type"] == "invoice"
    assert record["extracted"] == {"total": 42}
    assert record["pipeline_name"] == "p"


def test_volume_sink_optionally_writes_raw_content(tmp_path: Path) -> None:
    sink = VolumeSink({"path": str(tmp_path), "write_content": True})
    doc = Document(uri="/x/a.txt", content=b"raw bytes")

    sink.write(doc, PipelineContext(pipeline_name="p"))

    assert (tmp_path / f"{doc.doc_id}.txt").read_bytes() == b"raw bytes"


def test_delta_sink_buffers_until_batch_size(monkeypatch) -> None:
    sink = DeltaSink({"table": "main.docs.invoices", "batch_size": 2})
    flushed = []
    monkeypatch.setattr(sink, "flush", lambda: flushed.append(len(sink._buffer)))

    context = PipelineContext(pipeline_name="p")
    sink.write(Document(uri="a"), context)
    assert flushed == []
    sink.write(Document(uri="b"), context)
    assert flushed == [2]


def test_delta_sink_flush_writes_via_spark(monkeypatch) -> None:
    sink = DeltaSink({"table": "main.docs.invoices"})
    context = PipelineContext(pipeline_name="p", run_id="run-1")
    sink.write(Document(uri="a", doc_type="invoice"), context)

    written = {}

    class _FakeWriter:
        def format(self, fmt):
            written["format"] = fmt
            return self

        def mode(self, mode):
            written["mode"] = mode
            return self

        def saveAsTable(self, table):
            written["table"] = table

    class _FakeDataFrame:
        write = _FakeWriter()

    class _FakeSpark:
        def createDataFrame(self, rows):
            written["rows"] = rows
            return _FakeDataFrame()

    monkeypatch.setattr(sink, "_get_spark", lambda: _FakeSpark())
    sink.teardown(context)

    assert written["format"] == "delta"
    assert written["table"] == "main.docs.invoices"
    assert written["rows"][0]["doc_type"] == "invoice"
    assert sink._buffer == []
