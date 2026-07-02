from __future__ import annotations

from pathlib import Path

from tars.pipeline.context import PipelineContext
from tars.plugins.sources.api_source import ApiSource
from tars.plugins.sources.volume_listener import VolumeListenerSource


def test_volume_listener_discovers_new_files_once(tmp_path: Path) -> None:
    (tmp_path / "invoice_1.txt").write_text("hello")
    (tmp_path / "invoice_2.txt").write_text("world")

    source = VolumeListenerSource({"path": str(tmp_path)})
    context = PipelineContext(pipeline_name="p")

    first_pass = list(source.discover(context))
    assert {d.filename for d in first_pass} == {"invoice_1.txt", "invoice_2.txt"}
    assert first_pass[0].content is not None

    # Nothing new since the checkpoint was written.
    second_pass = list(source.discover(context))
    assert second_pass == []

    (tmp_path / "invoice_3.txt").write_text("new")
    third_pass = list(source.discover(context))
    assert {d.filename for d in third_pass} == {"invoice_3.txt"}


def test_volume_listener_filters_by_pattern(tmp_path: Path) -> None:
    (tmp_path / "a.pdf").write_text("pdf")
    (tmp_path / "b.txt").write_text("txt")

    source = VolumeListenerSource({"path": str(tmp_path), "patterns": ["*.pdf"]})
    docs = list(source.discover(PipelineContext(pipeline_name="p")))

    assert [d.filename for d in docs] == ["a.pdf"]


def test_volume_listener_can_skip_reading_content(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("hello")
    source = VolumeListenerSource({"path": str(tmp_path), "include_content": False})
    docs = list(source.discover(PipelineContext(pipeline_name="p")))
    assert docs[0].content is None


def test_api_source_submit_and_discover_drains_queue() -> None:
    source = ApiSource()
    context = PipelineContext(pipeline_name="p")

    source.submit("uri://a", content=b"a", metadata={"foo": "bar"})
    source.submit("uri://b", content=b"b")

    docs = list(source.discover(context))

    assert [d.uri for d in docs] == ["uri://a", "uri://b"]
    assert docs[0].metadata["foo"] == "bar"
    assert docs[0].metadata["source"] == "api"
    assert context.stats["received"] == 2

    # Draining again with nothing queued yields nothing.
    assert list(source.discover(context)) == []
