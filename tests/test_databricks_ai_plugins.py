from __future__ import annotations

from typing import Any

import pytest

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.classifiers.databricks_ai import DatabricksAiClassifier
from tars.plugins.parsers.databricks_ai_parser import DatabricksAiParser


class _FakeRow:
    def __init__(self, **values: Any) -> None:
        self._values = values

    def __getitem__(self, key: str) -> Any:
        return self._values[key]


class _FakeDataFrame:
    def __init__(self, rows: list[_FakeRow]) -> None:
        self._rows = rows

    def collect(self) -> list[_FakeRow]:
        return self._rows


class _FakeSpark:
    """Records every `sql()` call and returns a scripted response keyed by
    which AI function the query invokes, so tests can assert on both the
    query shape and the plugin's handling of the result."""

    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def sql(self, query: str, args: dict[str, Any] | None = None) -> _FakeDataFrame:
        self.calls.append((query, args or {}))
        for marker, value in self.responses.items():
            if marker in query:
                return _FakeDataFrame([_FakeRow(**value)])
        raise AssertionError(f"No scripted response for query: {query}")


def test_databricks_ai_classifier_returns_matching_label() -> None:
    spark = _FakeSpark({"ai_classify": {"label": "invoice"}})
    classifier = DatabricksAiClassifier({"doc_types": ["invoice", "contract"]}, spark=spark)
    doc = Document(uri="x", content=b"Please pay $500 for services rendered.")

    result = classifier.classify(doc, PipelineContext(pipeline_name="p"))

    assert result == "invoice"
    query, args = spark.calls[0]
    assert "ai_classify" in query
    assert "text" in args


def test_databricks_ai_classifier_rejects_unknown_label() -> None:
    spark = _FakeSpark({"ai_classify": {"label": "spam"}})
    classifier = DatabricksAiClassifier({"doc_types": ["invoice", "contract"]}, spark=spark)
    doc = Document(uri="x", content=b"content")

    assert classifier.classify(doc, PipelineContext(pipeline_name="p")) is None


def test_databricks_ai_classifier_skips_documents_without_content() -> None:
    spark = _FakeSpark({})
    classifier = DatabricksAiClassifier({"doc_types": ["invoice"]}, spark=spark)
    doc = Document(uri="x", content=None)

    assert classifier.classify(doc, PipelineContext(pipeline_name="p")) is None
    assert spark.calls == []


def test_databricks_ai_classifier_requires_doc_types() -> None:
    with pytest.raises(ValueError, match="doc_types"):
        DatabricksAiClassifier({})


def test_databricks_ai_parser_parses_document_only() -> None:
    spark = _FakeSpark({"ai_parse_document": {"parsed": {"document": {"text": "hello"}}}})
    parser = DatabricksAiParser({}, spark=spark)
    doc = Document(uri="x", content=b"%PDF-1.4 fake bytes")

    result = parser.parse(doc, PipelineContext(pipeline_name="p"))

    assert result["parsed_document"] == {"document": {"text": "hello"}}
    assert len(spark.calls) == 1


def test_databricks_ai_parser_extracts_fields_via_ai_query() -> None:
    spark = _FakeSpark(
        {
            "ai_parse_document": {"parsed": {"document": {"text": "Vendor: Acme, Total: 100"}}},
            "ai_query": {"extracted": {"vendor_name": "Acme", "total_amount": 100}},
        }
    )
    parser = DatabricksAiParser(
        {
            "endpoint": "databricks-claude-sonnet-5",
            "fields": [{"name": "vendor_name", "type": "string"}, {"name": "total_amount", "type": "number"}],
        },
        spark=spark,
    )
    doc = Document(uri="x", content=b"raw bytes")

    result = parser.parse(doc, PipelineContext(pipeline_name="p"))

    assert result["vendor_name"] == "Acme"
    assert result["total_amount"] == 100
    assert "parsed_document" in result
    assert len(spark.calls) == 2


def test_databricks_ai_parser_requires_endpoint_when_fields_given() -> None:
    with pytest.raises(ValueError, match="endpoint"):
        DatabricksAiParser({"fields": [{"name": "x"}]})


def test_databricks_ai_parser_returns_empty_for_no_content() -> None:
    parser = DatabricksAiParser({}, spark=_FakeSpark({}))
    doc = Document(uri="x", content=None)
    assert parser.parse(doc, PipelineContext(pipeline_name="p")) == {}
