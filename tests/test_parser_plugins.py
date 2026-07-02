from __future__ import annotations

import pytest

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.parsers.ai_parser import AiParser
from tars.plugins.parsers.text_parser import TextParser


def test_text_parser_decodes_content() -> None:
    parser = TextParser()
    doc = Document(uri="x", content=b"hello world")

    result = parser.parse(doc, PipelineContext(pipeline_name="p"))

    assert result == {"text": "hello world", "length": 11}


def test_text_parser_handles_no_content() -> None:
    parser = TextParser()
    doc = Document(uri="x", content=None)
    assert parser.parse(doc, PipelineContext(pipeline_name="p")) == {"text": "", "length": 0}


class _FakeToolUseBlock:
    type = "tool_use"

    def __init__(self, input: dict) -> None:
        self.input = input


class _FakeMessage:
    def __init__(self, content: list) -> None:
        self.content = content


class _FakeAnthropicClient:
    def __init__(self, tool_input: dict) -> None:
        self.tool_input = tool_input
        self.calls: list[dict] = []
        self.messages = self

    def create(self, **kwargs) -> _FakeMessage:
        self.calls.append(kwargs)
        return _FakeMessage([_FakeToolUseBlock(self.tool_input)])


def test_ai_parser_extracts_fields_via_tool_use() -> None:
    client = _FakeAnthropicClient({"vendor_name": "Acme", "total_amount": 42})
    parser = AiParser(
        {
            "fields": [
                {"name": "vendor_name", "type": "string"},
                {"name": "total_amount", "type": "number"},
            ]
        },
        client=client,
    )
    doc = Document(uri="x", content=b"Invoice from Acme, total $42")

    result = parser.parse(doc, PipelineContext(pipeline_name="p"))

    assert result == {"vendor_name": "Acme", "total_amount": 42}
    call = client.calls[0]
    assert call["tool_choice"] == {"type": "tool", "name": "extract_fields"}
    schema = call["tools"][0]["input_schema"]
    assert set(schema["properties"]) == {"vendor_name", "total_amount"}
    assert set(schema["required"]) == {"vendor_name", "total_amount"}


def test_ai_parser_optional_fields_are_not_required() -> None:
    parser = AiParser(
        {"fields": [{"name": "a"}, {"name": "b", "required": False}]},
        client=_FakeAnthropicClient({"a": "x"}),
    )
    schema = parser._build_schema()
    assert schema["required"] == ["a"]


def test_ai_parser_requires_fields_or_schema() -> None:
    with pytest.raises(ValueError, match="fields"):
        AiParser({})
