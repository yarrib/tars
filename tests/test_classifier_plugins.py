from __future__ import annotations

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.classifiers.ai_classifier import AiClassifier
from tars.plugins.classifiers.rule_based import RuleBasedClassifier


def _classify(rules: list[dict], uri: str, **doc_kwargs) -> str | None:
    classifier = RuleBasedClassifier({"rules": rules})
    return classifier.classify(Document(uri=uri, **doc_kwargs), PipelineContext(pipeline_name="p"))


def test_rule_based_matches_by_extension() -> None:
    rules = [{"doc_type": "image", "extensions": ["png", "jpg"]}]
    assert _classify(rules, "/x/a.png") == "image"
    assert _classify(rules, "/x/a.pdf") is None


def test_rule_based_matches_by_filename_regex() -> None:
    rules = [{"doc_type": "invoice", "filename_regex": "(?i)invoice"}]
    assert _classify(rules, "/x/Invoice_2024.pdf") == "invoice"
    assert _classify(rules, "/x/contract.pdf") is None


def test_rule_based_requires_all_conditions() -> None:
    rules = [{"doc_type": "invoice", "extensions": ["pdf"], "filename_regex": "(?i)invoice"}]
    assert _classify(rules, "/x/invoice.txt") is None
    assert _classify(rules, "/x/invoice.pdf") == "invoice"


def test_rule_based_matches_mime_type() -> None:
    rules = [{"doc_type": "invoice", "mime_types": ["application/pdf"]}]
    doc = Document(uri="/x/a", metadata={"mime_type": "application/pdf"})
    classifier = RuleBasedClassifier({"rules": rules})
    assert classifier.classify(doc, PipelineContext(pipeline_name="p")) == "invoice"


def test_rule_based_first_match_wins() -> None:
    rules = [
        {"doc_type": "invoice", "extensions": ["pdf"]},
        {"doc_type": "any_pdf", "extensions": ["pdf"]},
    ]
    assert _classify(rules, "/x/a.pdf") == "invoice"


def test_rule_based_returns_none_with_no_rules() -> None:
    assert _classify([], "/x/a.pdf") is None


class _FakeTextBlock:
    def __init__(self, text: str) -> None:
        self.text = text


class _FakeMessage:
    def __init__(self, text: str) -> None:
        self.content = [_FakeTextBlock(text)]


class _FakeAnthropicClient:
    def __init__(self, reply: str) -> None:
        self.reply = reply
        self.calls: list[dict] = []
        self.messages = self

    def create(self, **kwargs) -> _FakeMessage:
        self.calls.append(kwargs)
        return _FakeMessage(self.reply)


def test_ai_classifier_returns_matching_label() -> None:
    client = _FakeAnthropicClient("invoice")
    classifier = AiClassifier({"doc_types": {"invoice": "a bill", "contract": "a legal doc"}}, client=client)
    doc = Document(uri="x", content=b"please pay $100")

    result = classifier.classify(doc, PipelineContext(pipeline_name="p"))

    assert result == "invoice"
    assert client.calls[0]["model"] == "claude-sonnet-5"


def test_ai_classifier_rejects_label_outside_configured_set() -> None:
    client = _FakeAnthropicClient("unknown")
    classifier = AiClassifier({"doc_types": ["invoice", "contract"]}, client=client)
    doc = Document(uri="x", content=b"???")

    assert classifier.classify(doc, PipelineContext(pipeline_name="p")) is None
