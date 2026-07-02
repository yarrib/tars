from __future__ import annotations

import pytest

from tars.config.schema import PluginRef, RouterConfig
from tars.pipeline.context import Document, PipelineContext
from tars.plugins.registry import PluginRegistry
from tars.router.router import DocumentRouter, UnroutableDocumentError


@pytest.fixture
def registry() -> PluginRegistry:
    return PluginRegistry(load_entry_points=True)


def _router(registry: PluginRegistry, **overrides) -> DocumentRouter:
    config = RouterConfig(
        classifiers=[
            PluginRef(
                name="classifier.rule_based",
                config={
                    "rules": [
                        {"doc_type": "invoice", "filename_regex": "(?i)invoice"},
                        {"doc_type": "contract", "filename_regex": "(?i)contract"},
                    ]
                },
            )
        ],
        routes=[
            {"doc_type": "invoice", "pipeline": "invoice_pipeline"},
            {"doc_type": "contract", "pipeline": "contract_pipeline"},
        ],
        **overrides,
    )
    return DocumentRouter(config, registry=registry)


def test_routes_by_classified_doc_type(registry: PluginRegistry) -> None:
    router = _router(registry)
    context = PipelineContext(pipeline_name="router")
    doc = Document(uri="/landing/Invoice_123.pdf")

    pipeline = router.route(doc, context)

    assert doc.doc_type == "invoice"
    assert pipeline == "invoice_pipeline"


def test_falls_back_to_default_pipeline(registry: PluginRegistry) -> None:
    router = _router(registry, default_pipeline="catch_all")
    context = PipelineContext(pipeline_name="router")
    doc = Document(uri="/landing/random_file.pdf")

    assert router.route(doc, context) == "catch_all"


def test_unroutable_document_raises_when_strict(registry: PluginRegistry) -> None:
    router = _router(registry)
    context = PipelineContext(pipeline_name="router")
    doc = Document(uri="/landing/random_file.pdf")

    with pytest.raises(UnroutableDocumentError):
        router.route(doc, context)


def test_unroutable_document_returns_none_when_not_strict(registry: PluginRegistry) -> None:
    router = _router(registry)
    context = PipelineContext(pipeline_name="router")
    doc = Document(uri="/landing/random_file.pdf")

    assert router.route(doc, context, strict=False) is None


def test_already_classified_document_is_not_reclassified(registry: PluginRegistry) -> None:
    router = _router(registry)
    context = PipelineContext(pipeline_name="router")
    doc = Document(uri="/landing/random_file.pdf", doc_type="contract")

    assert router.route(doc, context) == "contract_pipeline"
