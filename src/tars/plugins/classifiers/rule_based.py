"""Deterministic classifier driven entirely by declarative rules -- no ML,
no LLM call, just extension/filename/mime-type matching. Cheap enough to
run on every document as a first pass before falling back to
`classifier.ai` for anything ambiguous.

Config:

    classifier:
      name: classifier.rule_based
      config:
        rules:
          - doc_type: invoice
            extensions: [pdf]
            filename_regex: "(?i)invoice"
          - doc_type: contract
            extensions: [pdf, docx]
            mime_types: [application/pdf]
            filename_regex: "(?i)(contract|agreement|msa)"
          - doc_type: image
            extensions: [png, jpg, jpeg]

Rules are evaluated in order; the first fully-matching rule wins. A rule
matches if *all* of its specified conditions match (conditions that are
omitted are ignored, not treated as failing).
"""

from __future__ import annotations

import re
from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Classifier


class RuleBasedClassifier(Classifier):
    kind = "classifier"

    def __init__(self, config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self.rules: list[dict[str, Any]] = self.config.get("rules", [])
        for rule in self.rules:
            if "doc_type" not in rule:
                raise ValueError(f"classifier.rule_based rule missing `doc_type`: {rule}")

    @staticmethod
    def _rule_matches(rule: dict[str, Any], document: Document) -> bool:
        extensions = rule.get("extensions")
        if extensions is not None and document.extension.lower() not in {
            e.lower().lstrip(".") for e in extensions
        }:
            return False

        mime_types = rule.get("mime_types")
        if mime_types is not None:
            mime_type = document.metadata.get("mime_type") or document.metadata.get("content_type")
            if mime_type not in mime_types:
                return False

        filename_regex = rule.get("filename_regex")
        if filename_regex is not None and not re.search(filename_regex, document.filename):
            return False

        path_regex = rule.get("path_regex")
        if path_regex is not None and not re.search(path_regex, document.uri):
            return False

        return True

    def classify(self, document: Document, context: PipelineContext) -> str | None:
        for rule in self.rules:
            if self._rule_matches(rule, document):
                return rule["doc_type"]
        return None
