"""LLM-based classifier: asks Claude to pick a doc_type from a configured
label set, given the filename and a snippet of content. Meant as a
fallback after `classifier.rule_based` for documents that don't match any
deterministic rule.

Config:

    classifier:
      name: classifier.ai
      config:
        doc_types:
          invoice: "A bill requesting payment for goods or services"
          contract: "A legal agreement between two or more parties"
          resume: "A CV / job application document"
        model: claude-sonnet-5
        max_content_chars: 4000

The Anthropic client is created lazily (and can be injected directly for
tests via `AiClassifier(config, client=...)`), so importing this module
never requires the `anthropic` package or a configured API key.
"""

from __future__ import annotations

import os
from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Classifier

_SYSTEM_PROMPT = (
    "You classify documents into exactly one label. Respond with only the "
    "label text and nothing else. If none of the labels fit, respond with "
    "the single word: unknown."
)


class AiClassifier(Classifier):
    kind = "classifier"

    def __init__(self, config: dict[str, Any] | None = None, *, client: Any = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        doc_types = self.config.get("doc_types")
        if not doc_types:
            raise ValueError("classifier.ai requires `doc_types` (list or {label: description})")
        self.doc_types: dict[str, str] = (
            doc_types if isinstance(doc_types, dict) else {label: "" for label in doc_types}
        )
        self.model: str = self.config.get("model", "claude-sonnet-5")
        self.max_content_chars: int = self.config.get("max_content_chars", 4000)
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            try:
                import anthropic
            except ImportError as exc:  # pragma: no cover - exercised only without extra
                raise ImportError(
                    "classifier.ai requires the `tars[ai]` extra: pip install 'tars[ai]'"
                ) from exc
            api_key = self.config.get("api_key") or os.environ.get("ANTHROPIC_API_KEY")
            self._client = anthropic.Anthropic(api_key=api_key)
        return self._client

    def _build_prompt(self, document: Document) -> str:
        labels = "\n".join(
            f"- {label}: {description}" if description else f"- {label}"
            for label, description in self.doc_types.items()
        )
        snippet = ""
        if document.content:
            snippet = document.content[: self.max_content_chars].decode("utf-8", errors="ignore")
        return (
            f"Labels:\n{labels}\n\n"
            f"Filename: {document.filename}\n"
            f"Content snippet:\n{snippet}\n\n"
            "Which label best matches this document?"
        )

    def classify(self, document: Document, context: PipelineContext) -> str | None:
        response = self.client.messages.create(
            model=self.model,
            max_tokens=20,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": self._build_prompt(document)}],
        )
        label = response.content[0].text.strip()
        return label if label in self.doc_types else None
