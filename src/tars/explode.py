"""Pure flattening of ai_extract v2.1 (and ai_query fallback) output into
per-field rows: {field_name, value, confidence, citations, error_message}.

ai_extract v2.1 shape (text input):
    {
      "response": {"<field>": {"value": ..., "confidence": 0.93, "citation_ids": [0]},
                   "<group>": {"<field>": {...}}},
      "error_message": null,
      "metadata": {"citations": [{"id": 0, "start": 120, "stop": 126}]}
    }

Citations are character offsets into the input text; the cited span is
materialized here via parsed_text[start:stop] so downstream never touches
offsets.
"""

from __future__ import annotations

import json
from typing import Any


def _row(field_name: str, value: Any, confidence: Any, citations: list[dict],
         error_message: str | None) -> dict:
    if value is not None and not isinstance(value, str):
        value = json.dumps(value)
    return {
        "field_name": field_name,
        "value": value,
        "confidence": float(confidence) if confidence is not None else None,
        "citations": citations,
        "error_message": error_message,
    }


def explode_extraction(result: str | dict, parsed_text: str | None) -> list[dict]:
    """Flatten one ai_extract result into per-field rows.

    A document-level error yields a single '_document' row carrying the error.
    Nested response objects produce dotted field names; arrays produce [i]
    suffixes. A node is a scalar output iff it is a dict with a 'value' key.
    """
    data = json.loads(result) if isinstance(result, str) else (result or {})
    error = data.get("error_message")
    if error:
        return [_row("_document", None, None, [], str(error))]

    meta_citations = {
        c.get("id"): c
        for c in ((data.get("metadata") or {}).get("citations") or [])
        if isinstance(c, dict)
    }

    def resolve_citations(node: dict) -> list[dict]:
        citations = []
        for cid in node.get("citation_ids") or []:
            meta = meta_citations.get(cid)
            if not meta or meta.get("start") is None or meta.get("stop") is None:
                continue
            start, stop = int(meta["start"]), int(meta["stop"])
            text = parsed_text[start:stop] if parsed_text else None
            citations.append({"start": start, "stop": stop, "text": text})
        return citations

    rows: list[dict] = []

    def emit(name: str, node: Any) -> None:
        if isinstance(node, dict) and "value" in node:
            confidence = node.get("confidence", node.get("confidence_score"))
            rows.append(_row(name, node["value"], confidence, resolve_citations(node), None))
        elif isinstance(node, dict):
            for key, value in node.items():
                emit(f"{name}.{key}" if name else key, value)
        elif isinstance(node, list):
            for i, item in enumerate(node):
                emit(f"{name}[{i}]", item)
        else:
            rows.append(_row(name, node, None, [], None))

    emit("", data.get("response") or {})
    return rows


def explode_ai_query(response: str | None, parsed_text: str | None) -> list[dict]:
    """Flatten the ai_query escape-hatch structured output:
    {"fields": [{"name", "value", "confidence", "evidence", "page"}, ...]}.

    Evidence is a model-quoted string, so it is grounded here by locating it in
    parsed_text; when found, real offsets are recorded, otherwise start/stop
    stay null and the quote is kept as-is (reviewers see it flagged offsetless).
    """
    if not response:
        return [_row("_document", None, None, [], "empty ai_query response")]
    try:
        data = json.loads(response) if isinstance(response, str) else response
    except json.JSONDecodeError as exc:
        return [_row("_document", None, None, [], f"unparseable ai_query response: {exc}")]

    rows: list[dict] = []
    for field in data.get("fields") or []:
        citations = []
        evidence = field.get("evidence")
        if evidence:
            start = (parsed_text or "").find(evidence)
            if start >= 0:
                citations.append({"start": start, "stop": start + len(evidence), "text": evidence})
            else:
                citations.append({"start": None, "stop": None, "text": evidence})
        rows.append(_row(field.get("name") or "_unnamed", field.get("value"),
                         field.get("confidence"), citations, None))
    return rows
