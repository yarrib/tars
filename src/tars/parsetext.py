"""Pure text extraction from ai_parse_document output.

The concatenated text produced here is the exact string later passed to
ai_extract, so citation offsets index into it. It is stored in
silver.parsed_docs and must be treated as immutable per pdf_hash.
"""

from __future__ import annotations

from typing import Any


def extract_text(parse: dict[str, Any]) -> tuple[str, int | None]:
    """Return (parsed_text, page_count) from an ai_parse_document result dict.

    Primary path walks document.elements in order, inserting a page marker on
    page transitions. Falls back to recursively collecting every 'content'
    string if the shape is unrecognized (parser versions drift).
    """
    doc = parse.get("document") or {}
    elements = doc.get("elements") or []
    pages = doc.get("pages") or []
    page_count = len(pages) or None

    if elements:
        parts: list[str] = []
        current_page: Any = object()  # sentinel that never equals a real id
        for el in elements:
            if not isinstance(el, dict):
                continue
            page_id = el.get("page_id", el.get("page_number"))
            if page_id is not None and page_id != current_page:
                label = page_id + 1 if isinstance(page_id, int) else page_id
                parts.append(f"\n\n[page {label}]\n")
                current_page = page_id
            content = el.get("content") or el.get("text") or ""
            if content:
                parts.append(content + "\n")
        text = "".join(parts).strip()
        if text:
            return text, page_count

    collected: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in ("content", "text") and isinstance(value, str):
                    collected.append(value)
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(parse)
    return "\n".join(collected).strip(), page_count
