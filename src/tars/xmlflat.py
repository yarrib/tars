"""Pure XML ground-truth flattening: leaf elements to (field_path, value) rows.

Field paths are dot-joined tag names excluding the root tag (which is constant
per doc type and pure noise). Namespaces are stripped; attributes are ignored.
Repeated paths keep their document order — the caller assigns occurrence
indexes if needed.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET


def _local(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def flatten_xml(xml_text: str) -> list[tuple[str, str]]:
    root = ET.fromstring(xml_text)
    rows: list[tuple[str, str]] = []

    def walk(element: ET.Element, path: str) -> None:
        name = f"{path}.{_local(element.tag)}" if path else _local(element.tag)
        children = list(element)
        if children:
            for child in children:
                walk(child, name)
        else:
            rows.append((name, (element.text or "").strip()))

    children = list(root)
    if not children:
        rows.append((_local(root.tag), (root.text or "").strip()))
    else:
        for child in children:
            walk(child, "")
    return rows
