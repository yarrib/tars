"""Pure pair-matching logic: within one folder, a PDF and an XML whose filename
stems contain one another (case-insensitive) form a ground-truth pair."""

from __future__ import annotations

import os


def _stem(name: str) -> str:
    return os.path.splitext(name)[0].lower()


def match_files(names: list[str]) -> tuple[list[tuple[str, str]], list[str]]:
    """Match PDFs to XMLs within a single folder's file names.

    Returns (pairs, unpaired) where pairs is a list of (pdf, xml) and unpaired
    contains PDFs and XMLs that found no partner. Each XML is used at most once;
    on multiple candidates an exact stem match wins, then the longest stem
    (deterministic: sorted input order breaks remaining ties).
    """
    pdfs = sorted(n for n in names if n.lower().endswith(".pdf"))
    xmls = sorted(n for n in names if n.lower().endswith(".xml"))
    available = list(xmls)
    pairs: list[tuple[str, str]] = []
    unpaired_pdfs: list[str] = []
    for pdf in pdfs:
        ps = _stem(pdf)
        cands = [x for x in available if ps in _stem(x) or _stem(x) in ps]
        if not cands:
            unpaired_pdfs.append(pdf)
            continue
        exact = [x for x in cands if _stem(x) == ps]
        best = exact[0] if exact else max(cands, key=lambda x: len(_stem(x)))
        available.remove(best)
        pairs.append((pdf, best))
    return pairs, unpaired_pdfs + available
