"""Value canonicalization for evaluation joins. Ground-truth XML and model
output disagree on formatting far more often than on substance: "$1,234.50"
vs "1234.5", "01/15/2024" vs "2024-01-15". Normalization happens on both
sides before comparing."""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

_WHITESPACE = re.compile(r"\s+")
_NUMBERISH = re.compile(r"^\(?\$?\s*-?[\d,]+(?:\.\d+)?\)?$")
_DATE_FORMATS = (
    "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y",
    "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%Y%m%d",
)


def normalize_value(value) -> str | None:
    """Canonical string form: collapsed whitespace + lowercase, with numbers
    (currency-tolerant, parens = negative) and dates canonicalized."""
    if value is None:
        return None
    s = _WHITESPACE.sub(" ", str(value)).strip()
    if not s:
        return ""

    if _NUMBERISH.match(s):
        cleaned = s.replace("$", "").replace(",", "").replace(" ", "")
        negative = cleaned.startswith("(") and cleaned.endswith(")")
        cleaned = cleaned.strip("()")
        try:
            number = Decimal(cleaned)
            if negative:
                number = -number
            return format(number.normalize(), "f")
        except InvalidOperation:
            pass

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue

    return s.lower()


def values_match(extracted, ground_truth_values: list) -> tuple[bool, bool]:
    """(exact_match, normalized_match) of one extracted value against every
    ground-truth value recorded for the field (repeated XML tags are legal)."""
    gt = [g for g in ground_truth_values if g is not None]
    exact = extracted is not None and str(extracted) in [str(g) for g in gt]
    norm_extracted = normalize_value(extracted)
    normalized = norm_extracted is not None and norm_extracted in [normalize_value(g) for g in gt]
    return exact, normalized
