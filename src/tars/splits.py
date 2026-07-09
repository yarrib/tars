"""Sticky, deterministic client-level split assignment.

The unit of independence is the client: all of a client's documents land in
exactly one of dev/val/test. Assignment is a pure function of the client name,
so new clients arriving later never reshuffle existing assignments — but every
assignment is still persisted to bronze.split_assignments for auditability and
manual pinning.
"""

from __future__ import annotations

import hashlib

METHOD = "sha256pct-v1"


def split_bucket(client: str) -> int:
    """Deterministic bucket in [0, 100) from the client name."""
    digest = hashlib.sha256(client.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % 100


def assign_split(client: str, dev_pct: int = 10, val_pct: int = 20) -> str:
    bucket = split_bucket(client)
    if bucket < dev_pct:
        return "dev"
    if bucket < dev_pct + val_pct:
        return "val"
    return "test"
