"""Core runtime data structures passed between plugins."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Document:
    """A single file moving through a tars pipeline.

    ``content`` is intentionally optional: sources may choose to only
    populate ``uri`` (e.g. a Unity Catalog Volume path or cloud object URL)
    and let a downstream parser stream the bytes itself, which matters for
    large files.
    """

    uri: str
    content: bytes | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    doc_type: str | None = None
    doc_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    ingested_at: float = field(default_factory=time.time)
    extracted: dict[str, Any] | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def filename(self) -> str:
        return self.uri.rsplit("/", 1)[-1]

    @property
    def extension(self) -> str:
        name = self.filename
        return name.rsplit(".", 1)[-1].lower() if "." in name else ""

    def fail(self, message: str) -> None:
        self.errors.append(message)

    @property
    def failed(self) -> bool:
        return bool(self.errors)


@dataclass
class PipelineContext:
    """Shared, mutable state for a single pipeline execution."""

    pipeline_name: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    params: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, int] = field(default_factory=dict)

    def increment(self, key: str, by: int = 1) -> None:
        self.stats[key] = self.stats.get(key, 0) + by
