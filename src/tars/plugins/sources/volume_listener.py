"""Polling source over a directory tree.

Unity Catalog Volumes are FUSE-mounted at `/Volumes/<catalog>/<schema>/<volume>/...`
on Databricks clusters, so plain `pathlib` operations work directly -- no
`dbutils` dependency required. This also makes the plugin trivially usable
against a local directory in tests or outside Databricks entirely.

Because polling has no natural "new since last time" signal, this source
persists a small JSON checkpoint (`state_path`, default
`<path>/.tars_checkpoint.json`) recording the URIs it has already emitted.

Config:

    source:
      name: source.volume_listener
      config:
        path: /Volumes/main/landing/raw_docs
        patterns: ["*.pdf", "*.docx"]     # optional glob filters, default all files
        include_content: true
        state_path: /Volumes/main/landing/_state/raw_docs.json   # optional
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from tars.pipeline.context import Document, PipelineContext
from tars.plugins.base import Source


class VolumeListenerSource(Source):
    kind = "source"

    def __init__(self, config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        if "path" not in self.config:
            raise ValueError("source.volume_listener requires `path`")
        self.path = Path(self.config["path"])
        self.patterns: list[str] = self.config.get("patterns") or ["*"]
        self.include_content: bool = self.config.get("include_content", True)
        state_path = self.config.get("state_path")
        self.state_path = Path(state_path) if state_path else self.path / ".tars_checkpoint.json"

    def _load_seen(self) -> set[str]:
        if self.state_path.exists():
            return set(json.loads(self.state_path.read_text()).get("seen", []))
        return set()

    def _save_seen(self, seen: set[str]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps({"seen": sorted(seen)}))

    def discover(self, context: PipelineContext) -> Iterator[Document]:
        seen = self._load_seen()
        found: list[Path] = []
        for pattern in self.patterns:
            found.extend(self.path.rglob(pattern))

        new_files = sorted(
            {
                f
                for f in found
                if f.is_file() and str(f) not in seen and f.resolve() != self.state_path.resolve()
            }
        )
        for file_path in new_files:
            stat = file_path.stat()
            yield Document(
                uri=str(file_path),
                content=file_path.read_bytes() if self.include_content else None,
                metadata={
                    "source": "volume_listener",
                    "size_bytes": stat.st_size,
                    "modified_at": stat.st_mtime,
                },
            )
            seen.add(str(file_path))
            context.increment("discovered")

        self._save_seen(seen)
