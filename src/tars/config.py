"""Runtime configuration. All jobs receive catalog/schema/volume via CLI args
(set from bundle variables) with environment-variable fallbacks."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    catalog: str
    schema: str
    volume_root: str  # e.g. /Volumes/main/tax/docs — tree is <doc_type>/<client>/...
    dev_pct: int = 10  # split buckets: [0, dev) dev, [dev, dev+val) val, rest test
    val_pct: int = 20
    parse_version: str = "ai_parse_document:default"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    @classmethod
    def from_env(cls, catalog=None, schema=None, volume_root=None, **kwargs) -> "Config":
        catalog = catalog or os.environ.get("TARS_CATALOG")
        schema = schema or os.environ.get("TARS_SCHEMA")
        volume_root = volume_root or os.environ.get("TARS_VOLUME_ROOT")
        missing = [n for n, v in
                   [("catalog", catalog), ("schema", schema), ("volume_root", volume_root)]
                   if not v]
        if missing:
            raise SystemExit(
                f"Missing config: {', '.join(missing)} "
                "(pass --catalog/--schema/--volume-root or set TARS_CATALOG/TARS_SCHEMA/TARS_VOLUME_ROOT)")
        return cls(catalog=catalog, schema=schema, volume_root=volume_root.rstrip("/"), **kwargs)
