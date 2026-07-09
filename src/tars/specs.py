"""Spec registry helpers. Specs are append-only: editing means inserting a new
auto-versioned spec_id, never updating a row — that keeps cache keys stable and
old results comparable."""

from __future__ import annotations

import getpass
import json
import re
from datetime import datetime, timezone

from tars.config import Config
from tars.tables import ensure_tables


def add_spec(spark, cfg: Config, doc_type: str, extraction_schema: str,
             method: str = "ai_extract", prompt_text: str | None = None,
             model: str | None = None, notes: str | None = None) -> str:
    if method not in ("ai_extract", "ai_query"):
        raise SystemExit("method must be ai_extract or ai_query")
    if method == "ai_query" and not (prompt_text and model):
        raise SystemExit("ai_query specs require --prompt-file and --model")
    json.loads(extraction_schema)  # fail fast on invalid JSON

    ensure_tables(spark, cfg)
    existing = [
        row["spec_id"] for row in spark.sql(
            f"SELECT spec_id FROM {cfg.table('extract_specs')} WHERE doc_type = :dt",
            args={"dt": doc_type}).collect()
    ]
    versions = [int(m.group(1)) for s in existing
                if (m := re.search(r"-v(\d+)$", s))]
    spec_id = f"{doc_type}-v{max(versions, default=0) + 1}"

    spark.createDataFrame([{
        "spec_id": spec_id,
        "doc_type": doc_type,
        "method": method,
        "extraction_schema": extraction_schema,
        "prompt_text": prompt_text,
        "model": model,
        "created_by": getpass.getuser(),
        "created_at": datetime.now(timezone.utc),
        "notes": notes,
    }], "spec_id STRING, doc_type STRING, method STRING, extraction_schema STRING, "
        "prompt_text STRING, model STRING, created_by STRING, created_at TIMESTAMP, "
        "notes STRING").write.mode("append").saveAsTable(cfg.table("extract_specs"))
    print(f"spec created: {spec_id}")
    return spec_id
