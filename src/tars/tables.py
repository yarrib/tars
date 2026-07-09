"""Table DDL. Every job calls ensure_tables() first, so the system
self-provisions on first run. All statements are CREATE TABLE IF NOT EXISTS —
safe to run every time."""

from __future__ import annotations

from tars.config import Config

_DDL = {
    "doc_pairs": """
        pair_id STRING NOT NULL,
        doc_type STRING,
        client STRING,
        pdf_path STRING NOT NULL,
        xml_path STRING,
        pdf_hash STRING NOT NULL,
        xml_hash STRING,
        pdf_size_bytes BIGINT,
        pdf_modified_at TIMESTAMP,
        discovered_at TIMESTAMP
    """,
    "unpaired": """
        doc_type STRING,
        client STRING,
        path STRING,
        discovered_at TIMESTAMP
    """,
    "split_assignments": """
        client STRING NOT NULL,
        split STRING NOT NULL,
        method STRING,
        assigned_at TIMESTAMP
    """,
    "parsed_docs": """
        pdf_hash STRING NOT NULL,
        parse_output STRING,
        parsed_text STRING,
        page_count INT,
        parse_version STRING,
        status STRING,
        error STRING,
        parsed_at TIMESTAMP
    """,
    "ground_truth": """
        pair_id STRING NOT NULL,
        xml_hash STRING,
        field_name STRING NOT NULL,
        field_value STRING,
        source_xpath STRING,
        occurrence INT
    """,
    "field_map": """
        doc_type STRING NOT NULL,
        xml_field STRING NOT NULL,
        canonical_field STRING NOT NULL
    """,
    "extract_specs": """
        spec_id STRING NOT NULL,
        doc_type STRING NOT NULL,
        method STRING NOT NULL,
        extraction_schema STRING,
        prompt_text STRING,
        model STRING,
        created_by STRING,
        created_at TIMESTAMP,
        notes STRING
    """,
    "extractions": """
        pair_id STRING NOT NULL,
        pdf_hash STRING NOT NULL,
        spec_id STRING NOT NULL,
        doc_type STRING,
        split STRING,
        field_name STRING NOT NULL,
        value STRING,
        confidence DOUBLE,
        citations ARRAY<STRUCT<start: INT, stop: INT, text: STRING>>,
        error_message STRING,
        extracted_at TIMESTAMP
    """,
    "eval_results": """
        spec_id STRING NOT NULL,
        split STRING NOT NULL,
        doc_type STRING,
        field_name STRING,
        n BIGINT,
        n_with_gt BIGINT,
        exact_match_rate DOUBLE,
        normalized_match_rate DOUBLE,
        null_rate DOUBLE,
        gt_coverage DOUBLE,
        avg_confidence DOUBLE,
        evaluated_at TIMESTAMP
    """,
    "eval_calibration": """
        spec_id STRING NOT NULL,
        split STRING NOT NULL,
        confidence_bucket INT,
        n BIGINT,
        accuracy DOUBLE,
        avg_confidence DOUBLE,
        evaluated_at TIMESTAMP
    """,
}


def ensure_tables(spark, cfg: Config) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
    for name, columns in _DDL.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg.table(name)} ({columns})")
