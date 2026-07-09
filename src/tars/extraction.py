"""Extraction job: run one spec (ai_extract v2.1 by default) over one split,
skipping documents already extracted for that spec.

Split guard: this job refuses the test split unless allow_test=True, which
only the final-eval entry point sets — the holdout cannot be burned from the
iteration workflow.
"""

from __future__ import annotations

from tars.config import Config
from tars.explode import explode_ai_query, explode_extraction
from tars.tables import ensure_tables

ITERATION_SPLITS = ("dev", "val")

_ROW_SCHEMA = (
    "ARRAY<STRUCT<field_name: STRING, value: STRING, confidence: DOUBLE, "
    "citations: ARRAY<STRUCT<start: INT, stop: INT, text: STRING>>, "
    "error_message: STRING>>"
)


def load_spec(spark, cfg: Config, spec_id: str):
    specs = spark.sql(
        f"SELECT * FROM {cfg.table('extract_specs')} WHERE spec_id = :sid",
        args={"sid": spec_id}).collect()
    if not specs:
        raise SystemExit(f"spec '{spec_id}' not found in {cfg.table('extract_specs')}")
    return specs[0]


def run_extract(spark, cfg: Config, spec_id: str, split: str = "dev",
                allow_test: bool = False) -> int:
    from pyspark.sql import functions as F

    allowed = ("dev", "val", "test") if allow_test else ITERATION_SPLITS
    if split not in allowed:
        raise SystemExit(
            f"split '{split}' not allowed here (allowed: {', '.join(allowed)}). "
            "The test split is reserved for the final-eval job.")

    ensure_tables(spark, cfg)
    spec = load_spec(spark, cfg, spec_id)

    candidates = spark.sql(f"""
        SELECT p.pair_id, p.pdf_hash, p.doc_type, s.split, d.parsed_text
        FROM {cfg.table('doc_pairs')} p
        JOIN {cfg.table('split_assignments')} s ON p.client = s.client
        JOIN {cfg.table('parsed_docs')} d ON p.pdf_hash = d.pdf_hash AND d.status = 'ok'
        LEFT ANTI JOIN (
            SELECT DISTINCT pdf_hash FROM {cfg.table('extractions')} WHERE spec_id = :sid
        ) done ON p.pdf_hash = done.pdf_hash
        WHERE s.split = :split AND p.doc_type = :doc_type
    """, args={"sid": spec_id, "split": split, "doc_type": spec["doc_type"]})
    candidates = candidates.dropDuplicates(["pdf_hash"])
    candidates.createOrReplaceTempView("tars_extract_candidates")

    if candidates.isEmpty():
        print(f"extract: nothing to do for spec={spec_id} split={split} "
              "(already extracted or no parsed documents)")
        return 0

    if spec["method"] == "ai_extract":
        extracted = spark.sql("""
            SELECT pair_id, pdf_hash, doc_type, split, parsed_text,
                   to_json(ai_extract(parsed_text, :schema,
                       map('version', '2.1',
                           'enableConfidenceScores', 'true',
                           'enableCitations', 'true'))) AS result
            FROM tars_extract_candidates
        """, args={"schema": spec["extraction_schema"]})
        explode_fn = explode_extraction
    elif spec["method"] == "ai_query":
        extracted = spark.sql("""
            SELECT pair_id, pdf_hash, doc_type, split, parsed_text,
                   ai_query(:model,
                            concat(:prompt, '\\n\\n<document>\\n', parsed_text, '\\n</document>'),
                            responseFormat => :schema) AS result
            FROM tars_extract_candidates
        """, args={"model": spec["model"], "prompt": spec["prompt_text"],
                   "schema": spec["extraction_schema"]})
        explode_fn = explode_ai_query
    else:
        raise SystemExit(f"unknown spec method '{spec['method']}'")

    @F.udf(_ROW_SCHEMA)
    def explode_result(result: str, parsed_text: str):
        try:
            return explode_fn(result, parsed_text)
        except Exception as exc:
            return [{"field_name": "_document", "value": None, "confidence": None,
                     "citations": [], "error_message": f"explode failed: {exc}"}]

    rows = (
        extracted
        .withColumn("fields", explode_result("result", "parsed_text"))
        .select("pair_id", "pdf_hash", "doc_type", "split",
                F.explode("fields").alias("f"))
        .select(
            "pair_id", "pdf_hash",
            F.lit(spec_id).alias("spec_id"),
            "doc_type", "split",
            F.col("f.field_name").alias("field_name"),
            F.col("f.value").alias("value"),
            F.col("f.confidence").alias("confidence"),
            F.col("f.citations").alias("citations"),
            F.col("f.error_message").alias("error_message"),
            F.current_timestamp().alias("extracted_at"),
        )
        .dropDuplicates(["pdf_hash", "spec_id", "field_name"])
    )
    rows.write.mode("append").saveAsTable(cfg.table("extractions"))
    count = candidates.count()
    print(f"extract: spec={spec_id} split={split} processed {count} documents")
    return count
