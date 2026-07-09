"""Parsing job: run ai_parse_document over PDFs not yet in silver.parsed_docs,
store the full parse JSON plus the concatenated text that extraction (and its
citation offsets) will index into. Cached by pdf_hash — the expensive call
never repeats for unchanged content."""

from __future__ import annotations

from tars.config import Config
from tars.parsetext import extract_text
from tars.tables import ensure_tables


def run_parse(spark, cfg: Config, batch_limit: int | None = None) -> int:
    import json

    from pyspark.sql import functions as F
    from pyspark.sql.types import (IntegerType, StringType, StructField,
                                   StructType)

    ensure_tables(spark, cfg)
    todo = spark.sql(f"""
        SELECT p.pdf_hash, min(p.pdf_path) AS pdf_path
        FROM {cfg.table('doc_pairs')} p
        LEFT ANTI JOIN {cfg.table('parsed_docs')} d ON p.pdf_hash = d.pdf_hash
        GROUP BY p.pdf_hash
    """)
    if batch_limit:
        todo = todo.limit(batch_limit)
    path_to_hash = {row["pdf_path"]: row["pdf_hash"] for row in todo.collect()}
    if not path_to_hash:
        print("parse: nothing new to parse")
        return 0
    # binaryFile reports paths with a dbfs: scheme; accept both forms.
    for path, pdf_hash in list(path_to_hash.items()):
        path_to_hash[f"dbfs:{path}"] = pdf_hash

    binary = spark.read.format("binaryFile").load(list(path_to_hash))
    parsed = binary.select(
        F.col("path"),
        F.to_json(F.expr("ai_parse_document(content)")).alias("parse_output"),
    )

    text_schema = StructType([
        StructField("parsed_text", StringType()),
        StructField("page_count", IntegerType()),
    ])

    @F.udf(text_schema)
    def text_of(parse_output: str):
        try:
            text, pages = extract_text(json.loads(parse_output))
            return {"parsed_text": text, "page_count": pages}
        except Exception:
            return {"parsed_text": None, "page_count": None}

    hash_of = F.udf(lambda p: path_to_hash.get(p), StringType())

    result = (
        parsed
        .withColumn("pdf_hash", hash_of("path"))
        .withColumn("extracted", text_of("parse_output"))
        .select(
            "pdf_hash",
            "parse_output",
            F.col("extracted.parsed_text").alias("parsed_text"),
            F.col("extracted.page_count").alias("page_count"),
            F.lit(cfg.parse_version).alias("parse_version"),
            F.when(F.length(F.col("extracted.parsed_text")) > 0, F.lit("ok"))
             .otherwise(F.lit("failed")).alias("status"),
            F.when(F.length(F.col("extracted.parsed_text")) > 0, F.lit(None).cast("string"))
             .otherwise(F.lit("empty or unreadable parse output")).alias("error"),
            F.current_timestamp().alias("parsed_at"),
        )
        .where(F.col("pdf_hash").isNotNull())
        .dropDuplicates(["pdf_hash"])
    )
    result.write.mode("append").saveAsTable(cfg.table("parsed_docs"))
    count = len(set(path_to_hash.values()))
    print(f"parse: processed {count} new documents")
    return count
