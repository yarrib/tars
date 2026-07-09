"""Evaluation job: join one spec's extractions to ground truth, compute
per-field accuracy and a confidence calibration curve. Cheap and derived —
results for the spec are replaced wholesale on every run."""

from __future__ import annotations

from tars.config import Config
from tars.normalize import values_match
from tars.tables import ensure_tables


def run_evaluate(spark, cfg: Config, spec_id: str) -> None:
    from pyspark.sql import functions as F

    ensure_tables(spark, cfg)

    extractions = (
        spark.table(cfg.table("extractions"))
        .where(F.col("spec_id") == spec_id)
        .where(F.col("field_name") != "_document")
    )
    ground_truth = (
        spark.table(cfg.table("ground_truth"))
        .groupBy("pair_id", "field_name")
        .agg(F.collect_list("field_value").alias("gt_values"))
    )

    @F.udf("STRUCT<exact: BOOLEAN, normalized: BOOLEAN>")
    def match(value, gt_values):
        exact, normalized = values_match(value, gt_values or [])
        return {"exact": exact, "normalized": normalized}

    joined = (
        extractions
        .join(ground_truth, ["pair_id", "field_name"], "left")
        .withColumn("has_gt", F.col("gt_values").isNotNull())
        .withColumn("m", match("value", "gt_values"))
    )
    joined.cache()

    results = (
        joined
        .groupBy("spec_id", "split", "doc_type", "field_name")
        .agg(
            F.count("*").alias("n"),
            F.sum(F.col("has_gt").cast("long")).alias("n_with_gt"),
            F.avg(F.when(F.col("has_gt"), F.col("m.exact").cast("double"))).alias("exact_match_rate"),
            F.avg(F.when(F.col("has_gt"), F.col("m.normalized").cast("double"))).alias("normalized_match_rate"),
            F.avg(F.col("value").isNull().cast("double")).alias("null_rate"),
            F.avg(F.col("has_gt").cast("double")).alias("gt_coverage"),
            F.avg("confidence").alias("avg_confidence"),
        )
        .withColumn("evaluated_at", F.current_timestamp())
    )

    calibration = (
        joined
        .where(F.col("has_gt") & F.col("confidence").isNotNull())
        .withColumn("confidence_bucket",
                    F.least(F.floor(F.col("confidence") * 10), F.lit(9)).cast("int"))
        .groupBy("spec_id", "split", "confidence_bucket")
        .agg(
            F.count("*").alias("n"),
            F.avg(F.col("m.normalized").cast("double")).alias("accuracy"),
            F.avg("confidence").alias("avg_confidence"),
        )
        .withColumn("evaluated_at", F.current_timestamp())
    )

    spark.sql(f"DELETE FROM {cfg.table('eval_results')} WHERE spec_id = :sid",
              args={"sid": spec_id})
    results.select("spec_id", "split", "doc_type", "field_name", "n", "n_with_gt",
                   "exact_match_rate", "normalized_match_rate", "null_rate",
                   "gt_coverage", "avg_confidence", "evaluated_at") \
        .write.mode("append").saveAsTable(cfg.table("eval_results"))

    spark.sql(f"DELETE FROM {cfg.table('eval_calibration')} WHERE spec_id = :sid",
              args={"sid": spec_id})
    calibration.select("spec_id", "split", "confidence_bucket", "n", "accuracy",
                       "avg_confidence", "evaluated_at") \
        .write.mode("append").saveAsTable(cfg.table("eval_calibration"))

    joined.unpersist()
    print(f"evaluate: refreshed eval_results and eval_calibration for spec={spec_id}")
