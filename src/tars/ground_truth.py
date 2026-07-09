"""Ground-truth job: flatten XML files not yet in silver.ground_truth into
long-format (pair_id, field_name, value) rows, applying the doc-type field map
(silver.field_map) where one exists; unmapped fields keep their raw XML path
as the field name so evaluation works before any mapping is curated."""

from __future__ import annotations

from tars.config import Config
from tars.tables import ensure_tables
from tars.xmlflat import flatten_xml


def run_ground_truth(spark, cfg: Config) -> int:
    todo = spark.sql(f"""
        SELECT p.pair_id, p.doc_type, p.xml_path, p.xml_hash
        FROM {cfg.table('doc_pairs')} p
        LEFT ANTI JOIN {cfg.table('ground_truth')} g ON p.xml_hash = g.xml_hash
        WHERE p.xml_path IS NOT NULL
    """).collect()
    if not todo:
        print("ground-truth: nothing new to parse")
        return 0

    field_map = {
        (row["doc_type"], row["xml_field"]): row["canonical_field"]
        for row in spark.table(cfg.table("field_map")).collect()
    }

    rows, failed = [], []
    for item in todo:
        try:
            with open(item["xml_path"], encoding="utf-8", errors="replace") as handle:
                flat = flatten_xml(handle.read())
        except Exception as exc:  # malformed XML: surface, don't crash the batch
            failed.append((item["xml_path"], str(exc)))
            continue
        seen: dict[str, int] = {}
        for xml_field, value in flat:
            occurrence = seen.get(xml_field, 0)
            seen[xml_field] = occurrence + 1
            rows.append({
                "pair_id": item["pair_id"],
                "xml_hash": item["xml_hash"],
                "field_name": field_map.get((item["doc_type"], xml_field), xml_field),
                "field_value": value,
                "source_xpath": xml_field,
                "occurrence": occurrence,
            })

    pair_ids = sorted({r["pair_id"] for r in rows})
    if pair_ids:
        # Replace, not append: a changed XML must not leave stale rows behind.
        id_list = ", ".join(f"'{p}'" for p in pair_ids)
        spark.sql(f"DELETE FROM {cfg.table('ground_truth')} WHERE pair_id IN ({id_list})")
        spark.createDataFrame(
            rows,
            "pair_id STRING, xml_hash STRING, field_name STRING, "
            "field_value STRING, source_xpath STRING, occurrence INT",
        ).write.mode("append").saveAsTable(cfg.table("ground_truth"))

    for path, error in failed:
        print(f"ground-truth: FAILED {path}: {error}")
    print(f"ground-truth: {len(pair_ids)} documents flattened to {len(rows)} field rows, "
          f"{len(failed)} failures")
    return len(pair_ids)
