"""Discovery job: scan the volume tree, match PDF/XML pairs, hash content,
MERGE into bronze.doc_pairs, snapshot bronze.unpaired, and assign splits for
newly seen clients. This is the only code that ever reads the volume listing —
everything downstream works off the Delta manifest."""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone

from tars.config import Config
from tars.pairing import match_files
from tars.splits import METHOD, assign_split
from tars.tables import ensure_tables


def sha256_file(path: str, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def scan_volume(volume_root: str) -> tuple[list[dict], list[dict]]:
    """Walk <volume_root>/<doc_type>/<client>/... and pair files per folder.

    Returns (pairs, unpaired) as dicts of paths + doc_type/client attribution.
    Files above the client level are attributed with empty doc_type/client and
    surface in bronze.unpaired rather than being silently dropped.
    """
    root = volume_root.rstrip("/")
    pairs: list[dict] = []
    unpaired: list[dict] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        names = [f for f in filenames if f.lower().endswith((".pdf", ".xml"))]
        if not names:
            continue
        rel = os.path.relpath(dirpath, root)
        parts = [] if rel == "." else rel.split(os.sep)
        doc_type = parts[0] if len(parts) >= 1 else ""
        client = parts[1] if len(parts) >= 2 else ""
        matched, alone = match_files(names)
        for pdf, xml in matched:
            pairs.append({
                "doc_type": doc_type, "client": client,
                "pdf_path": os.path.join(dirpath, pdf),
                "xml_path": os.path.join(dirpath, xml),
            })
        for name in alone:
            unpaired.append({
                "doc_type": doc_type, "client": client,
                "path": os.path.join(dirpath, name),
            })
    return pairs, unpaired


def run_discovery(spark, cfg: Config) -> int:
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StringType, StructField, StructType,
                                   LongType, TimestampType)

    ensure_tables(spark, cfg)
    pairs, unpaired = scan_volume(cfg.volume_root)
    now = datetime.now(timezone.utc)

    # Short-circuit hashing: reuse the stored pdf_hash when size+mtime match.
    known = {
        row["pdf_path"]: row
        for row in spark.table(cfg.table("doc_pairs"))
        .select("pdf_path", "pdf_size_bytes", "pdf_modified_at", "pdf_hash")
        .toLocalIterator()
    }

    rows = []
    for pair in pairs:
        stat = os.stat(pair["pdf_path"])
        mtime = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
        prev = known.get(pair["pdf_path"])
        if (prev and prev["pdf_size_bytes"] == stat.st_size
                and prev["pdf_modified_at"] == mtime.replace(tzinfo=None)):
            pdf_hash = prev["pdf_hash"]
        else:
            pdf_hash = sha256_file(pair["pdf_path"])
        rows.append({
            "pair_id": pdf_hash,
            "doc_type": pair["doc_type"],
            "client": pair["client"],
            "pdf_path": pair["pdf_path"],
            "xml_path": pair["xml_path"],
            "pdf_hash": pdf_hash,
            "xml_hash": sha256_file(pair["xml_path"]),
            "pdf_size_bytes": stat.st_size,
            "pdf_modified_at": mtime,
            "discovered_at": now,
        })

    if rows:
        schema = StructType([
            StructField("pair_id", StringType()),
            StructField("doc_type", StringType()),
            StructField("client", StringType()),
            StructField("pdf_path", StringType()),
            StructField("xml_path", StringType()),
            StructField("pdf_hash", StringType()),
            StructField("xml_hash", StringType()),
            StructField("pdf_size_bytes", LongType()),
            StructField("pdf_modified_at", TimestampType()),
            StructField("discovered_at", TimestampType()),
        ])
        spark.createDataFrame(rows, schema).createOrReplaceTempView("tars_pairs_incoming")
        spark.sql(f"""
            MERGE INTO {cfg.table('doc_pairs')} AS t
            USING tars_pairs_incoming AS s
            ON t.pdf_path = s.pdf_path
            WHEN MATCHED AND (t.pdf_hash <> s.pdf_hash
                              OR t.xml_hash <> s.xml_hash
                              OR t.xml_path <> s.xml_path) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    unpaired_df = spark.createDataFrame(
        [{**u, "discovered_at": now} for u in unpaired] or [],
        "doc_type STRING, client STRING, path STRING, discovered_at TIMESTAMP")
    unpaired_df.write.mode("overwrite").saveAsTable(cfg.table("unpaired"))

    assigned = assign_new_clients(spark, cfg)
    print(f"discovery: {len(rows)} pairs merged, {len(unpaired)} unpaired, "
          f"{assigned} new clients assigned to splits")
    return len(rows)


def assign_new_clients(spark, cfg: Config) -> int:
    """Assign a split to every client in doc_pairs that has no assignment yet.
    Existing rows are never touched — assignments are sticky by construction."""
    new_clients = [
        row["client"] for row in spark.sql(f"""
            SELECT DISTINCT p.client
            FROM {cfg.table('doc_pairs')} p
            LEFT ANTI JOIN {cfg.table('split_assignments')} s ON p.client = s.client
            WHERE p.client <> ''
        """).collect()
    ]
    if not new_clients:
        return 0
    now = datetime.now(timezone.utc)
    rows = [{
        "client": client,
        "split": assign_split(client, cfg.dev_pct, cfg.val_pct),
        "method": METHOD,
        "assigned_at": now,
    } for client in new_clients]
    spark.createDataFrame(
        rows, "client STRING, split STRING, method STRING, assigned_at TIMESTAMP"
    ).write.mode("append").saveAsTable(cfg.table("split_assignments"))
    return len(rows)
