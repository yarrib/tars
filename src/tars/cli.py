"""Single CLI entry point (console script `tars`) used by all Databricks jobs.

Commands:
  setup         create all tables
  discover      scan volume -> doc_pairs + unpaired + split assignments
  parse         ai_parse_document over new PDFs
  ground-truth  flatten new ground-truth XMLs
  add-spec      register a new extraction spec (auto-versioned)
  extract       run a spec over dev (or val) and evaluate
  final-eval    run a spec over the frozen test split and evaluate
"""

from __future__ import annotations

import argparse
import sys

from tars.config import Config


def _spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--catalog")
    parser.add_argument("--schema")
    parser.add_argument("--volume-root")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="tars")
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ("setup", "discover", "parse", "ground-truth"):
        _add_common(sub.add_parser(name))

    spec_parser = sub.add_parser("add-spec")
    _add_common(spec_parser)
    spec_parser.add_argument("--doc-type", required=True)
    spec_parser.add_argument("--schema-file", required=True,
                             help="path to the extraction schema JSON")
    spec_parser.add_argument("--method", default="ai_extract",
                             choices=("ai_extract", "ai_query"))
    spec_parser.add_argument("--prompt-file", help="ai_query only")
    spec_parser.add_argument("--model", help="ai_query only")
    spec_parser.add_argument("--notes")

    extract_parser = sub.add_parser("extract")
    _add_common(extract_parser)
    extract_parser.add_argument("--spec-id", required=True)
    extract_parser.add_argument("--split", default="dev", choices=("dev", "val"))
    extract_parser.add_argument("--skip-eval", action="store_true")

    final_parser = sub.add_parser("final-eval")
    _add_common(final_parser)
    final_parser.add_argument("--spec-id", required=True)

    args = parser.parse_args(argv)
    cfg = Config.from_env(args.catalog, args.schema, args.volume_root)
    spark = _spark()

    if args.command == "setup":
        from tars.tables import ensure_tables
        ensure_tables(spark, cfg)
        print(f"setup: tables ensured in {cfg.catalog}.{cfg.schema}")
    elif args.command == "discover":
        from tars.discovery import run_discovery
        run_discovery(spark, cfg)
    elif args.command == "parse":
        from tars.parsing import run_parse
        run_parse(spark, cfg)
    elif args.command == "ground-truth":
        from tars.ground_truth import run_ground_truth
        run_ground_truth(spark, cfg)
    elif args.command == "add-spec":
        from tars.specs import add_spec
        with open(args.schema_file, encoding="utf-8") as handle:
            schema = handle.read()
        prompt = None
        if args.prompt_file:
            with open(args.prompt_file, encoding="utf-8") as handle:
                prompt = handle.read()
        add_spec(spark, cfg, args.doc_type, schema, args.method, prompt,
                 args.model, args.notes)
    elif args.command == "extract":
        from tars.extraction import run_extract
        from tars.evaluation import run_evaluate
        run_extract(spark, cfg, args.spec_id, args.split)
        if not args.skip_eval:
            run_evaluate(spark, cfg, args.spec_id)
    elif args.command == "final-eval":
        from tars.extraction import run_extract
        from tars.evaluation import run_evaluate
        run_extract(spark, cfg, args.spec_id, "test", allow_test=True)
        run_evaluate(spark, cfg, args.spec_id)


if __name__ == "__main__":
    main(sys.argv[1:])
