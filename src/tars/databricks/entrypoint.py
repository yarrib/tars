"""Entry point used by the Databricks Job defined in `databricks_bundle/`.

A Databricks Workflow task running a Python wheel calls a module function,
not a CLI -- so this simply re-parses the same `--config`/`--pipeline`/
`--router` arguments `tars run` accepts (as Databricks Job parameters) and
delegates to the same `PipelineRunner` the CLI uses, keeping the two
invocation paths in lockstep.
"""

from __future__ import annotations

import argparse
import json
import sys

from tars.config.loader import load_config
from tars.pipeline.runner import PipelineRunner


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="tars-databricks-job")
    parser.add_argument("--config", required=True, help="Path to a tars config file or directory")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--pipeline", help="Run a single named pipeline")
    group.add_argument("--router", action="store_true", help="Run router-driven ingestion")
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    app_config = load_config(args.config)
    runner = PipelineRunner(app_config)
    context = runner.run_router() if args.router else runner.run_pipeline(args.pipeline)

    print(json.dumps({"run_id": context.run_id, "stats": context.stats}, indent=2))
    if context.stats.get("failed"):
        sys.exit(1)


if __name__ == "__main__":
    main()
