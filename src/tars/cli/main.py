"""`tars` command-line entry point.

    tars validate -c configs/examples          # load + validate config, build every pipeline
    tars list-plugins                          # show what's registered, built-in and custom
    tars run -c configs/examples --pipeline invoice_pipeline
    tars run -c configs/examples --router      # router-driven: one source, many pipelines (local/offline dev)
    tars generate-dlt -c configs/examples -o generated   # emit Lakeflow Declarative Pipeline source (Databricks)
    tars init pipeline my_new_pipeline         # scaffold a new pipeline YAML file
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import click

from tars.config.loader import ConfigError, load_config
from tars.databricks.dlt_codegen import DltCodegenError, write_generated_files
from tars.pipeline.builder import build_pipeline
from tars.pipeline.context import PipelineContext
from tars.pipeline.runner import PipelineRunner
from tars.plugins.registry import default_registry

_PIPELINE_TEMPLATE = """\
pipelines:
  - name: {name}
    description: "TODO: describe this pipeline"
    parser:
      name: parser.text
    sinks:
      - name: sink.volume
        config:
          path: /Volumes/main/docs/processed/{name}
"""


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def cli(verbose: bool) -> None:
    """tars: adaptable, config-driven document ingestion for Databricks."""
    _configure_logging(verbose)


@cli.command()
@click.option("-c", "--config", "config_path", required=True, type=click.Path(exists=True))
def validate(config_path: str) -> None:
    """Load, validate, and build every pipeline defined in the config."""
    try:
        app_config = load_config(config_path)
        for name, pipeline_config in app_config.pipelines.items():
            build_pipeline(pipeline_config)
    except ConfigError as exc:
        click.secho(f"Invalid config: {exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 - surfaced to the user as a CLI error
        click.secho(f"Failed to build pipelines: {exc}", fg="red", err=True)
        sys.exit(1)

    click.secho(
        f"OK: {len(app_config.pipelines)} pipeline(s), "
        f"{len(app_config.router.routes)} route(s) valid.",
        fg="green",
    )


@cli.command("list-plugins")
@click.option("-c", "--config", "config_path", type=click.Path(exists=True), default=None)
@click.option("--kind", type=click.Choice(["source", "classifier", "parser", "sink"]), default=None)
def list_plugins(config_path: str | None, kind: str | None) -> None:
    """List registered plugins (built-in, entry-point, and config-declared)."""
    if config_path:
        load_config(config_path)  # registers any custom plugins declared in the config
    for name in default_registry.names(kind):
        plugin_cls = default_registry.get(name)
        click.echo(f"{name}  ({plugin_cls.__module__}.{plugin_cls.__qualname__})")


@cli.command()
@click.option("-c", "--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("--pipeline", "pipeline_name", default=None, help="Run a single named pipeline.")
@click.option("--router", "use_router", is_flag=True, help="Run router-driven ingestion.")
def run(config_path: str, pipeline_name: str | None, use_router: bool) -> None:
    """Execute a pipeline (or the router) once."""
    if bool(pipeline_name) == use_router:
        raise click.UsageError("Pass exactly one of --pipeline NAME or --router")

    try:
        app_config = load_config(config_path)
    except ConfigError as exc:
        click.secho(f"Invalid config: {exc}", fg="red", err=True)
        sys.exit(1)

    runner = PipelineRunner(app_config)
    context: PipelineContext
    if use_router:
        context = runner.run_router()
    else:
        if pipeline_name not in app_config.pipelines:
            click.secho(f"Unknown pipeline: {pipeline_name!r}", fg="red", err=True)
            sys.exit(1)
        context = runner.run_pipeline(pipeline_name)

    click.echo(json.dumps({"run_id": context.run_id, "stats": context.stats}, indent=2))


@cli.command("generate-dlt")
@click.option("-c", "--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("-o", "--output-dir", "output_dir", required=True, type=click.Path())
def generate_dlt(config_path: str, output_dir: str) -> None:
    """Generate Lakeflow Declarative Pipeline (DLT) source from config.

    Emits one shared pipeline file covering every router-dispatched
    pipeline (adding a route in YAML adds a table here, not a new job),
    plus one file per `deploy.dedicated` or standalone pipeline. Run this
    before `databricks bundle deploy`.
    """
    try:
        app_config = load_config(config_path)
        written = write_generated_files(app_config, Path(output_dir), config_path=config_path)
    except ConfigError as exc:
        click.secho(f"Invalid config: {exc}", fg="red", err=True)
        sys.exit(1)
    except DltCodegenError as exc:
        click.secho(f"Cannot generate Lakeflow Declarative Pipeline source: {exc}", fg="red", err=True)
        sys.exit(1)

    for path in written:
        click.echo(f"Wrote {path}")
    click.secho(f"OK: generated {len(written)} file(s) in {output_dir}", fg="green")


@cli.command()
@click.argument("kind", type=click.Choice(["pipeline"]))
@click.argument("name")
@click.option("-o", "--output-dir", type=click.Path(), default="configs/pipelines")
def init(kind: str, name: str, output_dir: str) -> None:
    """Scaffold a new declarative config file, e.g. `tars init pipeline invoices`."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{name}.yaml"
    if out_path.exists():
        click.secho(f"{out_path} already exists.", fg="red", err=True)
        sys.exit(1)
    out_path.write_text(_PIPELINE_TEMPLATE.format(name=name))
    click.secho(f"Wrote {out_path}", fg="green")


if __name__ == "__main__":
    cli()
