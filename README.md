# tars

Like the robot from Interstellar, adaptable document processing on Databricks.

`tars` is a config-driven framework for ingesting, routing, classifying,
and parsing documents on Databricks. Pipelines are declared in YAML, not
code; the four moving parts (sources, classifiers, parsers, sinks) are all
plugins, so extending the system means writing a small Python class and
declaring it in config -- not forking the framework.

- **Event-driven ingestion, serverless**: `tars` compiles your YAML config
  into a serverless Databricks Lakeflow Declarative Pipeline -- Auto
  Loader (`cloudFiles`) for document bytes landing in a Volume, or Zerobus
  for low-latency metadata/event records. No cluster spec anywhere in the
  generated bundle.
- **Declarative pipeline builder**: each pipeline (`source -> classify ->
  parse -> sink(s)`) is a YAML document; `tars generate-dlt` compiles it
  to native `@dp.table` code and Databricks' own engine runs it.
- **Document router, routes not jobs**: a shared classifier chain assigns
  a `doc_type` to every incoming document and dispatches it to the
  pipeline configured to handle that type. Adding a route in YAML adds a
  table to the *same* generated pipeline -- it doesn't provision a new
  Databricks Job or Pipeline, unless that pipeline opts in via
  `deploy: {dedicated: true}`.
- **AI parse doc**: structured field extraction via an LLM -- Databricks'
  built-in `ai_parse_document()`/`ai_query()` SQL functions by default
  (governed, columnar, no external API key), or a direct Anthropic call
  when you need it (compiled to a pandas UDF).
- **Plugin system**: sources, classifiers, parsers, and sinks are all
  swappable; register your own via a Python entry point or declare one
  directly in YAML (`plugins: [{name, python_path}]`).

## Quick start (no Databricks required)

```bash
pip install -e ".[dev]"

export TARS_LANDING_DIR=./data/landing
export TARS_OUTPUT_DIR=./data/processed
mkdir -p "$TARS_LANDING_DIR"
echo "please pay 500 for consulting services" > "$TARS_LANDING_DIR/invoice_demo.txt"

tars validate -c configs/examples/local
tars run -c configs/examples/local --router
```

This runs entirely locally: `source.volume_listener` polls a directory,
`classifier.rule_based` routes by filename, `parser.text` extracts plain
text, and `sink.volume` writes one JSON record per document to
`$TARS_OUTPUT_DIR`. See `configs/examples/databricks` for the
production-shaped version (Auto Loader, Databricks AI Functions, Delta
Lake sinks) and `databricks_bundle/` for the matching Asset Bundle.

## Two execution engines, one config

The same YAML config drives two different execution paths:

- **`tars generate-dlt`** (primary, for Databricks) compiles the config
  into a serverless Lakeflow Declarative Pipeline -- Databricks' own
  engine handles orchestration, incremental state, and autoscaling. This
  is what `databricks_bundle/` deploys.
- **`tars run`** (the custom `PipelineRunner`) executes the same config
  directly in a Python process. No cluster, no workspace -- this is the
  local/offline dev-and-test path (what the Quick Start above uses, and
  what the test suite exercises), and a fallback for source/sink plugins
  that have no DLT codegen support.

Not every plugin combination compiles to both engines identically --
`classifier.databricks_ai`/`parser.databricks_ai` compile to native
columnar SQL function calls under codegen, while `classifier.ai`/`parser.ai`
(direct Anthropic) compile to a pandas UDF wrapping the same plugin class
`tars run` uses. Both work under `tars run` regardless.

## Architecture

```
                 ┌──────────────────────────┐
  events/API ──▶ │  Source (plugin)         │
                 └────────────┬─────────────┘
                              ▼
                 ┌──────────────────────────┐
                 │  Router                  │
                 │  classifier chain assigns│
                 │  doc_type, routes.yaml   │
                 │  resolves a pipeline     │
                 └────────────┬─────────────┘
                              ▼
         ┌───────────────────────────────────────┐
         │  Pipeline (per doc_type)               │
         │  extra classifiers -> Parser -> Sinks  │
         └───────────────────────────────────────┘
```

Under `tars generate-dlt`, the Source+Router+every non-dedicated Pipeline
above compile into *one* generated file (one Lakeflow pipeline): Source
becomes a bronze `@dp.table`, the Router's classifier chain becomes a
`doc_type` column on a classified `@dp.table`, and each Pipeline becomes
a gold `@dp.table` filtered to its `doc_type`(s). A pipeline doesn't need
to be reached through the router at all -- give it its own `source` and
it's compiled as its own standalone pipeline/file instead (or run it
directly with `tars run --pipeline NAME` under the local runner).

### Config layout

A `tars` config is one YAML file or a directory of them (merged before
validation):

```
configs/examples/local/
  plugins.yaml          # custom plugin registrations (optional)
  router.yaml            # source + classifiers + routes + default_pipeline
  pipelines/
    invoice_pipeline.yaml
    contract_pipeline.yaml
    unclassified_pipeline.yaml
```

`${ENV_VAR}` and `${ENV_VAR:-default}` are interpolated from the process
environment anywhere in the YAML, so the same config works across
dev/staging/prod by swapping environment variables, not files.

### Plugin kinds

| Kind         | Contract                                   | Built-ins |
|--------------|---------------------------------------------|-----------|
| `source.*`     | `discover(context) -> Iterator[Document]`  | `source.autoloader` (Auto Loader, **recommended for document bytes**), `source.zerobus` (low-latency metadata/event records, not raw file content -- see its docstring), `source.volume_listener` (poll a directory / UC Volume, local-runner only), `source.api` (push/webhook, local-runner only) |
| `classifier.*` | `classify(document, context) -> str \| None` | `classifier.rule_based` (extension/filename/mime rules), `classifier.databricks_ai` (**recommended**, native `ai_classify()`), `classifier.ai` (direct Anthropic, compiles to a pandas UDF under codegen) |
| `parser.*`     | `parse(document, context) -> dict`         | `parser.text` (passthrough), `parser.databricks_ai` (**recommended**, native `ai_parse_document()` + `ai_query()`), `parser.ai` (direct Anthropic tool-use, compiles to a pandas UDF under codegen) |
| `sink.*`       | `write(document, context) -> None`         | `sink.delta` (Delta Lake table, **the real DLT codegen sink**), `sink.volume` (JSON files on a Volume/local dir -- local-runner only; under codegen it falls back to a plain Delta table with a warning, since DLT can't write arbitrary files) |

`source.autoloader`/`source.zerobus` and `classifier.databricks_ai`/
`parser.databricks_ai` are the only plugins with `tars generate-dlt`
codegen support beyond the Anthropic-direct UDF path; anything else runs
under `tars run` (the local `PipelineRunner`) only.

Full plugin list for your environment (including any custom ones declared
in config): `tars list-plugins -c <config>`.

## CLI

```
tars validate -c <config>                    # load + build every pipeline, no execution
tars list-plugins [--kind source|classifier|parser|sink]
tars generate-dlt -c <config> -o <dir>        # compile to Lakeflow Declarative Pipeline source (primary, Databricks)
tars run -c <config> --pipeline NAME          # run one self-contained pipeline (local/offline)
tars run -c <config> --router                 # run router-driven ingestion (local/offline)
tars init pipeline NAME [-o DIR]              # scaffold a new pipeline YAML
```

## Writing a plugin

Subclass the relevant base class in `tars.plugins.base` and implement one
method. Example classifier:

```python
# my_org/tars_plugins.py
from tars.plugins.base import Classifier

class KeywordClassifier(Classifier):
    def classify(self, document, context):
        if b"purchase order" in (document.content or b"").lower():
            return "purchase_order"
        return None
```

Register it either as a Python entry point (for a package you distribute)
or directly in your config (for a plugin that lives in your own repo):

```yaml
plugins:
  - name: classifier.keyword
    python_path: my_org.tars_plugins.KeywordClassifier
```

Then reference it like any built-in: `classifier: {name: classifier.keyword}`.
See `docs/plugin-development.md` for the full guide.

## Databricks deployment

`databricks_bundle/` is a Databricks Asset Bundle deploying the generated
Lakeflow Declarative Pipeline, serverless throughout, plus a thin Job that
triggers it on file arrival in a Unity Catalog Volume:

```bash
tars generate-dlt -c configs/examples/databricks -o databricks_bundle/generated
cd databricks_bundle
databricks bundle deploy -t dev
databricks bundle run -t dev tars_ingestion_job
```

Regenerate (`tars generate-dlt`) whenever the config changes, then
redeploy. It targets `configs/examples/databricks` by default -- point
`var.config_path` (and the `catalog`/`schema` variables) at your own
config for a real deployment. Adding a pipeline that's reachable via
`router.routes`/`default_pipeline` adds a table to the existing
`tars_pipeline` resource; only pipelines marked `deploy: {dedicated: true}`
get their own generated file and need their own resource block in
`resources/pipeline.yml`.

## Development

```bash
pip install -e ".[dev,ai,api]"   # add `databricks` for pyspark locally
pytest
ruff check src tests
```
