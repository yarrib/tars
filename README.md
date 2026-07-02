# tars

Like the robot from Interstellar, adaptable document processing on Databricks.

`tars` is a config-driven framework for ingesting, routing, classifying,
and parsing documents on Databricks. Pipelines are declared in YAML, not
code; the four moving parts (sources, classifiers, parsers, sinks) are all
plugins, so extending the system means writing a small Python class and
declaring it in config -- not forking the framework.

- **Event-driven and API ingestion**: Databricks Auto Loader (`cloudFiles`)
  for landing-zone/Volume ingestion, or push-based ingestion via a direct
  Python call or an HTTP webhook.
- **Declarative pipeline builder**: each pipeline (`source -> classify ->
  parse -> sink(s)`) is a YAML document; `tars` builds and validates it.
- **Document router**: a shared classifier chain assigns a `doc_type` to
  every incoming document and dispatches it to the pipeline configured to
  handle that type.
- **AI parse doc**: structured field extraction via an LLM -- either
  Databricks' built-in `ai_parse_document()`/`ai_query()` SQL functions
  (governed, no external API key) or a direct Anthropic call, your choice.
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

A pipeline doesn't need to be reached through the router at all -- give it
its own `source` and run it standalone with `tars run --pipeline NAME`.
Router-driven ingestion (`tars run --router`) is for the common case of
one landing zone feeding many document types into different destinations.

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
| `source.*`     | `discover(context) -> Iterator[Document]`  | `source.autoloader` (Auto Loader), `source.volume_listener` (poll a directory / UC Volume), `source.api` (push/webhook) |
| `classifier.*` | `classify(document, context) -> str \| None` | `classifier.rule_based` (extension/filename/mime rules), `classifier.ai` (Anthropic), `classifier.databricks_ai` (`ai_classify()`) |
| `parser.*`     | `parse(document, context) -> dict`         | `parser.text` (passthrough), `parser.ai` (Anthropic tool-use extraction), `parser.databricks_ai` (`ai_parse_document()` + `ai_query()`) |
| `sink.*`       | `write(document, context) -> None`         | `sink.volume` (JSON files on a Volume/local dir), `sink.delta` (Delta Lake table, batched) |

Full plugin list for your environment (including any custom ones declared
in config): `tars list-plugins -c <config>`.

## CLI

```
tars validate -c <config>                    # load + build every pipeline, no execution
tars list-plugins [--kind source|classifier|parser|sink]
tars run -c <config> --pipeline NAME          # run one self-contained pipeline
tars run -c <config> --router                 # run router-driven ingestion
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

`databricks_bundle/` is a Databricks Asset Bundle wiring `tars run
--router` into a Job triggered by file arrival in a Unity Catalog Volume:

```bash
cd databricks_bundle
databricks bundle deploy -t dev
databricks bundle run -t dev tars_ingestion_job
```

It deploys `configs/examples/databricks` as the job's config -- point
`var.config_path` at your own config directory for a real deployment.

## Development

```bash
pip install -e ".[dev,ai,api]"   # add `databricks` for pyspark locally
pytest
ruff check src tests
```
