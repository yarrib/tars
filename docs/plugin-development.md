# Writing a tars plugin

Every plugin is a plain Python class with a `__init__(self, config, **kwargs)`
and exactly one required method, matching one of the four base classes in
`tars.plugins.base`:

| Base class    | Method                                              | Yields/returns |
|---------------|------------------------------------------------------|-----------------|
| `Source`      | `discover(self, context) -> Iterator[Document]`      | Documents ready for processing |
| `Classifier`  | `classify(self, document, context) -> str \| None`   | A `doc_type` label, or `None` to defer to the next classifier |
| `Parser`      | `parse(self, document, context) -> dict`             | Extracted fields, stored on `document.extracted` |
| `Sink`        | `write(self, document, context) -> None`             | Nothing; raise to signal failure |

`config` is the plugin's own YAML `config:` block, handed over as a plain
`dict` -- validate it yourself in `__init__` (see the built-in plugins in
`tars/plugins/*/` for the pattern: read required keys, raise `ValueError`
with a clear message if missing).

`context` is a `tars.pipeline.context.PipelineContext`: shared, mutable
state for one run (`context.stats`, a dict of counters you can increment
via `context.increment("my_counter")`, plus `context.params` for anything
you want to thread through from the CLI/job invocation).

Optional lifecycle hooks (default to no-ops): `setup(self, context)` runs
once before a pipeline starts, `teardown(self, context)` once after --
useful for opening/flushing a batched writer (see `sink.delta`, which
buffers rows and flushes in `teardown`).

## Example: a classifier

```python
# my_org/tars_plugins.py
from tars.plugins.base import Classifier

class KeywordClassifier(Classifier):
    def __init__(self, config=None, **kwargs):
        super().__init__(config, **kwargs)
        self.keyword = self.config["keyword"]
        self.doc_type = self.config["doc_type"]

    def classify(self, document, context):
        if self.keyword.encode() in (document.content or b"").lower():
            return self.doc_type
        return None
```

## Registering it

Two ways, both resolve the same YAML `name:` field to a class:

**In config** (simplest -- good for a plugin that lives in your own repo,
next to your pipeline configs):

```yaml
plugins:
  - name: classifier.keyword
    python_path: my_org.tars_plugins.KeywordClassifier
```

**As a Python entry point** (for a plugin you distribute as its own
package, so consumers don't need to know the import path):

```toml
# pyproject.toml of your plugin package
[project.entry-points."tars.plugins"]
"classifier.keyword" = "my_org.tars_plugins:KeywordClassifier"
```

Either way, reference it by name from a pipeline or the router exactly
like a built-in:

```yaml
router:
  classifiers:
    - name: classifier.keyword
      config:
        keyword: "purchase order"
        doc_type: purchase_order
```

## Testing a plugin

Plugins take their dependencies (an Anthropic client, a Spark session) as
constructor keyword arguments defaulting to a lazily-created real one --
inject a fake in tests instead of hitting the network or needing a
cluster. See `tests/test_databricks_ai_plugins.py` for the pattern used
with `spark=`, and `tests/test_classifier_plugins.py` for `client=`.

```python
from tars.pipeline.context import Document, PipelineContext

def test_keyword_classifier():
    classifier = KeywordClassifier({"keyword": "purchase order", "doc_type": "po"})
    doc = Document(uri="x", content=b"This is a Purchase Order.")
    assert classifier.classify(doc, PipelineContext(pipeline_name="p")) == "po"
```

No Databricks cluster, LLM API key, or `tars` config file needed to unit
test a plugin in isolation -- only `tars run`/`tars validate` need those.

## A custom plugin and `tars generate-dlt`

A custom plugin registered via `plugins:`/entry points works immediately
under `tars run` (the local `PipelineRunner`). It does **not**
automatically get `tars generate-dlt` (Lakeflow Declarative Pipeline)
codegen support -- `tars/databricks/dlt_codegen.py` only knows how to
compile the specific built-in plugins listed in the README's plugin table
into native Spark/SQL expressions. Using a custom source/classifier/parser
in a config that also runs through `tars generate-dlt` will raise a clear
`DltCodegenError` naming the unsupported plugin, rather than silently
skipping it. If you need a custom plugin on Databricks, either add codegen
support for it in that module, or run it via the entrypoint job
(`tars.databricks.entrypoint`) instead of generated DLT code.
