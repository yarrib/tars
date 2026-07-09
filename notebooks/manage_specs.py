# Databricks notebook source
# MAGIC %md
# MAGIC # tars — create an extraction spec
# MAGIC Fill in the widgets above, edit the schema JSON in the cell below, then
# MAGIC **Run all**. A new auto-versioned spec (e.g. `1040-v4`) is registered in
# MAGIC `extract_specs`; run the **tars — run extraction spec** job with that id.
# MAGIC
# MAGIC The field **descriptions inside the schema are the prompt** — that's what
# MAGIC you iterate on. Specs are append-only: every change becomes a new version,
# MAGIC so results stay comparable across iterations.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "tars")
dbutils.widgets.text("doc_type", "1040")
dbutils.widgets.text("notes", "")

# COMMAND ----------

# Edit the extraction schema for your doc type. Guidance goes in the
# per-field "description" — be specific: which line, what format, what units.
EXTRACTION_SCHEMA = """
{
  "taxpayer_name": {
    "type": "string",
    "description": "Primary taxpayer's full name as printed at the top of the return"
  },
  "total_tax": {
    "type": "number",
    "description": "Total tax, line 24, as a plain number with no currency symbol or thousands separators"
  }
}
"""

# COMMAND ----------

from tars.config import Config
from tars.specs import add_spec

cfg = Config(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    volume_root="unused-here",
)
spec_id = add_spec(
    spark, cfg,
    doc_type=dbutils.widgets.get("doc_type"),
    extraction_schema=EXTRACTION_SCHEMA,
    notes=dbutils.widgets.get("notes") or None,
)
displayHTML(f"<h3>Created spec: <code>{spec_id}</code></h3>"
            "<p>Now run the <b>tars — run extraction spec</b> job with this id.</p>")
