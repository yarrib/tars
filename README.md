# tars v2 — AI extraction & evaluation for tax documents on Databricks

Databricks pipeline that lets non-coders run and iterate LLM extraction over
tax documents, with per-field confidence scores, offset-grounded citations, and
an evaluation loop against ground-truth XMLs. Architecture rationale lives in
[docs/design/extraction-eval-design.md](docs/design/extraction-eval-design.md).

## How it works

Documents live in a UC volume as `<doc_type>/<client>/...`; a folder holding
both a PDF and an XML (matched by filename-stem containment) is a
**pair** — PDF in, ground truth out. From there:

| layer | table | what / cache key |
|---|---|---|
| bronze | `doc_pairs` | pair manifest, MERGEd by path, identity = content hash |
| bronze | `unpaired` | PDFs/XMLs with no partner (visibility, not dropped) |
| bronze | `split_assignments` | sticky client-level dev/val/test (10/20/70) |
| silver | `parsed_docs` | `ai_parse_document` output + text, cached by `pdf_hash` |
| silver | `ground_truth` | XML flattened long, cached by `xml_hash` |
| silver | `field_map` | XML path → canonical field name (optional, editable) |
| gold | `extract_specs` | append-only spec registry (the "prompts") |
| gold | `extractions` | per-field value + confidence + citations, keyed `(pdf_hash, spec_id)` |
| gold | `eval_results`, `eval_calibration` | accuracy per field + confidence calibration |

Extraction uses **`ai_extract` v2.1** (`enableConfidenceScores`,
`enableCitations`); citation offsets are materialized to text spans at write
time. An `ai_query` structured-output escape hatch exists per spec
(`method = 'ai_query'`) for doc types needing more guidance than field
descriptions allow.

Nothing reprocesses: parsing is cached by content hash, extraction by
`(pdf_hash, spec_id)`. Re-running any job is always safe.

## Deploy

```bash
pip install build databricks-cli
databricks bundle deploy -t dev \
  --var catalog=main --var schema=tars \
  --var volume_root=/Volumes/main/tars/docs
```

Three jobs are created:

1. **tars — discover & prepare**: scans the volume, updates the manifest,
   assigns splits for new clients, parses new PDFs and ground-truth XMLs.
   Schedule it or run on demand.
2. **tars — run extraction spec (dev/val)**: the iteration button. Parameters:
   `spec_id`, `split` (`dev` default, `val` to confirm a winner). Extracts only
   unprocessed documents for that spec, then refreshes eval tables.
3. **tars — FINAL EVAL on test holdout (manual)**: the only way to touch the
   `test` split. Run rarely; this is the number you report.

## Iterating (non-coders)

1. Open `notebooks/manage_specs.py`, edit the field descriptions in the schema
   (those descriptions **are** the prompt), Run all → get a new `spec_id`.
2. Run job 2 with that `spec_id` on `dev`.
3. Compare specs in `eval_results` / `eval_calibration` (dashboard these).
4. When a spec wins on dev, re-run job 2 with `split=val` to confirm.
5. Ship: run job 3 once for the final holdout number.

The CLI behind the jobs also works directly:

```bash
tars discover|parse|ground-truth|setup --catalog main --schema tars --volume-root /Volumes/...
tars add-spec --doc-type 1040 --schema-file specs/example-1040.json
tars extract --spec-id 1040-v1 --split dev
tars final-eval --spec-id 1040-v1
```

## Development

```bash
pip install -e '.[dev]'
pytest
```

Pure logic (pair matching, split hashing, XML flattening, ai_extract output
flattening, value normalization) is spark-free and unit-tested; Spark job
wrappers are thin and live beside it in `src/tars/`.
