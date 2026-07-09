# tars v2 — AI extraction & evaluation pipeline for tax documents

## Context

Non-coders need to run AI extraction over tax documents in Databricks and iterate on
prompts without writing code. Source material lives in a Unity Catalog volume organized
as `<doc_type>/<client>/<documents>`, where a folder that contains both a PDF and an XML
(matched by shared filename substring) constitutes a **pair**: the PDF is the input, the
XML is the ground truth. Pair-matching logic already exists and produces DataFrames.

The pipeline ahead: `ai_parse_document` on the PDFs, XML parsing of the ground truth,
LLM extraction with **per-field confidence scores and citations**, and an evaluation loop
so prompt changes can be measured instead of eyeballed. Hard requirements:

- **Never reprocess** work that's already done (parsing and extraction both cost money).
- **Train/validate/test independence** — iterate prompts on a small subset, keep a
  holdout that prompt decisions never touch.
- Results comparable **across prompt versions**, so iteration is measurable.

One framing note: there is no gradient training here, so "train" really means
**prompt-dev** — the small set you look at while writing prompts. The three splits are
called `dev` / `val` / `test` throughout to keep that honest.

## Architecture at a glance

```mermaid
flowchart LR
    V[(Volume\ndoc_type/client/docs)] --> B[bronze.doc_pairs\npair manifest]
    B --> SP[silver.parsed_docs\nai_parse_document, cached by pdf_hash]
    B --> SG[silver.ground_truth\nXML → long format, cached by xml_hash]
    B --> SA[bronze.split_assignments\nsticky client-level splits]
    P[gold.extract_specs\nschema registry] --> X
    SP --> X[gold.extractions\nai_extract v2.1\nconfidence + citations]
    X --> E[gold.eval_results]
    SG --> E
    SA --> E
    E --> D[Dashboard\nprompt A vs B, calibration]
```

Every arrow is incremental: each stage anti-joins its target table on the relevant key
and only processes what's new. Deleting a downstream table never re-triggers upstream
cost.

## 1. Pair manifest — `bronze.doc_pairs`

Persist the matched pairs to Delta **once**, then treat the manifest as the system of
record. Nothing downstream ever touches paths or the volume listing again.

| column | type | notes |
|---|---|---|
| `pair_id` | STRING | = `pdf_hash` (stable identity of the pair) |
| `doc_type` | STRING | from path segment 1 |
| `client` | STRING | from path segment 2 |
| `pdf_path`, `xml_path` | STRING | volume paths |
| `pdf_hash` | STRING | sha256 of PDF **content** |
| `xml_hash` | STRING | sha256 of XML content |
| `pdf_size_bytes`, `pdf_modified_at` | | cheap pre-filter before hashing |
| `discovered_at` | TIMESTAMP | |

Design points:

- **Key on content hash, not path.** A renamed or moved file doesn't reprocess; a file
  replaced in place (new hash) correctly does. Use `pdf_modified_at` + size as a cheap
  short-circuit so re-listing the volume doesn't re-hash unchanged files.
- **Refresh = MERGE**, matched on `pdf_path`: insert new pairs, update hashes when
  content changed. Run as a scheduled or on-demand "discover new documents" job — the
  only job that reads the volume tree.
- Unpaired files (PDF without XML, or vice versa) go to a companion `bronze.unpaired`
  table for visibility, not silently dropped.

## 2. Splits — `bronze.split_assignments`

**The unit of assignment is the client, not the document.** Documents from one client
share a preparer, formatting, and quirks; splitting at document level leaks that style
into the holdout and inflates every number. If the same client name appears under
multiple doc types, it still lands in exactly one split.

| column | type | notes |
|---|---|---|
| `client` | STRING | PK |
| `split` | STRING | `dev` \| `val` \| `test` |
| `assigned_at` | TIMESTAMP | |
| `method` | STRING | e.g. `xxhash64-v1` |

- **Deterministic and sticky:** `bucket = abs(xxhash64(client)) % 100`; e.g. bucket
  < 10 → `dev`, < 30 → `val`, else `test` (10 / 20 / 70 — tune to taste, small dev is
  the point). New clients arriving later get assigned by the same rule, so the table
  only ever grows — assignments never reshuffle.
- Persisting (rather than computing the hash on the fly) makes the split auditable and
  lets you manually pin a pathological client if needed, with `method = 'manual'`.
- **Access rule, enforced by the jobs, not by convention:** the prompt-iteration job
  hard-filters to `dev` (and optionally `val`); only a separate, manually-triggered
  final-eval job may read `test`. Non-coders physically cannot burn the holdout by
  clicking the wrong button.

## 3. Parsing layer (expensive — cache aggressively)

### `silver.parsed_docs` — one row per unique PDF

| column | type | notes |
|---|---|---|
| `pdf_hash` | STRING | PK |
| `parse_output` | VARIANT | full `ai_parse_document` JSON (elements, pages, bboxes) |
| `parsed_text` | STRING | concatenated text with page markers, extraction input |
| `page_count` | INT | |
| `parse_version` | STRING | pin/record the parser version — output drifts across releases |
| `status`, `error` | | `ok` \| `failed`; failures retried explicitly, not automatically forever |
| `parsed_at` | TIMESTAMP | |

Incremental rule: `doc_pairs ANTI JOIN parsed_docs ON pdf_hash` → parse only those.
Keep the **full** parse JSON, not just text: citations reference page/element IDs, and
keeping it means never re-parsing when the extraction schema evolves.

### `silver.ground_truth` — XML flattened to long format

| column | type |
|---|---|
| `pair_id`, `xml_hash` | STRING |
| `field_name` | STRING |
| `field_value` | STRING |
| `source_xpath` | STRING |

Long format makes evaluation a plain join, and tolerates different doc types having
different field sets. Re-parse only rows whose `xml_hash` isn't already present — XML
parsing is cheap, but the same idempotency pattern keeps the whole system uniform.

Field-name normalization (XML tag → canonical field name per doc type) lives in a small
mapping table, `silver.field_map(doc_type, xml_field, canonical_field)`, editable
without code changes.

## 4. Extraction layer — `ai_extract` v2.1

`ai_extract` version 2.1 provides everything this pipeline needs natively: per-field
**confidence scores** and **offset-grounded citations**, enabled via the options map.
Pin the version explicitly so behavior never shifts under you:

```sql
ai_extract(
  d.parsed_text,                -- also accepts the ai_parse_document VARIANT directly
  s.extraction_schema,          -- JSON schema string with per-field descriptions
  map(
    'version', '2.1',
    'enableConfidenceScores', 'true',
    'enableCitations', 'true'
  )
)
```

Output is a VARIANT shaped like:

```
{
  response: {
    <field>: { value, confidence, citation_ids: [0, ...] },
    ...
  },
  metadata: { citations: [ { id, start, stop }, ... ] },
  error_message
}
```

Three properties worth designing around:

- **Citations are character offsets** (`start`/`stop` into the input text), not
  model-quoted strings — grounded by construction, so there is no hallucinated-citation
  problem. Materialize the cited text with `substring(parsed_text, start, stop)` at
  write time so reviewers see the span without touching offsets.
- **Confidence is per field**, 0–1. It still needs **calibration** before non-coders
  use it as an auto-accept threshold — that's what the eval layer measures.
- **The schema is the prompting surface.** Field descriptions embedded in the
  extraction schema are where iteration happens ("the total tax due, from line 24, as
  a number with no currency symbol"). Schema limits: 128 fields, 7 nesting levels.

Escape hatch: if a doc type needs cross-field reasoning or guidance beyond what field
descriptions can express, that spec can use `ai_query` with structured output instead —
the registry's `method` column keeps both under the same experiment framework, same
caching, same eval.

### `gold.extract_specs` — the registry non-coders edit

| column | type | notes |
|---|---|---|
| `spec_id` | STRING | PK, e.g. `1040-v3` |
| `doc_type` | STRING | |
| `method` | STRING | `ai_extract` (default) \| `ai_query` (escape hatch) |
| `extraction_schema` | STRING | JSON schema with per-field descriptions |
| `prompt_text` | STRING | nullable; only for `ai_query` specs |
| `model` | STRING | nullable; only for `ai_query` specs |
| `created_by`, `created_at`, `notes` | | |

Specs are **append-only**: a change is a new `spec_id`, never an edit in place — that's
what makes results comparable and cache keys stable.

### `gold.extractions` — one row per (document, spec, field)

| column | type | notes |
|---|---|---|
| `pdf_hash` | STRING | ┐ |
| `spec_id` | STRING | ├ uniqueness key |
| `field_name` | STRING | ┘ |
| `pair_id`, `doc_type`, `split` | | denormalized for easy filtering |
| `value` | STRING | |
| `confidence` | DOUBLE | from `ai_extract`, 0–1 |
| `citations` | ARRAY\<STRUCT\<start INT, stop INT, text STRING\>\> | offsets + materialized span |
| `error_message` | STRING | from the function output; check before trusting `value` |
| `extracted_at` | TIMESTAMP | |

Incremental rule: for the selected `spec_id` and split, run only pairs with no row in
`extractions` for that `(pdf_hash, spec_id)`. Old spec versions' results stay put —
re-running spec v2 after trying v3 costs nothing.

## 5. Evaluation — `gold.eval_results`

Join `extractions` to `ground_truth` on `(pair_id, field_name)`, aggregate per
`(spec_id, split, doc_type, field_name)`:

- `n`, `exact_match_rate`, `normalized_match_rate` (case/whitespace/number-format
  normalization — money and dates need canonicalization before comparing)
- `null_rate` (model abstained), `gt_coverage` (field present in ground truth)
- `avg_confidence`, plus a calibration curve: bucket confidence into deciles and compute
  accuracy per bucket. If 0.9-confidence answers are right 60% of the time, the
  confidence threshold non-coders use for "auto-accept vs human review" must reflect
  that.

A Databricks AI/BI dashboard on this table gives the iteration surface: spec A vs
spec B per field, per doc type, on `val` — plus the calibration plot.

## 6. The iteration loop (what a non-coder actually does)

1. **Discover** (scheduled): volume → `doc_pairs` → new PDFs parsed → new XMLs parsed.
   Splits auto-assigned for new clients.
2. **Write a spec**: add a row to `gold.extract_specs` via a small widget notebook or
   Databricks App form (pick doc type, edit the field descriptions, auto-versioned id).
3. **Run extraction** (job with one dropdown: `spec_id`): extracts the **dev** split
   only, skipping anything already extracted for that spec.
4. **Look at the dashboard**: field-level accuracy vs the previous spec, null rates,
   calibration. Iterate → back to step 2.
5. **Promote**: when a spec wins on dev, run the same job against `val` to confirm the
   win generalizes. Iterate on dev, *confirm* on val.
6. **Final eval** (separate, manually-triggered, ideally rare): run the chosen spec on
   `test`. This is the number you report.

## 7. No-reprocess rules, in one table

| stage | cache key | re-runs only when |
|---|---|---|
| pair discovery | `pdf_path` (MERGE) | file content changed (hash differs) |
| `ai_parse_document` | `pdf_hash` | new/changed PDF, or deliberate `parse_version` bump |
| XML ground truth | `xml_hash` | new/changed XML |
| extraction | `(pdf_hash, spec_id)` | new document or new spec version |
| eval | derived | cheap — recompute freely |

## 8. Build order

1. `bronze.doc_pairs` + `bronze.split_assignments` (port the existing matching logic,
   add hashing + MERGE) — this answers "get the mappings into Delta in one go": yes.
2. `silver.parsed_docs` incremental job.
3. `silver.ground_truth` + `field_map` for the first doc type.
4. `gold.extract_specs` + extraction job (`ai_extract` v2.1 with confidence +
   citations, offsets materialized to text).
5. `gold.eval_results` + dashboard.
6. Widget notebook / App front-end for steps 2–4 of the iteration loop.

## Open items (defaults chosen, revisit if wrong)

- **Split ratios** default 10/20/70 (dev/val/test); with few clients per doc type,
  check per-doc-type balance after assignment and pin manually if a doc type has no
  dev representation.
- **Model choice**: `ai_extract` manages its own model; for `ai_query` escape-hatch
  specs the model lives in the registry, so comparing models is the same workflow as
  comparing specs.
- **Long documents**: if `parsed_text` exceeds the context window, chunk by page ranges
  at extraction time — the stored parse output already supports this without re-parsing.
