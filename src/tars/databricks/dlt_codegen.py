"""Generates Databricks Lakeflow Declarative Pipeline (formerly Delta Live
Tables) source code from a tars `AppConfig`.

This is the primary way tars runs on Databricks: rather than orchestrating
a Python loop ourselves, we translate the declarative YAML config into
native `@dp.table` definitions (`from pyspark import pipelines as dp` --
the current Lakeflow Declarative Pipelines convention, superseding the
legacy `import dlt` / `@dlt.table` spelling) and hand orchestration,
incremental state, and (serverless) compute entirely to Databricks' own
engine. The custom `PipelineRunner` (tars.pipeline.runner) still exists,
but is now positioned as the local/offline dev-and-test path -- it needs
no cluster and no Databricks workspace, which this generated code very
much does.

Opinionated defaults baked into this module (see docs/plugin-development.md
and the source plugin docstrings for the reasoning):

* `source.autoloader` is the supported, recommended bronze source for
  actual document bytes -- it's the native `cloudFiles` pattern Lakeflow
  Declarative Pipelines are built around.
* `source.zerobus` is supported for low-latency metadata/event ingestion
  (reading a table an external producer already pushed rows into), not
  for shipping raw file content.
* `classifier.databricks_ai` / `parser.databricks_ai` compile to native,
  columnar `ai_classify()`/`ai_parse_document()`/`ai_query()` calls --
  the fast, Databricks-recommended path.
* `classifier.ai` / `parser.ai` (direct Anthropic calls) compile to a
  pandas UDF wrapping the same plugin class used by the local runner --
  works, but is row-at-a-time and needs `tars[ai]` plus an API key
  available to the pipeline's serverless environment. Prefer the
  `databricks_ai` variants on Databricks.

**Routing, not new jobs**: every pipeline reachable through
`router.routes`/`router.default_pipeline` and not marked
`deploy.dedicated: true` is compiled into *one* shared generated file
(one Lakeflow pipeline) as an additional table -- adding a route in YAML
adds a table to the existing pipeline, it does not provision a new
Databricks Job or Pipeline resource. Only pipelines that opt in via
`deploy.dedicated: true` (or that have their own `source` and aren't
router-reachable at all) get their own generated file/pipeline resource.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from tars.config.schema import AppConfig, PipelineConfig, PluginRef, RouterConfig
from tars.plugins.classifiers.databricks_ai import DatabricksAiClassifier
from tars.plugins.classifiers.rule_based import RuleBasedClassifier
from tars.plugins.parsers.databricks_ai_parser import DatabricksAiParser
from tars.plugins.parsers.text_parser import TextParser
from tars.plugins.sources.autoloader import AutoloaderSource
from tars.plugins.sources.zerobus import ZerobusSource

_TEMPLATE_DIR = Path(__file__).parent / "templates"
_env = Environment(loader=FileSystemLoader(_TEMPLATE_DIR), trim_blocks=True, lstrip_blocks=True)
_TEMPLATE = _env.get_template("dlt_pipeline.py.jinja")


class DltCodegenError(ValueError):
    pass


@dataclass
class _Column:
    name: str
    expr: str


@dataclass
class _TableRef:
    table_name: str
    function_name: str


@dataclass
class _GoldTable:
    table_name: str
    function_name: str
    comment: str
    filter_expr: str
    columns: list[_Column]
    expand_columns: list[str] = field(default_factory=list)


def _sanitize(name: str) -> str:
    cleaned = "".join(c if c.isalnum() else "_" for c in name).strip("_").lower()
    return cleaned or "table"


def _render_source_bronze(source_ref: PluginRef, bronze_table: str) -> tuple[str, str, str]:
    """Returns (function_body_source, source_kind_description, content_column_name)."""
    if source_ref.name == "source.autoloader":
        plugin = AutoloaderSource(source_ref.config)
        lines = ["    return (", '        spark.readStream.format("cloudFiles")']
        for key, value in plugin.cloud_files_options().items():
            lines.append(f"        .option({key!r}, {value!r})")
        lines.append(f"        .load({plugin.path!r})")
        lines.append("    )")
        return "\n".join(lines), "Auto Loader (cloudFiles)", "content"

    if source_ref.name == "source.zerobus":
        plugin = ZerobusSource(source_ref.config)
        body = f"    return spark.readStream.table({plugin.table!r})"
        return body, "Zerobus (external producer writing directly into a landing table)", plugin.content_column

    raise DltCodegenError(
        f"source {source_ref.name!r} has no Lakeflow Declarative Pipeline codegen support -- only "
        "source.autoloader (recommended, for document bytes) and source.zerobus (for metadata/event "
        "records already landed in a table) can be expressed as a declarative bronze read. Run other "
        "sources through `tars run` (the local/offline PipelineRunner) instead."
    )


def _rule_condition(rule: dict[str, Any], path_column: str) -> str | None:
    conditions = []
    extensions = rule.get("extensions")
    if extensions:
        pattern = "|".join(e.lower().lstrip(".") for e in extensions)
        conditions.append(f'F.col("{path_column}").rlike(r"(?i)\\.({pattern})$")')
    filename_regex = rule.get("filename_regex")
    if filename_regex:
        conditions.append(f'F.col("{path_column}").rlike({filename_regex!r})')
    path_regex = rule.get("path_regex")
    if path_regex:
        conditions.append(f'F.col("{path_column}").rlike({path_regex!r})')
    # mime_types has no codegen equivalent: binaryFile/cloudFiles bronze
    # reads don't expose a mime-type column, so a mime_types-only rule
    # can't be expressed here and is silently skipped (it still works
    # under the local PipelineRunner, which reads it from Document.metadata).
    if not conditions:
        return None
    return " & ".join(f"({c})" for c in conditions)


def _rule_based_expr(plugin: RuleBasedClassifier, path_column: str) -> str | None:
    clauses = [
        (condition, rule["doc_type"])
        for rule in plugin.rules
        if (condition := _rule_condition(rule, path_column)) is not None
    ]
    if not clauses:
        return None
    lines = [f"F.when({clauses[0][0]}, F.lit({clauses[0][1]!r}))"]
    for condition, doc_type in clauses[1:]:
        lines.append(f".when({condition}, F.lit({doc_type!r}))")
    lines.append(".otherwise(F.lit(None))")
    return "\n            ".join(lines)


def _databricks_ai_classifier_expr(plugin: DatabricksAiClassifier, content_column: str) -> str:
    labels = ", ".join(repr(label) for label in plugin.doc_types)
    text_expr = f'F.substring(F.decode(F.col("{content_column}"), "utf-8"), 1, {plugin.max_content_chars})'
    return f'F.call_function("ai_classify", {text_expr}, F.array({labels}))'


def _udf_classifier(key: str, ref: PluginRef, content_column: str) -> tuple[str, str]:
    """Wraps classifier.ai (direct Anthropic) as a pandas UDF, reusing the
    exact same plugin class the local PipelineRunner uses. Returns
    (udf_source, call_expr)."""
    fn_name = f"_classify_{key}"
    udf_source = f'''\
_ai_classifier_{key} = AiClassifier({ref.config!r})


@F.pandas_udf("string")
def {fn_name}(content: "pd.Series") -> "pd.Series":
    from tars.pipeline.context import Document, PipelineContext

    ctx = PipelineContext(pipeline_name="router")

    def _run(raw):
        try:
            return _ai_classifier_{key}.classify(Document(uri="", content=raw), ctx)
        except Exception:
            return None

    return content.apply(_run)'''
    return udf_source, f'{fn_name}(F.col("{content_column}"))'


def _classify_expr(router: RouterConfig, content_column: str, path_column: str) -> tuple[str, list[str], list[str]]:
    """Builds the doc_type expression for the whole classifier chain
    (first non-null classifier wins, same semantics as
    DocumentRouter.classify). Returns (expr_source, udf_sources, extra_imports)."""
    if not router.classifiers:
        # No classifiers configured at all is a legitimate "everything goes
        # through default_pipeline" setup -- same as DocumentRouter.classify,
        # which just leaves doc_type as None when there's nothing to run.
        return "        F.lit(None)", [], []

    parts: list[str] = []
    udf_sources: list[str] = []
    extra_imports: list[str] = []

    for i, ref in enumerate(router.classifiers):
        if ref.name == "classifier.rule_based":
            expr = _rule_based_expr(RuleBasedClassifier(ref.config), path_column)
            if expr is not None:
                parts.append(f"(\n            {expr}\n        )")
        elif ref.name == "classifier.databricks_ai":
            parts.append(_databricks_ai_classifier_expr(DatabricksAiClassifier(ref.config), content_column))
        elif ref.name == "classifier.ai":
            extra_imports.append("from tars.plugins.classifiers.ai_classifier import AiClassifier")
            udf_source, call_expr = _udf_classifier(f"chain{i}", ref, content_column)
            udf_sources.append(udf_source)
            parts.append(call_expr)
        else:
            raise DltCodegenError(f"classifier {ref.name!r} has no Lakeflow Declarative Pipeline codegen support")

    if not parts:
        raise DltCodegenError(
            "router.classifiers is non-empty but none of its entries produced a usable codegen "
            "expression (e.g. a rule_based classifier with only mime_types-only rules, which has "
            "no bronze-table column to check)"
        )
    if len(parts) == 1:
        return f"        {parts[0]}", udf_sources, extra_imports
    joined = ",\n            ".join(parts)
    return f"        F.coalesce(\n            {joined},\n        )", udf_sources, extra_imports


def _parser_columns(
    key: str, ref: PluginRef, content_column: str
) -> tuple[list[_Column], list[str], list[str], list[str]]:
    """Returns (columns, expand_columns, udf_sources, extra_imports)."""
    if ref.name == "parser.text":
        plugin = TextParser(ref.config)
        text_expr = f'F.decode(F.col("{content_column}"), {plugin.encoding!r})'
        return [_Column("text", text_expr), _Column("length", 'F.length(F.col("text"))')], [], [], []

    if ref.name == "parser.databricks_ai":
        plugin = DatabricksAiParser(ref.config)
        columns = [_Column("parsed_document", f'F.call_function("ai_parse_document", F.col("{content_column}"))')]
        expand: list[str] = []
        if plugin.fields or plugin.json_schema:
            schema_json = json.dumps(plugin.schema())
            prompt_prefix = f"Extract fields per this JSON schema:\n{schema_json}\n\nDocument:\n"
            text_source = 'F.coalesce(F.col("parsed_document.document.text"), F.lit(""))'
            prompt_expr = f"F.concat(F.lit({prompt_prefix!r}), {text_source})"
            columns.append(
                _Column(
                    "extracted",
                    f'F.call_function("ai_query", F.lit({plugin.endpoint!r}), {prompt_expr}, '
                    f"F.lit({schema_json!r}))",
                )
            )
            expand.append("extracted")
        return columns, expand, [], []

    if ref.name == "parser.ai":
        fn_name = f"_extract_{key}"
        udf_source = f'''\
_ai_parser_{key} = AiParser({ref.config!r})


@F.pandas_udf("map<string,string>")
def {fn_name}(content: "pd.Series") -> "pd.Series":
    from tars.pipeline.context import Document, PipelineContext

    ctx = PipelineContext(pipeline_name={key!r})

    def _run(raw):
        try:
            result = _ai_parser_{key}.parse(Document(uri="", content=raw), ctx)
            return {{k: str(v) for k, v in result.items()}}
        except Exception as exc:
            return {{"_error": str(exc)}}

    return content.apply(_run)'''
        return (
            [_Column("extracted", f'{fn_name}(F.col("{content_column}"))')],
            [],
            [udf_source],
            ["from tars.plugins.parsers.ai_parser import AiParser"],
        )

    raise DltCodegenError(f"parser {ref.name!r} has no Lakeflow Declarative Pipeline codegen support")


def _sink_table_name(sinks: list[PluginRef], pipeline_name: str) -> tuple[str, list[str]]:
    """Returns (table_name, warning_comments)."""
    for sink in sinks:
        if sink.name == "sink.delta":
            table = sink.config.get("table", pipeline_name)
            return table.rsplit(".", 1)[-1], []
    for sink in sinks:
        if sink.name == "sink.volume":
            warning = (
                f"# NOTE: sink.volume can't write arbitrary files from a Lakeflow Declarative "
                f"Pipeline; materialized as Delta table {_sanitize(pipeline_name)!r} instead of "
                f"{sink.config.get('path')!r}. Use sink.delta for a native destination."
            )
            return _sanitize(pipeline_name), [warning]
    return _sanitize(pipeline_name), []


def _routed_doc_types(router: RouterConfig) -> dict[str, tuple[list[str], bool]]:
    """Maps pipeline name -> (explicit doc_types routed to it, is_default)."""
    result: dict[str, tuple[list[str], bool]] = {}
    for route in router.routes:
        doc_types, is_default = result.get(route.pipeline, ([], False))
        result[route.pipeline] = (doc_types + [route.doc_type], is_default)
    if router.default_pipeline:
        doc_types, _ = result.get(router.default_pipeline, ([], False))
        result[router.default_pipeline] = (doc_types, True)
    return result


def _filter_expr(doc_types: list[str], is_default: bool) -> str:
    clauses = []
    if doc_types:
        literal = ", ".join(repr(d) for d in doc_types)
        clauses.append(f"F.col('doc_type').isin({literal})")
    if is_default:
        clauses.append("F.col('doc_type').isNull()")
    return " | ".join(clauses) if len(clauses) > 1 else clauses[0]


def _build_gold_table(
    pipeline: PipelineConfig, filter_expr: str, content_column: str
) -> tuple[_GoldTable, list[str], list[str]]:
    if pipeline.parser is None:
        raise DltCodegenError(f"pipeline {pipeline.name!r} has no `parser`; required for codegen")
    columns, expand, udf_sources, extra_imports = _parser_columns(
        _sanitize(pipeline.name), pipeline.parser, content_column
    )
    table_name, warnings = _sink_table_name(pipeline.sinks, pipeline.name)
    gold = _GoldTable(
        table_name=table_name,
        function_name=_sanitize(pipeline.name),
        comment=(pipeline.description or f"Parsed output for pipeline {pipeline.name!r}."),
        filter_expr=filter_expr,
        columns=columns,
        expand_columns=expand,
    )
    return gold, udf_sources, extra_imports + warnings


def _render(
    group_name: str,
    config_path: str,
    bronze_ref: _TableRef,
    source_kind: str,
    bronze_body: str,
    gold_tables: list[_GoldTable],
    udfs: list[str],
    extra_imports: list[str],
    *,
    classified_ref: _TableRef | None = None,
    classify_expr: str | None = None,
) -> str:
    has_classified = classified_ref is not None
    read_table = classified_ref.table_name if has_classified else bronze_ref.table_name
    return _TEMPLATE.render(
        group_name=group_name,
        config_path=config_path,
        bronze={
            "table_name": bronze_ref.table_name,
            "function_name": bronze_ref.function_name,
            "source_kind": source_kind,
            "body": bronze_body,
        },
        has_classified=has_classified,
        classified={
            "table_name": classified_ref.table_name if classified_ref else "",
            "function_name": classified_ref.function_name if classified_ref else "",
        },
        classify_expr=classify_expr,
        read_table=read_table,
        gold_tables=gold_tables,
        udfs=udfs,
        extra_imports=sorted(set(extra_imports)),
    )


def generate(app_config: AppConfig, *, config_path: str = "<config>") -> dict[str, str]:
    """Returns {relative_filename: python_source} for the whole config.

    One shared file covers every router-reachable pipeline that isn't
    `deploy.dedicated`; each dedicated (or router-unreachable, standalone)
    pipeline gets its own file/pipeline.
    """
    routed = _routed_doc_types(app_config.router)
    files: dict[str, str] = {}

    shared_pipelines: list[PipelineConfig] = []
    dedicated_pipelines: list[PipelineConfig] = []

    for pipeline in app_config.pipelines.values():
        is_routed = pipeline.name in routed
        if pipeline.deploy.dedicated:
            if pipeline.source is None:
                raise DltCodegenError(
                    f"pipeline {pipeline.name!r} sets deploy.dedicated=true but is only reachable "
                    "via the router (no own `source`); dedicated router-dispatched pipelines aren't "
                    "supported yet -- give it its own `source`, or remove deploy.dedicated."
                )
            dedicated_pipelines.append(pipeline)
        elif is_routed:
            shared_pipelines.append(pipeline)
        elif pipeline.source is not None:
            dedicated_pipelines.append(pipeline)
        else:
            raise DltCodegenError(
                f"pipeline {pipeline.name!r} has no route in router.routes/default_pipeline and no "
                "own `source` -- nothing would ever invoke it"
            )

    if shared_pipelines:
        if app_config.router.source is None:
            raise DltCodegenError(
                "router.source is required to generate the shared Lakeflow Declarative Pipeline "
                "(it's the bronze/landing read every routed pipeline shares)"
            )
        bronze_body, source_kind, content_column = _render_source_bronze(app_config.router.source, "bronze_documents")
        path_column = "path" if app_config.router.source.name == "source.autoloader" else content_column
        classify_expr, classify_udfs, classify_imports = _classify_expr(
            app_config.router, content_column, path_column
        )

        gold_tables: list[_GoldTable] = []
        all_udfs = list(classify_udfs)
        all_imports = list(classify_imports)
        for pipeline in shared_pipelines:
            doc_types, is_default = routed[pipeline.name]
            gold, udf_sources, extra_imports = _build_gold_table(
                pipeline, _filter_expr(doc_types, is_default), content_column
            )
            gold_tables.append(gold)
            all_udfs.extend(udf_sources)
            all_imports.extend(extra_imports)

        files["tars_pipeline.py"] = _render(
            group_name="shared (router-dispatched)",
            config_path=config_path,
            bronze_ref=_TableRef("bronze_documents", "bronze_documents"),
            source_kind=source_kind,
            bronze_body=bronze_body,
            classified_ref=_TableRef("classified_documents", "classified_documents"),
            classify_expr=classify_expr,
            gold_tables=gold_tables,
            udfs=all_udfs,
            extra_imports=all_imports,
        )

    for pipeline in dedicated_pipelines:
        key = _sanitize(pipeline.name)
        bronze_table = f"{key}_bronze"
        bronze_body, source_kind, content_column = _render_source_bronze(pipeline.source, bronze_table)

        udfs: list[str] = []
        imports: list[str] = []
        classified_ref: _TableRef | None = None
        classify_expr: str | None = None

        if pipeline.classifiers:
            path_column = "path" if pipeline.source.name == "source.autoloader" else content_column
            classify_expr, classify_udfs, classify_imports = _classify_expr(
                RouterConfig(classifiers=pipeline.classifiers), content_column, path_column
            )
            udfs.extend(classify_udfs)
            imports.extend(classify_imports)
            classified_table = f"{key}_classified"
            classified_ref = _TableRef(classified_table, classified_table)

        # Dedicated pipelines have exactly one gold table and no routing
        # decision to make -- it consumes everything upstream of it.
        gold, gold_udfs, gold_imports = _build_gold_table(pipeline, "F.lit(True)", content_column)
        udfs.extend(gold_udfs)
        imports.extend(gold_imports)

        files[f"pipeline_{key}.py"] = _render(
            group_name=f"dedicated: {pipeline.name}",
            config_path=config_path,
            bronze_ref=_TableRef(bronze_table, bronze_table),
            source_kind=source_kind,
            bronze_body=bronze_body,
            classified_ref=classified_ref,
            classify_expr=classify_expr,
            gold_tables=[gold],
            udfs=udfs,
            extra_imports=imports,
        )

    return files


def write_generated_files(app_config: AppConfig, output_dir: Path, *, config_path: str = "<config>") -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for filename, source in generate(app_config, config_path=config_path).items():
        path = output_dir / filename
        path.write_text(source)
        written.append(path)
    return written
