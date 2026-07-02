from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tars.config.loader import load_config
from tars.config.schema import AppConfig, DeployConfig, PipelineConfig, PluginRef, RouterConfig
from tars.databricks.dlt_codegen import DltCodegenError, generate, write_generated_files
from tars.plugins.registry import PluginRegistry

DATABRICKS_EXAMPLE = Path(__file__).parent.parent / "configs" / "examples" / "databricks"


@pytest.fixture
def databricks_app_config() -> AppConfig:
    return load_config(DATABRICKS_EXAMPLE, registry=PluginRegistry(load_entry_points=True))


def test_shared_group_generates_one_valid_file(databricks_app_config: AppConfig) -> None:
    files = generate(databricks_app_config, config_path=str(DATABRICKS_EXAMPLE))

    assert set(files) == {"tars_pipeline.py"}
    source = files["tars_pipeline.py"]
    ast.parse(source)  # must be syntactically valid Python

    assert 'name="bronze_documents"' in source
    assert 'name="classified_documents"' in source
    assert 'name="invoices"' in source
    assert 'name="contracts"' in source
    assert 'name="unclassified_pipeline"' in source
    # adding a route added a table, not a new file/job
    assert source.count("@dp.table") == 5


def test_rule_based_and_databricks_ai_classifiers_combine_via_coalesce(databricks_app_config: AppConfig) -> None:
    source = generate(databricks_app_config, config_path=str(DATABRICKS_EXAMPLE))["tars_pipeline.py"]
    assert "F.coalesce(" in source
    assert 'F.col("path").rlike(r"(?i)\\.(pdf)$")' in source
    assert 'F.call_function("ai_classify"' in source


def test_databricks_ai_parser_uses_native_functions(databricks_app_config: AppConfig) -> None:
    source = generate(databricks_app_config, config_path=str(DATABRICKS_EXAMPLE))["tars_pipeline.py"]
    assert 'F.call_function("ai_parse_document", F.col("content"))' in source
    assert 'F.call_function("ai_query"' in source
    assert '.select("*", F.col("extracted.*"))' in source


def test_sink_volume_falls_back_to_delta_table_with_warning(databricks_app_config: AppConfig) -> None:
    source = generate(databricks_app_config, config_path=str(DATABRICKS_EXAMPLE))["tars_pipeline.py"]
    assert "NOTE: sink.volume can't write arbitrary files" in source
    assert 'name="unclassified_pipeline"' in source


def test_dedicated_pipeline_with_own_source_gets_its_own_file() -> None:
    app_config = AppConfig(
        router=RouterConfig(),
        pipelines={
            "standalone": PipelineConfig(
                name="standalone",
                source=PluginRef(
                    name="source.autoloader",
                    config={"path": "/Volumes/main/landing/special", "schema_location": "/tmp/schema"},
                ),
                parser=PluginRef(name="parser.text"),
                sinks=[PluginRef(name="sink.delta", config={"table": "main.docs.special"})],
            )
        },
    )

    files = generate(app_config, config_path="cfg")

    assert set(files) == {"pipeline_standalone.py"}
    source = files["pipeline_standalone.py"]
    ast.parse(source)
    assert 'name="special"' in source
    # no classifiers on this pipeline -> no classified layer, just bronze + gold
    assert source.count("@dp.table") == 2
    assert "classified" not in source


def test_explicit_dedicated_flag_creates_separate_file_from_shared_group() -> None:
    app_config = AppConfig(
        router=RouterConfig(
            source=PluginRef(name="source.autoloader", config={"path": "/Volumes/main/landing/docs"}),
            routes=[{"doc_type": "invoice", "pipeline": "invoice_pipeline"}],
        ),
        pipelines={
            "invoice_pipeline": PipelineConfig(
                name="invoice_pipeline",
                parser=PluginRef(name="parser.text"),
                sinks=[PluginRef(name="sink.delta", config={"table": "main.docs.invoices"})],
                deploy=DeployConfig(dedicated=True),
                source=PluginRef(name="source.autoloader", config={"path": "/Volumes/main/landing/invoices"}),
            )
        },
    )

    files = generate(app_config, config_path="cfg")

    assert "pipeline_invoice_pipeline.py" in files
    assert "tars_pipeline.py" not in files  # nothing left in the shared group


def test_dedicated_router_only_pipeline_raises() -> None:
    app_config = AppConfig(
        router=RouterConfig(
            source=PluginRef(name="source.autoloader", config={"path": "/x"}),
            routes=[{"doc_type": "invoice", "pipeline": "p"}],
        ),
        pipelines={
            "p": PipelineConfig(
                name="p",
                parser=PluginRef(name="parser.text"),
                sinks=[PluginRef(name="sink.delta", config={"table": "t"})],
                deploy=DeployConfig(dedicated=True),
            )
        },
    )

    with pytest.raises(DltCodegenError, match="dedicated"):
        generate(app_config, config_path="cfg")


def test_unreachable_pipeline_raises() -> None:
    app_config = AppConfig(
        pipelines={"orphan": PipelineConfig(name="orphan", parser=PluginRef(name="parser.text"))}
    )
    with pytest.raises(DltCodegenError, match="nothing would ever invoke it"):
        generate(app_config, config_path="cfg")


def test_unsupported_source_raises() -> None:
    app_config = AppConfig(
        router=RouterConfig(
            source=PluginRef(name="source.api"),
            default_pipeline="p",
        ),
        pipelines={
            "p": PipelineConfig(
                name="p",
                parser=PluginRef(name="parser.text"),
                sinks=[PluginRef(name="sink.delta", config={"table": "t"})],
            )
        },
    )
    with pytest.raises(DltCodegenError, match="source.api"):
        generate(app_config, config_path="cfg")


def test_write_generated_files_writes_to_disk(databricks_app_config: AppConfig, tmp_path: Path) -> None:
    written = write_generated_files(databricks_app_config, tmp_path, config_path=str(DATABRICKS_EXAMPLE))
    assert len(written) == 1
    assert written[0].read_text().startswith('"""AUTO-GENERATED')


def test_anthropic_direct_classifier_and_parser_compile_to_pandas_udfs() -> None:
    app_config = AppConfig(
        router=RouterConfig(
            source=PluginRef(name="source.autoloader", config={"path": "/x"}),
            classifiers=[PluginRef(name="classifier.ai", config={"doc_types": ["invoice", "contract"]})],
            routes=[{"doc_type": "invoice", "pipeline": "p"}],
        ),
        pipelines={
            "p": PipelineConfig(
                name="p",
                parser=PluginRef(name="parser.ai", config={"fields": [{"name": "vendor_name"}]}),
                sinks=[PluginRef(name="sink.delta", config={"table": "main.docs.p"})],
            )
        },
    )

    source = generate(app_config, config_path="cfg")["tars_pipeline.py"]
    ast.parse(source)

    assert "from tars.plugins.classifiers.ai_classifier import AiClassifier" in source
    assert "from tars.plugins.parsers.ai_parser import AiParser" in source
    assert '@F.pandas_udf("string")' in source
    assert '@F.pandas_udf("map<string,string>")' in source
    assert "_ai_classifier_chain0 = AiClassifier(" in source
    assert "_ai_parser_p = AiParser(" in source


def test_zerobus_source_reads_landing_table() -> None:
    app_config = AppConfig(
        router=RouterConfig(
            source=PluginRef(name="source.zerobus", config={"table": "main.landing.events"}),
            default_pipeline="p",
        ),
        pipelines={
            "p": PipelineConfig(
                name="p",
                parser=PluginRef(name="parser.text"),
                sinks=[PluginRef(name="sink.delta", config={"table": "main.docs.p"})],
            )
        },
    )

    source = generate(app_config, config_path="cfg")["tars_pipeline.py"]
    ast.parse(source)

    assert 'spark.readStream.table(\'main.landing.events\')' in source
    assert "Zerobus" in source
