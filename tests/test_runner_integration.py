from __future__ import annotations

import json
from pathlib import Path

from tars.config.loader import load_config
from tars.pipeline.runner import PipelineRunner
from tars.plugins.registry import PluginRegistry

EXAMPLES_DIR = Path(__file__).parent.parent / "configs" / "examples" / "local"


def test_router_driven_run_end_to_end(tmp_path: Path, monkeypatch) -> None:
    landing = tmp_path / "landing"
    output = tmp_path / "processed"
    landing.mkdir()
    monkeypatch.setenv("TARS_LANDING_DIR", str(landing))
    monkeypatch.setenv("TARS_OUTPUT_DIR", str(output))

    (landing / "Invoice_001.txt").write_text("please pay 100")
    (landing / "Service_Agreement.txt").write_text("this contract is between...")
    (landing / "random_notes.txt").write_text("just some notes")

    registry = PluginRegistry(load_entry_points=True)
    app_config = load_config(EXAMPLES_DIR, registry=registry)
    runner = PipelineRunner(app_config, registry=registry)

    context = runner.run_router()

    assert context.stats["received"] == 3
    assert context.stats["processed"] == 3

    invoice_files = list((output / "invoices").glob("*.json"))
    contract_files = list((output / "contracts").glob("*.json"))
    unclassified_files = list((output / "unclassified").glob("*.json"))
    assert len(invoice_files) == 1
    assert len(contract_files) == 1
    assert len(unclassified_files) == 1

    record = json.loads(invoice_files[0].read_text())
    assert record["doc_type"] == "invoice"
    assert record["extracted"]["text"] == "please pay 100"

    # A second run against the same landing dir should pick up nothing new
    # (volume_listener persists a checkpoint).
    context2 = runner.run_router()
    assert context2.stats.get("received", 0) == 0


def test_single_pipeline_run_without_router(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "a.txt").write_text("hello")

    registry = PluginRegistry(load_entry_points=True)
    from tars.config.schema import AppConfig, PipelineConfig, PluginRef

    app_config = AppConfig(
        pipelines={
            "standalone": PipelineConfig(
                name="standalone",
                source=PluginRef(name="source.volume_listener", config={"path": str(landing)}),
                parser=PluginRef(name="parser.text"),
                sinks=[PluginRef(name="sink.volume", config={"path": str(tmp_path / "out")})],
            )
        }
    )
    runner = PipelineRunner(app_config, registry=registry)

    context = runner.run_pipeline("standalone")

    assert context.stats["processed"] == 1
    assert len(list((tmp_path / "out").glob("*.json"))) == 1
