from __future__ import annotations

from pathlib import Path

import pytest

from tars.config.loader import ConfigError, load_config, load_raw_config
from tars.plugins.registry import PluginRegistry

EXAMPLES_DIR = Path(__file__).parent.parent / "configs" / "examples" / "local"


def test_load_local_example_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TARS_LANDING_DIR", "/tmp/tars-landing")
    monkeypatch.setenv("TARS_OUTPUT_DIR", "/tmp/tars-output")
    registry = PluginRegistry(load_entry_points=True)

    config = load_config(EXAMPLES_DIR, registry=registry)

    assert set(config.pipelines) == {"invoice_pipeline", "contract_pipeline", "unclassified_pipeline"}
    assert config.router.default_pipeline == "unclassified_pipeline"
    assert config.router.source.config["path"] == "/tmp/tars-landing"


def test_env_var_default_used_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TARS_LANDING_DIR", raising=False)
    config = load_config(EXAMPLES_DIR, registry=PluginRegistry(load_entry_points=True))
    assert config.router.source.config["path"] == "./data/landing"


def test_missing_env_var_without_default_raises(tmp_path: Path) -> None:
    (tmp_path / "app.yaml").write_text(
        "version: 1\n"
        "router:\n"
        "  default_pipeline: null\n"
        "pipelines: []\n"
        "plugins:\n"
        "  - name: classifier.x\n"
        "    python_path: ${MISSING_ENV_VAR}\n"
    )
    with pytest.raises(ConfigError, match="MISSING_ENV_VAR"):
        load_raw_config(tmp_path)


def test_duplicate_pipeline_across_files_raises(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text("pipelines:\n  - name: dup\n    parser: {name: parser.text}\n")
    (tmp_path / "b.yaml").write_text("pipelines:\n  - name: dup\n    parser: {name: parser.text}\n")
    with pytest.raises(ConfigError, match="duplicate pipeline"):
        load_raw_config(tmp_path)


def test_route_to_unknown_pipeline_raises(tmp_path: Path) -> None:
    (tmp_path / "app.yaml").write_text(
        "router:\n"
        "  routes:\n"
        "    - doc_type: invoice\n"
        "      pipeline: does_not_exist\n"
        "pipelines: []\n"
    )
    with pytest.raises(ConfigError, match="unknown pipeline"):
        load_config(tmp_path, registry=PluginRegistry(load_entry_points=False))


def test_dead_letter_without_sink_raises(tmp_path: Path) -> None:
    (tmp_path / "app.yaml").write_text(
        "pipelines:\n"
        "  - name: p\n"
        "    parser: {name: parser.text}\n"
        "    on_error: dead_letter\n"
    )
    with pytest.raises(ConfigError, match="dead_letter"):
        load_config(tmp_path, registry=PluginRegistry(load_entry_points=False))


def test_unknown_top_level_key_raises(tmp_path: Path) -> None:
    (tmp_path / "app.yaml").write_text("not_a_real_key: 1\n")
    with pytest.raises(ConfigError, match="unknown top-level config key"):
        load_raw_config(tmp_path)
