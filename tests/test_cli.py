from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from tars.cli.main import cli

EXAMPLES_DIR = Path(__file__).parent.parent / "configs" / "examples" / "local"


def test_validate_succeeds_on_local_example(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TARS_LANDING_DIR", str(tmp_path / "landing"))
    monkeypatch.setenv("TARS_OUTPUT_DIR", str(tmp_path / "out"))
    result = CliRunner().invoke(cli, ["validate", "-c", str(EXAMPLES_DIR)])
    assert result.exit_code == 0, result.output
    assert "OK" in result.output


def test_validate_fails_on_bad_config(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("router:\n  routes:\n    - doc_type: x\n      pipeline: missing\npipelines: []\n")
    result = CliRunner().invoke(cli, ["validate", "-c", str(bad)])
    assert result.exit_code == 1
    assert "Invalid config" in result.output


def test_list_plugins_shows_builtins() -> None:
    result = CliRunner().invoke(cli, ["list-plugins"])
    assert result.exit_code == 0
    assert "source.autoloader" in result.output
    assert "sink.volume" in result.output


def test_list_plugins_filters_by_kind() -> None:
    result = CliRunner().invoke(cli, ["list-plugins", "--kind", "parser"])
    assert result.exit_code == 0
    assert "parser.text" in result.output
    assert "sink.volume" not in result.output


def test_run_router_end_to_end(tmp_path: Path, monkeypatch) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "invoice_1.txt").write_text("pay up")
    monkeypatch.setenv("TARS_LANDING_DIR", str(landing))
    monkeypatch.setenv("TARS_OUTPUT_DIR", str(tmp_path / "out"))

    result = CliRunner().invoke(cli, ["run", "-c", str(EXAMPLES_DIR), "--router"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["stats"]["processed"] == 1
    assert (tmp_path / "out" / "invoices").exists()


def test_run_requires_exactly_one_mode(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TARS_LANDING_DIR", str(tmp_path / "landing"))
    monkeypatch.setenv("TARS_OUTPUT_DIR", str(tmp_path / "out"))
    result = CliRunner().invoke(cli, ["run", "-c", str(EXAMPLES_DIR)])
    assert result.exit_code != 0
    assert "exactly one" in result.output


def test_init_scaffolds_pipeline_file(tmp_path: Path) -> None:
    out_dir = tmp_path / "pipelines"
    result = CliRunner().invoke(cli, ["init", "pipeline", "my_new_pipeline", "-o", str(out_dir)])
    assert result.exit_code == 0
    assert (out_dir / "my_new_pipeline.yaml").exists()
