"""Loads tars YAML configuration into a validated :class:`AppConfig`.

Supports pointing at either a single YAML file or a directory of YAML
files (the common layout: `plugins.yaml`, `router.yaml`,
`pipelines/*.yaml`) which are merged before validation. `${ENV_VAR}` and
`${ENV_VAR:-default}` placeholders anywhere in the YAML text are
interpolated from the process environment before parsing, so secrets
(API keys, table names per-environment, ...) never need to be hardcoded.
"""

from __future__ import annotations

import importlib
import os
import re
from pathlib import Path
from typing import Any

import yaml

from tars.config.schema import AppConfig
from tars.plugins.registry import PluginRegistry, default_registry

_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}")


class ConfigError(ValueError):
    pass


def _interpolate_env(text: str) -> str:
    def replace(match: re.Match) -> str:
        var_name, _, default = match.groups()
        if var_name in os.environ:
            return os.environ[var_name]
        if default is not None:
            return default
        raise ConfigError(
            f"Environment variable {var_name!r} is referenced in config but not set "
            "(no default provided via ${VAR:-default})"
        )

    return _ENV_VAR_PATTERN.sub(replace, text)


def _load_yaml_file(path: Path) -> dict[str, Any]:
    raw = path.read_text()
    interpolated = _interpolate_env(raw)
    data = yaml.safe_load(interpolated)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: top-level YAML must be a mapping, got {type(data).__name__}")
    return data


def _merge(base: dict[str, Any], incoming: dict[str, Any], *, source: str) -> None:
    for key, value in incoming.items():
        if key == "plugins":
            base.setdefault("plugins", [])
            base["plugins"].extend(value or [])
        elif key == "pipelines":
            base.setdefault("pipelines", {})
            normalized = {item["name"]: item for item in value} if isinstance(value, list) else value
            for name, pipeline in (normalized or {}).items():
                if name in base["pipelines"]:
                    raise ConfigError(f"{source}: duplicate pipeline definition {name!r}")
                base["pipelines"][name] = pipeline
        elif key == "router":
            if "router" in base:
                raise ConfigError(
                    f"{source}: multiple files define `router`; only one is allowed"
                )
            base["router"] = value
        elif key == "version":
            base["version"] = value
        else:
            raise ConfigError(f"{source}: unknown top-level config key {key!r}")


def _collect_yaml_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.yaml") if p.is_file()) + sorted(
        p for p in root.rglob("*.yml") if p.is_file()
    )


def load_raw_config(path: str | Path) -> dict[str, Any]:
    """Load and merge YAML from `path` (file or directory) without validating."""
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Config path does not exist: {path}")

    merged: dict[str, Any] = {}
    if path.is_dir():
        files = _collect_yaml_files(path)
        if not files:
            raise ConfigError(f"No YAML files found under {path}")
        for file in files:
            _merge(merged, _load_yaml_file(file), source=str(file))
    else:
        merged = _load_yaml_file(path)
    return merged


def _import_plugin_class(python_path: str):
    module_path, _, attr = python_path.partition(":")
    if not attr:
        module_path, _, attr = python_path.rpartition(".")
    if not module_path:
        raise ConfigError(f"Invalid python_path for custom plugin: {python_path!r}")
    module = importlib.import_module(module_path)
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise ConfigError(f"{python_path!r}: module {module_path!r} has no attribute {attr!r}") from exc


def register_custom_plugins(config: AppConfig, registry: PluginRegistry) -> None:
    for declaration in config.plugins:
        cls = _import_plugin_class(declaration.python_path)
        registry.register(declaration.name, cls)


def load_config(
    path: str | Path,
    *,
    registry: PluginRegistry | None = None,
) -> AppConfig:
    """Load, validate, and (if a registry is given) register any custom
    plugins declared in the config. Returns the validated AppConfig."""
    raw = load_raw_config(path)
    try:
        config = AppConfig.model_validate(raw)
    except Exception as exc:
        raise ConfigError(f"Invalid configuration at {path}: {exc}") from exc

    register_custom_plugins(config, registry or default_registry)
    return config
