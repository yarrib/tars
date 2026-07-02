"""Pydantic models for tars' declarative YAML configuration.

The object graph mirrors the four plugin kinds plus two things that tie
them together: a `router` (decides which pipeline a document goes to) and
a map of `pipelines` (each a source -> classify -> parse -> sink chain).

Everything here is intentionally permissive about plugin `config` blocks
(`dict[str, Any]`) -- each plugin validates its own config, so tars core
never needs to know about e.g. Databricks-specific fields like
`schema_location`.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PluginRef(StrictModel):
    """A reference to a plugin instance: which plugin, and its config."""

    name: str
    config: dict[str, Any] = Field(default_factory=dict)


class CustomPluginDeclaration(StrictModel):
    """Registers a plugin class that isn't installed as a package entry
    point -- e.g. one defined in the same Databricks Repo as the config.

        plugins:
          - name: classifier.invoice_keywords
            python_path: my_org.tars_plugins.InvoiceKeywordClassifier
    """

    name: str
    python_path: str = Field(..., description="dotted.module.path:ClassName or dotted.module.ClassName")


class Route(StrictModel):
    """Maps a classified `doc_type` (or wildcard `*`) to a pipeline name."""

    doc_type: str
    pipeline: str


class RouterConfig(StrictModel):
    """The document router: runs classifiers to assign a doc_type, then
    dispatches to a pipeline by consulting `routes` in order.

    `source` is optional and only needed for router-driven ingestion, where
    a single shared source (e.g. one Autoloader stream over a landing
    zone) feeds documents into the router, which then dispatches each one
    to the classify/parse/sink chain of the matched pipeline (that
    pipeline's own `source`, if any, is ignored in this mode).
    """

    source: PluginRef | None = None
    classifiers: list[PluginRef] = Field(default_factory=list)
    routes: list[Route] = Field(default_factory=list)
    default_pipeline: str | None = None

    def resolve(self, doc_type: str | None) -> str | None:
        if doc_type is not None:
            for route in self.routes:
                if route.doc_type == doc_type:
                    return route.pipeline
        for route in self.routes:
            if route.doc_type == "*":
                return route.pipeline
        return self.default_pipeline


class DeployConfig(StrictModel):
    """Controls how this pipeline is materialized when deploying to
    Databricks. The default (`dedicated=False`) adds the pipeline as a
    route/table inside the *shared* generated Lakeflow Declarative
    Pipeline -- no new Databricks Job or Pipeline resource is created.
    Set `dedicated=True` only when a pipeline genuinely needs its own
    compute/schedule/permissions boundary (e.g. a much heavier workload,
    or one that must scale/fail independently of the rest)."""

    dedicated: bool = False


class PipelineConfig(StrictModel):
    """A single declarative ingestion pipeline: optional source (pipelines
    reached only via the router don't need their own), then classify,
    parse, and fan out to one or more sinks."""

    name: str
    description: str | None = None
    source: PluginRef | None = None
    classifiers: list[PluginRef] = Field(default_factory=list)
    parser: PluginRef | None = None
    sinks: list[PluginRef] = Field(default_factory=list)
    on_error: Literal["skip", "fail", "dead_letter"] = "skip"
    dead_letter_sink: PluginRef | None = None
    deploy: DeployConfig = Field(default_factory=DeployConfig)

    @model_validator(mode="after")
    def _dead_letter_requires_sink(self) -> "PipelineConfig":
        if self.on_error == "dead_letter" and self.dead_letter_sink is None:
            raise ValueError(
                f"pipeline {self.name!r}: on_error=dead_letter requires dead_letter_sink"
            )
        return self


class AppConfig(StrictModel):
    """Top-level tars configuration: the full declarative application."""

    version: int = 1
    plugins: list[CustomPluginDeclaration] = Field(default_factory=list)
    router: RouterConfig = Field(default_factory=RouterConfig)
    pipelines: dict[str, PipelineConfig] = Field(default_factory=dict)

    @field_validator("pipelines", mode="before")
    @classmethod
    def _key_pipelines_by_name(cls, value: Any) -> Any:
        # Allow either `pipelines: {name: {...}}` or `pipelines: [{name: ..., ...}]`
        if isinstance(value, list):
            return {item["name"]: item for item in value}
        return value

    @model_validator(mode="after")
    def _routes_point_at_real_pipelines(self) -> "AppConfig":
        for route in self.router.routes:
            if route.pipeline not in self.pipelines:
                raise ValueError(
                    f"router route for doc_type={route.doc_type!r} points at unknown "
                    f"pipeline {route.pipeline!r}"
                )
        if (
            self.router.default_pipeline is not None
            and self.router.default_pipeline not in self.pipelines
        ):
            raise ValueError(
                f"router.default_pipeline {self.router.default_pipeline!r} is not a known pipeline"
            )
        return self
