from tars.pipeline.context import Document, PipelineContext

__all__ = [
    "Document",
    "PipelineContext",
]

# `BuiltPipeline`/`build_pipeline` (tars.pipeline.builder) and
# `PipelineRunner` (tars.pipeline.runner) are intentionally not re-exported
# here: both import tars.plugins, and eagerly pulling them into this
# package's __init__ would make even `from tars.pipeline.context import
# Document` re-enter tars.plugins while it's mid-initialization (it starts
# by importing tars.pipeline.context itself). Import them from their
# submodules directly: `from tars.pipeline.builder import build_pipeline`.
