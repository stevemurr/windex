"""Canonical Pipeline domain.

Pipelines are reusable immutable graph lineages.  A Source binds one published
Pipeline revision to corpus identity, configuration, durable state, and Runs.
This is the sole reusable graph model used by the backend.
"""

from windex.pipeline.compile import (
    compile_pipeline,
    compile_source,
    compile_tasks,
    describe_placement,
    resolve,
    resolve_parameters,
    unavailable_modules,
)
from windex.pipeline.spec import (
    PIPELINE_SCHEMA,
    Pipeline,
    PipelineValidationError,
    parse,
    validate,
)

__all__ = [
    "PIPELINE_SCHEMA",
    "Pipeline",
    "PipelineValidationError",
    "compile_pipeline",
    "compile_source",
    "compile_tasks",
    "describe_placement",
    "parse",
    "resolve",
    "resolve_parameters",
    "unavailable_modules",
    "validate",
]
