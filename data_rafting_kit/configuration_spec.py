# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from pydantic import BaseModel, ConfigDict, Field

from data_rafting_kit.data_quality.data_quality_spec import DataQualityRootSpec
from data_rafting_kit.env_spec import EnvSpec
from data_rafting_kit.io.io_spec import InputRootSpec, OutputRootSpec
from data_rafting_kit.testing.testing_spec import TestingRootSpec
from data_rafting_kit.transformations.transformation_spec import TransformationRootSpec


class PipelineSpec(BaseModel):
    """Pipeline specification. Used to specify the inputs, outputs, and transformations for the pipeline."""

    inputs: list[InputRootSpec]
    outputs: list[OutputRootSpec] | None = Field(default_factory=list)
    transformations: list[TransformationRootSpec] | None = Field(default_factory=list)
    data_quality: list[DataQualityRootSpec] | None = Field(default_factory=list)


class ConfigurationSpec(BaseModel):
    """Data pipeline specification. This is the top-level specification for a data pipeline."""

    env: EnvSpec
    pipeline: PipelineSpec
    tests: TestingRootSpec | None = Field(default=None)

    model_config = ConfigDict(
        validate_default=True,
        extra_values="forbid",
    )
