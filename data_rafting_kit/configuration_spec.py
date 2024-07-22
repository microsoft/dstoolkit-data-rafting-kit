# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from pydantic import Field, model_validator

from data_rafting_kit.common.base_spec import BaseParamSpec
from data_rafting_kit.data_quality.data_quality_spec import DataQualityRootSpec
from data_rafting_kit.env_spec import EnvSpec
from data_rafting_kit.io.io_spec import InputRootSpec, OutputRootSpec
from data_rafting_kit.testing.testing_spec import TestingRootSpec
from data_rafting_kit.transformations.transformation_spec import TransformationRootSpec


class StreamingSpec(BaseParamSpec):
    """Streaming output specification."""

    await_termination: bool | None = Field(default=True)
    trigger: dict | None = Field(default=None)
    checkpoint: str | None = Field(default=None)

    @model_validator(mode="after")
    def validate_streaming_spec(self):
        """Validates the streaming spec."""
        if self.trigger is not None:
            if len(self.trigger) > 1:
                raise ValueError("Only one trigger can be set.")

            if self.trigger.keys()[0] not in [
                "once",
                "continuous",
                "processingTime",
                "availableNow",
            ]:
                raise ValueError(
                    "Invalid trigger. Must be either once, continuous, processingTime or availableNow. See spark documentation."
                )

            if self.await_termination and "processingTime" in self.trigger:
                raise ValueError("Cannot await termination when processingTime is set.")

        return self


def default_output_spec():
    """Default output specification."""
    return [OutputRootSpec(type="console", name="console")]


class PipelineSpec(BaseParamSpec):
    """Pipeline specification. Used to specify the inputs, outputs, and transformations for the pipeline."""

    streaming: StreamingSpec | None = Field(default_factory=StreamingSpec)
    inputs: list[InputRootSpec]
    outputs: list[OutputRootSpec] | None = Field(default_factory=default_output_spec)
    transformations: list[TransformationRootSpec] | None = Field(default_factory=list)
    data_quality: list[DataQualityRootSpec] | None = Field(default_factory=list)


class ConfigurationSpec(BaseParamSpec):
    """Data pipeline specification. This is the top-level specification for a data pipeline."""

    name: str
    env: EnvSpec
    pipeline: PipelineSpec
    tests: TestingRootSpec | None = Field(default=None)
