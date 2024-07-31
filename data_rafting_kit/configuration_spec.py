# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from pydantic import Field, model_validator

from data_rafting_kit.common.base_spec import BaseParamSpec, load_sub_config_file
from data_rafting_kit.data_quality.data_quality_spec import DataQualityRootSpec
from data_rafting_kit.env_spec import EnvSpec
from data_rafting_kit.io.io_spec import InputRootSpec, OutputRootSpec
from data_rafting_kit.testing.testing_spec import TestingRootSpec
from data_rafting_kit.transformations.transformation_spec import TransformationRootSpec


def load_list_of_sub_config_files(data: dict, key: str) -> dict:
    """Loads a list of config files and renders them with the given parameters."""
    if key in data and isinstance(data[key], list):
        for idx, data_item in enumerate(data[key]):
            if "type" in data_item and data_item["type"] == "config":
                if "params" not in data_item:
                    raise ValueError(
                        f"Field required: params for config file at index {idx}"
                    )

                loaded_config = load_sub_config_file(
                    data_item["params"],
                )

                if (
                    isinstance(loaded_config, dict)
                    and key in loaded_config
                    and isinstance(loaded_config[key], list)
                ):
                    data[key].pop(idx)

                    # Step 2: Insert the new values starting at the original index
                    for i, value in enumerate(loaded_config[key]):
                        data[key].insert(idx + i, value)
                elif isinstance(loaded_config, dict) and key in loaded_config:
                    data[key][idx] = loaded_config[key]
                elif isinstance(loaded_config, dict):
                    data[key][idx] = loaded_config
                else:
                    raise ValueError(
                        f"Invalid config file at {data_item['config']['path']}. The config file for a parameter must be a dictionary or a list of dictionaries contained within the key."
                    )

    return data


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

    @model_validator(mode="before")
    @classmethod
    def validate_and_load_submodule(cls, data: dict) -> dict:
        """Validates the spec to load any submodules."""
        data = super().validate_and_load_submodule(data)
        for key in list(data.keys()):
            data = load_list_of_sub_config_files(data, key)

        return data


class ConfigurationSpec(BaseParamSpec):
    """Data pipeline specification. This is the top-level specification for a data pipeline."""

    name: str | None = Field(default="Data Rafting Kit Pipeline")
    env: EnvSpec
    pipeline: PipelineSpec
    tests: TestingRootSpec | None = Field(default=None)
