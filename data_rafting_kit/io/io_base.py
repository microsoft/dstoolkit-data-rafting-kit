# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from enum import StrEnum
from logging import Logger
from typing import Literal

from pydantic import Field, model_validator
from pyspark.sql import SparkSession

from data_rafting_kit.common.base_spec import BaseParamSpec, BaseSpec
from data_rafting_kit.common.schema import SchemaFieldSpec


class IOEnum(StrEnum):
    """Enumeration class for IO types."""

    DELTA_TABLE = "delta_table"
    FILE = "file"
    EVENT_HUB = "event_hub"
    CONSOLE = "console"


class BatchOutputModeEnum(StrEnum):
    """Enumeration class for Delta Table modes."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR = "error"
    IGNORE = "ignore"
    MERGE = "merge"


class StreamingOutputModeEnum(StrEnum):
    """Enumeration class for Delta Table modes."""

    APPEND = "append"
    COMPLETE = "complete"
    UPDATE = "update"


class InputBaseParamSpec(BaseParamSpec):
    """Base input parameter specification."""

    expected_schema: list[SchemaFieldSpec] | None = Field(default=None)
    options: dict | None = Field(default_factory=dict)
    streaming: bool | None = Field(default=False)


class InputBaseSpec(BaseSpec):
    """Base input specification."""

    pass


class StreamingOutputSpec(BaseParamSpec):
    """Streaming output specification."""

    await_termination: bool | None = Field(default=True)
    trigger: dict | None = Field(default=None)
    checkpoint: str

    @model_validator(mode="after")
    def validate_streaming_output_spec(self):
        """Validates the streaming output spec."""
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


class OutputBaseParamSpec(BaseParamSpec):
    """Base output parameter specification."""

    expected_schema: list[SchemaFieldSpec] | None = Field(default=None)
    options: dict | None = Field(default_factory=dict)
    mode: (
        Literal[
            BatchOutputModeEnum.APPEND,
            BatchOutputModeEnum.OVERWRITE,
            BatchOutputModeEnum.ERROR,
            BatchOutputModeEnum.IGNORE,
            BatchOutputModeEnum.MERGE,
            StreamingOutputModeEnum.APPEND,
            StreamingOutputModeEnum.COMPLETE,
            StreamingOutputModeEnum.UPDATE,
        ]
        | None
    ) = Field(default=BatchOutputModeEnum.APPEND)
    streaming: StreamingOutputSpec | bool | None = Field(default=None)

    @model_validator(mode="after")
    def validate_output_param_spec(self):
        """Validates the output parameter specification."""
        if (
            self.streaming is not None
            and isinstance(self.streaming, bool)
            and self.streaming
        ):
            self.streaming = StreamingOutputSpec()

        if (
            self.streaming
            and self.mode not in StreamingOutputModeEnum.__members__.values()
        ):
            raise ValueError(f"Invalid mode '{self.mode}' for streaming output.")

        if (
            not self.streaming
            and self.mode not in BatchOutputModeEnum.__members__.values()
        ):
            raise ValueError(f"Invalid mode '{self.mode}' for batch output.")

        return self


class OutputBaseSpec(BaseSpec):
    """Base output specification."""

    input_df: str | None = Field(default=None)


class IOBase:
    """Represents an IO object for data pipelines.

    Attributes
    ----------
        _spark (SparkSession): The SparkSession object.
        _spec (BaseSpec): The specification object or child specification derirved from it.
        _logger (Logger): The logger object.

    """

    def __init__(self, spark: SparkSession, logger: Logger, env):
        """Initializes an instance of the IO class.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            logger (Logger): The logger object.
            env (EnvSpec): The environment specification.
        """
        self._spark = spark
        self._logger = logger
        self._env = env
