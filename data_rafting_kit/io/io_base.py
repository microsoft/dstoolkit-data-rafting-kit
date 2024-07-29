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


class WatermarkSpec(BaseParamSpec):
    """Watermark specification."""

    column: str
    delay: str


class StreamingInputSpec(BaseParamSpec):
    """Streaming input specification."""

    watermark: WatermarkSpec | None = Field(default=None)


class InputBaseParamSpec(BaseParamSpec):
    """Base input parameter specification."""

    expected_schema: list[SchemaFieldSpec] | None = Field(default=None)
    options: dict | None = Field(default_factory=dict)
    streaming: StreamingInputSpec | bool | None = Field(default=None)

    @model_validator(mode="after")
    def validate_input_param_spec(self):
        """Validates the input parameter specification."""
        if (
            self.streaming is not None
            and isinstance(self.streaming, bool)
            and self.streaming
        ):
            self.streaming = StreamingInputSpec()
        elif (
            self.streaming is not None
            and isinstance(self.streaming, bool)
            and self.streaming is False
        ):
            self.streaming = None

        return self


class InputBaseSpec(BaseSpec):
    """Base input specification."""

    pass


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
