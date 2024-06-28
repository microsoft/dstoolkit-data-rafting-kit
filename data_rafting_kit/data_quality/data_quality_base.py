# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from collections import OrderedDict
from enum import StrEnum
from logging import Logger

from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import SparkSession


class DataQualityExpectationEnum(StrEnum):
    """Enumeration of different types of data quality expectations."""

    GREAT_EXPECTATIONS = "great_expectations"


class DataQualityBaseSpec(BaseModel):
    """Base output specification."""

    input_df: str | None = Field(default=None)

    model_config = ConfigDict(
        validate_default=True,
        extra_values="forbid",
    )


class DataQualityBase:
    """Represents a data quality object for data pipelines.

    Attributes
    ----------
        _spark (SparkSession): The SparkSession object.
        _logger (Logger): The logger object.
        _dfs (OrderedDict): The ordered dictionary of DataFrames.
        _env (EnvSpec): The environment specification.
    """

    def __init__(self, spark: SparkSession, logger: Logger, dfs: OrderedDict, env):
        """Initializes an instance of the data quality class class.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            logger (Logger): The logger object.
            dfs (OrderedDict): The ordered dictionary of DataFrames.
            env (EnvSpec): The environment specification.

        """
        self._spark = spark
        self._logger = logger
        self._dfs = dfs
        self._env = env
