# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from enum import StrEnum
from logging import Logger

from pyspark.sql import SparkSession

from data_rafting_kit.common.base_spec import BaseParamSpec
from data_rafting_kit.common.pipeline_dataframe_holder import PipelineDataframeHolder


class DataQualityEnum(StrEnum):
    """Enumeration of different types of data quality expectations."""

    CHECKS = "checks"


class DataQualityBaseSpec(BaseParamSpec):
    """Base output specification."""

    pass


class DataQualityBase:
    """Represents a data quality object for data pipelines.

    Attributes
    ----------
        _spark (SparkSession): The SparkSession object.
        _logger (Logger): The logger object.
        _dfs (PipelineDataframeHolder): The ordered dictionary of DataFrames.
        _env (EnvSpec): The environment specification.
    """

    def __init__(
        self, spark: SparkSession, logger: Logger, dfs: PipelineDataframeHolder, env
    ):
        """Initializes an instance of the data quality class class.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            logger (Logger): The logger object.
            dfs (PipelineDataframeHolder): The ordered dictionary of DataFrames.
            env (EnvSpec): The environment specification.

        """
        self._spark = spark
        self._logger = logger
        self._dfs = dfs
        self._env = env
