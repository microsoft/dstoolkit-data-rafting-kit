# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from logging import Logger

from pyspark.sql import SparkSession

from data_rafting_kit.common.pipeline_dataframe_holder import PipelineDataframeHolder
from data_rafting_kit.configuration_spec import EnvSpec


class BaseFactory:
    """Represents a base factory object for data pipelines. All factories should inherit from this class."""

    def __init__(
        self,
        spark: SparkSession,
        logger: Logger,
        dfs: PipelineDataframeHolder,
        env: EnvSpec,
    ):
        """Initializes an instance of the Factory class.

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
