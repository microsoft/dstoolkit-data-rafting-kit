# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from data_rafting_kit.common.base_factory import BaseFactory
from data_rafting_kit.data_quality.data_quality_base import (
    DataQualityBaseSpec,
)
from data_rafting_kit.data_quality.data_quality_mapping import (
    DataQualityMapping,
)
from pyspark.sql import SparkSession

from data_rafting_kit.configuration_spec import EnvSpec
from collections import OrderedDict
from logging import Logger


class DataQualityFactory(BaseFactory):
    """Represents a Data Quality Expectations Factory object for data pipelines."""

    def __init__(
        self,
        spark: SparkSession,
        logger: Logger,
        dfs: OrderedDict,
        env: EnvSpec,
        output_dfs: OrderedDict | None = None,
    ):
        """Initializes an instance of the Factory class.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            logger (Logger): The logger object.
            dfs (OrderedDict): The ordered dictionary of DataFrames.
            env (EnvSpec): The environment specification.
        """
        super().__init__(spark, logger, dfs, env)

        if output_dfs is None:
            self._output_dfs = self._dfs
        else:
            self._output_dfs = output_dfs

    def process_data_quality(self, spec: DataQualityBaseSpec):
        """Processes the data quality expectation specification.

        Args:
        ----
            spec (DataQualityBaseSpec): The data quality expectation specification to process.
        """
        # Automatically use the last DataFrame if no input DataFrame is specified
        input_df = self._dfs.get_df(spec.input_df)

        (
            data_quality_class,
            data_quality_function,
        ) = DataQualityMapping.get_data_quality_map("great_expectations")

        print(input_df)

        if input_df.isStreaming:
            df = input_df.foreachBatch(
                lambda batch_df, _: getattr(
                    data_quality_class(self._spark, self._logger, self._dfs, self._env),
                    data_quality_function.__name__,
                )(spec, batch_df),
            )
        else:
            df = getattr(
                data_quality_class(self._spark, self._logger, self._dfs, self._env),
                data_quality_function.__name__,
            )(spec, input_df)

        if isinstance(df, tuple):
            self._output_dfs[f"{spec.name}_fails"] = df[1]

            self._output_dfs[spec.name] = df[0]
        else:
            self._output_dfs[spec.name] = df
