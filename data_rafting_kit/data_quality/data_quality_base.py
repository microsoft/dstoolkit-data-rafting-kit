# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from enum import StrEnum
from logging import Logger

import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.expectations.expectation import ExpectationConfiguration
from pyspark.sql import DataFrame, SparkSession

from data_rafting_kit.common.base_spec import BaseSpec
from data_rafting_kit.common.pipeline_dataframe_holder import PipelineDataframeHolder


class DataQualityEnum(StrEnum):
    """Enumeration of different types of data quality expectations."""

    CHECKS = "checks"
    METRICS = "metrics"


class DataQualityBaseSpec(BaseSpec):
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
        self,
        spark: SparkSession,
        logger: Logger,
        dfs: PipelineDataframeHolder,
        env,
        run_id: str | None = None,
    ):
        """Initializes an instance of the data quality class class.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            logger (Logger): The logger object.
            dfs (PipelineDataframeHolder): The ordered dictionary of DataFrames.
            env (EnvSpec): The environment specification.
            run_id (str | None): The run ID.
        """
        self._spark = spark
        self._logger = logger
        self._dfs = dfs
        self._env = env
        self._run_id = run_id

    def get_validator(self, spec: DataQualityBaseSpec, input_df: DataFrame):
        """Returns the Great Expectations validator object.

        Args:
        ----
            spec (DataQualityBaseSpec): The data quality expectation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
        Validator: The Great Expectations validator object.
        """
        context = gx.get_context()
        asset = context.sources.add_spark(
            "spark", spark_config=self._spark.sparkContext.getConf().getAll()
        ).add_dataframe_asset(spec.name)

        validator = context.get_validator(
            batch_request=asset.build_batch_request(dataframe=input_df)
        )

        return validator

    def build_expectation_configuration(
        self,
        spec: DataQualityBaseSpec,
        input_df: DataFrame,
        validate_unique_column_identifiers: bool = True,
    ) -> ExpectationSuite:
        """Builds the expectation configuration.

        Args:
        ----
            spec (GreatExpectationBaseSpec): The data quality expectation specification.
            input_df (DataFrame): The input DataFrame.
            validate_unique_column_identifiers (bool): A flag to validate unique column identifiers or not.

        Returns:
        -------
            ExpectationSuite: The expectation suite.
        """
        expectation_configs = []
        for expectation in spec.params.checks:
            expectation_config = ExpectationConfiguration(
                expectation_type=expectation.root.type,
                kwargs=expectation.root.params.model_dump(by_alias=False),
            )
            expectation_configs.append(expectation_config)

        expectation_suite = ExpectationSuite(
            expectation_suite_name=spec.name, expectations=expectation_configs
        )

        if validate_unique_column_identifiers:
            # Check that the column identifiers exist in the input DataFrame
            for column in spec.params.unique_column_identifiers:
                if column not in input_df.columns:
                    raise ValueError(
                        f"Column Identifier {column} not found in input DataFrame"
                    )

        return expectation_suite
